#!/usr/bin/env python3
"""
run_logger.py - permanent archive of GB distribution-network outage feeds.

Runs inside a GitHub Actions job (see .github/workflows/outage-logger.yml). Each job:
  * polls every source on its own interval (Northern Powergrid every 5 minutes),
  * every 30 minutes packs everything it fetched into one compressed batch
    data/YYYY/MM/DD/batch_<UTC>.tar.xz, updates latest_status.json and pushes,
  * stops a little after the next scheduled job is due, so jobs hand over
    without gaps (the workflow's concurrency group queues the next one).

Raw responses are stored exactly as received. Standard library only.
"""
from __future__ import annotations

import io
import json
import os
import ssl
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --------------------------------------------------------------------------- sources
GRID = [(round(53.25 + 0.25 * i, 2), round(-2.75 + 0.25 * j, 2)) for i in range(11) for j in range(13)]
SOURCES = [
    {"name": "npg_live_power_cuts", "ext": "json", "every_min": 5,
     "url": "https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            "live-power-cuts-data/exports/json"},
    {"name": "nged_live_power_cuts", "ext": "csv", "every_min": 15,
     "url": "https://connecteddata.nationalgrid.co.uk/dataset/d6672e1e-c684-4cea-bb78-c7e5248b62a2/"
            "resource/292f788f-4339-455b-8cc0-153e14509d4d/download/power_outage_ext.csv"},
    {"name": "nged_live_detailed_power_cuts", "ext": "csv", "every_min": 15,
     "url": "https://connecteddata.nationalgrid.co.uk/dataset/d6672e1e-c684-4cea-bb78-c7e5248b62a2/"
            "resource/a1365982-4e05-463c-8304-8323a2ba0ccd/download/live_detailed_power_cuts.csv"},
    {"name": "ukpn_live_faults", "ext": "json", "every_min": 15,
     "url": "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            "ukpn-live-faults/exports/json"},
    {"name": "metoffice_warnings_uk", "ext": "xml", "every_min": 30,
     "url": "https://weather.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region/UK"},
    {"name": "ukv_gusts_npg_area", "ext": "json", "every_min": 180, "kind": "openmeteo",
     "url": "https://api.open-meteo.com/v1/forecast"},
]
WX_VARS = "wind_gusts_10m,wind_speed_10m,wind_direction_10m,precipitation"

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", 300))        # main loop tick
BATCH_MINUTES = float(os.environ.get("BATCH_MINUTES", 30))      # commit cadence
END_AFTER_HOUR = int(os.environ.get("END_AFTER_HOUR", 30))      # minutes past next even hour
RUN_SECONDS = os.environ.get("RUN_SECONDS")                     # testing override
REPO = Path(os.environ.get("REPO_DIR", ".")).resolve()
STAGE = REPO / "stage"
UNPUSHED = REPO / "unpushed"
UA = "uk-outage-archive/1.0 (academic research; github actions)"


def _ctx():
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


CTX = _ctx()


def now() -> datetime:
    return datetime.now(timezone.utc)


def stamp(t: datetime) -> str:
    return t.strftime("%Y%m%dT%H%M%SZ")


def log(msg: str) -> None:
    print(f"{now():%Y-%m-%d %H:%M:%S}Z {msg}", flush=True)


# --------------------------------------------------------------------------- fetching
def http_get(url: str, timeout: int = 90) -> tuple[int, bytes]:
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read() if hasattr(e, "read") else b""
        except Exception as e:
            last = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(str(last))


def fetch(src: dict) -> tuple[int, bytes]:
    if src.get("kind") != "openmeteo":
        return http_get(src["url"])
    out = []
    for i in range(0, len(GRID), 50):
        chunk = GRID[i:i + 50]
        q = {"latitude": ",".join(str(p[0]) for p in chunk),
             "longitude": ",".join(str(p[1]) for p in chunk),
             "hourly": WX_VARS, "models": "ukmo_seamless", "wind_speed_unit": "ms",
             "timezone": "UTC", "past_days": 1, "forecast_days": 3}
        status, body = http_get(src["url"] + "?" + urllib.parse.urlencode(q))
        if status != 200:
            return status, body
        data = json.loads(body)
        data = data if isinstance(data, list) else [data]
        for (lat, lon), d in zip(chunk, data):
            out.append({"req_lat": lat, "req_lon": lon, "elevation": d.get("elevation"),
                        "hourly": d.get("hourly", {})})
        time.sleep(1)
    return 200, json.dumps({"model": "ukmo_seamless", "points": out}, separators=(",", ":")).encode()


def npg_summary(body: bytes) -> dict:
    """Live unplanned incidents and customers off (confirmed + predicted)."""
    try:
        recs = json.loads(body)
    except Exception:
        return {}
    inc = {}
    for r in recs:
        if "planned" in str(r.get("natureofoutage", "")).lower():
            continue
        a = inc.setdefault(r.get("reference"), {"c": 0, "live": False})
        a["c"] = max(a["c"], int(r.get("totalconfirmedpowercut") or 0) + int(r.get("totalpredictedpowercut") or 0))
        a["live"] = a["live"] or r.get("isaffected") in (1, "1", True)
    live = [a for a in inc.values() if a["live"]]
    return {"rows": len(recs), "live_unplanned_incidents": len(live),
            "customers_off_confirmed_plus_predicted": sum(a["c"] for a in live)}


# --------------------------------------------------------------------------- git
def git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    env = dict(os.environ, GIT_AUTHOR_NAME="outage-logger", GIT_COMMITTER_NAME="outage-logger",
               GIT_AUTHOR_EMAIL="outage-logger@users.noreply.github.com",
               GIT_COMMITTER_EMAIL="outage-logger@users.noreply.github.com")
    r = subprocess.run(["git", *args], cwd=REPO, env=env, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {r.stderr.strip()[:400]}")
    return r


BRANCH = os.environ.get("GITHUB_REF_NAME") or "main"
_base = {"sha": None}


def push_with_retry(tries: int = 4) -> bool:
    for k in range(tries):
        if git("push", "origin", f"HEAD:refs/heads/{BRANCH}", check=False).returncode == 0:
            _base["sha"] = git("rev-parse", "HEAD").stdout.strip()
            return True
        # someone else pushed: replay only our new commits onto the new remote tip
        if git("fetch", "--depth=1", "origin", BRANCH, check=False).returncode == 0:
            r = git("rebase", "--onto", f"origin/{BRANCH}", _base["sha"], check=False)
            if r.returncode != 0:
                git("rebase", "--abort", check=False)
        time.sleep(10 * (k + 1))
    return False


# --------------------------------------------------------------------------- batching
def make_batch(t: datetime, status: dict) -> Path | None:
    files = sorted(p for p in STAGE.glob("*") if p.is_file())
    if not files:
        return None
    rel = Path("data") / t.strftime("%Y/%m/%d") / f"batch_{stamp(t)}.tar.xz"
    out = REPO / rel
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    with tarfile.open(tmp, "w:xz", preset=9) as tf:
        for p in files:
            tf.add(p, arcname=p.name)
        manifest = json.dumps({"created_utc": t.isoformat(), "files": [p.name for p in files]}, indent=1)
        info = tarfile.TarInfo("manifest.json")
        info.size = len(manifest.encode())
        info.mtime = int(t.timestamp())
        tf.addfile(info, io.BytesIO(manifest.encode()))
    tmp.replace(out)
    for p in files:
        p.unlink()
    (REPO / "latest_status.json").write_text(json.dumps(status, indent=2) + "\n")
    return rel


def commit_and_push(rel: Path, n_files: int) -> None:
    size_kb = (REPO / rel).stat().st_size / 1024
    git("add", "--sparse", "--", str(rel), "latest_status.json")
    git("commit", "-q", "-m", f"batch {rel.name} ({n_files} snapshots)")
    if push_with_retry():
        log(f"pushed {rel} ({size_kb:.0f} kB)")
    else:
        UNPUSHED.mkdir(exist_ok=True)
        # the file may already be gone from a sparse worktree, so take it from the commit
        blob = subprocess.run(["git", "show", f"HEAD:{rel.as_posix()}"], cwd=REPO, capture_output=True).stdout
        if blob:
            (UNPUSHED / rel.name).write_bytes(blob)
        log(f"PUSH FAILED for {rel}; kept for the workflow artifact and will retry")


# --------------------------------------------------------------------------- main loop
def end_time(start: datetime) -> datetime:
    if RUN_SECONDS:
        return start + timedelta(seconds=float(RUN_SECONDS))
    nxt = start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    while nxt.hour % 2:
        nxt += timedelta(hours=1)
    return nxt + timedelta(minutes=END_AFTER_HOUR)


def main() -> int:
    sources = json.loads(os.environ["SOURCES_JSON"]) if os.environ.get("SOURCES_JSON") else SOURCES
    STAGE.mkdir(exist_ok=True)
    _base["sha"] = git("rev-parse", "HEAD").stdout.strip()
    start = now()
    stop = end_time(start)
    log(f"logging until {stop:%H:%M}Z; {len(sources)} sources; batch every {BATCH_MINUTES:g} min")
    last: dict[str, datetime] = {}
    status = {"sources": {}}
    next_batch = start + timedelta(minutes=BATCH_MINUTES)
    while True:
        t = now()
        for s in sources:
            due = s["name"] not in last or t - last[s["name"]] >= timedelta(minutes=s["every_min"]) - timedelta(seconds=30)
            if not due:
                continue
            last[s["name"]] = t
            try:
                code, body = fetch(s)
            except Exception as e:
                status["sources"][s["name"]] = {"last_try_utc": t.isoformat(), "ok": False, "error": str(e)[:200]}
                log(f"{s['name']}: FAILED {e}")
                continue
            (STAGE / f"{s['name']}_{stamp(t)}.{s['ext']}").write_bytes(body)
            st = {"last_try_utc": t.isoformat(), "ok": code == 200, "http": code, "bytes": len(body)}
            if s["name"].startswith("npg") and code == 200:
                st.update(npg_summary(body))
            status["sources"][s["name"]] = st
            log(f"{s['name']}: {code}, {len(body) / 1024:.0f} kB")
        t = now()
        if t >= next_batch or t >= stop:
            status["updated_utc"] = t.isoformat()
            n = len(list(STAGE.glob("*")))
            try:
                rel = make_batch(t, status)
                if rel:
                    commit_and_push(rel, n)
            except Exception as e:
                log(f"batch/commit error: {e}")
            next_batch = t + timedelta(minutes=BATCH_MINUTES)
        if t >= stop:
            break
        # next poll 20 s after the next 5-minute mark (NPG updates on the 5-minute marks)
        nxt_poll = (int(time.time()) // POLL_SECONDS + 1) * POLL_SECONDS + min(20, POLL_SECONDS // 10)
        time.sleep(max(1, min(nxt_poll, stop.timestamp() + 1) - time.time()))
    # anything still unpushed from earlier attempts
    if git("status", "--porcelain", check=False).stdout.strip() or \
            git("rev-list", "--count", f"origin/{BRANCH}..HEAD", check=False).stdout.strip() not in ("", "0"):
        push_with_retry()
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
