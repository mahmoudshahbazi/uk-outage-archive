# GB distribution network outage archive

An automatic archive of live power-cut feeds published by GB distribution network operators, with matching Met Office UKV wind forecasts. It is kept for research into storm impacts on distribution networks (Durham University, Department of Engineering).

The live feeds show only *current* incidents, and the operators keep no public history, so this archive saves them as they are published.

## What is collected

| Source | Every | Licence / attribution |
|---|---|---|
| Northern Powergrid live power cuts (NE England, Yorkshire, N Lincs) | 5 min | Northern Powergrid Open Data Licence v1.0 — Supported by Northern Powergrid Open Data |
| NGED live power cuts, standard and detailed (Midlands, South West, South Wales) | 15 min | NGED (WPD) Open Data Licence — Supported by NGED Open Data |
| UK Power Networks live faults (London, East, South East) | 15 min | CC BY 4.0 — UK Power Networks |
| Met Office UK weather warnings (RSS) | 30 min | © Crown copyright, Met Office |
| Met Office UKV 10 m gust, wind and rain, 0.25° grid over the Northern Powergrid area (via Open-Meteo) | 3 h | Open-Meteo (CC BY 4.0), Met Office data |

No operator endorses this archive or the way the data are used.

## Layout

- `data/YYYY/MM/DD/batch_<UTC time>.tar.xz` — one file per 30 minutes. Each holds every raw response fetched in that period, exactly as received, plus `manifest.json`. File names inside are `<source>_<UTC time>.<ext>`.
- `latest_status.json` — when each source was last fetched, whether it worked, and Northern Powergrid's current count of live unplanned incidents and customers off.
- `run_logger.py` and `.github/workflows/outage-logger.yml` — the logger. It runs on GitHub Actions every two hours, and each job hands over to the next, so there are no gaps.

To unpack a batch: `tar -xJf batch_20261030T163000Z.tar.xz`.

## Notes

- Northern Powergrid's feed lists one row per postcode per incident, and its timestamps are UK local time even though they are labelled `+00:00`. Customers off are given as confirmed plus predicted.
- An incident is not a fault: locations are customers' postcodes, and causes are not given.
