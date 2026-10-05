# EDA Findings (M0 gate — ARCHITECTURE.md §6.3)

> Fill one row per source before any build. These findings fix grains,
> business keys, and the full-vs-incremental load-mode table (§6.2).
> Notebooks: `notebooks/mongo_eda.ipynb`, `notebooks/databricks_eda.ipynb`.

## Per-source profile

| # | Source | Table | Rows | Key candidate | Key unique? | `loaded_at` null % | `loaded_at` monotonic? | Late-arrival lag | STRING parse issues | Orphan FKs | Load mode |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | mongo | delivery_events | | event_id | | | | | | | incremental |
| 2 | mongo | maintenance_records | | maintenance_id | | | | | | | incremental |
| 3 | mongo | safety_incidents | | incident_id | | | | | | | incremental |
| 4 | databricks | customers | | customer_id | | | | | | | incremental |
| 5 | databricks | drivers | | driver_id | | | | | | | incremental |
| 6 | databricks | facilities | | facility_id | | | | | | | full |
| 7 | databricks | fuel_purchases | | fuel_purchase_id | | | | | | | incremental |
| 8 | databricks | loads | | load_id | | | | | | | incremental |
| 9 | databricks | trips | | trip_id | | | | | | | incremental |
| 10 | databricks | routes | | route_id | | | | | | | full |
| 11 | databricks | trailers | | trailer_id | | | | | | | incremental |
| 12 | databricks | trucks | | truck_id | | | | | | | incremental |

Status: **pending** — modes above are the §6.2 proposals, confirm or change each one here.

## Decisions out of EDA

- [ ] Final load mode per table (§23 Q1)
- [ ] Business keys and fact grains (§23 Q2)
- [ ] `loaded_at` reliability verdict (§23 Q3)
- [ ] Volumes + 10x projection for capacity plan (§23 Q4)
