# EDA Findings (M0 gate — ARCHITECTURE.md §6.3)

> Sources profiled live on 2026-10-05: Mongo `fleet_operations` (3 collections)
> plus Databricks `logistics_operations.default` (9 tables).
> Code: `notebooks/mongo_eda.ipynb`, `notebooks/databricks_eda.ipynb`
> (run with `uv run run_notebooks.py`). Plots: `notebooks/plots/`.

## Per-source profile

| # | Source | Table | Rows | Key candidate | Key unique? | `loaded_at` null % | `loaded_at` monotonic? | Late-arrival lag | Null / cardinality notes | STRING parse | Orphan FKs | Load mode |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | mongo | delivery_events | 170,820 | event_id | yes | 0 | no (1 value) | n/a (static dump) | no nulls; on_time True 95,095 / False 75,725 (55.7% on-time); Pickup 85,410 / Delivery 85,410 = 2 per trip | datetimes parse | 0 | incremental |
| 2 | mongo | maintenance_records | 2,920 | maintenance_id | yes | 0 | no (1 value) | n/a | no nulls; labor $1.28M + parts $4.45M = $5.73M; downtime 72,230h | dates parse | 0 | incremental |
| 3 | mongo | safety_incidents | 170 | incident_id | yes | 0 | no (1 value) | n/a | truck_id + driver_id 1 null row (0.59%); claims $2.65M; preventable 64 / at-fault 54 | dates parse | 0 | incremental |
| 4 | databricks | customers | 200 | customer_id | yes | 0 | no (1 value) | n/a | no nulls; revenue potential $537.65M; contracts 2020-01 → 2022-01 | dates parse | 0 | incremental |
| 5 | databricks | drivers | 150 | driver_id | yes | 0 | no (1 value) | n/a | termination_date 82.67% null (= active drivers, expected); hired 2012 → 2021 | dates parse | 0 | incremental |
| 6 | databricks | facilities | 50 | facility_id | yes | 0 | no (1 value) | n/a | no nulls; tiny reference table | n/a | 0 | full |
| 7 | databricks | fuel_purchases | 196,442 | fuel_purchase_id | yes | 0 | no (1 value) | n/a | truck_id 1.98% + driver_id 2.03% null; 24.52M gal / $95.59M; purchase_date stored as YYYYMMDD int, needs format cast in silver | ints parse as dates | 0 | incremental |
| 8 | databricks | loads | 85,410 | load_id | yes | 0 | no (1 value) | n/a | no nulls; revenue $262.53M; 2.347B lbs; dates 2022-01-01 → 2024-12-31 | dates parse | 0 | incremental |
| 9 | databricks | trips | 85,410 | trip_id | yes | 0 | no (1 value) | n/a | driver/truck/trailer_id ~2% null each; 122.16M miles; 18.95M gal; idle 598,791h; 1 trip per load | dates parse | 0 | incremental |
| 10 | databricks | routes | 58 | route_id | yes | 0 | no (1 value) | n/a | no nulls; tiny reference table | n/a | 0 | full |
| 11 | databricks | trailers | 180 | trailer_id | yes | 0 | no (1 value) | n/a | no nulls; acquired 2015 → 2021 | dates parse | 0 | incremental |
| 12 | databricks | trucks | 120 | truck_id | yes | 0 | no (1 value) | n/a | no nulls; acquired 2015 → 2021 | dates parse | 0 | incremental |

Status: **done** — modes above confirmed (only change vs §6.2 proposal: none).

## Cross-cutting verdicts

- Keys: every candidate PK unique, zero duplicate rows in all 12 tables.
- `loaded_at`: 0% null everywhere but a single distinct value per table — useless as an
  incremental filter until `add_loaded_at` is re-run per batch. Until then, increment
  on business dates (`load_date`, `dispatch_date`, `purchase_date`, `incident_date`,
  `maintenance_date`, event datetimes).
- FKs: zero orphans on all 16 checked relationships, both directions across sources.
- Detention: mean 91.5 min, median 88, max 239 — no wild outliers.
- Grain check: 85,410 loads = 85,410 trips = 2 × 85,410 delivery events. One-to-one
  loads↔trips and exactly Pickup+Delivery per trip confirmed in the data.
- Silver typing watchlist: `purchase_date` is an int (YYYYMMDD), all other dates are
  strings; money columns parse clean; ~2% null vehicle assignments on trips/fuel
  need an unknown-member or null-tolerant join in gold.

## Decisions out of EDA

- [x] Final load mode per table (§23 Q1) — table above
- [x] Business keys and fact grains (§23 Q2) — keys above; grains hold (1 row per event/purchase/load/trip)
- [x] `loaded_at` reliability verdict (§23 Q3) — present, not monotonic; refresh per batch
- [x] Volumes + 10x projection for capacity plan (§23 Q4) — largest table 196k rows → 10x ≈ 2M rows, single-host Postgres handles it
