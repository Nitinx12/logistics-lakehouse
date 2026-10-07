# Testing

`make test` runs lint plus unit plus smoke. `make dq` runs the three GX
gates plus every SQL check in `tests/dq/`. A change is done when both
pass with no secrets in the diff.

| Level | Location | What |
|---|---|---|
| Unit, Python | tests/unit/ | Config, watermark math, DAG structure, alert helpers, contracts |
| Unit, SQL | tests/dq/bronze, silver, gold, analytics/ | Loop checks per layer plus mart presence and reconciliation |
| Smoke | tests/smoke/test_idempotency.py | Double run keeps silver and gold checksums identical |
| Streaming | tests/streaming/ | Envelope validation, topics, sink upsert, status logic, checkpoints |
| Load, SQL suites | Planned | tests/load and tests/sql per the architecture |

Severity policy: a critical failure blocks the next layer and the
watermark; a warning quarantines rows and continues. Add a table by
adding its YAML contract, its loader, its GX suite rows, and one DQ
check. Add an analytics mart by adding the view, the dashboard query,
the presence entry, and one reconciliation assertion.
