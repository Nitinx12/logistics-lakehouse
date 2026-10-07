# Monitoring

Prometheus collects, Grafana shows, Alertmanager mails. Full reference
is `monitoring/README.md`; this page is the short map.

```mermaid
flowchart LR
    EX["postgres-exporter<br/>lh_* metrics"] --> PM["Prometheus<br/>rules"]
    PM --> GF["Grafana<br/>dashboards"]
    PM --> AM["Alertmanager<br/>gmail"]
    AM --> M["mail"]

    classDef mon fill:#991b1b,stroke:#fca5a5,color:#fff
    classDef serve fill:#c2410c,stroke:#fdba74,color:#fff
    class EX,PM,GF,AM mon
    class M serve
```

Warehouse metrics come from `ops.pipeline_run_log`, `ops.layer_snapshot`,
`ops.freshness`, and `ops.slo_status`. Page alerts fire at once, warn
alerts batch hourly. Every alert links to `docs/runbook.md`. Start with
`docker compose --profile obs up -d` after the `core` warehouse is up,
migrated, and loaded.
