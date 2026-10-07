# Streaming

`delivery_events` only. The replay producer simulates live traffic at
100x from Mongo history, so state plainly that the live tab is a replay.
Batch gold stays authoritative; turning Kafka off never breaks reports.

```mermaid
flowchart LR
    M[("Mongo<br/>delivery_events")] --> P["replay<br/>producer"]
    P --> T1{{"lh.delivery_events.v1"}}
    P -. invalid .-> DLQ{{"dlq"}}
    T1 --> J["status job<br/>watermarks + alerts"]
    J --> T2{{"lh.trip_status.v1<br/>compacted"}}
    J --> T3{{"lh.delivery_alerts.v1"}}
    T2 --> RS["redis sink"] --> RD[("Redis<br/>trip status TTL")]
    T1 --> SK["postgres sink"] --> PG[("bronze<br/>events stream")]
    T3 --> AL["alert consumer"] --> AM["Alertmanager"]

    classDef src fill:#1e3a8a,stroke:#93c5fd,color:#fff
    classDef orch fill:#166534,stroke:#86efac,color:#fff
    classDef bronze fill:#92400e,stroke:#fcd34d,color:#fff
    classDef mon fill:#991b1b,stroke:#fca5a5,color:#fff
    class M src
    class P,T1,T2,T3,DLQ,J,RS,SK,AL orch
    class RD,PG bronze
    class AM mon
```

| Topic | Key | Partitions | Cleanup | Retention |
|---|---|---|---|---|
| lh.delivery_events.v1 | trip_id | 6 | delete | 7 days |
| lh.delivery_events.v1.dlq | trip_id | 1 | delete | 30 days |
| lh.trip_status.v1 | trip_id | 6 | compact | infinite |
| lh.delivery_alerts.v1 | trip_id | 3 | delete | 7 days |

Run order: `ensure_topics.py`, replay producer, status job,
Postgres sink, Redis sink. Producer uses `acks=all` with idempotence;
every consumer commits only after its sink write, and every sink
upserts on `event_id` or `trip_id`, so replays are harmless. State
checkpoints to `FLINK_CHECKPOINT_DIR`. The stream never writes to
silver or gold. See ADR 0002 for why the runtime is a Python consumer
loop instead of a Flink cluster.
