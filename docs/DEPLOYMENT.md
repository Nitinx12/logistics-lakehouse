# Deployment

One compose profile at a time on the 8GB host. Images are pinned, every
service has a healthcheck, a restart policy, and a memory limit.

| Profile | Services | Start |
|---|---|---|
| core | postgres, metadata db, pgbouncer, redis broker, airflow, spark runner | `make up` |
| stream | kafka KRaft, redis cache | `PROFILE=stream make up` |
| obs | prometheus, exporter, grafana, alertmanager | `PROFILE=obs make up` |
| serve | Planned in M6 | Streamlit app first |

First boot from a clean checkout:

```bash
cp .env.example .env
make up
make migrate
make run-batch
```

Never commit `.env`. Role passwords are set out of band; nothing in
this repo sets them. Kafka and Postgres stay inside the Docker network
except where local runs need host access, which compose marks. See
`monitoring/README.md` for the `obs` prerequisites.
