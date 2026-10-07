# Monitoring (obs profile)

Pipeline metrics, dashboards, and mail alerts for the lakehouse platform.
Design authority is `ARCHITECTURE.md` §13. The stack is Prometheus for
collection and rules, `postgres-exporter` for warehouse metrics, Grafana
for dashboards and managed alerts, and Alertmanager for mail routing.

## Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| prometheus | prom/prometheus:v2.55.1 | 9090 | Scrapes targets, evaluates `rules.yml`, pages Alertmanager |
| postgres-exporter | prometheuscommunity/postgres-exporter:v0.20.1 | 9187 | Exposes `ops.*` tables as `lh_*` metrics |
| grafana | grafana/grafana:11.5.2 | 3000 | Dashboards and one managed freshness alert |
| alertmanager | prom/alertmanager:v0.27.0 | 9093 | Routes page and warn mail through Gmail |

All tags are pinned. Every service has a healthcheck, a restart policy,
and a memory limit inside the 8GB budget from `ARCHITECTURE.md` §15.2.

## Metrics

Custom queries live in `postgres-exporter/queries.yml`. The exporter
connects as `dq_runner`, which already holds SELECT on these tables.

| Metric family | Source table | Used for |
|---|---|---|
| `lh_pipeline_run_status`, `lh_pipeline_run_duration_s`, `lh_pipeline_run_rows_*` | `ops.pipeline_run_log` | Run success, stage duration, throughput |
| `lh_layer_rows_rows_n` | `ops.layer_snapshot` | Bronze, silver, gold counts and reconcile drift |
| `lh_freshness_is_fresh` | `ops.freshness` | Freshness SLO per table |
| `lh_slo_status_is_met` | `ops.slo_status` | SLO compliance |

## Alerts

| Alert | Condition | Severity | Path |
|---|---|---|---|
| PipelineStageFailed | Any stage reports failure for 5m | page | Prometheus to Alertmanager, immediate mail |
| FreshnessBreach | A table stays stale for 15m | page | Prometheus to Alertmanager, immediate mail |
| SloBreach | An SLO stays unmet for 15m | warn | Batched into the mail digest |
| ExporterDown | Exporter unreachable for 5m | warn | Batched into the mail digest |
| Warehouse data stale | Freshness minimum below 1 for 15m | page | Grafana managed rule, mirrors FreshnessBreach |

Page means immediate mail. Warn means grouped mail at most once an hour
with a daily repeat. Every alert annotation points at `docs/runbook.md`.

## Prerequisites

1. The `core` warehouse is up and migrated, including `08_ops_freshness_slo.sql`.
   The exporter reads live tables, so an empty warehouse yields no `lh_*` series.
2. Run the pipeline at least once so `pipeline_run_log`, `layer_snapshot`,
   `freshness`, and `slo_status` hold rows.
3. Set the `dq_runner` password in Postgres out of band to match
   `PG_DQ_PASSWORD` in `.env`. No script in this repo sets role passwords.
4. Nothing to edit for mail: the Alertmanager entrypoint renders Gmail
   values from the `ALERTMANAGER_SMTP_*` names in `.env` at boot. The
   config file holds `${VAR}` tokens only, never secrets.
5. Use a Google App Password for all SMTP settings, never the account
   password. Grafana picks its mail settings up from `GF_SMTP_*` on its own.

## Run

```bash
docker compose --profile obs up -d
```

Then open Prometheus on `localhost:9090`, Grafana on `localhost:3000`
(admin credentials from `GRAFANA_ADMIN_*` in `.env`), and Alertmanager on
`localhost:9093`. Exporter metrics are on `localhost:9187/metrics`.
Stop with `docker compose --profile obs down`.

The exporter needs the `core` postgres, so start `core` first and keep
`stream` down while both run to stay inside the local memory budget.

## Dashboards

Provisioned automatically from `grafana/dashboards/` into the Lakehouse
folder. Pipeline overview shows watermark status, stage durations,
freshness, and SLO tables. Layer health shows bronze and silver counts,
gold counts, and the maximum bronze versus silver drift. Infrastructure
shows target status, warehouse size, and connection counts.

## Files

| Path | Contents |
|---|---|
| `prometheus/prometheus.yml` | Scrape jobs for Prometheus itself and the exporter |
| `prometheus/rules.yml` | The four pipeline alert rules |
| `postgres-exporter/queries.yml` | Warehouse to metric mappings |
| `statsd/mapping.yml` | Airflow statsd mappings, used when the statsd exporter lands |
| `grafana/provisioning/` | Datasource, dashboard provider, mail contact point, policy, one rule |
| `grafana/dashboards/` | The three dashboard definitions |
| `alertmanager/alertmanager.yml` | Gmail routing with `${VAR}` tokens, rendered at boot |
| `alertmanager/entrypoint.sh` | Renders tokens from the environment, then starts Alertmanager |

## Troubleshooting

Exporter unhealthy: wrong `dq_runner` password or the `core` postgres is
down. Missing `lh_*` series: the pipeline never ran, or migration `08`
is not applied. Alerts never arrive: `.env` mail values missing when the
render ran, or a normal Gmail password where an App Password belongs. Grafana shows
no data: Prometheus has no targets, check `localhost:9090/targets` first.
