# ADR 0002: Python consumer loop as the M8 streaming runtime

Date: 2026-10-07. Decided while closing the M5 streaming audit.

## Context

`ARCHITECTURE.md` §8.5 designs a Flink job with event time windows,
RocksDB state, and 60 second checkpoints. The 8GB single host in §15.2
cannot comfortably run a Flink cluster next to Kafka, Postgres, Spark,
and Airflow, and the §8 fallback names Spark Structured Streaming, not
the plain consumer loop that `streaming/flink/trip_status_job.py` is.

## Decision

1. Keep the Python consumer loop as the M8 development runtime. It
   ports the Flink logic one to one: event time from `event_ts`,
   bounded out-of-orderness, allowed lateness with side output to the
   alert topic, stuck trip sweeps, and idempotent keyed sinks.
2. Crash safety comes from two mechanisms the cluster would give us:
   file checkpoints of keyed state plus watermark
   (`FLINK_CHECKPOINT_DIR`), and at-least-once delivery with
   idempotent sinks keyed on `event_id` and `trip_id`.
3. Graduate to Flink or Spark Structured Streaming when the second
   consumer appears or lag threatens the 60 second SLO, reusing the
   unchanged topic design from §8.2.

## Consequences

No parallel processing and no exactly once transactions; duplicates
are absorbed by upserts instead. State rebuilds from the compacted
status topic stay available because the topic contract is unchanged.
