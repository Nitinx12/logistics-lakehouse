# ADR 0001: Incremental strategy with full load for small tables

Date: 2026-10-06. Decided by the owner during Step 3 of `NEXT_STEPS.md`.

## Context

EDA found one distinct `loaded_at` value per table, so `loaded_at` cannot
drive incrementals until it is refreshed per batch. The live extract
confirmed this on 2026-10-06 by logging `no watermark column, using full
reload`. Small tables stay cheap to reload in full, large tables do not.

## Decision

1. Small tables load in full by design: `facilities`, `routes`, and the
   small mongo collections. Reference tables use truncate and reload
   through a staging swap.
2. Large tables (`loads`, `trips`, `fuel_purchases`, `delivery_events`)
   increment on business dates (`load_date`, `dispatch_date`,
   `purchase_date`, event datetimes) once live increments matter.
3. The silver latest wins guard orders by `_ingested_at` and batch order,
   because source `loaded_at` ties on every row.
4. Contracts in `contracts/` stay the single record of mode per table.

## Consequences

Reruns stay safe through idempotent upserts, proven live for the full
mongo path. A two slice incremental proof still needs extractor date
window arguments, which do not exist yet, so it moves to Step 6 alongside
the double run proof. No loader changes were needed for this decision.
