# END/Stats Centralization Refactor (Monitor-Driven Barrier)

This document captures the current state of the refactor that centralizes END/STATS handling in the monitor. It complements `QUERIES.md` and the code paths in workers/monitor/server.

## Overview
- All workers now publish coordination messages (END and STATS) to a shared RabbitMQ exchange `coordination_exchange` using shard-aware routing keys.
- The monitor (leader) ingests these messages, tracks per-client/per-stage/per-shard progress, gates on both END counts and STATS totals, and emits `BARRIER_FORWARD` when conditions are met.
- Downstream workers consume `BARRIER_FORWARD` (using stage/shard-specific bindings) instead of peer END messages to advance the pipeline.

## Message Schema (coordination)
- Exchange: `coordination_exchange` (topic).
- Routing key for worker → monitor: `coordination.barrier.<stage>.<shard>`.
- `WORKER_END` payload:
  - `type`: `"WORKER_END"`
  - `id`: sender id (worker id/shard id)
  - `client_id`: client identifier
  - `stage`: logical stage (see below)
  - `shard`: shard id (`global` for non-sharded)
  - `expected`: expected_chunks or worker-count (component-specific; see below)
  - `chunks`: chunks produced/sent
  - `sender`: same as `id`
- `WORKER_STATS` payload:
  - `type`: `"WORKER_STATS"`
  - `id`, `client_id`, `stage`, `shard`
  - `expected`: expected_chunks
  - `chunks`: chunks received/sent
  - `processed`: chunks processed
  - `sender`

## Stages (non-sharded unless noted)
- Filters: `filter_year`, `filter_hour`, `filter_amount`
- Aggregators: `agg_products`, `agg_tpv`, `agg_purchases`
- Maximizers:
  - Partial: `max_partials` (MAX) / `top3_partials` (TOP3), sharded by shard_id
  - Absolute: `max_absolute` / `top3_absolute`, shard=`global`
- Joiners: `join_items`, `join_stores_tpv`, `join_stores_top3`, `join_users`
- Server final: `server_results`

## Routing Keys (standard)
- Worker → monitor: `coordination.barrier.<stage>.<shard>`
- Monitor → workers (BARRIER_FORWARD): `coordination.barrier.<stage>.<shard>`
  - Workers bind to their own stage/shard key; maximizers bind per shard; others bind to `<stage>.global`.

## Monitor Logic
- State keyed by (client_id, stage, shard):
  - END count, sender_ids, total_chunks
  - Stats: expected_chunks, stats_received, stats_processed
  - Expected worker count per stage loaded from `config/config.ini` (via `MONITOR_CONFIG_PATH`).
- Gating condition to forward:
  - ENDs received ≥ expected_workers (from config)
  - Stats present and max(received, processed) ≥ expected_chunks
  - Forward interval elapsed (default 60s, `BARRIER_FORWARD_INTERVAL`).
- On forward: emit `BARRIER_FORWARD` with stage, shard, client_id, total_chunks, senders on `coordination.barrier.<stage>.<shard>`.

## Worker Publishing (summary)
- Filters/Aggregators: publish END+STATS with stage, shard=`global`, expected=total_expected, chunks=sent/received.
- Maximizers:
  - shard-aware (partials use shard_id; absolute uses `global`)
  - expected in END/STATS = chunks produced for that client.
- Joiners: shard=`global`, expected=1, chunks=1, processed=1.
- Server: shard=`global`, END+STATS with expected=total_chunks sent to pipeline.

## Worker Consumption (summary)
- Filters/Aggregators/Joiners: consume `coordination.barrier.<stage>.global` and treat as upstream END (send downstream END/results).
- Maximizers: consume `coordination.barrier.<stage>.<shard_id>` matching their shard and call `process_client_end`.
- Server: consumes `coordination.barrier.server_results.global` and treats it as completion of results for the client.

## Config / Compose
- Monitor reads expected worker counts from `config/config.ini` (set `MONITOR_CONFIG_PATH` env in monitor services).
- RabbitMQ memory tweak (when used): `RABBITMQ_VM_MEMORY_HIGH_WATERMARK=0.6` (replace if configured otherwise).

## Known Caveats / Next Steps
- Validate that all consumers bind to the correct shard-specific keys after deployment.
- Ensure expected_chunks semantics are correct for every stage (especially filters/aggregators) to avoid premature barrier forwards.
- Persistence/backoff for monitor barrier state is not implemented (in-memory only).
- RabbitMQ config should avoid invalid env; prefer `RABBITMQ_VM_MEMORY_HIGH_WATERMARK` over low-level args.
