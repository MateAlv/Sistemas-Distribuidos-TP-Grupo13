# EOF / END Synchronization Protocol (Current Behavior)

This document describes how END/EOF synchronization and barrier coordination work across all workers after the latest changes (filters, aggregators, maximizers, joiners, monitor, server).

## Pipeline Overview
Queries:
- Q1: FILTER_YEAR → FILTER_HOUR → FILTER_AMOUNT → server merge (QUERY_END).
- Q2: FILTER_YEAR → AGG_PRODUCTS (sharded) → MAX_PRODUCTS → JOIN_ITEMS → server.
- Q3: FILTER_YEAR → FILTER_HOUR → AGG_TPV (sharded) → MAX_TPV → JOIN_STORES_TPV → server.
- Q4: FILTER_YEAR → AGG_PURCHASES (sharded) → MAX_TOP3 → JOIN_STORES_TOP3 → JOIN_USERS → server.

Global properties:
- Deterministic `message_id` for all results/END publishes (idempotent on crash/retry).
- END is always sent to **all** downstream shards (even expected=0) so shards close cleanly.
- Monitor coordinates the Filter → Aggregator barrier; no peer fanout between filters or aggregators.
- State persistence + sent flags allow restart/resend without duplication.

## Server
- For each query/shard, sends END (MessageQueryEnd/MessageEnd) to **every** filter shard with the explicit expected chunk count for that shard.
- Filters use these expected counts (not peer gossip) to decide completion.

## Filters (year / hour / amount)
- Track `chunks_received` per client/shard vs `expected_for_shard` from the server (or prior filter stage).
- Completion per client/shard: `chunks_received == expected_for_shard` AND END seen.
- On completion:
  - To Monitor: `MSG_WORKER_STATS` (expected per downstream shard) and `MSG_WORKER_END`.
  - Downstream: any remaining data chunks; END to **all** downstream shard queues (expected=0 included).
- No filter↔filter peer sync; only server counts + own counters.

## Aggregators (products / purchases / tpv)
- On END from filters: store `agg_expected` (from END payload) and send `MSG_WORKER_END` to Monitor (with processed so far).
- Periodically send `MSG_WORKER_STATS` (processed counts) to Monitor.
- Do **not** forward END/results until Monitor sends `MSG_BARRIER_FORWARD`.
- When barrier arrives:
  - Flush buffered chunks if any.
  - Publish final aggregation results downstream with deterministic `message_id`.
  - Send downstream END.
  - Persist `results_sent`/`end_sent`; delete state only after flags are set (idempotent resend on restart).
- Aggregator peer fanout is removed; only monitor-based coordination remains.

## Maximizers (max / top3 / tpv)
- Track finished_senders vs expected_inputs (or expected_shards) per client.
- When all expected senders are finished:
  - Publish final results (deterministic ids).
  - Send END downstream.
  - Persist `results_sent`/`end_sent`; delete state only after flags are set.
- Crash/restart resends are safe because of deterministic ids + sent flags.

## Joiners (items / stores_tpv / stores_top3 / users)
- Wait for END from both main stream (maximizer) and side data (server-provided dimensions).
- Only when both inputs are finished and data is present:
  - Perform join, publish results to server, send END.
  - Use sent flags for idempotent resend on restart.

## Monitor (barrier coordinator)
- Collects `MSG_WORKER_STATS`/`MSG_WORKER_END` from filters and aggregators.
- For each agg shard/client/type, computes:
  - `agg_expected`: sum of expected chunks reported by all filters for that shard.
  - `agg_processed`: processed reported by aggregators.
- Barrier condition: `agg_processed >= agg_expected` AND all relevant filter ENDs arrived.
- When satisfied, emits `MSG_BARRIER_FORWARD` to that aggregator shard (idempotent).
- Leader-based; heartbeats determine active coordinator.

## Key Invariants
- Deterministic ids across results/END → safe resends.
- END goes to every shard (even zero-work shards).
- Filters finish based on server-provided expected counts, not peer stats.
- Aggregators release only after monitor barrier; no peer fanout.
- Persistent sent flags prevent duplicate downstream outputs and allow restart completion.
