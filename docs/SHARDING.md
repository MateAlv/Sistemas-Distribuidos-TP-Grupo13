# Sharding overview

## Filters (year → hour → amount)
- **Objetivo:** que cada chunk de entrada se procese solo por un filtro (no por todas las réplicas).
- **Colas de entrada:** `to_filter_year_shard_<id>`, `to_filter_hour_shard_<id>`, `to_filter_amount_shard_<id>`; cada instancia consume solo su shard (`FILTER_SHARD_ID`).
- **Asignación de shard:** hoy el server elige `shard_id` usando `message_id` del `ProcessChunk`. Ese `message_id` es un `uuid4` aleatorio, por lo que el sharding no es determinista entre corridas. Para evitar variaciones, se recomienda usar un contador por `(client_id, table_type)` o un hash estable en vez del UUID.
- **Propagación:** el mismo `shard_id` se mantiene de year → hour → amount. Los filtros routéan a la siguiente etapa usando colas shard (ej. `to_filter_hour_shard_<id>`).
- **Stats/END:** cada filtro lleva contadores por `(stage, client_id, table_type, shard_id)` y emite `MSG_WORKER_STATS`/`MSG_WORKER_END` a `coordination.barrier.<stage>.shard.<id>` con `expected/chunks` = lo realmente enviado por ese shard. Se ignoran stats/END de otras réplicas para no mezclar contadores.

## Aggregators (products, purchases, tpv)
- **Colas de entrada:** `to_agg_products_shard_<id>`, `to_agg_purchases_shard_<id>`, `to_agg_tpv_shard_<id>`; cada instancia consume solo su shard (`AGGREGATOR_SHARD_ID`).
- **Asignación de shard:** la hace el filtro (determinista, usando contador por `(dest, client_id, table_type)`), no el aggregator.
- **Propagación:** los agregadores no re‑shardean; publican resultados/END/Stats con su `shard_id` a `coordination.barrier.<stage>.shard.<id>`.
- **Stats/END:** se basan en los chunks realmente recibidos/procesados por ese shard. No consumen barreras de otros shards.

## Monitor
- Considera filtros y agregadores como etapas shardeadas (expected=1 por shard). Recibe `coordination.barrier.<stage>.shard.<id>` y forwardea `BARRIER_FORWARD` por shard cuando recibe END/Stats suficientes.

## Notas de determinismo
- Mientras el server use UUID aleatorio para asignar shard a year, la distribución puede cambiar entre corridas aunque el dataset sea el mismo. Cambiar a un esquema estable (contador o hash) haría el sharding determinista.
- Los aggs y filtros, una vez fijado el shard de entrada, se comportan de manera determinista (no re-shardean y usan contadores propios)."
