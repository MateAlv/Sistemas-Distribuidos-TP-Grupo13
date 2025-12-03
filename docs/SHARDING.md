# Sharding overview

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
