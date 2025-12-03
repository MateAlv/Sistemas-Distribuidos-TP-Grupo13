# Maximizers Refactor (Parciales removidos)

## Objetivo
Eliminar maximizers parciales y hacer que los agregadores shardeados envíen directamente sus parciales al maximizer absoluto (o al joiner TPV), sincronizados por barreras del monitor.

## Flujo por query (nuevo)
- Q2: FILTER_YEAR → AGG_PRODUCTS (sharded) → MAX absoluto → JOIN_ITEMS → server.
- Q3: FILTER_YEAR → FILTER_HOUR → AGG_TPV (sharded) → JOIN_STORES_TPV → server.
- Q4: FILTER_YEAR → AGG_PURCHASES (sharded) → TOP3 absoluto → JOIN_STORES_TOP3 → JOIN_USERS → server.

## Sharding
- Config: `AGGREGATOR_PRODUCTS/TPV/PURCHASES` = cantidad de shards. Maximizers parciales en 0.
- Filtros rutean chunks a colas por shard (`to_agg_products_shard_<id>`, etc.) con `shard_id = (chunk_idx % shards) + 1`.
- Aggregators consumen su cola de shard (env `AGGREGATOR_SHARD_ID/SHARDS`), publican STATS/END con `shard` y mandan parciales a colas únicas:
  - Products → `to_absolute_max` (rows marcados `transaction_id=agg_shard_<id>`)
  - Purchases → `to_top3_absolute` (rows marcados `store_name=agg_shard_<id>`)
  - TPV → `to_absolute_tpv`

## Maximizers absolutos
- MAX/TOP3 consumen una única cola (`to_absolute_max` / `to_top3_absolute`).
- Esperan todos los shards (`AGGREGATOR_SHARDS`) por cliente antes de END/STATS. Detectan shard en los marcadores de rows o campo `shard`.
- Publican END/STATS en coordinación con `shard=global` hacia joiners.

## Joiner TPV
- Consume `to_absolute_tpv`, acumula TPV por (store, year_half) y espera barreras de todos los shards de `agg_tpv` antes de joinear. Logging: `tpv_chunk_received` con rows y shards vistos/esperados.

## Config/Compose
- Env a filtros: `PRODUCTS_SHARDS`, `PURCHASES_SHARDS`, `TPV_SHARDS`.
- Env a aggregators: `AGGREGATOR_SHARD_ID/SHARDS`.
- Env a max absolutos: `AGGREGATOR_SHARDS` (para gating de shards).
- Env a joiner TPV: `AGGREGATOR_TPV`, `EXPECTED_INPUTS`=shards.

## Conducta final
- Q2/Q4: max absolutos no avanzan hasta recibir todos los shards; joiners reciben barrera global.
- Q3: joiner TPV no avanza hasta recibir barreras de todos los shards de `agg_tpv` y suma parciales antes de emitir.

## Consideraciones
- No hay persistence de estado de barrera en monitor (in-memory).
- Asegurar que todas las bindings usen las nuevas colas/routing keys tras rebuild.
