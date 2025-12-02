# Solución

## Objetivo
El cliente envía toda la informacion de las tablas (TRANSACTIONS, TRANSACTIONS_ITEMS, STORES, MENU_ITEMS, USERS), se procesa por el sistema y se devuelven los resultados de las queries (QUERY1RESULT, QUERY2_1RESULT, QUERY2_2RESULT, QUERY3RESULT, QUERY4RESULT)

## Flujo por query 
- Q1: FILTER_YEAR → FILTER_HOUR → FILTER_AMOUNT → server.
- Q2: FILTER_YEAR → AGG_PRODUCTS (sharded) → MAX absoluto → JOIN_ITEMS → server.
- Q3: FILTER_YEAR → FILTER_HOUR → AGG_TPV (sharded) → JOIN_STORES_TPV → server.
- Q4: FILTER_YEAR → AGG_PURCHASES (sharded) → TOP3 absoluto → JOIN_STORES_TOP3 → JOIN_USERS → server.

## Persitencia

Se aplica de igual manera en todos los workers (filters, aggregators, maximizers, joiners).
Detalles en PERSISTENCE.md

## Lógica del END

Es igual para todos los workers. Detalles en END_PROTOCOL.md

## Server
- Recive file chunks y los envia a filtros y joiners (las tablas de join - STORES, MENU_ITEMS, USERS).
- Envía resultados proveniente de `to_merge_data_{client_id}` para cada cliente apenas lo tiene.
- Espera a que todos los filtros y joiners terminen y avisa al cliente.

### Filters

- El server rutea chunks a colas por shard (`to_filter_year_shard_<id>`) con `shard_id = (message_id % shards) + 1`.
- Filters consumen su cola de shard (env `FILTER_SHARD_ID/SHARDS`) y mandan parciales a colas:
  - year -> `to_agg_products_shard_<id>` con `shard_id = (item_id % shards) + 1`
  - year -> `to_agg_purchases_shard_<id>` con `shard_id = (store_id % shards) + 1`
  - year -> `to_filter_hour_shard_<id>` con `shard_id = (message_id % shards) + 1`
  - hour -> `to_filter_amount_shard_<id>` con `shard_id = (message_id % shards) + 1`
  - hour -> `to_agg_tpv_shard_<id>` con `shard_id = (store_id % shards) + 1`
  - amount -> `to_merge_data_{client_id}` al cliente
- Los filtros apenas reciben su información la envian a sus respectivas colas.


### Aggregators
- Config: `AGGREGATOR_PRODUCTS/TPV/PURCHASES` = cantidad de shards. Maximizers parciales en 0.
- Aggregators consumen su cola de shard (env `AGGREGATOR_SHARD_ID/SHARDS`), publican STATS/END con `shard` y mandan parciales a colas únicas:
  - Products → `to_absolute_max` 
  - Purchases → `to_top3_absolute`
  - TPV → `to_absolute_tpv`

## Maximizers absolutos

No pueden escalarse

### MAX
- Consume de una única cola (`to_absolute_max`)
- Acumula todos los mensajes 

- MAX/TOP3/TPV consumen una única cola (`to_absolute_max` / `to_top3_absolute` / `to_absolute_tpv`).
- Esperan todos los shards (`AGGREGATOR_SHARDS`) por cliente antes de END/STATS. Detectan shard en los marcadores de rows o campo `shard`.
- Publican END/STATS en coordinación con `shard=global` hacia joiners.

- Consume `to_absolute_tpv`, acumula TPV por (store, year_half) y espera barreras de todos los shards de `agg_tpv` antes de joinear. Logging: `tpv_chunk_received` con rows y shards vistos/esperado

## Joiners



## Monitor/barrier
- Sigue centralizando END/STATS y forwardea `BARRIER_FORWARD` por shard para stages shardeados (filters/aggs). Max absolutos emiten global.
- Joiner TPV usa barreras de `agg_tpv` (por shard); otros joiners usan barrera global del upstream.

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
