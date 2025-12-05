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

## Tipos de Workers y Procesamiento

### Filter Workers

**Función**: Filtrar datos por criterios específicos y rutear a workers downstream.

**Tipos**:
- **FilterYear**: Filtra transacciones por año (2020).
- **FilterHour**: Filtra transacciones por hora (12:00-15:00).
- **FilterAmount**: Filtra transacciones por monto ($500+).

**Método `run()`**:
1. Consume chunks de su cola de shard (`to_filter_{tipo}_shard_{id}`)
2. Para cada row en el chunk:
   - Aplica el filtro específico (año, hora o monto)
   - Si pasa el filtro, rutea a colas downstream según criterio de sharding:
     - **FilterYear**: Envía a `to_agg_products` (shard por item_id), `to_agg_purchases` (shard por store_id), y `to_filter_hour`
     - **FilterHour**: Envía a `to_filter_amount` y `to_agg_tpv` (shard por store_id)
     - **FilterAmount**: Envía directamente al cliente (`to_merge_data_{client_id}`)
3. No acumula estado de datos (solo filtra y reenvía)

**Rol por Query**:
- **Q1**: FilterYear → FilterHour → FilterAmount → Cliente
- **Q2**: FilterYear → filtra año 2020 para agregación de productos
- **Q3**: FilterYear → FilterHour → filtra transacciones para TPV
- **Q4**: FilterYear → filtra año 2020 para agregación de compras

### Aggregator Workers

**Función**: Agregar y acumular datos parciales por shard.

**Tipos**:
- **AggregatorProducts**: Agrupa transacciones por (item_id, month_year) y suma ventas/ganancias.
- **AggregatorPurchases**: Agrupa transacciones por (user_id, store_id) y suma compras totales.
- **AggregatorTPV**: Agrupa transacciones por (store_id, year_half) y suma TPV (Total Payment Value).

**Método `run()`**:
1. Consume chunks de su cola de shard (`to_agg_{tipo}_shard_{id}`)
2. Para cada row:
   - Extrae clave de agregación (item_id+month, user_id+store, o store_id+year_half)
   - Actualiza acumuladores en memoria (suma, cuenta, etc.)
3. Al recibir END de todos los shards upstream:
   - Publica resultados parciales a maximizer absoluto
   - Cada shard envía sus propios máximos/top3/tpv parciales

**Rol por Query**:
- **Q2**: AggregatorProducts → Calcula ventas y ganancias por item_id y mes
- **Q3**: AggregatorTPV → Calcula TPV por store_id y semestre
- **Q4**: AggregatorPurchases → Calcula compras totales por user_id y store_id

### Maximizer Workers

**Función**: Calcular máximos globales, top3 o TPV final desde múltiples shards.

**Tipos**:
- **MAX**: Encuentra máximo de ventas y ganancias por (item_id, month_year).
- **TOP3**: Calcula top 3 clientes por store_id basado en compras totales.
- **TPV**: Suma TPV de todos los shards por (store_id, year_half).

**Método `run()`**:
1. Consume de una única cola (`to_absolute_{tipo}`)
2. Acumula datos de TODOS los shards de aggregators:
   - **MAX**: Mantiene máximos globales de ventas/ganancias por item_id
   - **TOP3**: Mantiene top 3 clientes por store_id usando heap
   - **TPV**: Suma TPV parcial de cada shard
3. Espera END de TODOS los shards (`AGGREGATOR_SHARDS`)
4. Al completar, publica resultados finales a joiner

**Rol por Query**:
- **Q2**: MAX → Encuentra ítems con máximas ventas y ganancias por mes
- **Q3**: TPV → Calcula TPV total por tienda y semestre
- **Q4**: TOP3 → Determina top 3 clientes por tienda

### Joiner Workers

**Función**: Enriquecer datos con información de tablas auxiliares.

**Tipos**:
- **ItemsJoiner**: Agrega nombres de items (de MENU_ITEMS).
- **StoresJoiner** (TPV y TOP3): Agrega nombres de tiendas (de STORES).
- **UsersJoiner**: Agrega nombres y cumpleaños de usuarios (de USERS).

**Método `run()`**:
1. Consume de DOS colas:
   - Cola de datos del maximizer (`to_join_{tipo}`)
   - Cola de tabla auxiliar del server (MENU_ITEMS, STORES, o USERS)
2. Almacena tabla auxiliar en memoria (por client_id)
3. Espera recibir:
   - Tabla auxiliar completa
   - Datos del maximizer + END de todos los shards
4. Aplica join:
   - Para cada row de datos, busca en tabla auxiliar y agrega campos
   - **ItemsJoiner**: agrega item_name
   - **StoresJoiner**: agrega store_location
   - **UsersJoiner**: agrega full_name y birthday_in_year
5. Publica resultados finales al cliente

**Rol por Query**:
- **Q2**: ItemsJoiner → Agrega nombres de items a los máximos
- **Q3**: StoresJoinerTPV → Agrega ubicación de tienda al TPV
- **Q4**: StoresJoinerTOP3 → Agrega ubicación; UsersJoiner → Agrega info de usuarios al top3



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
