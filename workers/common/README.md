# Sharding dinámico de maximizers

Este directorio contiene utilidades compartidas para los workers, incluyendo la lógica de particionamiento dinámico (`sharding.py`). A continuación se describe el flujo de configuración:

1. **Definición de cantidad de maximizers**  
   En el archivo de configuración que consume `scripts/generar-compose.py` se especifica cuántos maximizers parciales deben crearse para cada consulta:
   - `MAXIMIZER_MAX_PARTIAL` controla cuántos shards tendrá la query de productos.
   - `MAXIMIZER_TOP3_PARTIAL` controla cuántos shards tendrá la query de top 3.

2. **Generación automática de shards**  
   El script `generar-compose.py` conoce los dominios válidos:
   - Items de productos con IDs del 1 al 8.
   - Stores con IDs del 1 al 10.

   A partir de la cantidad solicitada, distribuye equitativamente los IDs disponibles y genera una declaración de shards similar a `items_1_3:1-3;items_4_6:4-6;items_7_8:7-8`. Si hay un excedente de IDs, reparte uno adicional a los primeros shards. Si se piden más shards que IDs disponibles, el script falla explícitamente.

3. **Variables de entorno en docker-compose**  
   El script inyecta las variables en cada servicio:
   - `AGGREGATOR_PRODUCTS` recibe `MAX_SHARDS=...`.
   - `AGGREGATOR_PURCHASES` recibe `TOP3_SHARDS=...`.
   - Cada maximizer parcial obtiene `MAX_SHARD_ID` o `TOP3_SHARD_ID`.
   - Los maximizers absolutos reciben `MAX_PARTIAL_SHARDS` o `TOP3_PARTIAL_SHARDS` con la lista de shards esperados.

4. **Consumo de configuración en runtime**  
   - `workers/aggregators/common/aggregator.py` carga `MAX_SHARDS` o `TOP3_SHARDS`, crea las colas según el nombre de cada shard y enruta los mensajes basándose en el ID (`build_id_lookup`).
   - `workers/maximizers/common/maximizer.py` usa la configuración para saber si el worker es parcial o absoluto, a qué cola suscribirse y cuántos shards esperar antes de declarar que recibió toda la información.

5. **Validaciones**  
   `generar-compose.py` aplica varias comprobaciones:
   - Si aparece un agregador de productos debe existir al menos un maximizer parcial de productos, y viceversa.
   - Lo mismo para compras y top 3.
   - Se evita crear shards vacíos o IDs repetidos.

### Ejemplo rápido

```
MAXIMIZER_MAX_PARTIAL: 3
MAXIMIZER_TOP3_PARTIAL: 2
```

Produce:
```
MAX_SHARDS=items_1_3:1-3;items_4_6:4-6;items_7_8:7-8
TOP3_SHARDS=stores_1_5:1-5;stores_6_10:6-10
```

Los maximizers parciales quedan suscritos a `to_max_items_1_3`, `to_max_items_4_6`, etc., mientras que los absolutos esperan los shards `items_1_3`, `items_4_6`, `items_7_8`.

### Cómo extender el rango

Si en el futuro se incorporan más items o stores, basta con actualizar la constante correspondiente en `scripts/generar-compose.py` (`PRODUCT_ITEM_IDS` o `PURCHASE_STORE_IDS`). El resto del pipeline se adapta automáticamente al nuevo universo de IDs.
