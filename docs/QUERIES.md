### Se debe obtener:

1. Transacciones (Id y monto) realizadas durante 2024 y 2025 entre las 06:00 AM y las 11:00 PM con monto total mayor o igual a 75.
2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado (nombre y monto), para cada mes en 2024 y 2025.
3. TPV (Total Payment Value) por cada semestre en 2024 y 2025, para cada sucursal, para transacciones realizadas entre las 06:00 AM y las 11:00 PM.
4. Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y 2025, para cada sucursal.

- Q1: FILTER_YEAR → FILTER_HOUR → FILTER_AMOUNT → server merge (QUERY_END).
- Q2: FILTER_YEAR → AGG_PRODUCTS (sharded) → MAX_PRODUCTS → JOIN_ITEMS → server.
- Q3: FILTER_YEAR → FILTER_HOUR → AGG_TPV (sharded) → MAX_TPV → JOIN_STORES_TPV → server.
- Q4: FILTER_YEAR → AGG_PURCHASES (sharded) → MAX_TOP3_PURCHASES → JOIN_STORES_TOP3 → JOIN_USERS → server.
