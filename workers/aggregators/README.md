# Lista de aggregators

## 1) agg_tx_items_to_tx_totals

Para: Query 1 (base de Q1 y también útil para Q3).

Input queue: transaction_items (o equivalente granular de ítems).

Output queue: transactions_totals

Group by: transaction_id

Métrica: sum(amount) por transacción.

Notas: después otro filtro (no-agg) limita a 2024–2025, 06:00–23:00 y total >= 75.

## 2) agg_products_qty_by_month

Para: Query 2 (parte “más vendidos”).

Input queue: transaction_items (o transactions ya explotadas a nivel item).

Output queue: product_quantity_by_month

Group by: (year, month, product_id)

Métrica: count(*) o sum(quantity) (según formato de filas).

Filtro temporal: 2024–2025.

## 3) agg_products_revenue_by_month

Para: Query 2 (parte “más ganancias”).

Input queue: transaction_items (o fuente con price * qty/amount).

Output queue: product_revenue_by_month

Group by: (year, month, product_id)

Métrica: sum(revenue) (precio×cantidad o campo amount).

Filtro temporal: 2024–2025.

## 4) agg_top_product_by_month_qty (reducción a top-1)

Para: Query 2 (producto más vendido por mes).

Input queue: product_quantity_by_month

Output queue: most_sold_product_by_month

Group by: (year, month)

Métrica: argmax de quantity → deja (product_id, quantity).

## 5) agg_top_product_by_month_revenue (reducción a top-1)

Para: Query 2 (producto que más facturó por mes).

Input queue: product_revenue_by_month

Output queue: most_profitable_product_by_month

Group by: (year, month)

Métrica: argmax de revenue → deja (product_id, revenue).

## 6) agg_tpv_by_store_semester

Para: Query 3.

Input queue: transactions_totals (provenientes de agg_tx_items_to_tx_totals) ya filtradas por franja horaria.

Output queue: tpv_by_store_semester

Group by: (store_id, year, semester)

Métrica: sum(transaction_total)

Filtro temporal: 2024–2025 y 06:00–23:00.

## 7) agg_purchases_by_client_store

Para: Query 4 (base del top3).

Input queue: transactions o transactions_totals (con user_id y store_id).

Output queue: purchases_by_client_store

Group by: (store_id, user_id)

Métrica: count(*) (compras por cliente en la sucursal).

Filtro temporal: 2024–2025 (si aplica al query).

## 8) agg_top3_clients_by_store (reducción a top-3)

Para: Query 4 (antes del join de cumpleaños).

Input queue: purchases_by_client_store

Output queue: top3_clients_by_store

Group by: store_id

Métrica: top-3 por count (compras).

Salida: lista de (user_id, purchases) por store_id.