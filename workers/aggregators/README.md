# Lista de aggregators

## 1) agg_products_qty_by_month.py
Agg de query 2, ventas por producto. Le llegan batches con las transaction items de 2024 y 2025 y hay que desencolar batches con trxs_items que tiene las columnas item_id, created_at, subtotal y quantity  y agrupar todos los productos de transactions_items + su mes + su año con su SUM(quantity). El joiner después agarra los mayores

## 2) 
Agg de query 2, ventas por producto. Le llegan batches con las transaction items de 2024 y 2025 y hay que desencolar batches con trxs_items que tiene las columnas item_id, created_at, subtotal y quantity y agrupar todos los productos de transactions_items + su mes + su año con su SUM(subtotal). El joiner después agarra los mayores de cada grupo

## 3) 
Agg de query 3, TPV por sucursal y semestre. Le llegan los batches con tabla transactions filtrada a 2024 y 2025 entre 6 y 11 AM; y hay que desencolar batches con trxs que tiene las columnas transaction_id, store_id, created_at y final_amount. Hay que agrupar por semestre + año + sucursal haciendo SUM(final_amount). Publicar a results_query_3

## 4) 
Agg de query 4, compras por cliente, recibe batches de la tabla transactions filtrada a 2024 y 2025; hay que desencolar batches con los trxs que tienen las cols user_id y store_id por lo menos. Hay que desencolar estos batches y agrupar por client_id + store_id haciendo un COUNT(), es decir contando solo la cantidad de filas de estos clientes.