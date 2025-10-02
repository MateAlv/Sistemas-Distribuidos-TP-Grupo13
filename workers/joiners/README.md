# Lista de joiners

## 1) join_items.py
El join se hace por item_id usando el diccionario cargado desde menu_items.

## 2) join_birthdates.py
El joiner consume todos los agregados de compras por cliente y todos los usuarios; arma top3 por store_id, joinea con birth_date y publica en una cola por sucursal usando el patrón top_3_clients_for_{store_id}.