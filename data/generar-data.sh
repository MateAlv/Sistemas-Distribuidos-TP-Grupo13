#!/bin/bash

curl -L -o ./g-coffee-shop-transaction-202307-to-202506.zip\
  https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506

mkdir -p ./data

unzip ./g-coffee-shop-transaction-202307-to-202506.zip -d ./.data

rm ./g-coffee-shop-transaction-202307-to-202506.zip
rm -rf ./.data/payment_methods
rm -rf ./.data/vouchers

mkdir -p ./.data-reduced ./.data-reduced/transactions ./.data-reduced/transaction_items ./.data-reduced/users
cp -r ./.data/stores ./.data-reduced/stores
cp -r ./.data/menu_items ./.data-reduced/menu_items
cp -r ./.data/users ./.data-reduced/users
cp ./.data/transactions/transactions_202401.csv ./.data-reduced/transactions/
cp ./.data/transactions/transactions_202501.csv ./.data-reduced/transactions/
cp ./.data/transaction_items/transaction_items_202401.csv ./.data-reduced/transaction_items/
cp ./.data/transaction_items/transaction_items_202501.csv ./.data-reduced/transaction_items/

unzip -o ./data/data-test.zip -d ./

mkdir -p ./.data-test-reduced
cp -r ./.data-test/stores ./.data-test-reduced/stores
cp -r ./.data-test/menu_items ./.data-test-reduced/menu_items

unzip -o ./data/kaggle_results.zip -d ./
unzip -o ./data/kaggle_reduced_results.zip -d ./