#!/bin/bash
NO_DATA=false

for arg in "$@"; do
  if [ "$arg" == "--no-data" ]; then
    NO_DATA=true
    break
  fi
done

if [ "$NO_DATA" = true ]; then
  curl -L -o ./g-coffee-shop-transaction-202307-to-202506.zip\
  https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506

  mkdir -p ./data

  unzip ./g-coffee-shop-transaction-202307-to-202506.zip -d ./.data

  rm ./g-coffee-shop-transaction-202307-to-202506.zip
  rm -rf ./.data/payment_methods
  rm -rf ./.data/vouchers
fi

mkdir -p ./.data-test ./.data-test/transactions ./.data-test/transaction_items ./.data-test/users
cp -r ./.data/stores ./.data-test/stores
cp -r ./.data/menu_items ./.data-test/menu_items
cp ./.data/transactions/transactions_202401.csv ./.data-test/transactions/
cp ./.data/transaction_items/transaction_items_202401.csv ./.data-test/transaction_items/
cp ./.data/users/users_202307.csv ./.data-test/users/
cp ./.data/users/users_202308.csv ./.data-test/users/
cp ./.data/users/users_202309.csv ./.data-test/users/
cp ./.data/users/users_202310.csv ./.data-test/users/
cp ./.data/users/users_202311.csv ./.data-test/users/
cp ./.data/users/users_202312.csv ./.data-test/users/
cp ./.data/users/users_202401.csv ./.data-test/users/

mkdir -p ./.data-test-mini
cp -r ./.data/stores ./.data-test-mini/stores
cp -r ./.data/menu_items ./.data-test-mini/menu_items