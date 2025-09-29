#!/bin/bash

curl -L -o ./g-coffee-shop-transaction-202307-to-202506.zip\
  https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506

mkdir -p ./data

unzip ./g-coffee-shop-transaction-202307-to-202506.zip -d ./.data

rm ./g-coffee-shop-transaction-202307-to-202506.zip
rm -rf ./.data/payment_methods
rm -rf ./.data/vouchers