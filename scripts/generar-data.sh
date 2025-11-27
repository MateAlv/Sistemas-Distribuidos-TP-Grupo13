#!/bin/bash

DATA_PATH=./data
DATASET_ZIP_PATH=$DATA_PATH/data-test
RESULTS_ZIP_PATH=$DATA_PATH/results
# Dataset completo
KAGGLE_PATH=$DATA_PATH/.kaggle
RESULTS_KAGGLE_PATH=$DATA_PATH/.results-kaggle
# Dataset reducido
KAGGLE_REDUCED_PATH=$DATA_PATH/.kaggle-reduced
RESULTS_KAGGLE_REDUCED_PATH=$DATA_PATH/.results-kaggle-reduced
# Dataset muy pequeño para pruebas rápidas
DATA_REDUCED_PATH=$DATA_PATH/.data-reduced
RESULTS_DATA_REDUCED_PATH=$DATA_PATH/.results-data-reduced
# Dataset de pruebas internas de la librería
DATA_TEST_PATH=$DATA_PATH/.data-test

# Download dataset from Kaggle
curl -L -o ./g-coffee-shop-transaction-202307-to-202506.zip\
  https://www.kaggle.com/api/v1/datasets/download/geraldooizx/g-coffee-shop-transaction-202307-to-202506

# Unzip dataset
unzip ./g-coffee-shop-transaction-202307-to-202506.zip -d $KAGGLE_PATH

# Remove unnecessary files and folders to reduce dataset size
rm ./g-coffee-shop-transaction-202307-to-202506.zip
rm -rf $KAGGLE_PATH/payment_methods
rm -rf $KAGGLE_PATH/vouchers
mv $KAGGLE_PATH/stores $KAGGLE_PATH/1.stores
mv $KAGGLE_PATH/menu_items $KAGGLE_PATH/2.menu_items
mv $KAGGLE_PATH/users $KAGGLE_PATH/3.users
mv $KAGGLE_PATH/transactions $KAGGLE_PATH/4.transactions
mv $KAGGLE_PATH/transaction_items $KAGGLE_PATH/5.transaction_items

# Create reduced dataset with only January 2024 and January 2025 data
mkdir -p $KAGGLE_REDUCED_PATH

# Copy necessary transaction files
mkdir -p $KAGGLE_REDUCED_PATH/4.transactions
cp $KAGGLE_PATH/4.transactions/transactions_202401.csv $KAGGLE_REDUCED_PATH/4.transactions/
cp $KAGGLE_PATH/4.transactions/transactions_202501.csv $KAGGLE_REDUCED_PATH/4.transactions/

# Copy necessary transaction items files
mkdir -p $KAGGLE_REDUCED_PATH/5.transaction_items
cp $KAGGLE_PATH/5.transaction_items/transaction_items_202401.csv $KAGGLE_REDUCED_PATH/5.transaction_items/
cp $KAGGLE_PATH/5.transaction_items/transaction_items_202501.csv $KAGGLE_REDUCED_PATH/5.transaction_items/

# Copy other necessary files
cp -r $KAGGLE_PATH/1.stores/ $KAGGLE_REDUCED_PATH/1.stores/
cp -r $KAGGLE_PATH/2.menu_items/ $KAGGLE_REDUCED_PATH/2.menu_items/
cp -r $KAGGLE_PATH/3.users/ $KAGGLE_REDUCED_PATH/3.users/

# Unzip test data
unzip -o $DATASET_ZIP_PATH.zip -d $DATA_PATH
mv $DATA_PATH/.data-test $DATA_REDUCED_PATH

# Create internal test dataset copying only stores and menu_items
mkdir -p $DATA_TEST_PATH
cp -r $DATA_REDUCED_PATH/stores $DATA_TEST_PATH/stores
cp -r $DATA_REDUCED_PATH/menu_items $DATA_TEST_PATH/menu_items

# Unzip kaggle results
unzip -o $RESULTS_ZIP_PATH.zip -d $DATA_PATH
cp -r $RESULTS_ZIP_PATH/.results-kaggle $RESULTS_KAGGLE_PATH
cp -r $RESULTS_ZIP_PATH/.results-kaggle-reduced $RESULTS_KAGGLE_REDUCED_PATH
cp -r $RESULTS_ZIP_PATH/.results-data-reduced $RESULTS_DATA_REDUCED_PATH
rm -rf $RESULTS_ZIP_PATH