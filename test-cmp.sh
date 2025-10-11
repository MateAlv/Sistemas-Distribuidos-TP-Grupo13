#!/bin/bash

# Uso: ./comparar-resultados.sh N
# Ejemplo: ./comparar-resultados.sh 1

N=$1

BASE_DIR=".kaggle_reduced_results"

# Procesar flag --base-dir
if [ "$2" == "--full" ] && [ -n "$3" ]; then
  BASE_DIR=".kaggle_results"
fi


if [ -z "$N" ]; then
  echo "Uso: $0 <número_de_cliente>"
  exit 1
fi


RESULTS_DIR=".results/client-$N"

# Verificar si el directorio existe
if [ ! -d "$RESULTS_DIR" ]; then
  echo "ERROR: No existe el directorio $RESULTS_DIR"
  exit 1
fi

# Archivos a comparar
FILES=(
  "results_query_1.csv"
  "results_query_2_1.csv"
  "results_query_2_2.csv"
  "results_query_3.csv"
  "results_query_4.csv"
)

for file in "${FILES[@]}"; do
  SRC="$BASE_DIR/$file"
  DST="$RESULTS_DIR/$file"

  if [ -f "$DST" ]; then
    SRC_LINES=$(wc -l < "$SRC")
    DST_LINES=$(wc -l < "$DST")

    if [ "$SRC_LINES" -eq "$DST_LINES" ]; then
      echo "✅ $file: OK ($SRC_LINES líneas)"
    else
      echo "❌ $file: ERROR ($SRC_LINES ≠ $DST_LINES líneas)"
    fi
  else
    echo "  $file no existe en $RESULTS_DIR, se omite."
  fi

  echo
done