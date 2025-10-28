# Sistemas-Distribuidos-TP-Grupo13
Se solicita un sistema distribuido que analice la información de ventas en una cadena de negocios de Cafés en Malasia. 

## Requisitos del sistema

### Python environment

    python3 -m venv .venv 
    source .venv/bin/activate 
    pip install -r requirements.txt

### Docker y Docker-Compose

- Instalar Docker: https://docs.docker.com/get-docker/
- Instalar Docker-Compose: https://docs.docker.com/compose/install/

### Datasets

    ./scripts/generar-data.sh

## Instrucciones de uso
El repositorio cuenta con un **Makefile** que incluye distintos comandos en forma de targets. Los targets se ejecutan mediante la invocación de:  **make \<target\>**.

Se cuenta con otros archivos de configuración para pruebas sobre queries específicas, ubicados en `./config/`. Entre los que se encuentran:

- `./config/q1-config.yml` para la query 1
- `./config/q2-config.yml` para la query 2
- `./config/q3-config.yml` para la query 3
- `./config/q4-config.yml` para la query 4

Y la forma de utilizarlos es pasando el número de query como parámetro al target de make. Por ejemplo, para utilizar la configuración de la query 1:

    make up QUERY=1

Por defecto, si no se pasa el parámetro QUERY, se utilizará la configuración general ubicada en `./config/config.yml` que contiene la información para ejecutar todas las queries.

Los targets disponibles son:

- **compose** - Crea el docker compose según el archivo de configuración

    make compose [QUERY=<number_of_query>]

- **build** - Construye las imágenes Docker necesarias para el sistema según el archivo de configuración

    make build [QUERY=<number_of_query>]

- **up** - Inicia el sistema distribuido

    make up [QUERY=<number_of_query>]

- **test** - Inicia el sistema distribuido en modo test (con logs más detallados)

    make test [QUERY=<number_of_query>]

- **down** - Detiene el sistema distribuido

    make down

- **rebuild** - Reconstruye las imágenes Docker y reinicia el sistema distribuido con up

    make rebuild [QUERY=<number_of_query>]

- **logs** - Muestra los logs de todos los contenedores en ejecución escribiendo un archivo `logs.txt`

    make logs

- **clean-results** - Elimina los resultados generados en la carpeta `./results/` (esta accion se realiza por defecto al ejecutar `make up` y `make test`)

    make clean-results

- **hard-down** - Detiene y elimina todos los contenedores, redes e imágenes creadas por Docker Compose

    make hard-down

- **prune** - Elimina todos los contenedores, redes e imágenes no utilizados por Docker

    make prune

### Comparación de resultados

    python3 scripts/results_compare.py <number_of_clients>
    -- full - Compara con los resultados de dataset completo (./data/.kaggle-results) - (por defecto: ./data/.kaggle-results-reduced)
    -- verbose - Muestra información detallada de las diferencias encontradas

Ejemplo de uso:

Para 5 clientes y comparación con dataset completo y salida detallada:

    python3 scripts/results_compare.py 5 --full --verbose


## Queries Disponibles

- 1. Transacciones (Id y monto) realizadas durante 2024 y 2025 entre las 06:00 AM y las
11:00 PM con monto total mayor o igual a 75.

- 2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado
(nombre y monto), para cada mes en 2024 y 2025.

- 3. TPV (Total Payment Value) por cada semestre en 2024 y 2025, para cada sucursal, para
transacciones realizadas entre las 06:00 AM y las 11:00 PM.

- 4. Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y
2025, para cada sucursal.

     
## Protocolo de comunicación (Cliente ↔ Servidor) - DEPRECATED

### 1. Handshake inicial
- Cliente → Servidor:
  I:H <client_id>\n

- Servidor → Cliente:
  I:O\n

Esto establece la identidad del cliente y confirma que el servidor está listo para recibir archivos.

---

### 2. Envío de archivos (pueden ser varios en la misma conexión TCP)
Por cada archivo CSV encontrado en el directorio del cliente:

1. Cliente → Servidor: Header de inicio de archivo
   F:\n
   CLI_ID: <id>\n
   FILENAME: <rel_path>\n
   SIZE: <size_bytes>\n
   \n

   El \n\n (línea en blanco) marca el fin del header.

2. Cliente → Servidor: Cuerpo binario del archivo (<size_bytes> exactos).

3. Servidor → Cliente: ACK de recepción
   I:O\n

---

### 3. Señal de fin de transmisión
Cuando el cliente termina de enviar todos los archivos:

- Cliente → Servidor:
  I:F\n

- Servidor → Cliente:
  I:O\n

---

### 4. Características adicionales
- Conexión: persistente (un solo socket TCP durante toda la sesión).
- Timeouts: fijos (connect_timeout=10s, io_timeout=30s).
- Archivos válidos: únicamente con extensión .csv.
- Handshake: siempre obligatorio.

## Canales de Comunicacion RabbitMQ
- Server:
  - Envia a Filter 1, Join Items, Join Users, Top 3, Join Stores
  - Recibe de Join Items, Join Users, Merge Q1, Join Stores
- Filter 1:
  - Envia a Filter 2, Agg 1+2, Agg 4
  - Recibe de Server
- Filter 2:
  - Envia a Filter 3, Agg 3
  - Recibe de Filter 1
- Filter 3:
  - Envia a Merge Q1
  - Recibe de Filter 2
- Agg 1+2:
  - Envia a Max 1-3, Max 4-6, Max 7-8
  - Recibe de Filter 1
- Agg 3:
  - Envia a Merge Q3
  - Recibe de Filter 2
- Agg 4:
  - Envia a Top3 1-3, Top3  4-6, Top3 7-10
  - Recibe de Filter 1
- Max 1-3:
  - Envia a Max
  - Recibe de Agg 1+2
- Max 4-6:
  - Envia a Max
  - Recibe de Agg 1+2
- Max 7-8:
  - Envia a Max
  - Recibe de Agg 1+2
- Max:
  - Envia a Join Items
  - Recibe de Max 1-3, Max 4-6, Max 7-8
- Top3 1-3:
  - Envia a Top3 + Join Stores
  - Recibe de Agg 4
- Top3 4-6:
  - Envia a Top3 + Join Stores
  - Recibe de Agg 4
- Top3 7-10:
  - Envia a Top3 + Join Stores
  - Recibe de Agg 4
- Top3 + Join Stores:
  - Envia a Join Users
  - Recibe de Top3 1-3, Top3  4-6, Top3 7-10, Server
- Join Items:
  - Envia a Server
  - Recibe de Max
- Join Stores:
  - Envia a Server
  - Recibe de Merge Q3, Server
- Join Users:
  - Envia a Server
  - Recibe de Top3 + Join Stores, Server
- Merge Q1:
  - Envia a Server
  - Recibe de Filtro 3
- Merge Q3:
  - Envia a Join Stores

  - Recibe de Agg 3
