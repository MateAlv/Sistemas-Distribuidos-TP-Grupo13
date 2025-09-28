# Sistemas-Distribuidos-TP-Grupo13
Se solicita un sistema distribuido que analice la información de ventas en una cadena de negocios de Cafés en Malasia. 


## Instrucciones de uso
El repositorio cuenta con un **Makefile** que incluye distintos comandos en forma de targets. Los targets se ejecutan mediante la invocación de:  **make \<target\>**. Los target imprescindibles para iniciar y detener el sistema son **docker-compose-up** y **docker-compose-down**, siendo los restantes targets de utilidad para el proceso de depuración.

Los targets disponibles son:

| target  | accion  |
|---|---|
|  `docker-compose-up`  | Inicializa el ambiente de desarrollo. Construye las imágenes del cliente y el servidor, inicializa los recursos a utilizar (volúmenes, redes, etc) e inicia los propios containers. |
| `docker-compose-down`  | Ejecuta `docker-compose stop` para detener los containers asociados al compose y luego  `docker-compose down` para destruir todos los recursos asociados al proyecto que fueron inicializados. Se recomienda ejecutar este comando al finalizar cada ejecución para evitar que el disco de la máquina host se llene de versiones de desarrollo y recursos sin liberar. |
|  `docker-compose-logs` | Permite ver los logs actuales del proyecto. Acompañar con `grep` para lograr ver mensajes de una aplicación específica dentro del compose. |
| `docker-image`  | Construye las imágenes a ser utilizadas tanto en el servidor como en el cliente. Este target es utilizado por **docker-compose-up**, por lo cual se lo puede utilizar para probar nuevos cambios en las imágenes antes de arrancar el proyecto. |
| `build` | Compila la aplicación cliente para ejecución en el _host_ en lugar de en Docker. De este modo la compilación es mucho más veloz, pero requiere contar con todo el entorno de Golang y Python instalados en la máquina _host_. |


## Protocolo de comunicación (Cliente ↔ Servidor)

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