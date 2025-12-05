# Fault Tolerance & Persistence Design


### State to Persist
- Dictionaries: `[Client_id]` / `[Client_id][Table_type]`
- Stats Counters
- Global States (Aggregators/Maximizers)

### Methods
- `serialize()`
- `deserialize()`

## Commit Types

### 1. Send Commit
Records that a message was sent.
```python
commit_send_ack(self, x.client_id(), x.message_id())
```
*Meaning: "Mandé este mensaje de este usuario"*

### 2. Data Commit
Records that a message was received for processing.
```python
commit_processing_chunk(self, x)
```
*Meaning: "Me llegó este mensaje para procesar"*

### 3. State Commit
Records the updated state of the worker.
```python
commit_working_state(self, estado.to_bytes, x.message_id())
```
*Meaning: "Se actualizó el estado el worker"*

## Data Structures
- **Tracking**: `[ (user_id, msg_id) ]`
- **Types**:
  - `ProcessChunk`: bytes
  - `WorkingState`: bytes

## Funciones de `utils/tolerance`

### `PersistenceService`
Servicio principal de persistencia que gestiona los commits de estado, procesamiento y envío.

#### Archivos de Persistencia
Cada worker mantiene 4 archivos en su directorio de persistencia (ej: `/data/persistence/aggregator_PRODUCTS_1/`):

1. **`persistence_state`**: Estado completo del worker (WorkingState serializado)
   - Contiene: contadores, datos acumulados, flags de END, sets de idempotencia
   - Formato: `[16 bytes: last_processed_id UUID][estado serializado con pickle]`

2. **`persistence_send_commit`**: Registro de mensajes enviados (ACKs)
   - Contiene: lista de (message_id, client_id) enviados
   - Formato: secuencia de `[16 bytes: UUID][4 bytes: client_id]`
   - Se usa para idempotencia de envíos

3. **`persistence_processing_commit`**: Último chunk en procesamiento
   - Contiene: chunk completo serializado
   - Se limpia después de procesar exitosamente
   - Permite recovery del último chunk interrumpido

4. **`persistence_chunk_buffer`**: Buffer temporal de chunks (solo Aggregator)
   - Contiene: múltiples chunks en espera de commit de estado
   - Formato: `[4 bytes: length][chunk][4 bytes: length][chunk]...`
   - Se limpia al hacer commit de estado

#### Operaciones Atómicas
Todas las escrituras usan operaciones atómicas para garantizar consistencia ante crashes:

**`atomic_file_upsert(file_path, data)`**:
- Escribe datos a archivo usando **temp file + rename atómico**
- Proceso: 
  1. Crea archivo temporal en mismo directorio
  2. Escribe datos + `fsync()` para forzar flush a disco
  3. `os.replace()` renombra atómicamente (operación atómica del filesystem)
- Garantía: el archivo siempre tiene contenido válido (todo o nada)

**`atomic_file_append(file_path, data)`**:
- Agrega datos al final de archivo de forma atómica
- Proceso:
  1. Lee contenido actual
  2. Concatena nuevo contenido
  3. Usa `atomic_file_upsert()` para escribir todo
- Usado para: commits de envío (se van acumulando)

#### Funciones principales:
- **`commit_working_state(state_data, last_processed_id)`**: Persiste el estado del worker (contador, datos agregados, etc.) en `persistence_state`
- **`commit_processing_chunk(chunk)`**: Registra que un chunk fue recibido para procesar en `persistence_processing_commit`
- **`commit_send_ack(client_id, message_id)`**: Registra que un mensaje fue enviado, agrega a `persistence_send_commit`
- **`recover_working_state()`**: Recupera el estado del worker desde `persistence_state`
- **`recover_last_processing_chunk()`**: Recupera el último chunk desde `persistence_processing_commit` si no fue enviado
- **`send_has_been_acknowledged(client_id, message_id)`**: Verifica si un mensaje ya fue enviado (consulta `persistence_send_commit`)
- **`process_has_been_counted(message_id)`**: Verifica si un mensaje ya fue contado en el estado
- **`clear_processing_commit()`**: Limpia `persistence_processing_commit` después de procesar exitosamente

#### Funciones de Buffering de Chunks (Aggregator):
- **`append_chunk_to_buffer(chunk)`**: Agrega un chunk a `persistence_chunk_buffer`
- **`recover_buffered_chunks()`**: Recupera todos los chunks desde `persistence_chunk_buffer`
- **`should_commit_state()`**: Verifica si debe commitear estado basado en cantidad de chunks
- **`_clear_chunk_buffer()`**: Limpia `persistence_chunk_buffer` (llamado tras commit de estado)

*Configuración*: `commit_interval` determina cada cuántos chunks se persiste el estado (default: 10)

### `ChunkBuffer`
Gestiona un archivo buffer para chunks usando formato length-prefixed.

#### Funciones:
- **`append_chunk(chunk)`**: Agrega un chunk al buffer de forma atómica usando `fsync()`
- **`read_all_chunks()`**: Lee todos los chunks del buffer en orden
- **`clear()`**: Limpia el archivo buffer de forma atómica
- **`get_chunk_count()`**: Obtiene el número de chunks en buffer
- **`get_buffer_size()`**: Obtiene el tamaño del buffer en bytes

*Formato*: `[4-byte length][chunk bytes][4-byte length][chunk bytes]...`

### `crash_helper`
Funciones auxiliares para simular crashes en testing.

#### Funciones:
- **`crash_after_two_chunks(key, env_var)`**: Simula crash después de procesar 2 chunks
- **`crash_after_end_processed(key, env_var)`**: Simula crash inmediatamente después de procesar END

### `WorkingState`
Clase base para serializar/deserializar estado de workers.

#### Funciones:
- **`to_bytes()`**: Serializa el estado a bytes usando pickle (excluye locks)
- **`from_bytes(bytes)`**: Deserializa el estado desde bytes y reconstruye el objeto

---

## Recovery Logic (Pseudo-code)

```python
x = mensaje original
enviado = commit de procesamiento
contador = estado del nodo

def __init__(self):
    self.service = PersistenceService("/data/persistence/worker_1") # Recover Working State
    self.recover_from_persistence()

def recover_from_persistence(self):

# 1. Recover last processing chunk
# busco último valor x commiteado en disco si no fue enviado
x = self.service.recover_last_processing_chunk()  # Recover Last Processing Chunk

if x:
    # Re-process if needed
    apply(x) # Procesar
    
    if not process_has_been_counted(self, x.message_id()):
        actualizar_estado(x)
        commit_working_state(self, estado.to_bytes, x.message_id())
    
    # Re-send
    send_to_queues(x)
    commit_send_ack(self, x.client_id(), x.message_id())
```

## Main Processing Loop (Pseudo-code)

```python
def callback(x):
    commit_processing_chunk(self, x)
    results.append(x)

while True:
    x = pop_from_queue()
    apply(x) # Procesar
    actualizar_estado(x) # Registro de mensajes recibidos y no enviados
    commit_working_state(self, estado.to_bytes, x.message_id())
    # envio a todas las colas correspondientes 
    send_to_queues(x)
    commit_send_ack(self, x.client_id(), x.message_id())
```

---

## Recovery & Persistence - Aggregator

### WorkingState: `AggregatorWorkingState`
Gestiona el estado de agregación y tracking de chunks para cada cliente.

**Componentes principales:**
- `global_accumulator[client_id]`: Datos agregados (productos/compras/TPV)
- `chunks_to_receive[client_id][table_type]`: Total esperado de chunks
- `chunks_received_per_client`: Contador de chunks recibidos
- `chunks_processed`: Chunks procesados totales
- `processed_ids`: Set para idempotencia (evita procesar duplicados)
- `end_messages_received[client_id][table_type]`: Flags de END recibidos

**Propósito**: Acumular datos agregados y gestionar el protocolo END con buffering de chunks.

### Patrón de Procesamiento

#### Recepción de Mensajes (con Buffering)
```
Al recibir mensaje:
    Si es END:
        → Marcar END recibido en estado
        → commit_working_state(estado)            # PersistenceService
    Si no:
        → append_chunk_to_buffer(chunk)           # PersistenceService
    
    Agregar a cola de procesamiento
    RabbitMQ hace ACK automático
```

**Concepto clave**: Los chunks se acumulan en buffer temporal. Solo los END se persisten inmediatamente para no perder información crítica del protocolo.

#### Procesamiento con Commits Periódicos
```
Para cada mensaje en cola:
    Si ya_procesado(id):
        → Saltar (idempotencia)
    
    apply(mensaje)  → Agregar a acumulador
    estado.marcar_procesado(id)
    
    Si alcanzó_intervalo_commit:
        → commit_working_state(estado, id)       # PersistenceService
        → Limpiar buffer automáticamente
```

**Concepto clave**: Commits periódicos reducen I/O sin sacrificar mucho progreso. El buffer se limpia automáticamente al commitear estado, liberando memoria.

#### Recovery
```
Al iniciar:
    estado ← recover_working_state()             # PersistenceService
    
    chunks_pendientes ← recover_buffered_chunks()  # PersistenceService
    Para cada chunk en chunks_pendientes:
        apply(chunk)
        estado.actualizar(chunk)
    commit_working_state(estado)                  # PersistenceService
    
    Para cada (cliente, tabla) con END_recibido:
        publicar_resultados(cliente, tabla)
        enviar_END(cliente, tabla)
```

**Concepto clave**: Recovery procesa chunks buffereados que no llegaron a commitearse antes del crash. Luego verifica si hay END pendientes para reenviar resultados (idempotencia garantizada por IDs determinísticos).

#### Envío de Resultados Finales
```
Al recibir barrier_forward:
    # Procesar chunks tardíos (out-of-order)
    chunks_restantes ← recover_buffered_chunks()  # PersistenceService
    Para cada chunk:
        apply(chunk)
    commit_working_state(estado)                  # PersistenceService
    limpiar_buffer()
    
    publicar_resultados_finales()
    enviar_END_message()
    eliminar_datos_cliente()
```

**Concepto clave**: El buffer captura chunks que llegaron después del END (por delays de red). Se procesan todos antes de publicar para garantizar completitud.

---

## Recovery & Persistence - Joiner

### WorkingState: `JoinerMainWorkingState` + `JoinerJoinWorkingState`
El Joiner usa **dos estados separados** debido a sus dos fuentes de datos.

#### Main State (datos del Maximizer)
**Componentes:**
- `client_data[client_id]`: Datos recibidos del maximizer
- `client_chunks[client_id]`: Lista de chunks por cliente
- `finished_senders[client_id]`: Set de senders que enviaron END
- `expected_chunks_per_client`: Total esperado de cada sender
- `processed_ids`: Idempotencia
- `client_completed`: Clientes totalmente procesados
- `pending_end_messages`: END messages pendientes de envío
- `client_results[client_id]`: Resultados del join

**Propósito**: Tracking de múltiples senders (aggregators) y gestión de resultados del join.

#### Join State (datos del Server - tabla auxiliar)
**Componentes:**
- `join_data_by_client[client_id]`: Mapeo para join (ej: item_id → item_name)
- `ready_to_join`: Set de clientes con datos de join listos
- `processed_ids`: Idempotencia

**Propósito**: Almacenar tabla auxiliar necesaria para el join (productos, stores, users).

**Concepto clave**: Dos fuentes de datos independientes requieren dos estados separados para gestionar su persistencia y recovery de forma aislada.

### Patrón de Procesamiento

#### Recepción y Tracking Multi-Sender
```
Al recibir datos del maximizer:
    Si no ya_procesado(id):
        main_state.guardar(datos)
        main_state.marcar_procesado(id)
        commit_working_state(main_state)          # PersistenceService (main)
    
    Si tiene_todos_datos Y recibió_END:
        → Ejecutar join

Al recibir END de un sender:
    Si sender_ya_reportó(sender_id):
        → Saltar (idempotencia)
    
    main_state.marcar_sender_terminado(sender_id)
    commit_working_state(main_state)              # PersistenceService (main)
    
    Si todos_senders_terminaron:
        main_state.marcar_end_global()
        commit_working_state(main_state)          # PersistenceService (main)
        → Ejecutar join si tiene datos auxiliares
```

**Concepto clave**: Multi-sender tracking permite recibir END de múltiples fuentes (shards) de forma independiente. Solo cuando todos terminan se procede al join.

#### Aplicación del Join
```
join(cliente):
    Si cliente_ya_completado:
        → Saltar (idempotencia)
    
    Para cada chunk del cliente:
        resultados ← aplicar_join(chunk, datos_auxiliares)
        main_state.agregar_resultados(resultados)
    commit_working_state(main_state)              # PersistenceService (main)
    
    publicar_resultados()
    
    main_state.marcar_completado(cliente)
    main_state.agregar_end_pendiente(cliente)
    commit_working_state(main_state)              # PersistenceService (main)
```

**Concepto clave**: El join completo se persiste antes de publicar. Si crashea durante publicación, recovery reenvía desde END pendientes.

#### Recovery
```
Al iniciar:
    main_state ← recover_working_state(main)      # PersistenceService (main)
    join_state ← recover_working_state(join)      # PersistenceService (join)
    
    chunk_main ← recover_last_processing_chunk(main)    # PersistenceService (main)
    chunk_join ← recover_last_processing_chunk(join)    # PersistenceService (join)
    Reprocesar chunks si existen
    
    Para cada cliente en end_pendientes:
        enviar_END(cliente)
    limpiar_pendientes()
    
    Para cada cliente con END_global:
        Si no completado Y tiene_datos_auxiliares:
            → Ejecutar join
```

**Concepto clave**: Recovery verifica tres escenarios: chunks interrumpidos, END pendientes de envío, y joins incompletos. Cada uno se maneja de forma independiente.

---

## Recovery & Persistence - Maximizer

### WorkingState: `MaximizerWorkingState`
Gestiona el cálculo de máximos/top3/TPV y tracking de múltiples senders (aggregators).

**Componentes principales:**
- `sellings_max[client_id][(item_id, month_year)]`: Máximo de ventas
- `profit_max[client_id][(item_id, month_year)]`: Máximo de ganancias
- `top3_by_store[client_id][store_id]`: Top 3 clientes por tienda
- `tpv_results[client_id][(store_id, year_half)]`: TPV acumulado
- `finished_senders[client_id]`: Set de senders que enviaron END
- `expected_chunks_per_client`: Total esperado por sender
- `processed_ids`: Idempotencia
- `client_end_processed`: Clientes completados
- `results_sent`: Tracking de resultados enviados (idempotencia)
- `end_sent`: Tracking de END messages enviados (idempotencia)

**Propósito**: Calcular agregaciones globales de múltiples shards.

### Patrón de Procesamiento

#### Procesamiento con Persistencia Inmediata
```
Al recibir chunk:
    Si ya_procesado(id):
        → Saltar (idempotencia)
    
    commit_processing_chunk(chunk)                # PersistenceService
    
    aplicar_maximización(chunk)  # Actualizar máximos
    estado.marcar_procesado(id)
    commit_working_state(estado)                  # PersistenceService
    
    clear_processing_commit()                     # PersistenceService
```

**Concepto clave**: Los maximizers persisten estado inmediatamente después de cada chunk (sin buffering) porque el costo de reprocesar chunks es bajo comparado con su valor agregado.

#### Multi-Sender END y Finalización
```
Al recibir END de sender:
    Si sender_ya_reportó(sender_id):
        → Saltar (idempotencia)
    
    estado.marcar_sender_terminado(sender_id)
    commit_working_state(estado)                  # PersistenceService
    
    Si todos_senders_terminaron:
        → Finalizar cliente

Finalizar cliente:
    Si cliente_ya_finalizado:
        → Saltar (idempotencia de finalización)
    
    publicar_resultados()
    enviar_END_con_idempotencia()
    
    Si resultados_publicados Y end_enviado:
        estado.marcar_finalizado(cliente)
        commit_working_state(estado)              # PersistenceService
        eliminar_datos_cliente()
```

**Concepto clave**: La finalización requiere verificar que AMBOS (resultados + END) fueron enviados antes de marcar como completado. Esto previene reenvíos parciales en recovery.

#### Envío Idempotente de END
```
enviar_END(cliente):
    label ← generar_label_único()
    
    Si estado.end_enviado(cliente, label):
        → Saltar (ya enviado)
    
    enviar_mensaje(END)
    enviar_coordinación(END, STATS)
    
    estado.marcar_end_enviado(cliente, label)
    commit_working_state(estado)                  # PersistenceService
```

**Concepto clave**: Labels únicos por tipo de END permiten idempotencia. El estado se persiste solo después del envío exitoso.

#### Recovery con Reanudación
```
Al iniciar:
    estado ← recover_working_state()              # PersistenceService
    
    chunk ← recover_last_processing_chunk()       # PersistenceService
    Si chunk:
        procesar(chunk)  # Reanudar procesamiento
    
    Para cada cliente en estado:
        Si no finalizado Y todos_senders_terminaron:
            → Reanudar finalización
```

**Concepto clave**: Recovery es simple: reprocesar último chunk interrumpido y reanudar finalizaciones pendientes. El estado persistido tiene toda la información necesaria.
