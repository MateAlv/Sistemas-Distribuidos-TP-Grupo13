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

#### Funciones principales:
- **`commit_working_state(state_data, last_processed_id)`**: Persiste el estado del worker (contador, datos agregados, etc.)
- **`commit_processing_chunk(chunk)`**: Registra que un chunk fue recibido para procesar
- **`commit_send_ack(client_id, message_id)`**: Registra que un mensaje fue enviado
- **`recover_working_state()`**: Recupera el estado del worker desde disco
- **`recover_last_processing_chunk()`**: Recupera el último chunk que estaba siendo procesado
- **`send_has_been_acknowledged(client_id, message_id)`**: Verifica si un mensaje ya fue enviado
- **`process_has_been_counted(message_id)`**: Verifica si un mensaje ya fue contado en el estado
- **`clear_processing_commit()`**: Limpia el commit de procesamiento después de manejarlo exitosamente

#### Funciones de Buffering de Chunks:
- **`append_chunk_to_buffer(chunk)`**: Agrega un chunk al buffer temporal
- **`recover_buffered_chunks()`**: Recupera todos los chunks del buffer
- **`should_commit_state()`**: Verifica si debe commitear estado basado en cantidad de chunks
- **`_clear_chunk_buffer()`**: Limpia el buffer de chunks (llamado tras commit de estado)

*Configuración*: `commit_interval` determina cada cuántos chunks se persiste el estado (default: 10)

### `ChunkBuffer`
Gestiona un archivo buffer para chunks usando formato length-prefixed.

#### Funciones:
- **`append_chunk(chunk)`**: Agrega un chunk al buffer de forma atómica
- **`read_all_chunks()`**: Lee todos los chunks del buffer en orden
- **`clear()`**: Limpia el archivo buffer
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
- **`to_bytes()`**: Serializa el estado a bytes usando pickle
- **`from_bytes(bytes)`**: Deserializa el estado desde bytes

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

### Estado a Persistir
- `global_accumulator`: Datos agregados por client_id y tipo de tabla
- `chunks_to_receive`: Total de chunks esperados por END message
- `chunks_received_per_client`: Contador de chunks recibidos por aggregator
- `chunks_processed`: Chunks procesados totales
- `accumulated_chunks`: Chunks acumulados
- `processed_ids`: Set de message_ids procesados (idempotencia)
- `end_messages_received`: Flags de END recibidos

### Patrón de Commit

#### 1. Receive Callback (con Buffering)
```python
def chunk_callback(msg):
    # [CRASH POINT] SE ROMPE ANTES DEL COMMIT
    
    if msg.startswith(b"END;"):
        # END: persistir inmediatamente (sin buffering)
        end_message = MessageEnd.decode(msg)
        working_state.mark_end_message_received(client_id, table_type)
        working_state.set_chunks_to_receive(client_id, table_type, total_expected)
        _save_state(uuid.uuid4())
    else:
        # Datos: agregar a buffer
        chunk = ProcessBatchReader.from_bytes(msg)
        persistence.append_chunk_to_buffer(chunk)
    
    # Agregar a cola de procesamiento
    data_chunks.append(msg)
    
    # [CRASH POINT] SE ROMPE ANTES DEL ACK
    # ACK automático por RabbitMQ al retornar del callback
    # [CRASH POINT] SE ROMPE DESPUÉS DEL ACK
```

#### 2. Processing Loop
```python
def _handle_data_chunk(raw_msg):
    # [CRASH POINT] CRASH_BEFORE_PROCESS
    chunk = ProcessBatchReader.from_bytes(raw_msg)
    
    # 1 & 2. apply(x) + actualizar_estado(x)
    _apply_and_update_state(chunk)
    
    # [CRASH POINT] CRASH_AFTER_PROCESS_BEFORE_COMMIT
    
    # 3. Commit periódico basado en cantidad de chunks
    if persistence.should_commit_state():
        _save_state(chunk.message_id())
        # Buffer se limpia automáticamente en commit_working_state
    
    # 4. Si END recibido, enviar stats al Monitor
    if working_state.is_end_message_received(client_id, table_type):
        _send_stats_to_monitor(client_id, table_type)
```

#### 3. Apply & Update State
```python
def _apply_and_update_state(chunk):
    # 1. Validar table_type (prevenir corrupción de estado)
    if not _is_valid_table_type(table_type):
        persistence.clear_processing_commit()
        return
    
    # 2. Check idempotencia
    if working_state.is_processed(chunk.message_id()):
        persistence.clear_processing_commit()
        return
    
    # 3. Actualizar estado: incrementar chunks recibidos
    working_state.increment_chunks_received(client_id, table_type, aggregator_id, 1)
    
    # 4. apply(x) - Agregar datos
    if aggregator_type == "PRODUCTS":
        aggregated_rows = apply_products(chunk)
        accumulate_products(client_id, aggregated_rows)
    # ... similar para PURCHASES y TPV
    
    # 5. actualizar_estado(x) - Marcar como procesado
    working_state.increment_chunks_processed(client_id, table_type, aggregator_id, 1)
    working_state.mark_processed(chunk.message_id())
```

#### 4. Recovery Logic
```python
def handle_processing_recovery():
    # 1. Estado ya recuperado en _recover_state() durante __init__
    
    # 2. Recuperar chunks buffereados
    buffered_chunks = persistence.recover_buffered_chunks()
    
    if buffered_chunks:
        # 3. Procesar chunks + actualizar estado
        for chunk in buffered_chunks:
            _apply_and_update_state(chunk)
        
        # 4. Commit working state
        if buffered_chunks:
            last_chunk = buffered_chunks[-1]
            _save_state(last_chunk.message_id())
    
    # 5. Reenviar END/Results pendientes (idempotencia por IDs determinísticos)
    for client_id in working_state.chunks_to_receive.keys():
        for table_type in working_state.chunks_to_receive[client_id].keys():
            if working_state.is_end_message_received(client_id, table_type):
                publish_final_results(client_id, table_type)
                _send_end_message(client_id, table_type)
```

#### 5. Barrier Forward & END
```python
def _send_end_message(client_id, table_type):
    # CRÍTICO: Procesar chunks buffereados ANTES de publicar
    # Permite capturar chunks que llegaron después de END (out-of-order)
    buffered_chunks = persistence.recover_buffered_chunks()
    if buffered_chunks:
        for chunk in buffered_chunks:
            _apply_and_update_state(chunk)
        _save_state(buffered_chunks[-1].message_id())
        persistence._clear_chunk_buffer()
    
    # Publicar resultados finales
    publish_final_results(client_id, table_type)
    
    # Enviar END con ID determinístico (idempotencia)
    my_processed = working_state.get_processed_for_aggregator(client_id, table_type, aggregator_id)
    end_msg = MessageEnd(client_id, send_table_type, my_processed, str(aggregator_id))
    queue.send(end_msg.encode())
    
    # Limpiar datos del cliente
    delete_client_data(client_id, table_type)
```

---

## Recovery & Persistence - Joiner

### Estado a Persistir (Dos Working States)
**Main State** (datos del Maximizer):
- `client_data`: Chunks recibidos del maximizer por client_id
- `client_chunks`: Lista de chunks por client_id
- `client_end_messages_received`: Set de clientes con END recibido
- `finished_senders`: Tracking de senders que finalizaron (multi-input)
- `expected_chunks_per_client`: Total esperado por cada sender
- `processed_ids`: Set de message_ids procesados (idempotencia)
- `client_completed`: Set de clientes ya procesados completamente
- `pending_end_messages`: Cola de END messages pendientes de envío
- `client_results`: Resultados del join por client_id

**Join State** (datos del Server - tabla para join):
- `join_data_by_client`: Datos de join (ej: item_id -> item_name) por client_id
- `ready_to_join`: Set de clientes listos para join
- `processed_ids`: Set de message_ids procesados (idempotencia)

### Patrón de Commit

#### 1. Receive Callbacks
```python
# Main Data (Maximizer)
def callback(msg):
    if not msg.startswith(b"END;"):
        chunk = ProcessBatchReader.from_bytes(msg)
        persistence_main.commit_processing_chunk(chunk)
    results.append(msg)

# Join Data (Server)
def callback(msg):
    if not msg.startswith(b"END;"):
        chunk = ProcessBatchReader.from_bytes(msg)
        persistence_join.commit_processing_chunk(chunk)
    results.append(msg)
```

#### 2. Processing Main Data
```python
def _handle_data_chunk(data):
    # [CRASH POINT] CRASH_BEFORE_PROCESS
    chunk = ProcessBatchReader.from_bytes(data)
    
    with lock:
        # Idempotencia
        if working_state_main.is_processed(chunk.message_id()):
            return
        
        # Guardar datos
        save_data(chunk)
        
        # Marcar como procesado
        working_state_main.add_processed_id(chunk.message_id())
        
        # Persistir estado ANTES de verificar readiness
        _save_state_main(chunk.message_id())
        
        # [CRASH POINT] CRASH_AFTER_PROCESS_BEFORE_JOIN_CHECK
    
    # Verificar si está listo para join
    if is_ready_to_join_for_client(client_id) and working_state_main.is_end_message_received(client_id):
        _process_client_if_ready(client_id)
```

#### 3. Processing Join Data
```python
def _handle_join_chunk_bytes(data):
    chunk = ProcessBatchReader.from_bytes(data)
    
    with lock:
        # Idempotencia
        if working_state_join.is_processed(chunk.message_id()):
            return
        
        # Guardar datos de join
        save_data_join(chunk)
        
        # Marcar como procesado
        working_state_join.add_processed_id(chunk.message_id())
        
        # Persistir estado
        _save_state_join(chunk.message_id())
```

#### 4. Multi-Sender END Tracking
```python
def handle_end_message(end_message):
    client_id = end_message.client_id()
    sender_id = end_message.sender_id()
    count = end_message.total_chunks()
    
    with lock:
        # Verificar si sender ya terminó (idempotencia)
        if working_state_main.is_sender_finished(client_id, sender_id):
            return
        
        # Marcar sender como terminado
        working_state_main.mark_sender_finished(client_id, sender_id)
        working_state_main.add_expected_chunks(client_id, count)
        
        # Persistir tras marcar sender
        _save_state_main(uuid.uuid4())
        # [CRASH POINT] CRASH_AFTER_END_RECEIVED
        
        finished_count = working_state_main.get_finished_senders_count(client_id)
        
        # ¿Todos los inputs terminaron?
        if finished_count >= expected_inputs:
            working_state_main.mark_end_message_received(client_id)
            _save_state_main(uuid.uuid4())
            _process_client_if_ready(client_id)
```

#### 5. Process Client (Apply Join)
```python
def _process_client_if_ready(client_id):
    # Evitar reprocesar
    if working_state_main.is_client_completed(client_id):
        return
    
    if not (working_state_main.is_end_message_received(client_id) and is_ready_to_join_for_client(client_id)):
        return
    
    # 1. Apply join
    apply_for_client(client_id)
    # [CRASH POINT] CRASH_AFTER_APPLY_BEFORE_PUBLISH
    
    # 2. Publish results
    publish_results(client_id)
    # [CRASH POINT] CRASH_AFTER_PUBLISH_BEFORE_CLEANUP
    
    # 3. Cleanup
    clean_client_data(client_id)
    
    # 4. Marcar completado y agregar END pendiente
    working_state_main.mark_client_completed(client_id)
    working_state_main.add_pending_end_message(client_id)
    
    # Persistir
    _save_state_main(uuid.uuid4())
```

#### 6. Recovery Logic
```python
def handle_processing_recovery():
    # 1. Recuperar estados (main y join)
    _recover_state()
    
    # 2. Recuperar y reprocesar chunks interrumpidos
    last_chunk_main = persistence_main.recover_last_processing_chunk()
    if last_chunk_main:
        _handle_data_chunk(last_chunk_main.serialize())
    
    last_chunk_join = persistence_join.recover_last_processing_chunk()
    if last_chunk_join:
        _handle_join_chunk_bytes(last_chunk_join.serialize())
    
    # 3. Reenviar END messages pendientes
    pending_end_messages = working_state_main.get_pending_end_messages()
    if pending_end_messages:
        for client_id in pending_end_messages:
            send_end_query_msg(client_id)
            _send_coordination_messages(client_id)
        
        working_state_main.clear_pending_end_messages()
        persistence_main.commit_working_state(working_state_main.to_bytes(), uuid.uuid4())
    
    # 4. Verificar clientes listos después de recovery
    with lock:
        for client_id in working_state_main.client_end_messages_received:
            if not working_state_main.is_client_completed(client_id):
                if is_ready_to_join_for_client(client_id):
                    _process_client_if_ready(client_id)
```

---

## Recovery & Persistence - Maximizer

### Estado a Persistir
- `sellings_max`: Máximos de ventas por (item_id, month_year)
- `profit_max`: Máximos de ganancias por (item_id, month_year)
- `top3_by_store`: Top 3 clientes por store_id
- `tpv_results`: TPV acumulado por (store_id, year_half)
- `finished_senders`: Tracking de senders que finalizaron (multi-input)
- `expected_chunks_per_client`: Total esperado por cada sender
- `processed_ids`: Set de message_ids procesados (idempotencia)
- `client_end_processed`: Set de clientes ya procesados completamente
- `results_sent`: Tracking de resultados ya enviados (idempotencia)
- `end_sent`: Tracking de END messages ya enviados (idempotencia)

### Patrón de Commit

#### 1. Receive Callback
```python
def callback(msg):
    if msg.startswith(b"END;"):
        _handle_end_message(msg)
    else:
        _handle_data_chunk(msg)
```

#### 2. Processing Data Chunks
```python
def _handle_data_chunk(data):
    # [CRASH POINT] CRASH_BEFORE_PROCESS
    chunk = ProcessBatchReader.from_bytes(data)
    
    # Idempotencia
    if working_state.is_processed(chunk.message_id()):
        persistence.clear_processing_commit()
        return
    
    # Commit para recovery
    persistence.commit_processing_chunk(chunk)
    
    # Validar table_type
    if not _is_valid_table_type(table_type):
        persistence.clear_processing_commit()
        return
    
    # Apply (actualizar máximos/top3/tpv)
    apply(client_id, chunk)
    
    # Marcar como procesado + persistir inmediatamente
    working_state.mark_processed(chunk.message_id())
    _save_state(chunk.message_id())
    
    # [CRASH POINT] CRASH_AFTER_PROCESS_BEFORE_COMMIT
    
    # Limpiar commit de procesamiento
    persistence.clear_processing_commit()
```

#### 3. Multi-Sender END Tracking
```python
def _handle_end_message(raw_message):
    end_message = MessageEnd.decode(raw_message)
    client_id = end_message.client_id()
    sender_id = end_message.sender_id()
    count = end_message.total_chunks()
    
    with lock:
        # Idempotencia de sender
        if working_state.is_sender_finished(client_id, sender_id):
            return
        
        # Marcar sender terminado
        working_state.mark_sender_finished(client_id, sender_id)
        working_state.add_expected_chunks(client_id, count)
        
        finished_count = working_state.get_finished_senders_count(client_id)
        
        # ¿Todos los inputs terminaron?
        if finished_count >= expected_inputs:
            # Para TPV absolute: verificar shards esperados
            if maximizer_type == "TPV" and finished_count >= expected_shards:
                process_client_end(client_id, table_type)
            else:
                process_client_end(client_id, table_type)
    
    # Persistir END tracking
    _save_state(uuid.uuid4())
```

#### 4. Process Client End
```python
def process_client_end(client_id, table_type):
    # Evitar reprocesar
    if working_state.is_client_end_processed(client_id):
        return
    
    # Validar table_type
    expected_table = _expected_table_type()
    if table_type != expected_table:
        return
    
    chunks_sent = 0
    sender_id = shard_slug if shard_slug else maximizer_range
    
    # Publicar resultados según tipo
    if maximizer_type == "MAX":
        chunks_sent = publish_absolute_max_results(client_id)
        _send_end_message(client_id, TableType.TRANSACTION_ITEMS, "joiner", chunks_sent, sender_id)
    elif maximizer_type == "TOP3":
        chunks_sent = publish_absolute_top3_results(client_id)
        _send_end_message(client_id, TableType.PURCHASES_PER_USER_STORE, "joiner", chunks_sent, sender_id)
    elif maximizer_type == "TPV":
        chunks_sent = publish_tpv_results(client_id)
        _send_end_message(client_id, TableType.TPV, "joiner", chunks_sent, sender_id)
    
    # Marcar como completado solo si resultados y END fueron enviados
    label_end = f"end-{stage}"
    if working_state.results_already_sent(client_id, _results_label()) and working_state.end_already_sent(client_id, label_end):
        working_state.mark_client_end_processed(client_id)
        _save_state(uuid.uuid4())
        delete_client_data(client_id)
```

#### 5. Send END with Idempotency
```python
def _send_end_message(client_id, table_type, target, count, sender_id):
    label = f"end-{stage}"
    
    # Idempotencia: verificar si ya se envió
    if working_state.end_already_sent(client_id, label):
        return
    
    # [CRASH POINT] CRASH_BEFORE_SEND_END
    end_msg = MessageEnd(client_id, table_type, count, sender_id)
    data_sender.send(end_msg.encode())
    # [CRASH POINT] CRASH_AFTER_SEND_END
    
    # Publicar coordinación END y STATS
    payload = {
        "type": MSG_WORKER_END,
        "id": sender_id,
        "client_id": client_id,
        "stage": stage,
        "expected": count,
        "chunks": count,
    }
    middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
    
    # Marcar como enviado
    working_state.mark_end_sent(client_id, label)
    _save_state(uuid.uuid4())
```

#### 6. Recovery Logic
```python
def run():
    # Recuperar último chunk interrumpido
    last_chunk = persistence.recover_last_processing_chunk()
    if last_chunk:
        _handle_data_chunk(last_chunk.serialize())
    
    # _resume_pending_finalization() ya se ejecutó en __init__
    
    # Continuar con loop normal
    while __running:
        # Consumir mensajes...
        pass

def _resume_pending_finalization():
    """Llamado después de _recover_state() en __init__"""
    # Si hay clientes con END recibido pero no finalizados, reanudar
    for client_id in working_state.get_all_client_ids():
        if not working_state.is_client_end_processed(client_id):
            finished_count = working_state.get_finished_senders_count(client_id)
            if finished_count >= expected_inputs:
                # Reanudar proceso de finalización
                process_client_end(client_id, _expected_table_type())
```
