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
