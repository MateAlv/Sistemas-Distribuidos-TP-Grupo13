# Fault Tolerance & Persistence Design

## WorkingState
**TODO: Implement in each component**

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

service = PersistenceService()

# 1. Restore State
estado = estado.from_bytes(service.recover_working_state()) # restauro contadores

# 2. Recover last processing chunk
# busco último valor x commiteado en disco si no fue enviado
x = service.recover_last_processing_chunk() 

if x:
    # Re-process if needed
    apply(x) # Procesar
    
    if not process_has_been_counted(self, x.message_id()):
        actualizar_estado(x)
        commit_working_state(self, estado.to_bytes, x.message_id())
    
    # Re-send
    Mando_a_cola() # Enviar
    commit_send_ack(self, x.client_id(), x.message_id())
```

## Main Processing Loop (Pseudo-code)

```python
while True:
    x = pop_from_queue()
    
    # Check if message was already acknowledged/processed
    if message_acked(): # le mando al MessageChecker si lo recibio o no
        commit_processing_chunk(self, x)
        
        apply(x) # Procesar
        actualizar_estado(x)
        
        commit_working_state(self, estado.to_bytes, x.message_id())
    else:
        pass
        
    # Send ACK to RabbitMQ
    mando_ACK_a_cola() 
    
    # Check if output was already sent
    if not send_has_been_acknowledged(self, x.client_id(), x.message_id()):
        Mando_a_cola() 
        commit_send_ack(self, x.client_id(), x.message_id())
    else:
        pass
```
