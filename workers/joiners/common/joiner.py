
from utils.processing.process_table import TableProcessRow
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.eof_protocol.end_messages import MessageEnd
from middleware.middleware_interface import MessageMiddlewareMessageError, MessageMiddlewareExchange
import logging
import threading
import sys
import os
import pickle
import uuid
from collections import deque
import json
from utils.tolerance.persistence_service import PersistenceService
from utils.tolerance.crash_helper import crash_after_two_chunks, crash_after_end_processed
from utils.protocol import (
    COORDINATION_EXCHANGE,
    MSG_WORKER_END,
    MSG_WORKER_STATS,
    DEFAULT_SHARD,
    STAGE_JOIN_ITEMS,
    STAGE_JOIN_STORES_TPV,
    STAGE_JOIN_STORES_TOP3,
    STAGE_JOIN_USERS,
)

TIMEOUT = 3

ITEMS_JOINER = "ITEMS"
STORES_TPV_JOINER = "STORES_TPV"
STORES_TOP3_JOINER = "STORES_TOP3"
USERS_JOINER = "USERS"

from .joiner_working_state import JoinerMainWorkingState, JoinerJoinWorkingState

class Joiner:
    
    def __init__(self, join_type: str, expected_inputs: int = 1, monitor=None):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        self.monitor = monitor

        self.joiner_type = join_type
        self.expected_inputs = int(expected_inputs) # Number of upstream workers (Aggregators)
        if self.joiner_type == ITEMS_JOINER:
            self.stage = STAGE_JOIN_ITEMS
        elif self.joiner_type == STORES_TPV_JOINER:
            self.stage = STAGE_JOIN_STORES_TPV
        elif self.joiner_type == STORES_TOP3_JOINER:
            self.stage = STAGE_JOIN_STORES_TOP3
        elif self.joiner_type == USERS_JOINER:
            self.stage = STAGE_JOIN_USERS
        else:
            self.stage = f"join_{self.joiner_type.lower()}"
        
        # State is now managed by WorkingState classes
        self.working_state_main = JoinerMainWorkingState()
        self.working_state_join = JoinerJoinWorkingState()

        self.lock = threading.Lock()
        self.data_receiver = None
        self.data_join_receiver = None
        self.data_sender = None
        self.data_receiver_timer = None
        self.data_join_receiver_timer = None

        self.__running = True
        
        # Iniciar hilos para manejar data y join_data (eliminamos end_message_handler_thread)
        self.data_handler_thread = threading.Thread(target=self.handle_data, name="DataHandler")
        self.join_data_handler_thread = threading.Thread(target=self.handle_join_data, name="JoinDataHandler")

        # Persistence
        # Main persistence (Maximizer data)
        persistence_dir = os.getenv("PERSISTENCE_DIR", "/data/persistence")
        base_dir = f"{persistence_dir}/joiner_{self.joiner_type}_{self.id}" if hasattr(self, "id") else f"{persistence_dir}/joiner_{self.joiner_type}"
        self.persistence_main = PersistenceService(directory=os.path.join(base_dir, "main"))
        
        # Join persistence (Server data)
        self.persistence_join = PersistenceService(directory=os.path.join(base_dir, "join"))
        # Coordination publisher
        # Coordination publisher and consumer with shard-aware routing (joiners are non-sharded: use global)
        self.middleware_coordination = MessageMiddlewareExchange(
            "rabbitmq",
            COORDINATION_EXCHANGE,
            [""],
            "topic",
            routing_keys=[f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"],
        )

        self.define_queues()

    def _send_coordination_messages(self, client_id):
        """
        Helper method to send coordination END and STATS messages for a client.
        This encapsulates the common logic used in both recovery and normal processing.
        """
        try:
            payload = {
                "type": MSG_WORKER_END,
                "id": str(self.id) if hasattr(self, "id") else self.joiner_type,
                "client_id": client_id,
                "stage": self.stage,
                "shard": DEFAULT_SHARD,
                "expected": 1,
                "chunks": 1,
                "sender": str(self.id) if hasattr(self, "id") else self.joiner_type,
            }
            stats_payload = {
                "type": MSG_WORKER_STATS,
                "id": str(self.id) if hasattr(self, "id") else self.joiner_type,
                "client_id": client_id,
                "stage": self.stage,
                "shard": DEFAULT_SHARD,
                "expected": 1,
                "chunks": 1,
                "processed": 1,
                "sender": str(self.id) if hasattr(self, "id") else self.joiner_type,
            }
            rk = f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"
            self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
            self.middleware_coordination.send(json.dumps(stats_payload).encode("utf-8"), routing_key=rk)
            
            logging.info(f"action: coordination_messages_sent | stage:{self.stage} | cli_id:{client_id}")
        except Exception as e:
            logging.error(f"action: coordination_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
            raise


    def _save_state_main(self, last_processed_id):
        self.persistence_main.commit_working_state(self.working_state_main.to_bytes(), last_processed_id)

    def _save_state_join(self, last_processed_id):
        self.persistence_join.commit_working_state(self.working_state_join.to_bytes(), last_processed_id)

    def _recover_state(self):
        # Recover Main State
        state_data_main = self.persistence_main.recover_working_state()
        if state_data_main:
            try:
                self.working_state_main = JoinerMainWorkingState.from_bytes(state_data_main)
                logging.info(f"Main state recovered for joiner {self.joiner_type}")
            except Exception as e:
                logging.error(f"Error recovering main state: {e}")

        # Recover Join State
        state_data_join = self.persistence_join.recover_working_state()
        if state_data_join:
            try:
                self.working_state_join = JoinerJoinWorkingState.from_bytes(state_data_join)
                logging.info(f"Join state recovered for joiner {self.joiner_type}")
            except Exception as e:
                logging.error(f"Error recovering join state: {e}")

    def handle_processing_recovery(self):
        """
        Recovers persisted state and processes any incomplete work.
        This includes:
        1. Recovering working states (main and join)
        2. Reprocessing last interrupted chunks
        3. Sending any pending END messages that were queued but not sent
        4. Checking if any clients are ready to be processed after recovery
        """
        # Step 1: Recover working states
        self._recover_state()
        
        # Step 2: Recover and reprocess last interrupted chunks
        # Recover Main Chunk
        last_chunk_main = self.persistence_main.recover_last_processing_chunk()
        if last_chunk_main:
            logging.info("Recovering last main chunk...")
            # _handle_data_chunk has idempotency check, safe to call
            self._handle_data_chunk(last_chunk_main.serialize())

        # Recover Join Chunk
        last_chunk_join = self.persistence_join.recover_last_processing_chunk()
        if last_chunk_join:
            logging.info("Recovering last join chunk...")
            # _handle_join_chunk_bytes has idempotency check, safe to call
            self._handle_join_chunk_bytes(last_chunk_join.serialize())
        
        # Step 3: Send any pending END messages that were queued but not sent
        pending_end_messages = self.working_state_main.get_pending_end_messages()
        if pending_end_messages:
            logging.info(f"action: recovering_pending_end_messages | count:{len(pending_end_messages)}")
            for client_id in pending_end_messages:
                try:
                    logging.info(f"action: sending_recovered_end_message | type:{self.joiner_type} | client_id:{client_id}")
                    self.send_end_query_msg(client_id)
                    
                    # Publish coordination END and STATS
                    self._send_coordination_messages(client_id)
                    
                except Exception as e:
                    logging.error(f"action: error_sending_recovered_end_message | type:{self.joiner_type} | client_id:{client_id} | error:{e}")
            
            # Clear pending messages after sending them
            self.working_state_main.clear_pending_end_messages()
            self.persistence_main.commit_working_state(self.working_state_main.to_bytes(), uuid.uuid4())
            logging.info(f"action: cleared_recovered_pending_end_messages | type:{self.joiner_type}")
        
        # Step 4: Check if any clients are ready to be processed after recovery
        # This handles the case where we crashed after receiving all data and END messages
        # but before processing the join
        with self.lock:
            # Get all clients that have received END messages
            for client_id in self.working_state_main.client_end_messages_received:
                if not self.working_state_main.is_client_completed(client_id):
                    if self.is_ready_to_join_for_client(client_id):
                        logging.info(f"action: processing_ready_client_after_recovery | type:{self.joiner_type} | client_id:{client_id}")
                        self._process_client_if_ready(client_id)
                    else:
                        logging.debug(f"action: client_not_ready_after_recovery | type:{self.joiner_type} | client_id:{client_id} | has_chunks:{self.working_state_main.has_chunks(client_id)} | ready_flag:{self.working_state_main.is_ready_flag_set(client_id)}")

    def handle_join_data(self):
        """Maneja datos de join (tabla de productos del server)"""
        results = deque()
        
        def callback(msg):
            # Only deserialize and commit if it's not an END message
            # END messages will be handled in the main processing loop
            if not msg.startswith(b"END;"):
                chunk = ProcessBatchReader.from_bytes(msg)
                self.persistence_join.commit_processing_chunk(chunk)
            results.append(msg)
        def stop():
            self.data_join_receiver.stop_consuming()

        while self.__running:
            # Escuchar datos de join (productos)
            self.data_join_receiver_timer = self.data_join_receiver.connection.call_later(TIMEOUT, stop)
            try:
                self.data_join_receiver.start_consuming(callback)
            except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                logging.error(f"Error en consumo: {e}")

            while results:
                data = results.popleft()
                try:
                    if data.startswith(b"END;"):
                        end_message = MessageEnd.decode(data)
                        logging.info(f"action: received_server_end_message_for_join_data | type:{self.joiner_type} | client_id:{end_message.client_id()} | table_type:{end_message.table_type()} | count:{end_message.total_chunks()}")
                        with self.lock:
                            self.working_state_main.set_ready_to_join(end_message.client_id())
                            
                            # Persist state after marking ready to join
                            self._save_state_main(uuid.uuid4())
                            self._check_crash_point("CRASH_AFTER_JOIN_END_RECEIVED")
                            
                            # Si ya tenemos END de datos principales, intentar procesar ahora
                            self._process_client_if_ready(end_message.client_id())
                    else:
                        logging.info(
                            f"action: join_data_received | type:{self.joiner_type} | bytes:{len(data)}"
                        )
                        self._handle_join_chunk_bytes(data)
                except ValueError as e:
                    logging.error(f"action: error_parsing_join_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | data_preview:{data[:100] if len(data) > 100 else data}")
                except Exception as e:
                    logging.error(f"action: unexpected_error_join_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | error_traceback:", exc_info=True)

    def _handle_join_chunk_bytes(self, data: bytes):
        chunk = ProcessBatchReader.from_bytes(data)
        
        with self.lock:
            # Idempotency
            if self.working_state_join.is_processed(chunk.message_id()):
                return
            
            logging.info(f"action: receive_join_table_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
            self.save_data_join(chunk)
            
            self.working_state_join.add_processed_id(chunk.message_id())

            self._save_state_join(chunk.message_id())
                
    def handle_data(self):
        """Maneja datos del maximizer y END messages del exchange"""
        results = deque()
        
        def callback(msg):
            # Only deserialize and commit if it's not an END message
            # END messages will be handled in the main processing loop
            if not msg.startswith(b"END;"):
                chunk = ProcessBatchReader.from_bytes(msg)
                # Only persist for recovery - actual processing happens in _handle_data_chunk
                self.persistence_main.commit_processing_chunk(chunk)
            results.append(msg)
        def stop():
            self.data_receiver.stop_consuming()

        while self.__running:

            # Escuchar datos del maximizer con timeout
            try:
                self.data_receiver_timer = self.data_receiver.connection.call_later(TIMEOUT, stop)
                self.data_receiver.start_consuming(callback)
            except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                logging.error(f"Error en consumo: {e}")

            # Procesar datos del maximizer
            while results:
                data = results.popleft()

                try:
                    if data.startswith(b"END;"):
                        end_message = MessageEnd.decode(data)
                        client_id = end_message.client_id()
                        table_type = end_message.table_type()
                        sender_id = end_message.sender_id()
                        count = end_message.total_chunks()
                        
                        with self.lock:
                            # Multi-Sender Tracking Logic
                            if self.working_state_main.is_sender_finished(client_id, sender_id):
                                logging.debug(f"action: duplicate_end_from_sender | type:{self.joiner_type} | client_id:{client_id} | sender_id:{sender_id}")
                            else:
                                self.working_state_main.mark_sender_finished(client_id, sender_id)
                                self.working_state_main.add_expected_chunks(client_id, count)
                                
                                # Persist state after marking sender finished
                                self._save_state_main(uuid.uuid4())
                                self._check_crash_point("CRASH_AFTER_END_RECEIVED")
                                
                                finished_count = self.working_state_main.get_finished_senders_count(client_id)
                                logging.info(f"action: end_received | type:{self.joiner_type} | client_id:{client_id} | sender_id:{sender_id} | count:{count} | finished:{finished_count}/{self.expected_inputs}")
                                
                                if finished_count >= self.expected_inputs:
                                    logging.info(f"action: all_inputs_finished | type:{self.joiner_type} | client_id:{client_id}")
                                    self.working_state_main.mark_end_message_received(client_id)
                                    
                                    # Persist after marking end message received
                                    self._save_state_main(uuid.uuid4())
                                    
                                    self._process_client_if_ready(client_id)
                                else:
                                    logging.info(f"action: waiting_for_more_senders | type:{self.joiner_type} | client_id:{client_id} | finished:{finished_count} | expected:{self.expected_inputs}")

                    else:
                        logging.info(
                            f"action: maximizer_data_received | type:{self.joiner_type} | bytes:{len(data)}"
                        )
                        self._handle_data_chunk(data)
                    
                    # Enviar END messages para clientes recién completados
                    pending_msgs = self.working_state_main.get_pending_end_messages()
                    if pending_msgs:
                        for client_id in pending_msgs:
                            try:
                                logging.debug(f"action: starting_send_end_query_msg | type:{self.joiner_type} | client_id:{client_id}")
                                self.send_end_query_msg(client_id)
                                
                                # Publish coordination END and STATS
                                self._send_coordination_messages(client_id)
                                
                                logging.debug(f"action: completed_send_end_query_msg | type:{self.joiner_type} | client_id:{client_id}")
                            except Exception as end_msg_error:
                                logging.error(f"action: error_sending_end_message | type:{self.joiner_type} | client_id:{client_id} | error:{end_msg_error} | error_type:{type(end_msg_error).__name__}")
                                raise end_msg_error
                        self.working_state_main.clear_pending_end_messages()
                        self.persistence_main.commit_working_state(self.working_state_main.to_bytes(), uuid.uuid4())
                        logging.debug(f"action: cleared_pending_end_messages | type:{self.joiner_type}")
                    
                except ValueError as e:
                    logging.error(f"action: error_parsing_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | data_preview:{data[:100] if len(data) > 100 else data}")
                except Exception as e:
                    logging.error(f"action: unexpected_error | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | error_traceback:", exc_info=True)

    def _handle_data_chunk(self, data: bytes):
        self._check_crash_point("CRASH_BEFORE_PROCESS")
        chunk = ProcessBatchReader.from_bytes(data)
        crash_after_two_chunks("joiner")
        
        with self.lock:
            # Idempotency
            is_proc = self.working_state_main.is_processed(chunk.message_id())
            logging.info(f"action: check_idempotency | type:{self.joiner_type} | msg_id:{chunk.message_id()} | is_processed:{is_proc}")
            if is_proc:
                return

            logging.info(f"action: receive_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
            
            # Save data (this internally adds to state)
            self.save_data(chunk)
            
            # Mark as processed
            self.working_state_main.add_processed_id(chunk.message_id())
            
            # Persist state BEFORE checking readiness
            self._save_state_main(chunk.message_id())
            
            self._check_crash_point("CRASH_AFTER_PROCESS_BEFORE_JOIN_CHECK")
                        
        with self.lock:
            # Check if we're ready to join for this specific client
            client_id = chunk.client_id()
            if self.is_ready_to_join_for_client(client_id) and self.working_state_main.is_end_message_received(client_id):
                logging.info(f"action: ready_to_join | type:{self.joiner_type} | client_id:{client_id}")
                self._process_client_if_ready(client_id)
            else:
                logging.debug(f"action: waiting_join_data | type:{self.joiner_type} | cli_id:{client_id} | file_type:{chunk.table_type()}")

    def _check_crash_point(self, point_name):
        if os.environ.get("CRASH_POINT") == point_name:
            logging.critical(f"Simulating crash at {point_name}")
            sys.exit(1)

    def save_data(self, chunk) -> bool:
        """
        Guarda los datos para la tabla que debe joinearse.
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        
        self.working_state_main.add_data(client_id, rows)
        self.working_state_main.add_chunk(client_id, chunk)
        
        return True

    def _process_client_if_ready(self, client_id: int):
        """
        Ejecuta apply/publish/cleanup si ya se recibieron todos los END esperados
        y el join data está listo. Evita reprocesar clientes completados.
        """
        if self.working_state_main.is_client_completed(client_id):
            return
        if not (self.working_state_main.is_end_message_received(client_id) and self.is_ready_to_join_for_client(client_id)):
            return

        logging.info(f"action: processing_client | type:{self.joiner_type} | client_id:{client_id} | reason:ready_and_end_received")
        try:
            logging.debug(f"action: starting_apply_for_client | type:{self.joiner_type} | client_id:{client_id}")
            self.apply_for_client(client_id)
            logging.debug(f"action: completed_apply_for_client | type:{self.joiner_type} | client_id:{client_id}")

            self._check_crash_point("CRASH_AFTER_APPLY_BEFORE_PUBLISH")

            logging.debug(f"action: starting_publish_results | type:{self.joiner_type} | client_id:{client_id}")
            self.publish_results(client_id)
            logging.debug(f"action: completed_publish_results | type:{self.joiner_type} | client_id:{client_id}")
            
            self._check_crash_point("CRASH_AFTER_PUBLISH_BEFORE_CLEANUP")

            logging.debug(f"action: starting_clean_client_data | type:{self.joiner_type} | client_id:{client_id}")
            self.clean_client_data(client_id)
            logging.debug(f"action: completed_clean_client_data | type:{self.joiner_type} | client_id:{client_id}")

            self.working_state_main.mark_client_completed(client_id)
            self.working_state_main.add_pending_end_message(client_id)
            
            # Persist state after marking client completed
            self._save_state_main(uuid.uuid4())
            
            logging.debug(f"action: processing_complete | type:{self.joiner_type} | client_id:{client_id}")
            crash_after_end_processed("joiner")
        except Exception as inner_e:
            logging.error(f"action: error_in_join_processing | type:{self.joiner_type} | client_id:{client_id} | error:{inner_e} | error_type:{type(inner_e).__name__}")
            raise inner_e

    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de productos).
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        
        # Guardar mapping item_id → nombre
        for row in rows:
            if hasattr(row, 'item_id') and hasattr(row, 'item_name'):
                self.working_state_join.add_join_data(client_id, row.item_id, row.item_name)
                logging.debug(f"action: save_join_data | type:{self.joiner_type} | item_id:{row.item_id} | name:{row.item_name}")
            else:
                logging.warning(f"action: invalid_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_item_id:{hasattr(row, 'item_id')} | has_item_name:{hasattr(row, 'item_name')}")
            
        logging.info(f"action: saved_join_data | type:{self.joiner_type} | client_id:{client_id} | products_loaded:{self.working_state_join.get_join_data_count(client_id)}")
        return True

    def run(self):
        logging.info(f"Joiner iniciado. Tipo: {self.joiner_type}")
        self.handle_processing_recovery()
        
        self.data_handler_thread.start() 
        self.join_data_handler_thread.start()

    def define_queues(self):
        raise NotImplementedError("Subclasses must implement define_queues method")

    def save_data_join_fields(self, row: TableProcessRow, client_id):
        raise NotImplementedError("Subclasses must implement save_data_join_fields method")
    
    def join_result(self, row: TableProcessRow, client_id):
        raise NotImplementedError("Subclasses must implement join_result method")
    
    def publish_results(self):
        raise NotImplementedError("Subclasses must implement publish_results method")
    
    def send_end_query_msg(self, client_id):
        raise NotImplementedError("Subclasses must implement send_end_query_msg method")
    
    def is_ready_to_join_for_client(self, client_id):
        """Check if we have received all necessary data and end messages for this client"""
        # Check if we have data from maximizers for this client
        has_maximizer_data = self.working_state_main.has_chunks(client_id)
        # Check if we have received end message for this client  
        has_end_message = self.working_state_main.is_end_message_received(client_id)
        # Check if we haven't already processed this client
        not_processed = not self.working_state_main.is_client_completed(client_id)
        
        # Detailed logging to debug join readiness
        logging.debug(f"action: checking_join_readiness | type:{self.joiner_type} | client_id:{client_id} | has_maximizer_data:{has_maximizer_data} | has_end_message:{has_end_message} | not_processed:{not_processed}")
        
        if has_maximizer_data and has_end_message and not_processed:
            logging.info(f"action: ready_to_join | client_id:{client_id} | has_data:{has_maximizer_data} | has_end:{has_end_message}")
        else:
            if not has_maximizer_data:
                logging.debug(f"action: not_ready_to_join | reason:no_maximizer_data | client_id:{client_id}")
            if not has_end_message:
                logging.debug(f"action: not_ready_to_join | reason:no_end_message | client_id:{client_id}")
            if not not_processed:
                logging.debug(f"action: not_ready_to_join | reason:already_processed | client_id:{client_id}")
        
        return has_maximizer_data and has_end_message and not_processed
    
    def apply_for_client(self, client_id):
        """Apply join operation for a specific client"""
        if not self.working_state_main.has_chunks(client_id):
            logging.warning(f"action: no_data_to_join | client_id:{client_id}")
            return
            
        chunks = self.working_state_main.get_chunks(client_id)
        logging.debug(f"action: processing_chunks | type:{self.joiner_type} | client_id:{client_id} | chunks_count:{len(chunks)}")
        
        for chunk_idx, chunk in enumerate(chunks):

            logging.debug(f"action: processing_chunk | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | rows_count:{len(chunk.rows)}")
            
            for row_idx, row in enumerate(chunk.rows):
                try:
                    logging.debug(f"action: joining_row | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx}")
                    joined_row = self.join_result(row, client_id)
                    
                    self.working_state_main.add_result(client_id, chunk.message_id(), joined_row)
                    
                    logging.debug(f"action: row_joined_successfully | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx}")
                except Exception as row_error:
                    logging.error(f"action: error_joining_row | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx} | error:{row_error} | error_type:{type(row_error).__name__}")
                    raise row_error
            self.persistence_main.commit_working_state(self.working_state_main.to_bytes(), chunk.message_id())
        
        logging.info(f"action: applied_join | client_id:{client_id} | joined_rows:{len(self.working_state_main.get_results(client_id))}")
    
    def clean_client_data(self, client_id):
        """Clean client data after processing to prevent reprocessing"""
        self.working_state_main.clean_client_data(client_id)
        logging.info(f"action: client_data_cleaned | type:{self.joiner_type} | client_id:{client_id}")

    def reset_for_new_session(self):
        """Reset joiner state for a new processing session (new client batch)"""
        with self.lock:
            self.working_state_main.reset()
            logging.info(f"action: joiner_reset_for_new_session | type:{self.joiner_type}")

    def shutdown(self, signum=None, frame=None):
        logging.info(f"SIGTERM recibido: cerrando joiner {self.joiner_type}")

        try:
            if getattr(self, "data_receiver_timer", None):
                self.data_receiver_timer.cancel()
        except Exception:
            pass

        try:
            if getattr(self, "data_join_receiver_timer", None):
                self.data_join_receiver_timer.cancel()
        except Exception:
            pass

        # Detener consumos
        try:
            self.data_receiver.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_join_receiver.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.middleware_coordination.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass

        # Cerrar conexiones
        try:
            self.data_receiver.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_join_receiver.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_sender.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.middleware_coordination.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.middleware_coordination.close()
        except (OSError, RuntimeError, AttributeError):
            pass

        # Detener bucle de manejo de datos
        self.__running = False

        # Esperar hilos
        try:
            if self.data_handler_thread.is_alive():
                self.data_handler_thread.join(timeout=5)
        except (OSError, RuntimeError, AttributeError):
            pass

        try:
            if self.data_handler_thread.is_alive():
                self.join_data_handler_thread.join(timeout=5)
        except (OSError, RuntimeError, AttributeError):
            pass

        # Limpiar estructuras
        try:
            self.working_state_main.reset()
            self.working_state_join.reset()
        except Exception:
            pass

        logging.info(f"Joiner {self.joiner_type} cerrado correctamente.")
