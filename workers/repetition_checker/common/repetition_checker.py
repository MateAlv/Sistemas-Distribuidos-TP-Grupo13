#!/usr/bin/env python3
import socket
import threading
import json
import os
import uuid
import logging
from utils.communication.socket_utils import ensure_socket, recv_exact, sendall
from utils.repetition_checker_protocol.repetition_checker_protocol import RepetitionCheckerMessage, RepetitionCheckerMessageHeader, HEADER_SIZE
from utils.repetition_checker_protocol.repetition_checker_payload import PROCESSING, BEING_PROCESSED_BY, PROCESSED, PROCESSED_ACK, ProcessingMessage, BeingProcessedByMessage, ProcessedMessage, ProcessedAckMessage
from pathlib import Path


class RepetitionChecker:
    def __init__(self, port:int, data_dir="./data"):
        self.host = "0.0.0.1"
        self.port = port
        self.data_dir = Path(data_dir)

        self.processing_file = self.data_dir / "processing.json"
        self.processed_file = self.data_dir / "processed.bin"

        self.processing_map = {}   # uuid_str -> processor_id
        self.processed_set = set() # uuid_str

        self.state_lock = threading.Lock()
        self.clients_lock = threading.Lock()

        self._running = True
        self.threads = []

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(100)

    def ensure_data_dir(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def load_processing(self):
        if self.processing_file.exists():
            try:
                with open(self.processing_file, "r", encoding="utf-8") as f:
                    self.processing_map = json.load(f)
            except Exception as e:
                logging.error(f"No se pudo leer {self.processing_file}: {e}")
                self.processing_map = {}
        else:
            self.processing_map = {}

    def save_processing_atomic(self):
        tmp = self.processing_file.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.processing_map, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.processing_file)
        except Exception as e:
            logging.error(f"No se pudo guardar el estado de procesamiento en {self.processing_file}: {e}")

    def load_processed(self):
        self.processed_set = set()
        if self.processed_file.exists():
            try:
                with open(self.processed_file, "rb") as f:
                    while True:
                        chunk = f.read(16)
                        if not chunk:
                            break
                        if len(chunk) != 16:
                            logging.error(f"Archivo {self.processed_file} corrupto: tamaño de chunk inválido.")
                            break
                        uid = uuid.UUID(bytes=chunk)
                        self.processed_set.add(str(uid))
            except Exception as e:
                logging.error(f"No se pudo leer {self.processed_file}: {e}")

    def append_processed_bin(self, u: uuid.UUID):
        try:
            with open(self.processed_file, "ab") as f:
                f.write(u.bytes)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logging.error(f"No se pudo actualizar {self.processed_file}: {e}")

    def handle_client(self, sock: socket.socket, addr):
        peer = f"{addr[0]}:{addr[1]}"

        try:
            processing_uuid = None
            processor_id = None

            logging.info(f"[CONN] Cliente conectado: {peer}")
            header_bytes = recv_exact(sock, HEADER_SIZE)
            header = RepetitionCheckerMessageHeader.deserialize(header_bytes)
            payload_bytes = recv_exact(sock, header.size)
            message = RepetitionCheckerMessage.deserialize(header, payload_bytes)
            logging.info(f"[RECV] Mensaje recibido del cliente {peer}: Tipo {header.message_type}, Tamaño {header.size} bytes")
            processing_msg = message.payload()
            if isinstance(processing_msg, ProcessingMessage):
                processing_uuid = str(processing_msg.message_id)
                processor_id = processing_msg.processor_id
            else:
                logging.error(f"Mensaje inesperado del cliente {peer}. Cerrando conexión.")
                return

            with self.state_lock:
                if processing_uuid in self.processed_set:
                    logging.info(f"Mensaje {processing_uuid} ya procesado. Informando al cliente.")
                    being_processed_msg = BeingProcessedByMessage(0)
                    being_processed_header = RepetitionCheckerMessageHeader(BEING_PROCESSED_BY)
                    being_processed_message = RepetitionCheckerMessage(being_processed_header, being_processed_msg)
                    sendall(sock, being_processed_message.serialize())
                    return
                
                elif processing_uuid in self.processing_map:
                    existing_processor = self.processing_map[processing_uuid]
                    logging.info(f"Mensaje {processing_uuid} ya en procesamiento por {existing_processor}. Informando al cliente.")
                    being_processed_msg = BeingProcessedByMessage(existing_processor)
                    being_processed_header = RepetitionCheckerMessageHeader(BEING_PROCESSED_BY)
                    being_processed_message = RepetitionCheckerMessage(being_processed_header, being_processed_msg)
                    sendall(sock, being_processed_message.serialize())
                    return
                
                else:
                    logging.info(f"Mensaje {processing_uuid} no procesado. Marcando como en procesamiento por {processor_id}.")
                    self.processing_map[processing_uuid] = processor_id
                    self.save_processing_atomic()
                    being_processed_msg = BeingProcessedByMessage(processor_id)
                    being_processed_header = RepetitionCheckerMessageHeader(BEING_PROCESSED_BY)
                    being_processed_message = RepetitionCheckerMessage(being_processed_header, being_processed_msg)
                    sendall(sock, being_processed_message.serialize())

            logging.info(f"Esperando confirmación de procesamiento para mensaje {processing_uuid} del cliente {peer}.")
            header_bytes = recv_exact(sock, HEADER_SIZE)
            header = RepetitionCheckerMessageHeader.deserialize(header_bytes)
            payload_bytes = recv_exact(sock, header.size)
            message = RepetitionCheckerMessage.deserialize(header, payload_bytes)
            if isinstance(message.payload(), ProcessedMessage):
                processed_msg = message.payload()
                processed_uuid = str(processed_msg.message_id)
                if processed_uuid != processing_uuid:
                    logging.error(f"El UUID del mensaje procesado {processed_uuid} no coincide con el UUID esperado {processing_uuid}. Cerrando conexión.")
                    return

                with self.state_lock:
                    logging.info(f"Mensaje {processed_uuid} marcado como procesado.")
                    self.processed_set.add(processed_uuid)
                    self.append_processed_bin(processed_msg.message_id)
                    if processed_uuid in self.processing_map:
                        del self.processing_map[processed_uuid]
                        self.save_processing_atomic()

                processed_ack_msg = ProcessedAckMessage(processed_msg.message_id)
                processed_ack_header = RepetitionCheckerMessageHeader(PROCESSED_ACK)
                processed_ack_message = RepetitionCheckerMessage(processed_ack_header, processed_ack_msg)
                sendall(sock, processed_ack_message.serialize())
                logging.info(f"Confirmación de procesamiento enviada para mensaje {processed_uuid}.")
            else:
                logging.error(f"Mensaje inesperado del cliente {peer}. Cerrando conexión.")
                return
        finally:
            try:
                sock.close()
            except:
                pass


    def start(self):
        logging.info(f"[START] Iniciando MessageChecker en puerto {self.port}")

        self.ensure_data_dir()
        self.load_processing()
        self.load_processed()

        try:
            while self._running:
                try:
                    client_sock, addr = self._socket.accept()
                except OSError:
                    break

                t = threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True)
                t.start()

                with self.clients_lock:
                    self.threads.append(t)

        finally:
            self.shutdown()

    def shutdown(self):
        self._running = False

        try:
            self._socket.close()
        except:
            pass

        with self.clients_lock:
            threads = list(self.client_threads.values())

        for t in threads:
            t.join(timeout=2)

        logging.info("[STOP] Servidor detenido.")