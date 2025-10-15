# common/client.py
import os
import logging
from datetime import datetime
from typing import Dict, Tuple

from utils.communication.directory_reader import DirectoryReader
from utils.communication.batch_reader import BatchReader
from utils.file_utils.result_table import ResultTableType
from common.sender import Sender


# ============================
# Constantes del cliente
# ============================
# Conexión: SIEMPRE 1 socket TCP persistente
CONNECT_TIMEOUT_S: float = 10.0
IO_TIMEOUT_S: float = 30.0

class Client:
   
    def __init__(self, config: Dict, default_data_dir: str = "/data", default_output_dir: str = None) -> None:
        # Identidad del cliente
        self.id: str = str(config.get("id", ""))
        
        self.query_chunks = dict()  # query_type -> chunks amount for query

        # Tamaño de batch (bytes) para leer/enviar en chunks (se mantiene configurable)
        mp = config.get("message_protocol", {}) or {}
        self.batch_size: int = int(mp.get("batch_size", 64 * 1024))

        # Dirección del servidor "host:port"
        self.server_address_str: str = str(config.get("server_address", "server:5000"))
        self.server_host, self.server_port = self._parse_host_port(self.server_address_str)

        # Directorio de datos de entrada
        self.data_dir: str = str(config.get("data_dir", default_data_dir))
        
        # Directorio de salida para resultados - por defecto usar data_dir/output
        if default_output_dir is None:
            default_output_dir = os.path.join(self.data_dir, "output")
        self.output_dir: str = str(config.get("output_dir", default_output_dir))
        
        # Lector de batches
        self.reader = BatchReader(
            client_id=int(self.id),
            root=self.data_dir,
            max_batch_size=self.batch_size,
        )

        # Inicialización del sender
        self.sender = Sender(self.server_host, self.server_port, connect_timeout=CONNECT_TIMEOUT_S, io_timeout=IO_TIMEOUT_S)
            
        logging.debug(
            "client_init | id=%s host=%s port=%s data_dir=%s output_dir=%s batch_size=%s",
            self.id, self.server_host, self.server_port, self.data_dir, self.output_dir, self.batch_size
        )

    # ---------------------------
    # API pública
    # ---------------------------
    def start_client_loop(self) -> None:
        """
        Único modo soportado: conexión TCP persistente para TODO el directorio.
        Solo procesa archivos con extensión .csv (case-insensitive).
        """

        # Conexión persistente + handshake obligatorio
        with self.sender as sender:
            try:
                logging.info("Cliente %s: conectando a %s:%s", self.id, self.server_host, self.server_port)
                # Handshake SIEMPRE encendido
                sender.send_handshake_request(self.id)
                
                logging.info("Cliente %s: handshake OK con %s:%s", self.id, self.server_host, self.server_port)
                
                logging.info("Cliente %s: comenzando envío de datos a %s:%s", self.id, self.server_host, self.server_port)
                
                # Envío de batches (iterativo)
                for chunk in self.reader.iter():
                    logging.debug("Cliente %s: enviando chunk: file=%s bytes=%s", self.id, chunk.path(), chunk.payload_size())
                    sender.send_file_chunk(chunk.serialize())
                    # Espera ACK de fin de archivo
                    sender.wait_end_file_ack()

                # Señal de fin - Todos los archivos enviados
                sender.send_finished()
                logging.info("Cliente %s: todos los archivos enviados, esperando resultados...", self.id)

                # Mantener socket abierto y esperar resultados
                self._wait_for_results(sender)

                logging.info("Cliente %s: envío y recepción completados.", self.id)
                
                self.graceful_shutdown()
                
            except Exception as e:
                logging.error("Cliente %s: error en el envío: %s", self.id, e)
                raise
            
            
    # ---------------------------
    # Helpers
    # ---------------------------
    def _parse_host_port(self, s: str) -> Tuple[str, int]:
        if ":" not in s:
            return s, 5000
        host, port = s.rsplit(":", 1)
        try:
            return host, int(port)
        except ValueError:
            logging.warning("Puerto inválido en %s; uso 5000", s)
            return host, 5000

    def _wait_for_results(self, sender: Sender) -> None:
        """
        Espera resultados del servidor después de enviar todos los datos.
        Los resultados vienen como ProcessChunk serializado directamente.
        Guarda los resultados en archivos de salida en el directorio configurado.
        """
        logging.info("Cliente %s: esperando resultados del servidor...", self.id)
        
        try:
            # Configurar timeout más largo para esperar resultados
            sender._sock.settimeout(300.0)  # 5 minutos
            
            results_received = 0
            total_bytes = 0
            
            # Crear directorio de salida usando la configuración
            os.makedirs(self.output_dir, exist_ok=True)
            logging.info("Cliente %s: directorio de salida: %s", self.id, self.output_dir)
            
            # Variables para rastrear el tipo de query
            current_query = None
            
            while True:
                try:
                    # Leer header del resultado
                    header = sender._recv_header_id(sender._sock)
                        
                    if header == 2:  # H_ID_DATA - son resultados como ProcessChunk
                        # Leer el ProcessChunk serializado
                        from utils.file_utils.result_batch_reader import ResultBatchReader
                        from utils.communication.socket_utils import recv_exact
                        from utils.file_utils.result_chunk import ResultChunkHeader
                        
                        # Leer header del ProcessChunk para saber el tamaño
                        header_data = recv_exact(sender._sock, ResultChunkHeader.HEADER_SIZE)
                        result_header = ResultChunkHeader.deserialize(header_data)
                        
                        # Leer el payload
                        payload_data = recv_exact(sender._sock, result_header.size)
                        
                        # Reconstruir el chunk completo
                        full_chunk_data = header_data + payload_data
                        result_chunk = ResultBatchReader.from_bytes(full_chunk_data)
                        
                        results_received += 1
                        total_bytes += len(full_chunk_data)
                        
                        logging.info("Cliente %s: resultado recibido #%d: query=%s rows=%s bytes=%s", 
                                   self.id, results_received, result_chunk.query_type().name, 
                                   len(result_chunk.rows), len(full_chunk_data))

                        output_path = os.path.join(
                                        self.output_dir,
                                        f"results_{result_chunk.query_type().name.lower()}.csv"
                                    )
                        csv_header = result_chunk.query_type().obtain_csv_header()
                        self.query_chunks[result_chunk.query_type()] = self.query_chunks.get(result_chunk.query_type(), 0) + 1
                        
                        self._save_process_chunk_as_csv(result_chunk, output_path, csv_header)
                        
                        logging.debug("Cliente %s: resultado guardado en: %s", self.id, output_path)
                        
                    elif header == 3:  # H_ID_FINISH - fin de resultados
                        logging.info("Cliente %s: señal FINISHED recibida, terminando recepción", self.id)
                        break
                        
                    elif header == 4:  # H_ID_OK - ACK del server, archivos recibidos correctamente
                        logging.info("Cliente %s: ACK recibido del server, archivos procesados correctamente, continuando...", self.id)
                        # Seguir esperando resultados, no hacer break
                        continue
                        
                    else:
                        # Otro tipo de mensaje, terminar
                        logging.info("Cliente %s: mensaje no reconocido como resultado: %d", self.id, header)
                        break
                        
                except Exception as e:
                    # Timeout o error de conexión - asumir que terminaron los resultados
                    logging.info("Cliente %s: fin de resultados: %s", self.id, e)
                    break
            
            logging.info("Cliente %s: recibí respuesta completa - %d resultados, %d bytes total", 
                        self.id, results_received, total_bytes)
            
            if results_received > 0:
                logging.info("Cliente %s: resultados guardados en directorio: %s", self.id, self.output_dir)
            else:
                logging.info("Cliente %s: no se recibieron resultados", self.id)
                        
        except Exception as e:
            logging.error("Cliente %s: error esperando resultados: %s", self.id, e)

    def _save_process_chunk_as_csv(self, process_chunk, output_path: str, csv_header: str) -> None:
        """
        Convierte un ProcessChunk de vuelta a formato CSV y lo guarda.
        """
        try:
            with open(output_path, "r") as f:
                file_exists = True
        except FileNotFoundError:
            file_exists = False

        with open(output_path, "ab") as f:
            if not file_exists:
                f.write(csv_header.encode())
            for row in process_chunk.rows:
                data = row.to_csv()
                f.write(data.encode("utf-8")) # Asegurar UTF-8
    
    def close(self) -> None:
        try:
            self.sender.close()
        except Exception as e:
            logging.error("Error al cerrar el cliente %s: %s", self.id, e)

    def _begin_shutdown(self, signum, frame) -> None:
        logging.info("SIGTERM recibida para el cliente %s: apagando cliente", self.id)
        self.close()

    def graceful_shutdown(self) -> None:
        logging.info("Cliente %s: apagando cliente", self.id)
        self.close()