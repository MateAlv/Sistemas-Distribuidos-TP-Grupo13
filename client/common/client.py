# common/client.py
import os
import logging
from datetime import datetime
from typing import Dict, Tuple

from utils.communication.directory_reader import DirectoryReader
from utils.communication.batch_reader import BatchReader
from common.sender import Sender


# ============================
# Constantes del cliente
# ============================
# Conexión: SIEMPRE 1 socket TCP persistente
CONNECT_TIMEOUT_S: float = 10.0
IO_TIMEOUT_S: float = 30.0

class Client:
   
    def __init__(self, config: Dict, default_data_dir: str = "/data") -> None:
        # Identidad del cliente
        self.id: str = str(config.get("id", ""))

        # Tamaño de batch (bytes) para leer/enviar en chunks (se mantiene configurable)
        mp = config.get("message_protocol", {}) or {}
        self.batch_size: int = int(mp.get("batch_size", 64 * 1024))

        # Dirección del servidor "host:port"
        self.server_address_str: str = str(config.get("server_address", "server:5000"))
        self.server_host, self.server_port = self._parse_host_port(self.server_address_str)

        # Directorio de datos
        self.data_dir: str = str(config.get("data_dir", default_data_dir))
        
        # Lector de batches
        self.reader = BatchReader(
            client_id=int(self.id),
            root=self.data_dir,
            max_batch_size=self.batch_size,
        )
            
        logging.debug(
            "client_init | id=%s host=%s port=%s data_dir=%s batch_size=%s",
            self.id, self.server_host, self.server_port, self.data_dir, self.batch_size
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
        with Sender(
            self.server_host,
            self.server_port,
            connect_timeout=CONNECT_TIMEOUT_S,
            io_timeout=IO_TIMEOUT_S,
        ) as sender:
            try:
                logging.info("Cliente %s: conectando a %s:%s", self.id, self.server_host, self.server_port)
                # Handshake SIEMPRE encendido
                sender.send_handshake_request(self.id)
                
                logging.info("Cliente %s: handshake OK con %s:%s", self.id, self.server_host, self.server_port)
                
                logging.info("Cliente %s: comenzando envío de datos a %s:%s", self.id, self.server_host, self.server_port)
                
                # Envío de batches (iterativo)
                for chunk in self.reader.iter():
                    logging.info("Cliente %s: enviando chunk: file=%s bytes=%s last=%s",
                                 self.id, chunk.path(), chunk.payload_size(), chunk.is_last_file_chunk())
                    sender.send_file_chunk(chunk.serialize())
                    if chunk.is_last_file_chunk():
                        # Espera ACK de fin de archivo
                        sender.wait_end_file_ack()
                        logging.info("Cliente %s: archivo completado: %s", self.id, chunk.path())

                # Señal de fin - Todos los archivos enviados
                sender.send_finished()
                logging.info("Cliente %s: todos los archivos enviados, esperando resultados...", self.id)

                # Mantener socket abierto y esperar resultados
                self._wait_for_results(sender)

                logging.info("Cliente %s: envío y recepción completados.", self.id)
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
        Guarda los resultados en archivos de salida en el directorio de datos.
        """
        logging.info("Cliente %s: esperando resultados del servidor...", self.id)
        
        try:
            # Configurar timeout más largo para esperar resultados
            sender._sock.settimeout(300.0)  # 5 minutos
            
            results_received = 0
            total_bytes = 0
            output_dir = None  # Se creará cuando sea necesario
            
            while True:
                try:
                    # Leer header del resultado
                    header = sender._recv_header_id(sender._sock)
                    
                    if header == 2:  # H_ID_DATA - son resultados
                        # Leer el FileChunk con los resultados
                        from utils.communication.file_chunk import FileChunk
                        result_chunk = FileChunk.recv(sender._sock)
                        
                        results_received += 1
                        total_bytes += result_chunk.payload_size()
                        
                        logging.info("Cliente %s: resultado recibido #%d: file=%s bytes=%s", 
                                   self.id, results_received, result_chunk.path(), result_chunk.payload_size())
                        
                        # Crear directorio de salida solo cuando recibamos el primer resultado
                        if output_dir is None:
                            output_dir = os.path.join(self.data_dir, "output")
                            os.makedirs(output_dir, exist_ok=True)
                            logging.info("Cliente %s: directorio de salida creado: %s", self.id, output_dir)
                        
                        # Guardar resultado en archivo
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_filename = f"Output_Query1_Result_{results_received}_{timestamp}.processed"
                        output_path = os.path.join(output_dir, output_filename)
                        
                        with open(output_path, 'wb') as f:
                            f.write(result_chunk.payload())
                        
                        logging.info("Cliente %s: resultado guardado en: %s", self.id, output_path)
                        
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
            
            if results_received > 0 and output_dir is not None:
                logging.info("Cliente %s: resultados guardados en directorio: %s", self.id, output_dir)
            elif results_received == 0:
                logging.info("Cliente %s: no se recibieron resultados", self.id)
                        
        except Exception as e:
            logging.error("Cliente %s: error esperando resultados: %s", self.id, e)
