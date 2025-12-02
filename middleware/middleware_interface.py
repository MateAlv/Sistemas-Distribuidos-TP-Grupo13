import pika
import logging
import time
import socket
from abc import ABC, abstractmethod

logging.getLogger("pika").setLevel(logging.CRITICAL)

TIMEOUT = 3
RETRY_DELAY = 3
HEARTBEAT = 3000

class MessageMiddlewareMessageError(Exception):
    pass


class MessageMiddlewareDisconnectedError(Exception):
    pass


class MessageMiddlewareCloseError(Exception):
    pass


class MessageMiddlewareDeleteError(Exception):
    pass


class MessageMiddleware(ABC):
    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    @abstractmethod
    def stop_consuming(self):
        pass

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def delete(self):
        pass


# ----------------------------
# Exchange Middleware
# ----------------------------
class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host: str, exchange_name: str, consumer_id: str, exchange_type: str = "fanout", routing_keys=None):
        self.host = host
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.consumer_id = consumer_id
        self.queue_name = f"{exchange_name}_{consumer_id}"
        self.routing_keys = routing_keys or [""]
        self._connect()

    def _connect(self):
        for attempt in range(15):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        heartbeat=HEARTBEAT,
                        blocked_connection_timeout=30,
                    )
                )
                self.channel = self.connection.channel()
                self.channel.exchange_declare(
                    exchange=self.exchange_name,
                    exchange_type=self.exchange_type,
                    durable=True,
                )
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                # Bind with routing keys
                for rk in self.routing_keys:
                    self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=rk)
                self.channel.basic_qos(prefetch_count=1)
                self.channel.confirm_delivery()
                return
            except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
                logging.warning(f"Connection attempt {attempt+1}/15 failed: {e}")
                if attempt < 14:
                    time.sleep(RETRY_DELAY)
                else:
                    raise MessageMiddlewareDisconnectedError(f"Error al conectar con RabbitMQ: {e}")
            except Exception as e:
                logging.error(f"Unexpected error connecting to RabbitMQ: {e}")
                if attempt < 14:
                    time.sleep(RETRY_DELAY)
                else:
                    raise MessageMiddlewareDisconnectedError(f"Error al conectar con RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                try:
                    on_message_callback(body)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")

            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            self._connect()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en consumo: {e}")

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Conexión perdida al detener consumo.")

    def send(self, message: bytes, routing_key: str = ""):
        try:
            props = pika.BasicProperties(delivery_mode=2)
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=props,
                mandatory=False,
            )
        except pika.exceptions.AMQPConnectionError:
            self._connect()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al enviar mensaje.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje: {e}")

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error cerrando conexión: {e}")

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.queue_name)
            self.channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error eliminando exchange: {e}")

    def purge(self):
        try:
            self.channel.queue_purge(queue=self.queue_name)
        except pika.exceptions.AMQPConnectionError:
            self._connect()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al purgar exchange.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al purgar exchange: {e}")


# ----------------------------
# Queue Middleware
# ----------------------------
class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self._connect()

    def _connect(self):
        """Conecta y declara la cola durable con confirm_delivery."""
        for attempt in range(15):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, heartbeat=HEARTBEAT, blocked_connection_timeout=30)
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                self.channel.confirm_delivery()
                self.channel.basic_qos(prefetch_count=1)
                return
            except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
                logging.warning(f"Connection attempt {attempt+1}/15 failed: {e}")
                if attempt < 14:
                    time.sleep(RETRY_DELAY)
                else:
                    raise MessageMiddlewareDisconnectedError(f"Error al conectar con RabbitMQ: {e}")
            except Exception as e:
                logging.error(f"Unexpected error connecting to RabbitMQ: {e}")
                if attempt < 14:
                    time.sleep(RETRY_DELAY)
                else:
                    raise MessageMiddlewareDisconnectedError(f"Error al conectar con RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        """Consume con ACK manual y requeue en errores."""
        try:
            def callback(ch, method, properties, body):
                try:
                    logging.debug(f"action: middleware_received_msg | queue:{self.queue_name} | size:{len(body)}")
                    on_message_callback(body)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")

            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            self._connect()
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en consumo: {e}")

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Conexión perdida al detener consumo.")

    def send(self, message):
        """Envía mensajes persistentes y confirmados."""
        try:
            props = pika.BasicProperties(delivery_mode=2)
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=props,
                mandatory=False,
            )
            logging.debug(f"action: middleware_sent_msg | queue:{self.queue_name} | size:{len(message)}")
        except pika.exceptions.AMQPConnectionError:
            self._connect()
            raise MessageMiddlewareDisconnectedError("Conexión perdida al enviar mensaje.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar: {e}")

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error cerrando conexión: {e}")

    def delete(self):
        try:
            self.channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error eliminando queue: {e}")
