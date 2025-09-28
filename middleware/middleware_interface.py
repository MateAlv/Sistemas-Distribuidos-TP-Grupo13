import pika
from abc import ABC, abstractmethod
TIMEOUT = 3

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
    def __init__(self, host, exchange_name, route_keys, exchange_type="direct"):
        self.host = host
        self.exchange_name = exchange_name
        self.route_keys = route_keys if isinstance(route_keys, list) else [route_keys]
        self.exchange_type = exchange_type

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type, durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Error al conectar con RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            queue_name = result.method.queue

            for rk in self.route_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=rk)

            def callback(ch, method, properties, body):
                try:
                    on_message_callback(body.decode())
                except Exception as e:
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")

            self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
            self.connection.call_later(TIMEOUT, self.channel.stop_consuming)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en consumo: {e}")

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Conexión perdida al detener consumo.")

    def send(self, message):
        try:
            for rk in self.route_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=rk, body=message)
        except pika.exceptions.AMQPConnectionError:
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
            self.channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error eliminando exchange: {e}")


# ----------------------------
# Queue Middleware
# ----------------------------
class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Error al conectar con RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                try:
                    on_message_callback(body.decode())
                except Exception as e:
                    raise MessageMiddlewareMessageError(f"Error procesando mensaje: {e}")

            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en consumo: {e}")

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Conexión perdida al detener consumo.")

    def send(self, message):
        try:
            self.channel.basic_publish(exchange="", routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError:
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
