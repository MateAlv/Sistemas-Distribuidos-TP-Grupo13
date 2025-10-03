import threading, time
from middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange

def run_consumer(consumer, callback, timeout=3):
    """
    Arranca el consumidor, programa un stop automático en el loop de pika,
    y espera a que termine sin cerrar la conexión de golpe.
    """
    def consume():
        def stop():
            try:
                consumer.stop_consuming()
            except Exception:
                pass
        # programar cierre limpio
        consumer.connection.call_later(timeout, stop)
        consumer.start_consuming(callback)

    t = threading.Thread(target=consume)
    t.start()
    t.join()
    consumer.close()


# -------------------- TESTS --------------------

def test_working_queue_1_to_1():
    qname = "task_queue_1to1"
    producer = MessageMiddlewareQueue("rabbitmq", qname)
    consumer = MessageMiddlewareQueue("rabbitmq", qname)

    results = []
    def callback(msg): results.append(msg)

    # arrancar consumer primero
    t = threading.Thread(target=lambda: run_consumer(consumer, callback))
    t.start()
    time.sleep(0.5)  # dar tiempo a bindear

    producer.send("mensaje unico")
    producer.close()
    t.join()

    assert "mensaje unico".encode() in results


def test_working_queue_1_to_n():
    qname = "task_queue_1toN"
    producer = MessageMiddlewareQueue("rabbitmq", qname)

    results_1, results_2 = [], []
    consumer1 = MessageMiddlewareQueue("rabbitmq", qname)
    consumer2 = MessageMiddlewareQueue("rabbitmq", qname)

    def callback1(msg): results_1.append(msg)
    def callback2(msg): results_2.append(msg)

    # arrancar consumers antes de producir
    t1 = threading.Thread(target=lambda: run_consumer(consumer1, callback1))
    t2 = threading.Thread(target=lambda: run_consumer(consumer2, callback2))
    t1.start(); t2.start()
    time.sleep(0.5)

    producer.send("msgA")
    producer.send("msgB")
    producer.close()

    t1.join(); t2.join()

    # ambos deben haber procesado entre los dos los 2 mensajes
    assert (len(results_1) + len(results_2)) == 2
    assert len(results_1) > 0
    assert len(results_2) > 0


def test_exchange_1_to_1():
    ex = "direct_test"
    rk = ["only_one"]

    producer = MessageMiddlewareExchange("rabbitmq", ex, rk)
    consumer = MessageMiddlewareExchange("rabbitmq", ex, rk)

    results = []
    def callback(msg): results.append(msg)

    # arrancar consumidor primero
    t = threading.Thread(target=lambda: run_consumer(consumer, callback))
    t.start()
    time.sleep(0.5)

    producer.send("direct message")
    producer.close()
    t.join()

    assert "direct message".encode() in results


def test_exchange_1_to_n():
    ex = "fanout_test"

    consumer1 = MessageMiddlewareExchange("rabbitmq", ex, [""], exchange_type="fanout")
    consumer2 = MessageMiddlewareExchange("rabbitmq", ex, [""], exchange_type="fanout")

    results_1, results_2 = [], []
    def callback1(msg): results_1.append(msg)
    def callback2(msg): results_2.append(msg)

    t1 = threading.Thread(target=lambda: run_consumer(consumer1, callback1))
    t2 = threading.Thread(target=lambda: run_consumer(consumer2, callback2))
    t1.start(); t2.start()
    time.sleep(0.5)

    producer = MessageMiddlewareExchange("rabbitmq", ex, [""], exchange_type="fanout")
    producer.send("broadcast")
    producer.close()

    t1.join(); t2.join()

    assert "broadcast".encode() in results_1
    assert "broadcast".encode() in results_2
