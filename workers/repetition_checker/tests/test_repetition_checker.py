import socket
import time
import uuid
import json
from pathlib import Path
import pytest

from utils.repetition_checker_protocol.repetition_checker_payload import (
    PROCESSING, BEING_PROCESSED_BY, PROCESSED, PROCESSING,
    ProcessingMessage, BeingProcessedByMessage, ProcessedMessage
)
from utils.repetition_checker_protocol.repetition_checker_protocol import (
    RepetitionCheckerMessageHeader, RepetitionCheckerMessage
)

HOST = "repetition_checker"    # 🚨 Nombre del servicio en Docker
PORT = 9999


def connect():
    s = socket.socket()
    s.connect((HOST, PORT))
    return s


def send(sock, msg_type, payload):
    payload_bytes = payload.serialize()
    header = RepetitionCheckerMessageHeader(msg_type, len(payload_bytes))
    sock.sendall(header.serialize() + payload_bytes)


def recv(sock):
    header = RepetitionCheckerMessageHeader.deserialize(sock.recv(8))
    payload = sock.recv(header.size)
    message = RepetitionCheckerMessage.deserialize(header, payload)
    return (header.message_type, message.payload())
    


@pytest.fixture(scope="session", autouse=True)
def wait_for_server():
    time.sleep(2)  # espera breve post-compose
    yield


# 1) Nuevo procesamiento
def test_new_processing(tmp_path):
    s = connect()
    msg_id = uuid.uuid4()

    send(s, PROCESSING, ProcessingMessage(1, msg_id))
    msg_type, resp = recv(s)

    assert msg_type == BEING_PROCESSED_BY
    assert resp.processor_id == 1


# 2) Ya procesado por otro user
def test_processing_other_user():
    msg_id = uuid.uuid4()

    s1 = connect()
    send(s1, PROCESSING, ProcessingMessage(1, msg_id))
    recv(s1)            # first claim

    s2 = connect()
    send(s2, PROCESSING, ProcessingMessage(2, msg_id))
    msg_type, resp = recv(s2)

    assert resp.processor_id == 1


# 3) mismo usuario retomando
def test_same_user_retry():
    msg_id = uuid.uuid4()

    s1 = connect()
    send(s1, PROCESSING, ProcessingMessage(3, msg_id))
    recv(s1)

    s2 = connect()
    send(s2, PROCESSING, ProcessingMessage(3, msg_id))
    msg_type, resp = recv(s2)

    assert resp.processor_id == 3


# 4) Procesado y ACK recibido
def test_processed_ack():
    msg_id = uuid.uuid4()
    s = connect()

    send(s, PROCESSING, ProcessingMessage(9, msg_id))
    recv(s)  # processing OK

    send(s, PROCESSED, ProcessedMessage(msg_id))
    msg_type, _ = recv(s)

    assert msg_type == PROCESSED


# 5) persistencia processing.json
def test_persistence_processing():
    msg_id = uuid.uuid4()

    s = connect()
    send(s, PROCESSING, ProcessingMessage(7, msg_id))
    recv(s)

    time.sleep(0.3)
    data = json.load(open("/data/processing.json"))

    assert str(msg_id) in data
    assert data[str(msg_id)] == 7


# 6) persistencia processed.bin
def test_persistence_processed_bin():
    msg_id = uuid.uuid4()
    s = connect()

    send(s, PROCESSING, ProcessingMessage(11, msg_id))
    recv(s)

    send(s, PROCESSED, ProcessedMessage(msg_id))
    recv(s)

    content = open("/data/processed.bin","rb").read()
    assert msg_id.bytes in content
