import datetime
import uuid

from utils.file_utils.file_table import DateTime, TransactionsFileRow, TransactionsItemsFileRow
from utils.file_utils.table_type import TableType
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TransactionsProcessRow, TransactionItemsProcessRow
from utils.tolerance.persistence_service import PersistenceService

def test_empty_persistence_service():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)
    assert service.recover_from_shutdown() is None
    assert service.recover_working_state_data() is None

def test_processing_steps():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])
    serialized = chunk.serialize()

    chunk_deserialized = ProcessChunk.deserialize(
        ProcessChunkHeader.deserialize(serialized[:ProcessChunkHeader.HEADER_SIZE]),
        serialized[ProcessChunkHeader.HEADER_SIZE:]
    )

    # Se commitea el chunk procesado
    service.commit_processing_chunk(chunk_deserialized)
    # Se recupera el chunk luego de un shutdown
    recovered_chunk = service.recover_from_shutdown()
    # Se verifica que los datos sean correctos
    assert recovered_chunk is not None
    assert len(recovered_chunk.rows) == 1
    assert recovered_chunk.rows[0].store_id == 1
    assert recovered_chunk.rows[0].transaction_id == "tx1"
    assert recovered_chunk.rows[0].user_id == 4
    assert recovered_chunk.rows[0].final_amount == 100
    assert recovered_chunk.rows[0].created_at.date == datetime.date(2023, 5, 1)
    assert recovered_chunk.rows[0].created_at.time == datetime.time(0, 0)
    assert str(recovered_chunk.rows[0].year_half_created_at) == "2023-H1"

    # Se limpia el commit de procesamiento
    service.clean_processing_commit()
    # Se verifica que no haya datos para recuperar
    service = PersistenceService(directory_path)
    assert service.recover_from_shutdown() is None

    # Llega otro chunk de otra tabla
    date2 = DateTime(datetime.date(2022, 3, 21), datetime.time(20, 20))
    row2 = TransactionsItemsFileRow("txi1", 1, 2, 50, 60, date2)
    process_row2 = TransactionItemsProcessRow.from_file_row(row2)
    header2 = ProcessChunkHeader(888, TableType.TRANSACTION_ITEMS)
    chunk2 = ProcessChunk(header2, [process_row2])

    serialized2 = chunk2.serialize()

    chunk2_deserialized = ProcessChunk.deserialize(
        ProcessChunkHeader.deserialize(serialized2[:ProcessChunkHeader.HEADER_SIZE]),
        serialized2[ProcessChunkHeader.HEADER_SIZE:]
    )

    # Se commitea el chunk procesado
    service.commit_processing_chunk(chunk2_deserialized)
    # Se recupera el chunk luego de un shutdown
    service = PersistenceService(directory_path)
    recovered_chunk2 = service.recover_from_shutdown()
    # Se verifica que los datos sean correctos
    assert recovered_chunk2 is not None
    assert len(recovered_chunk2.rows) == 1
    assert recovered_chunk2.rows[0].transaction_id == "txi1"
    assert recovered_chunk2.rows[0].item_id == 1
    assert recovered_chunk2.rows[0].quantity == 2
    assert recovered_chunk2.rows[0].subtotal == 60
    assert recovered_chunk2.rows[0].created_at.date == datetime.date(2022, 3, 21)
    assert recovered_chunk2.rows[0].created_at.time == datetime.time(20, 20)
    service.clean_persisted_data()

def test_working_state_data():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])

    data = b"working state data example"
    # Se commitea
    print("UUID commited: ", chunk.message_id())

    service.commit_working_state(data, chunk.message_id())
    # Se cae el servicio y se recupera
    service = PersistenceService(directory_path)
    service.recover_from_shutdown()
    # Se verifica que los datos sean correctos
    recovered_data = service.recover_working_state_data()
    print ("UUID recovered: ", service.working_state.last_processed_id)
    assert recovered_data == data
    assert service.process_has_been_counted(chunk.message_id())
    assert not service.process_has_been_counted(uuid.uuid4())

    data = b"updated working state data"
    new_uuid = uuid.uuid4()
    # Se commitea nuevo estado
    service.commit_working_state(data, new_uuid)

    # Se cae el servicio y se recupera
    service = PersistenceService(directory_path)
    service.recover_from_shutdown()
    # Se verifica que los datos sean correctos
    recovered_data = service.recover_working_state_data()
    assert recovered_data == data
    assert service.process_has_been_counted(new_uuid)
    assert not service.process_has_been_counted(uuid.uuid4())
    service.clean_persisted_data()


def test_single_send_commit():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])

    message_id = chunk.message_id()
    client_id = chunk.client_id()

    # Se commitea el envío
    service.commit_send_ack(client_id, message_id)

    # Se cae el servicio y se recupera
    service = PersistenceService(directory_path)
    service.recover_from_shutdown()

    assert service.send_has_been_acknowledged(client_id, message_id)
    service.clean_persisted_data()


def test_multiple_send_commits():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])

    message_id1 = chunk.message_id()
    client_id1 = chunk.client_id()

    # Se commitea el primer envío
    service.commit_send_ack(client_id1, message_id1)

    date2 = DateTime(datetime.date(2022, 3, 21), datetime.time(20, 20))
    row2 = TransactionsFileRow("tx2", 10, 20, 30, 40, 200, 0, 200, date2)
    process_row2 = TransactionsProcessRow.from_file_row(row2)

    header2 = ProcessChunkHeader(888, TableType.TRANSACTIONS)
    chunk2 = ProcessChunk(header2, [process_row2])

    message_id2 = chunk2.message_id()
    client_id2 = chunk2.client_id()

    # Se commitea el segundo envío
    service.commit_send_ack(client_id2, message_id2)

    # Se cae el servicio y se recupera
    service = PersistenceService(directory_path)
    service.recover_from_shutdown()

    assert service.send_has_been_acknowledged(client_id1, message_id1)
    assert service.send_has_been_acknowledged(client_id2, message_id2)
    service.clean_persisted_data()


def test_multiple_send_commits_same_client():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])

    message_id1 = chunk.message_id()
    client_id = chunk.client_id()

    # Se commitea el primer envío
    service.commit_send_ack(client_id, message_id1)

    date2 = DateTime(datetime.date(2022, 3, 21), datetime.time(20, 20))
    row2 = TransactionsFileRow("tx2", 10, 20, 30, 40, 200, 0, 200, date2)
    process_row2 = TransactionsProcessRow.from_file_row(row2)

    header2 = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk2 = ProcessChunk(header2, [process_row2])

    message_id2 = chunk2.message_id()

    # Se commitea el segundo envío
    service.commit_send_ack(client_id, message_id2)

    # Se cae el servicio y se recupera
    service = PersistenceService(directory_path)
    service.recover_from_shutdown()

    assert service.send_has_been_acknowledged(client_id, message_id1)
    assert service.send_has_been_acknowledged(client_id, message_id2)
    service.clean_persisted_data()

def test_remove_client_data_on_finish():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])

    message_id = chunk.message_id()
    client_id = chunk.client_id()

    # Se commitea el envío
    service.commit_send_ack(client_id, message_id)

    # Se elimina la data del cliente al finalizar
    service.remove_client_data(client_id)

    # Se cae el servicio y se recupera
    service = PersistenceService(directory_path)
    service.recover_from_shutdown()

    assert not service.send_has_been_acknowledged(client_id, message_id)
    service.clean_persisted_data()

def test_clean_persisted_data_without_data():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)
    service.clean_persisted_data()
    # Se verifica que no haya datos para recuperar
    service = PersistenceService(directory_path)
    assert service.recover_from_shutdown() is None
    assert service.recover_working_state_data() is None
    service.clean_persisted_data()

def test_complete_steps():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])

    serialized = chunk.serialize()

    chunk_deserialized = ProcessChunk.deserialize(
        ProcessChunkHeader.deserialize(serialized[:ProcessChunkHeader.HEADER_SIZE]),
        serialized[ProcessChunkHeader.HEADER_SIZE:]
    )

    # Se commitea el chunk procesado
    service.commit_processing_chunk(chunk_deserialized)
    # Se commitea el working state
    data = b"working state data example"
    service.commit_working_state(data, chunk.message_id())
    # Se commitea el envío
    service.commit_send_ack(chunk.client_id(), chunk.message_id())

    # Se apaga y recupera el servicio
    service = PersistenceService(directory_path)
    recovered_chunk = service.recover_from_shutdown()
    recovered_data = service.recover_working_state_data()

    # Se verifica que los datos sean correctos
    assert recovered_chunk is not None
    assert len(recovered_chunk.rows) == 1
    assert recovered_chunk.rows[0].store_id == 1
    assert recovered_chunk.rows[0].transaction_id == "tx1"
    assert recovered_chunk.rows[0].user_id == 4
    assert recovered_chunk.rows[0].final_amount == 100
    assert recovered_chunk.rows[0].created_at.date == datetime.date(2023, 5, 1)
    assert recovered_chunk.rows[0].created_at.time == datetime.time(0, 0)
    assert str(recovered_chunk.rows[0].year_half_created_at) == "2023-H1"

    assert recovered_data == data
    assert service.send_has_been_acknowledged(chunk.client_id(), chunk.message_id())

    assert service.process_has_been_counted(recovered_chunk.message_id())
    
    service.clean_persisted_data()


def test_cleaning():
    directory_path = "/tmp"
    service = PersistenceService(directory_path)

    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    process_row = TransactionsProcessRow.from_file_row(row)

    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    chunk = ProcessChunk(header, [process_row])

    # Se commitea el chunk procesado
    service.commit_processing_chunk(chunk)
    # Se commitea el working state
    data = b"working state data example"
    service.commit_working_state(data, chunk.message_id())
    # Se commitea el envío
    service.commit_send_ack(chunk.client_id(), chunk.message_id())

    # Se limpia toda la data persistida
    service.clean_persisted_data()

    # Se verifica que no haya datos para recuperar
    service = PersistenceService(directory_path)
    assert service.recover_from_shutdown() is None
    assert service.recover_working_state_data() is None
