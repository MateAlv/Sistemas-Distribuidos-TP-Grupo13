import logging
import os
import uuid
import tempfile

from utils.processing.process_batch_reader import ProcessBatchReader
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader

STATE_COMMIT_FILE = "persistence_state"
SEND_COMMIT_FILE = "persistence_send_commit"
PROCESSING_DATA_COMMIT_FILE = "persistence_processing_commit"

def ensure_directory_exists(directory: str):
    if not os.path.exists(directory):
        os.makedirs(directory)

def atomic_file_upsert(file_path: str, data: bytes):
    """Writes data to a file atomically using a temporary file."""
    temp_dir = "./"

    # Use a temporary file in the same directory as the target file to ensure atomicity
    with tempfile.NamedTemporaryFile("wb", dir=temp_dir, delete=False) as temp_file:
        temp_file.write(data)
        temp_file.flush()
        os.fsync(temp_file.fileno())
        temp_file_path = temp_file.name

    # Move the temporary file to the target location atomically
    os.replace(temp_file_path, file_path)


def atomic_file_append(file_path: str, data: bytes):
    """Appends data to a file atomically using a temporary file."""
    with open(file_path, "rb") as original_file:
        original_data = original_file.read()

    total_data = original_data + data
    atomic_file_upsert(file_path, total_data)

class StateCommit:

    def __init__(self, state_data: bytes, last_processed_id: uuid.UUID):
        self.last_processed_id = last_processed_id
        self.state_data = state_data

    def serialize(self) -> bytes:
        return self.last_processed_id.bytes + self.state_data

    @staticmethod
    def deserialize(data: bytes):
        last_processed_id = uuid.UUID(bytes=data[0:16])
        state_data = data[16:]
        return StateCommit(state_data, last_processed_id)

    def has_been_processed(self, message_id: uuid.UUID) -> bool:
        return self.last_processed_id == message_id

class SendAckCommit:

    COMMIT_SIZE = 16 + 4  # UUID (16 bytes) + client_id (4 bytes)

    def __init__(self, last_sent_id: uuid.UUID, client_id: int):
        self.last_sent_id = last_sent_id
        self.client_id = client_id

    def serialize(self) -> bytes:
        return self.last_sent_id.bytes + self.client_id.to_bytes(4, byteorder="big")

    @staticmethod
    def deserialize(data: bytes):
        last_sent_id = uuid.UUID(bytes=data[0:16])
        client_id = int.from_bytes(data[16:20], byteorder="big")
        return SendAckCommit(last_sent_id, client_id)


class PersistenceService:

    @staticmethod
    def _parse_send_commits(file_data: bytes):
        """Parses the binary data from the send commit file into a list of SendAckCommit objects."""
        commits = []
        data_size = SendAckCommit.COMMIT_SIZE
        offset = 0
        while offset + data_size <= len(file_data):
            commit_data = file_data[offset:offset + data_size]
            commit = SendAckCommit.deserialize(commit_data)
            offset += data_size
            commits.append(commit)

        return commits

    def _recover_send_commits(self):
        """Recovers the send commits from the commit file."""
        logging.info("Recovering sends commit...")
        messages_sent_by_user = {}

        try:
            with open(self.send_commit_path, "rb") as f:
                file_data = f.read()
                send_commits = self._parse_send_commits(file_data)
                for commit in send_commits:
                    messages_sent_by_user.setdefault(commit.client_id, []).append(commit.last_sent_id)
                return messages_sent_by_user

        except FileNotFoundError:
            logging.warning("Send commit file `%s` not found. Creating a new one.", SEND_COMMIT_FILE)
            try:
                with open(self.send_commit_path, "wb") as f:
                    f.write(b"")
                return {}
            except Exception:
                logging.exception("Failed to create send commit file `%s`.", SEND_COMMIT_FILE)
                raise
        except Exception:
            logging.exception("Failed to read send commit file `%s`.", SEND_COMMIT_FILE)
            raise

    def _recover_processing_chunk_commit(self):
        """Recovers the last processing chunk commit from the commit file."""
        logging.info("Recovering last chunks processing commit...")
        try:
            with open(self.processing_data_commit_path, "rb") as f:
                file_data = f.read()
                if len(file_data) == 0:
                    logging.debug("No processing chunk commit found.")
                    return None
                chunk = ProcessBatchReader.from_bytes(file_data)
                return chunk

        except FileNotFoundError:
            logging.warning("Processing data commit file `%s` not found. Creating a new one.", PROCESSING_DATA_COMMIT_FILE)
            try:
                with open(self.processing_data_commit_path, "wb") as f:
                    f.write(b"")
                return None
            except Exception:
                logging.exception("Failed to create processing data commit file `%s`.", PROCESSING_DATA_COMMIT_FILE)
                raise
        except Exception:
            logging.exception("Failed to read processing data commit file `%s`.", PROCESSING_DATA_COMMIT_FILE)
            raise

    def _recover_working_state_commit(self) -> StateCommit | None:
        """Recovers the working state from the state commit file."""
        logging.info("Recovering working state...")
        try:
            with open(self.state_commit_path, "rb") as f:
                state_data = f.read()
                if not state_data:
                    logging.debug("No working state commit found.")
                    return None
                state_commit = StateCommit.deserialize(state_data)
                return state_commit
        except FileNotFoundError:
            logging.warning("State commit file `%s` not found. Creating a new one.", STATE_COMMIT_FILE)
            try:
                with open(self.state_commit_path, "wb") as f:
                    f.write(b"")
                return None
            except Exception:
                logging.exception("Failed to create state commit file `%s`.", STATE_COMMIT_FILE)
                raise
        except Exception:
            logging.exception("Failed to read state commit file `%s`.", STATE_COMMIT_FILE)
            raise

    def _clean_processing_commit(self):
        """"Cleans the processing data commit file when the processing chunk has been sent."""
        try:
            atomic_file_upsert(self.processing_data_commit_path, b"")
        except Exception:
            logging.exception("Failed to clean processing data commit file `%s`.", PROCESSING_DATA_COMMIT_FILE)
            raise


    def __init__(self, directory: str = "./"):
        # path directories
        ensure_directory_exists(directory)

        self.state_commit_path = os.path.join(directory, STATE_COMMIT_FILE)
        self.processing_data_commit_path = os.path.join(directory, PROCESSING_DATA_COMMIT_FILE)
        self.send_commit_path = os.path.join(directory, SEND_COMMIT_FILE)
        # recover data
        self.messages_sent_by_user = self._recover_send_commits()
        self.working_state = self._recover_working_state_commit()
        pass

    def recover_last_processing_chunk(self):
        """Recovers the last processing chunk commit from the commit file, if it was not sent yet."""
        processing_chunk = self._recover_processing_chunk_commit()

        if not processing_chunk:
            logging.debug("No processing chunk to recover.")
            return None

        if processing_chunk.client_id() not in self.messages_sent_by_user:
            logging.debug("The last processing chunk was not sent. Reprocessing.")
            return processing_chunk

        if self.send_has_been_acknowledged(processing_chunk.client_id(), processing_chunk.message_id()):
            logging.debug("The last processing chunk was already sent. No need to reprocess.")
            self._clean_processing_commit()
            return None

        return processing_chunk

    def recover_working_state(self) -> bytes | None:
        """Recovers the working state data from the state commit file."""
        return self.working_state.state_data if self.working_state else None

    def process_has_been_counted(self, message_id: uuid.UUID) -> bool:
        """Checks if a message_id has been counted in working state."""
        if not self.working_state:
            return False
        return self.working_state.has_been_processed(message_id)

    def commit_processing_chunk(self, chunk: ProcessChunk):
        """Commits the processing chunk to the commit file."""
        try:
            atomic_file_upsert(self.processing_data_commit_path, chunk.serialize())
        except Exception:
            logging.exception("Failed to commit processing chunk for client_id `%s`.", chunk.client_id())
            raise

    def commit_working_state(self, state_data: bytes, last_processed_id: uuid.UUID):
        """Commits the working state to the state commit file."""
        try:
            state_commit = StateCommit(state_data, last_processed_id)
            atomic_file_upsert(self.state_commit_path, state_commit.serialize())
        except Exception:
            logging.exception("Failed to commit working state.")
            raise

    def commit_send_ack(self, client_id: int, last_sent_id: uuid.UUID):
        """Commits the send ack for a given client_id and last_sent_id."""
        self.messages_sent_by_user[client_id] = last_sent_id
        try:
            commit = SendAckCommit(last_sent_id, client_id)
            atomic_file_append(self.send_commit_path, commit.serialize())

        except Exception:
            logging.exception("Failed to commit send ack for client_id `%s`.", client_id)
            raise

    def send_has_been_acknowledged(self, client_id: int, message_id: uuid.UUID) -> bool:
        """Checks if a send ack has been acknowledged for a given client_id and message_id."""
        if client_id not in self.messages_sent_by_user:
            return False
        return message_id in self.messages_sent_by_user[client_id]

    def remove_client_data(self, client_id: int):
        """Removes all send acks for a given client_id."""
        if client_id in self.messages_sent_by_user:
            del self.messages_sent_by_user[client_id]
        try:
            commits = b""
            for cid, last_sent_id in self.messages_sent_by_user.items():
                c = SendAckCommit(last_sent_id, cid)
                commits += c.serialize()
            atomic_file_upsert(self.send_commit_path, commits)
        except Exception:
            logging.exception("Failed to remove ack for client_id `%s`.", client_id)
            raise

    def clean_persisted_data(self):
        """Shuts down the service."""
        logging.info("Shutting down Persistence Service.")
        try:
            if os.path.exists(self.send_commit_path):
                os.remove(self.send_commit_path)
            if os.path.exists(self.processing_data_commit_path):
                os.remove(self.processing_data_commit_path)
            if os.path.exists(self.state_commit_path):
                os.remove(self.state_commit_path)
        except Exception:
            logging.exception("Failed to clean up files during shutdown.")
            raise

        logging.info("Persistence Service shut down completed.")