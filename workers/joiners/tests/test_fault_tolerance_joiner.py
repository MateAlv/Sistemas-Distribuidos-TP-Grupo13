import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workers.joiners.common.joiner import Joiner, ITEMS_JOINER
from utils.processing.process_chunk import ProcessChunk
from utils.file_utils.table_type import TableType

class TestJoiner(Joiner):
    def define_queues(self):
        self.data_receiver = MagicMock()
        self.data_join_receiver = MagicMock()
        self.data_sender = MagicMock()
        
    def save_data_join_fields(self, row, client_id):
        pass
        
    def join_result(self, row, client_id):
        return row
        
    def publish_results(self, client_id=None):
        pass
        
    def send_end_query_msg(self, client_id):
        pass

class TestFaultToleranceJoiner(unittest.TestCase):
    def setUp(self):
        self.env_patcher = patch.dict(os.environ, {
            "CRASH_POINT": ""
        })
        self.env_patcher.start()
        
        self.threading_patcher = patch('workers.joiners.common.joiner.threading')
        self.MockThreading = self.threading_patcher.start()
        
        self.persistence_patcher = patch('workers.joiners.common.joiner.PersistenceService')
        self.MockPersistence = self.persistence_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.threading_patcher.stop()
        self.persistence_patcher.stop()

    def test_crash_before_process_main(self):
        joiner = TestJoiner(ITEMS_JOINER)
        joiner.persistence_main = MagicMock()
        joiner.working_state_main = MagicMock()
        joiner.working_state_main.is_processed.return_value = False
        
        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_BEFORE_PROCESS"}):
            with self.assertRaises(SystemExit):
                dummy_msg = b"dummy_chunk"
                with patch('workers.joiners.common.joiner.ProcessBatchReader.from_bytes') as mock_reader:
                    mock_chunk = MagicMock()
                    mock_chunk.client_id.return_value = 1
                    mock_chunk.table_type.return_value = TableType.TRANSACTIONS
                    mock_chunk.rows = []
                    mock_reader.return_value = mock_chunk
                    
                    joiner._handle_data_chunk(dummy_msg)
                    
        joiner.persistence_main.commit_working_state.assert_not_called()

    def test_idempotency_main(self):
        joiner = TestJoiner(ITEMS_JOINER)
        joiner.persistence_main = MagicMock()
        joiner.working_state_main = MagicMock()
        
        joiner.working_state_main.is_processed.return_value = True
        
        dummy_msg = b"dummy_chunk"
        with patch('workers.joiners.common.joiner.ProcessBatchReader.from_bytes') as mock_reader:
            mock_chunk = MagicMock()
            mock_chunk.message_id.return_value = b"1234"
            mock_reader.return_value = mock_chunk
            
            joiner._handle_data_chunk(dummy_msg)
            
        joiner.persistence_main.commit_processing_chunk.assert_not_called()
        joiner.persistence_main.commit_working_state.assert_not_called()

    def test_idempotency_join(self):
        joiner = TestJoiner(ITEMS_JOINER)
        joiner.persistence_join = MagicMock()
        joiner.working_state_join = MagicMock()
        
        joiner.working_state_join.is_processed.return_value = True
        
        dummy_msg = b"dummy_chunk"
        with patch('workers.joiners.common.joiner.ProcessBatchReader.from_bytes') as mock_reader:
            mock_chunk = MagicMock()
            mock_chunk.message_id.return_value = b"5678"
            mock_reader.return_value = mock_chunk
            
            joiner._handle_join_chunk_bytes(dummy_msg)
            
        joiner.persistence_join.commit_processing_chunk.assert_not_called()
        joiner.persistence_join.commit_working_state.assert_not_called()

    def test_normal_processing_main(self):
        joiner = TestJoiner(ITEMS_JOINER)
        joiner.persistence_main = MagicMock()
        joiner.working_state_main = MagicMock()
        joiner.working_state_main.is_processed.return_value = False
        joiner.working_state_main.to_bytes.return_value = b"pickled_state"
        
        dummy_msg = b"dummy_chunk"
        with patch('workers.joiners.common.joiner.ProcessBatchReader.from_bytes') as mock_reader:
            mock_chunk = MagicMock()
            mock_chunk.message_id.return_value = b"9999"
            mock_chunk.client_id.return_value = 1
            mock_chunk.table_type.return_value = TableType.TRANSACTIONS
            mock_chunk.rows = []
            mock_reader.return_value = mock_chunk
            
            joiner._handle_data_chunk(dummy_msg)
            
        joiner.persistence_main.commit_processing_chunk.assert_called_once_with(mock_chunk)
        joiner.persistence_main.commit_working_state.assert_called_once()
        joiner.working_state_main.add_processed_id.assert_called_with(b"9999")

if __name__ == '__main__':
    unittest.main()
