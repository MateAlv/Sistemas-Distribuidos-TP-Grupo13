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
        
        # We don't need to patch MessageMiddlewareQueue in joiner.py as it is not imported there
        
        # Mock threading to avoid starting threads
        self.threading_patcher = patch('workers.joiners.common.joiner.threading')
        self.MockThreading = self.threading_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.threading_patcher.stop()

    def test_crash_before_process(self):
        joiner = TestJoiner(ITEMS_JOINER)
        joiner.persistence = MagicMock()
        joiner.persistence.process_has_been_counted.return_value = False
        
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
                    
        joiner.persistence.commit_working_state.assert_not_called()

    def test_crash_after_process_before_commit(self):
        joiner = TestJoiner(ITEMS_JOINER)
        joiner.persistence = MagicMock()
        joiner.persistence.process_has_been_counted.return_value = False
        
        # Setup conditions to trigger processing
        joiner.is_ready_to_join_for_client = MagicMock(return_value=True)
        joiner.apply_for_client = MagicMock()
        
        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_AFTER_PROCESS_BEFORE_COMMIT"}):
            with self.assertRaises(SystemExit):
                dummy_msg = b"dummy_chunk"
                with patch('workers.joiners.common.joiner.ProcessBatchReader.from_bytes') as mock_reader:
                    mock_chunk = MagicMock()
                    mock_chunk.client_id.return_value = 1
                    mock_chunk.table_type.return_value = TableType.TRANSACTIONS
                    mock_chunk.rows = [MagicMock()]
                    mock_reader.return_value = mock_chunk
                    
                    joiner._handle_data_chunk(dummy_msg)

        joiner.persistence.commit_working_state.assert_not_called()

if __name__ == '__main__':
    unittest.main()
