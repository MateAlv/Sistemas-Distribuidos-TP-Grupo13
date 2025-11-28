import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workers.maximizers.common.maximizer import Maximizer
from utils.processing.process_chunk import ProcessChunk
from utils.file_utils.table_type import TableType

class TestFaultToleranceMaximizer(unittest.TestCase):
    def setUp(self):
        self.env_patcher = patch.dict(os.environ, {
            "CRASH_POINT": ""
        })
        self.env_patcher.start()
        
        self.middleware_queue_patcher = patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue')
        self.MockQueue = self.middleware_queue_patcher.start()
        
        self.middleware_exchange_patcher = patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange')
        self.MockExchange = self.middleware_exchange_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.middleware_queue_patcher.stop()
        self.middleware_exchange_patcher.stop()

    def test_crash_before_process(self):
        max_worker = Maximizer("MAX", "absolute", None, ["shard1"])
        max_worker.persistence = MagicMock()
        max_worker.persistence.process_has_been_counted.return_value = False
        
        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_BEFORE_PROCESS"}):
            with self.assertRaises(SystemExit):
                dummy_msg = b"dummy_chunk"
                with patch('workers.maximizers.common.maximizer.ProcessBatchReader.from_bytes') as mock_reader:
                    mock_chunk = MagicMock()
                    mock_chunk.client_id.return_value = 1
                    mock_chunk.table_type.return_value = TableType.TRANSACTION_ITEMS
                    mock_chunk.rows = []
                    mock_reader.return_value = mock_chunk
                    
                    max_worker._handle_data_chunk(dummy_msg)
                    
        max_worker.persistence.commit_working_state.assert_not_called()

    def test_crash_after_process_before_commit(self):
        max_worker = Maximizer("MAX", "absolute", None, ["shard1"])
        max_worker.persistence = MagicMock()
        max_worker.persistence.process_has_been_counted.return_value = False
        
        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_AFTER_PROCESS_BEFORE_COMMIT"}):
            with self.assertRaises(SystemExit):
                dummy_msg = b"dummy_chunk"
                with patch('workers.maximizers.common.maximizer.ProcessBatchReader.from_bytes') as mock_reader:
                    mock_chunk = MagicMock()
                    mock_chunk.client_id.return_value = 1
                    mock_chunk.table_type.return_value = TableType.TRANSACTION_ITEMS
                    mock_chunk.rows = []
                    mock_reader.return_value = mock_chunk
                    
                    max_worker._handle_data_chunk(dummy_msg)

        max_worker.persistence.commit_working_state.assert_not_called()

if __name__ == '__main__':
    unittest.main()
