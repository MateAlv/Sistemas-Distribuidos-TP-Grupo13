import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workers.filter.common.filter import Filter
from utils.processing.process_chunk import ProcessChunk
from utils.file_utils.table_type import TableType

class TestFaultToleranceFilter(unittest.TestCase):
    def setUp(self):
        self.env_patcher = patch.dict(os.environ, {
            "CRASH_POINT": ""
        })
        self.env_patcher.start()
        
        self.middleware_queue_patcher = patch('workers.filter.common.filter.MessageMiddlewareQueue')
        self.MockQueue = self.middleware_queue_patcher.start()
        
        self.middleware_exchange_patcher = patch('workers.filter.common.filter.MessageMiddlewareExchange')
        self.MockExchange = self.middleware_exchange_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.middleware_queue_patcher.stop()
        self.middleware_exchange_patcher.stop()

    def test_crash_before_process(self):
        cfg = {"id": 1, "filter_type": "amount", "min_amount": 10}
        filter_worker = Filter(cfg)
        filter_worker.persistence = MagicMock()
        filter_worker.persistence.process_has_been_counted.return_value = False
        
        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_BEFORE_PROCESS"}):
            with self.assertRaises(SystemExit):
                dummy_msg = b"dummy_chunk"
                with patch('workers.filter.common.filter.ProcessBatchReader.from_bytes') as mock_reader:
                    mock_chunk = MagicMock()
                    mock_chunk.client_id.return_value = 1
                    mock_chunk.table_type.return_value = TableType.TRANSACTIONS
                    mock_chunk.rows = []
                    mock_reader.return_value = mock_chunk
                    
                    filter_worker._handle_data_chunk(dummy_msg)
                    
        # Since I haven't refactored yet, I will write the test assuming I will refactor `run` loop 
        # to call `_handle_data_chunk`.
        
    def test_crash_before_process_refactored(self):
        # This test assumes I will refactor Filter to have _handle_data_chunk
        cfg = {"id": 1, "filter_type": "amount", "min_amount": 10}
        filter_worker = Filter(cfg)
        filter_worker.persistence = MagicMock()
        filter_worker.persistence.process_has_been_counted.return_value = False
        
        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_BEFORE_PROCESS"}):
            with self.assertRaises(SystemExit):
                dummy_msg = b"dummy_chunk"
                # We need to mock the method that parses bytes, which is inside the loop currently.
                # I will move the parsing and processing to _handle_data_chunk
                filter_worker._handle_data_chunk(dummy_msg)

        filter_worker.persistence.commit_working_state.assert_not_called()

if __name__ == '__main__':
    unittest.main()
