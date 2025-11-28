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

        self.persistence_patcher = patch('workers.filter.common.filter.PersistenceService')
        self.MockPersistence = self.persistence_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.middleware_queue_patcher.stop()
        self.middleware_exchange_patcher.stop()
        self.persistence_patcher.stop()

    def test_idempotency_on_recovery(self):
        """
        Test that if a message was already processed (in processed_ids), it is skipped.
        """
        cfg = {"id": 1, "filter_type": "amount", "min_amount": 10}
        filter_worker = Filter(cfg)
        # Mock persistence service
        filter_worker.persistence_service = self.MockPersistence.return_value
        
        # Simulate message already processed
        filter_worker.working_state.processed_ids.add(b"1234")
        
        dummy_msg = b"dummy_chunk"
        with patch('workers.filter.common.filter.ProcessBatchReader.from_bytes') as mock_reader:
            mock_chunk = MagicMock()
            mock_chunk.message_id.return_value = b"1234"
            mock_reader.return_value = mock_chunk
            
            filter_worker._handle_process_message(dummy_msg)
            
        # Should not commit processing chunk or working state again
        filter_worker.persistence_service.commit_processing_chunk.assert_not_called()
        filter_worker.persistence_service.commit_working_state.assert_not_called()

    def test_normal_processing_commits_state(self):
        """
        Test that normal processing commits chunk and state.
        """
        cfg = {"id": 1, "filter_type": "amount", "min_amount": 10}
        filter_worker = Filter(cfg)
        filter_worker.persistence_service = self.MockPersistence.return_value
        
        dummy_msg = b"dummy_chunk"
        with patch('workers.filter.common.filter.ProcessBatchReader.from_bytes') as mock_reader:
            mock_chunk = MagicMock()
            mock_chunk.message_id.return_value = b"5678"
            mock_chunk.client_id.return_value = 1
            mock_chunk.table_type.return_value = TableType.TRANSACTIONS
            mock_chunk.rows = []
            mock_reader.return_value = mock_chunk
            
            # Mock process_chunk to return True (sent)
            filter_worker.process_chunk = MagicMock(return_value=True)
            
            filter_worker._handle_process_message(dummy_msg)
            
        # Should commit processing chunk
        filter_worker.persistence_service.commit_processing_chunk.assert_called_once_with(mock_chunk)
        # Should commit working state
        filter_worker.persistence_service.commit_working_state.assert_called_once()
        # Should add to processed_ids
        self.assertIn(b"5678", filter_worker.working_state.processed_ids)

if __name__ == '__main__':
    unittest.main()
