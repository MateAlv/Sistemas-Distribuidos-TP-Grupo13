import unittest
from unittest.mock import MagicMock, patch, call
import sys
import os
import shutil
from collections import deque

# Add root to path to allow imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workers.aggregators.common.aggregator import Aggregator
from utils.processing.process_chunk import ProcessChunk
from utils.file_utils.table_type import TableType

class TestFaultToleranceAggregator(unittest.TestCase):
    def setUp(self):
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            "MAX_SHARDS": "shard1:1",
            "TOP3_SHARDS": "shard2:2",
            "CRASH_POINT": "" # Default no crash
        })
        self.env_patcher.start()
        
        # Mock middleware
        self.middleware_queue_patcher = patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
        self.MockQueue = self.middleware_queue_patcher.start()
        
        self.middleware_exchange_patcher = patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
        self.MockExchange = self.middleware_exchange_patcher.start()

        self.persistence_patcher = patch('workers.aggregators.common.aggregator.PersistenceService')
        self.MockPersistence = self.persistence_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.middleware_queue_patcher.stop()
        self.middleware_exchange_patcher.stop()
        self.persistence_patcher.stop()

    def test_crash_before_process(self):
        """
        Test that if the worker crashes before processing a message,
        it will re-process it upon restart (simulated by re-delivery).
        """
        # Setup Aggregator
        agg = Aggregator("PRODUCTS", 1)
        
        # Mock persistence service (to be implemented)
        # Mock persistence service (already patched class, but we need to configure the instance)
        agg.persistence = self.MockPersistence.return_value
        agg.persistence.recover_working_state.return_value = None
        agg.persistence.recover_last_processing_chunk.return_value = None
        
        # processed_ids is empty by default
        agg.processed_ids = set()
        
        # Simulate crash
        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_BEFORE_PROCESS"}):
            with self.assertRaises(SystemExit):
                # Trigger processing
                # We need to manually call the handler because run() is an infinite loop
                # Construct a dummy message
                dummy_msg = b"dummy_chunk"
                with patch('workers.aggregators.common.aggregator.ProcessBatchReader.from_bytes') as mock_reader:
                    mock_chunk = MagicMock()
                    mock_chunk.client_id.return_value = 1
                    mock_chunk.table_type.return_value = TableType.TRANSACTION_ITEMS
                    mock_chunk.rows = []
                    mock_reader.return_value = mock_chunk
                    
                    agg._handle_data_chunk(dummy_msg)

        # Verify state was NOT committed
        agg.persistence.commit_working_state.assert_not_called()

    def test_crash_after_process_before_commit(self):
        """
        Test crash after processing but before committing state.
        Should re-process.
        """
        agg = Aggregator("PRODUCTS", 1)
        agg.persistence = self.MockPersistence.return_value
        agg.processed_ids = set()

        with patch.dict(os.environ, {"CRASH_POINT": "CRASH_AFTER_PROCESS_BEFORE_COMMIT"}):
            with self.assertRaises(SystemExit):
                 dummy_msg = b"dummy_chunk"
                 with patch('workers.aggregators.common.aggregator.ProcessBatchReader.from_bytes') as mock_reader:
                    mock_chunk = MagicMock()
                    mock_chunk.client_id.return_value = 1
                    mock_chunk.table_type.return_value = TableType.TRANSACTION_ITEMS
                    mock_chunk.rows = [MagicMock()] # Provide a row so has_output becomes True
                    # We also need to mock apply_products to return something
                    agg.apply_products = MagicMock(return_value=[MagicMock()])
                    agg.accumulate_products = MagicMock()
                    agg._build_products_payload = MagicMock(return_value={"products": []})
                    
                    mock_reader.return_value = mock_chunk
                    
                    agg._handle_data_chunk(dummy_msg)
        
        # Verify state commit was attempted (or not, depending on where exactly we crash)
        # If we crash BEFORE commit, it should not be called.
        agg.persistence.commit_working_state.assert_not_called()

    def test_idempotency_on_recovery(self):
        """
        Test that if a message was already processed (committed), it is skipped.
        """
        agg = Aggregator("PRODUCTS", 1)
        agg.persistence = self.MockPersistence.return_value
        
        # Simulate that the message was already counted
        agg.processed_ids = {b"1234"}
        
        dummy_msg = b"dummy_chunk"
        with patch('workers.aggregators.common.aggregator.ProcessBatchReader.from_bytes') as mock_reader:
            mock_chunk = MagicMock()
            mock_chunk.message_id.return_value = b"1234"
            mock_reader.return_value = mock_chunk
            
            agg._handle_data_chunk(dummy_msg)
            
        # Should NOT update state again
        # (Assuming _handle_data_chunk checks process_has_been_counted)
        # Since implementation isn't there, this test expects the call to be skipped
        # We can't verify internal state update easily without real implementation, 
        # but we can verify commit_working_state wasn't called for a new update
        agg.persistence.commit_working_state.assert_not_called()

if __name__ == '__main__':
    unittest.main()
