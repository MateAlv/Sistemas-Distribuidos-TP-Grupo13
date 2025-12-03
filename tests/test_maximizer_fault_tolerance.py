import unittest
from unittest.mock import MagicMock, patch, call
import os
import sys
import shutil
import tempfile
import uuid
from collections import defaultdict

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from workers.maximizers.common.maximizer import Maximizer
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TransactionItemsProcessRow
from utils.file_utils.table_type import TableType
from utils.file_utils.file_table import DateTime
from utils.tolerance.persistence_service import PersistenceService
import datetime

class TestMaximizerFaultTolerance(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory for persistence
        self.test_dir = tempfile.mkdtemp()
        os.environ["PERSISTENCE_DIR"] = self.test_dir
        # Mock environment variables
        os.environ["AGGREGATOR_SHARDS"] = "1"
        
    def tearDown(self):
        # Remove temporary directory
        shutil.rmtree(self.test_dir)

    @patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue')
    @patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange')
    def test_ack_after_persist(self, mock_exchange, mock_queue):
        """
        Test that ACK happens AFTER persistence.
        This test simulates the flow where we verify that commit_processing_chunk 
        and commit_working_state are called BEFORE the callback returns (implicit ACK).
        """
        # Setup Maximizer
        maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        
        # Mock persistence service methods to track order
        maximizer.persistence = MagicMock(wraps=maximizer.persistence)
        
        # Create a dummy chunk
        header = ProcessChunkHeader(client_id=1, table_type=TableType.TRANSACTION_ITEMS)
        row = TransactionItemsProcessRow(
            transaction_id="tx1", item_id=1, quantity=10, subtotal=100.0, 
            created_at=DateTime(datetime.date(2024, 1, 1), datetime.time(10, 0))
        )
        chunk = ProcessChunk(header, [row])
        chunk_bytes = chunk.serialize()

        # Simulate callback execution
        # In the real implementation, the callback is executed by the middleware.
        # We want to verify that inside _handle_data_chunk (called by callback),
        # persistence happens.
        
        # Manually call _handle_data_chunk as if it was the callback logic
        maximizer._handle_data_chunk(chunk_bytes)
        
        # Verify call order
        # 1. commit_processing_chunk
        # 2. commit_working_state
        # 3. (Implicit ACK by returning)
        
        # Check if commit_processing_chunk was called
        maximizer.persistence.commit_processing_chunk.assert_called()
        
        # Check if commit_working_state was called
        maximizer.persistence.commit_working_state.assert_called()
        
        # Ensure processing commit happened before working state commit (logic flow)
        # This is guaranteed by the code structure, but good to verify if we could spy on timestamps.
        # For now, asserting they were called is a good start.

    @patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue')
    @patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange')
    def test_crash_after_apply_before_save(self, mock_exchange, mock_queue):
        """
        Test recovery when crashing after apply() but before _save_state().
        """
        maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        
        # Mock _save_state to simulate crash
        original_save_state = maximizer._save_state
        maximizer._save_state = MagicMock(side_effect=SystemExit("Simulated Crash"))
        
        # Create chunk
        header = ProcessChunkHeader(client_id=1, table_type=TableType.TRANSACTION_ITEMS)
        row = TransactionItemsProcessRow(
            transaction_id="tx1", item_id=1, quantity=10, subtotal=100.0, 
            created_at=DateTime(datetime.date(2024, 1, 1), datetime.time(10, 0))
        )
        chunk = ProcessChunk(header, [row])
        chunk_bytes = chunk.serialize()

        # Run and expect crash
        with self.assertRaises(SystemExit):
            maximizer._handle_data_chunk(chunk_bytes)
            
        # Verify that processing chunk WAS persisted (it happens at start of handle)
        # We need to check the file on disk
        persistence = PersistenceService(f"{self.test_dir}/maximizer_MAX_absolute")
        recovered_chunk = persistence.recover_last_processing_chunk()
        self.assertIsNotNone(recovered_chunk)
        self.assertEqual(recovered_chunk.message_id(), chunk.message_id())
        
        # Verify that state was NOT updated (because we crashed before save)
        # Ideally, we check the persisted state file.
        # Since we crashed before _save_state, the file should be empty or old.
        state = persistence.recover_working_state()
        # It should be None or empty since we started fresh
        self.assertIsNone(state)

        # Now restart (create new Maximizer) and recover
        # Restore _save_state
        Maximizer._save_state = original_save_state
        
        new_maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        # Run recovery loop manually (usually done in run())
        last_chunk = new_maximizer.persistence.recover_last_processing_chunk()
        if last_chunk:
            new_maximizer._handle_data_chunk(last_chunk.serialize())
            
        # Verify state is now consistent
        client_sellings = new_maximizer.working_state.get_sellings_max(1)
        self.assertTrue(len(client_sellings) > 0)
        # Key is (item_id, MonthYear)
        # We need to construct the key correctly to check
        # But just checking len > 0 proves it was processed.

    @patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue')
    @patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange')
    def test_crash_after_mark_end_before_publish(self, mock_exchange, mock_queue):
        """
        Test crash after marking client_end_processed but before publishing results.
        This exposes the 'Data Loss' bug identified.
        """
        maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        
        # Mock publish_absolute_max_results to crash
        maximizer.publish_absolute_max_results = MagicMock(side_effect=SystemExit("Crash before publish"))
        
        # Setup state: client has some data
        maximizer.working_state.get_sellings_max(1)[(1, 1)] = 10
        
        # Trigger end processing
        with self.assertRaises(SystemExit):
            maximizer.process_client_end(1, TableType.TRANSACTION_ITEMS)
            
        # Verify state is marked processed
        self.assertTrue(maximizer.working_state.is_client_end_processed(1))
        
        # Verify state is persisted (because mark_client_end_processed calls _save_state immediately)
        # Wait, in current code:
        # self.working_state.mark_client_end_processed(client_id)
        # self._save_state(uuid.uuid4())
        # ...
        # self.publish_absolute_max_results(client_id)  <-- CRASH HERE
        
        # So on disk, it is marked processed.
        
        # Restart
        new_maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        
        # Mock publish again to see if it's called
        new_maximizer.publish_absolute_max_results = MagicMock()
        
        # Trigger end processing (simulating redelivery of END message)
        new_maximizer.process_client_end(1, TableType.TRANSACTION_ITEMS)
        
        # ASSERT FAILURE: This is where the current code FAILS.
        # It sees "is_client_end_processed" and returns early.
        # So publish is NEVER called.
        new_maximizer.publish_absolute_max_results.assert_not_called()
        
        # Note: This test confirms the bug. If we fix it, this assertion should change to assert_called.

    @patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue')
    @patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange')
    def test_invalid_table_type_persistence(self, mock_exchange, mock_queue):
        """
        Test that invalid table type chunks do not cause infinite recovery loops.
        """
        maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        
        # Create chunk with WRONG table type (e.g. TPV for MAX maximizer)
        header = ProcessChunkHeader(client_id=1, table_type=TableType.TPV)
        chunk = ProcessChunk(header, [])
        chunk_bytes = chunk.serialize()
        
        # Handle chunk
        maximizer._handle_data_chunk(chunk_bytes)
        
        # Verify it was persisted as processing chunk
        # In current code, it commits processing chunk BEFORE validation.
        # Then it returns early.
        # It does NOT clear the processing commit.
        
        persistence = PersistenceService(f"{self.test_dir}/maximizer_MAX_absolute")
        bad_chunk = persistence.recover_last_processing_chunk()
        self.assertIsNotNone(bad_chunk)
        
        # This confirms the "Loop" bug. 
        # On restart, it will recover this chunk, fail validation, return, and leave it there.
        # Forever.

    @patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue')
    @patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange')
    def test_shard_tracking_persistence(self, mock_exchange, mock_queue):
        """
        Test that received_shards is persisted (or not).
        """
        maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        
        # Add a shard to tracking
        maximizer.received_shards[1].add("shard_1")
        
        # Save state (simulate processing a chunk)
        maximizer._save_state(uuid.uuid4())
        
        # Restart
        new_maximizer = Maximizer("MAX", "absolute", expected_inputs=1)
        
        # Check if shards are recovered
        # Current code: received_shards is NOT in MaximizerWorkingState.
        # So it will be empty.
        self.assertEqual(len(new_maximizer.received_shards[1]), 0)
        
        # This confirms the "Shard Tracking" bug.

if __name__ == '__main__':
    unittest.main()
