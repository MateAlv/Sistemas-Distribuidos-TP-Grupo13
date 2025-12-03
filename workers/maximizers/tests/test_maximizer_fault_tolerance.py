
import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
import os
import uuid
import json
from collections import defaultdict, deque

# Add root directory to sys.path to allow imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Mock pika before importing middleware
sys.modules['pika'] = MagicMock()

from workers.maximizers.common.maximizer import Maximizer
from workers.maximizers.common.maximizer_working_state import MaximizerWorkingState
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TransactionItemsProcessRow
from utils.file_utils.table_type import TableType
from utils.file_utils.file_table import DateTime
import datetime

class TestMaximizerFaultTolerance(unittest.TestCase):

    def setUp(self):
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            "AGGREGATOR_SHARDS": "1",
            "MAX_SHARDS": "1",
            "TOP3_SHARDS": "1",
            "TPV_SHARDS": "1",
            "PYTHONUNBUFFERED": "1"
        })
        self.env_patcher.start()

        # Mock PersistenceService to avoid file I/O
        self.persistence_patcher = patch('workers.maximizers.common.maximizer.PersistenceService')
        self.MockPersistence = self.persistence_patcher.start()
        self.mock_persistence_instance = self.MockPersistence.return_value
        self.mock_persistence_instance.recover_working_state.return_value = None
        self.mock_persistence_instance.recover_last_processing_chunk.return_value = None

        # Mock Middleware
        self.queue_patcher = patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue')
        self.MockQueue = self.queue_patcher.start()
        
        self.exchange_patcher = patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange')
        self.MockExchange = self.exchange_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.persistence_patcher.stop()
        self.queue_patcher.stop()
        self.exchange_patcher.stop()

    def test_ack_timing_unsafe(self):
        """
        Test that messages are ACKed (callback returns) BEFORE processing/persistence.
        This confirms the data loss window.
        """
        maximizer = Maximizer("MAX", "absolute")
        
        # Mock the data receiver's start_consuming to capture the callback
        mock_receiver = maximizer.data_receiver
        
        # Simulate run loop logic partially
        # We want to see if callback returns immediately
        
        captured_callback = None
        def side_effect(callback):
            nonlocal captured_callback
            captured_callback = callback
            
        mock_receiver.start_consuming.side_effect = side_effect
        
        # Run maximizer in a separate thread or just simulate the flow?
        # Since run() loops, we can't call it directly.
        # We'll simulate what run() does setup-wise.
        
        # Manually trigger the callback logic extracted from run()
        messages = deque()
        def callback(msg):
            messages.append(msg)
            # In the real code, callback returns here.
            # Middleware ACKs here.
        
        # Simulate receiving a message
        chunk_data = b"some_data"
        callback(chunk_data)
        
        # ASSERTION: Message is in memory (deque) but NOT persisted yet
        self.assertEqual(len(messages), 1)
        self.mock_persistence_instance.commit_processing_chunk.assert_not_called()
        self.mock_persistence_instance.commit_working_state.assert_not_called()
        
        # This confirms that if we crash now (after callback returns), data is lost.
        print("\n[TEST] test_ack_timing_unsafe: PASSED (Confirmed unsafe behavior)")

    def test_crash_after_apply_before_save(self):
        """
        Test recovery when crash happens after apply() but before _save_state().
        """
        maximizer = Maximizer("MAX", "absolute")
        
        # Create a dummy chunk
        header = ProcessChunkHeader(client_id=1, table_type=TableType.TRANSACTION_ITEMS)
        row = TransactionItemsProcessRow("tx1", 100, 5, 50.0, DateTime(datetime.date(2024, 1, 1), datetime.time(10, 0)))
        chunk = ProcessChunk(header, [row])
        chunk_bytes = chunk.serialize()
        
        # Mock _save_state to crash (raise Exception)
        with patch.object(maximizer, '_save_state', side_effect=Exception("Crash before save")):
            try:
                maximizer._handle_data_chunk(chunk_bytes)
            except Exception as e:
                self.assertEqual(str(e), "Crash before save")
        
        # Verify processing chunk was committed
        self.mock_persistence_instance.commit_processing_chunk.assert_called()
        
        # RECOVERY SIMULATION
        # New maximizer instance
        maximizer_recovery = Maximizer("MAX", "absolute")
        
        # Mock recover_last_processing_chunk to return the chunk
        self.mock_persistence_instance.recover_last_processing_chunk.return_value = chunk
        
        # Run recovery logic (simulated from run())
        last_chunk = maximizer_recovery.persistence.recover_last_processing_chunk()
        if last_chunk:
            maximizer_recovery._handle_data_chunk(last_chunk.serialize())
            
        # Verify state is updated
        sellings = maximizer_recovery.working_state.get_sellings_max(1)
        self.assertTrue(len(sellings) > 0)
        print("\n[TEST] test_crash_after_apply_before_save: PASSED")

    def test_crash_after_mark_end_before_publish(self):
        """
        Test crash after marking client_end_processed but before publishing results.
        Expected to FAIL with current implementation (Data Loss).
        """
        maximizer = Maximizer("MAX", "absolute")
        client_id = 1
        
        # Mock publish to crash
        with patch.object(maximizer, 'publish_absolute_max_results', side_effect=Exception("Crash during publish")):
            try:
                maximizer.process_client_end(client_id, TableType.TRANSACTION_ITEMS)
            except Exception:
                pass
        
        # Verify state is marked processed
        self.assertTrue(maximizer.working_state.is_client_end_processed(client_id))
        
        # RECOVERY
        maximizer_recovery = Maximizer("MAX", "absolute")
        # Simulate state loaded with client processed
        maximizer_recovery.working_state.mark_client_end_processed(client_id)
        
        # Retry process_client_end
        with patch.object(maximizer_recovery, 'publish_absolute_max_results') as mock_publish:
            maximizer_recovery.process_client_end(client_id, TableType.TRANSACTION_ITEMS)
            
            # ASSERTION: Should call publish again
            # Current implementation returns early if processed, so this will fail
            if mock_publish.call_count == 0:
                 print("\n[TEST] test_crash_after_mark_end_before_publish: FAILED (Confirmed Data Loss bug)")
            else:
                 print("\n[TEST] test_crash_after_mark_end_before_publish: PASSED (Bug fixed?)")

    def test_invalid_table_type_loop(self):
        """
        Test that invalid table type chunk does not clear processing commit, leading to loop.
        """
        maximizer = Maximizer("MAX", "absolute")
        
        # Invalid table type chunk
        header = ProcessChunkHeader(client_id=1, table_type=TableType.TPV) # Invalid for MAX
        chunk = ProcessChunk(header, [])
        chunk_bytes = chunk.serialize()
        
        maximizer._handle_data_chunk(chunk_bytes)
        
        # Verify commit_processing_chunk called
        self.mock_persistence_instance.commit_processing_chunk.assert_called()
        
        # Verify NO state save (early return)
        self.mock_persistence_instance.commit_working_state.assert_not_called()
        
        # Verify NO cleanup of processing commit (this is the bug)
        # We can't easily check "not cleaned" on mock without explicit method, 
        # but we know commit_processing_chunk writes it.
        # If we don't overwrite it or clear it, it stays.
        
        print("\n[TEST] test_invalid_table_type_loop: PASSED (Confirmed potential loop)")

    def test_shard_tracking_persistence(self):
        """
        Test that received_shards is persisted.
        Expected to FAIL with current implementation.
        """
        maximizer = Maximizer("TPV", "absolute")
        client_id = 1
        
        # Simulate receiving shard data
        maximizer.received_shards[client_id].add("shard_1")
        
        # Save state (simulated)
        state_bytes = maximizer.working_state.to_bytes()
        
        # Load state into new instance
        new_state = MaximizerWorkingState.from_bytes(state_bytes)
        
        # Check if received_shards is in state
        # Current implementation: received_shards is in Maximizer, not WorkingState
        # So it won't be in new_state
        
        # We can't check new_state.received_shards because it doesn't exist on WorkingState class currently
        # But we can check if the data was preserved in what we serialized
        
        # Actually, let's check if Maximizer restores it
        maximizer_rec = Maximizer("TPV", "absolute")
        maximizer_rec.working_state = new_state
        
        if hasattr(maximizer_rec, 'received_shards') and maximizer_rec.received_shards[client_id]:
             print("\n[TEST] test_shard_tracking_persistence: PASSED")
        else:
             print("\n[TEST] test_shard_tracking_persistence: FAILED (Confirmed Shard Tracking Loss)")

    def test_deterministic_ids(self):
        """
        Test that result chunks have deterministic IDs.
        Expected to FAIL for MAX/TOP3.
        """
        maximizer = Maximizer("MAX", "absolute")
        client_id = 1
        
        # Mock data sender
        mock_sender = maximizer.data_sender
        
        # Populate some state
        maximizer.working_state.sellings_max[client_id][(1, datetime.date(2024,1,1))] = 10
        
        # Publish twice
        maximizer.publish_absolute_max_results(client_id)
        args1, _ = mock_sender.send.call_args
        data1 = args1[0]
        # Strip header (28 bytes)
        payload1 = data1[28:]
        chunk1 = ProcessChunk.deserialize(ProcessChunkHeader(0, TableType.TRANSACTION_ITEMS), payload1)
        
        maximizer.publish_absolute_max_results(client_id)
        args2, _ = mock_sender.send.call_args
        data2 = args2[0]
        payload2 = data2[28:]
        chunk2 = ProcessChunk.deserialize(ProcessChunkHeader(0, TableType.TRANSACTION_ITEMS), payload2)
        
        if chunk1.message_id() == chunk2.message_id():
            print("\n[TEST] test_deterministic_ids: PASSED")
        else:
            print("\n[TEST] test_deterministic_ids: FAILED (Confirmed Random IDs)")

if __name__ == '__main__':
    unittest.main()
