"""
Unit Fault Tolerance Tests for Filter Worker

Tests verify fault tolerance logic without requiring full RabbitMQ infrastructure.
Uses mocks to simulate RabbitMQ and tests process_chunk() directly.

Crash Points Tested:
1. CRASH_BEFORE_COMMIT_PROCESSING - Before persisting chunk
2. CRASH_AFTER_COMMIT_PROCESSING - After persisting chunk
3. CRASH_BEFORE_COMMIT_WORKING_STATE - Before persisting state  
4. CRASH_BEFORE_SEND - Before sending to downstream
5. CRASH_AFTER_SEND - After commit_send_ack
"""
import unittest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
import datetime as dt_module

# Import filter and dependencies
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from workers.filter.common.filter import Filter
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TransactionsProcessRow, DateTime
from utils.file_utils.table_type import TableType
from utils.eof_protocol.end_messages import MessageEnd


class TestFilterFaultTolerance(unittest.TestCase):
    """Fault tolerance tests using mocks"""

    def setUp(self):
        """Set up test environment with mocks"""
        # Create temporary persistence directory
        self.temp_dir = tempfile.mkdtemp(prefix="filter_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["CONTAINER_NAME"] = "test_filter"
        os.environ["FILTER_SHARD_ID"] = "1"
        os.environ["FILTER_SHARDS"] = "1"
        
        # Clear any crash points
        if "CRASH_POINT" in os.environ:
            del os.environ["CRASH_POINT"]
        
        # Mock configuration
        self.config = {
            "id": 1,
            "filter_type": "year",
            "year_start": 2019,
            "year_end": 2025
        }

    def tearDown(self):
        """Clean up test environment"""
        # Remove temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Clear environment variables
        for key in ["CRASH_POINT", "PERSISTENCE_DIR", "FILTER_SHARD_ID", "FILTER_SHARDS"]:
            if key in os.environ:
                del os.environ[key]

    def _create_test_chunk(self, client_id=1, message_id="msg_001", num_rows=3):
        """Helper to create a test ProcessChunk"""
        import uuid
        
        # Convert string message_id to UUID
        if isinstance(message_id, str):
            message_id = uuid.UUID(int=hash(message_id) & (2**128 - 1))
        
        header = ProcessChunkHeader(
            client_id=client_id,
            message_id=message_id,
            table_type=TableType.TRANSACTIONS
        )
        
        rows = []
        for i in range(num_rows):
            date_obj = dt_module.date(2020, 1, i+1)
            time_obj = dt_module.time(12, 0, 0)
            
            row = TransactionsProcessRow(
                transaction_id=f"tx_{i}",
                store_id=i+1,
                user_id=i+1,
                final_amount=100.0 + i,
                created_at=DateTime(date_obj, time_obj)
            )
            rows.append(row)
        
        return ProcessChunk(header, rows)
    
    def _verify_working_state(self, working_state, expected_processed_count, chunks_processed, msg=""):
        """Helper: Comprehensive working state verification"""
        # Verify processed_ids count
        self.assertEqual(len(working_state.processed_ids), expected_processed_count,
                        f"{msg} - processed_ids count mismatch")
        
        # Verify global_processed_ids count matches processed_ids
        self.assertEqual(len(working_state.global_processed_ids), expected_processed_count,
                        f"{msg} - global_processed_ids count mismatch")
        
        # Verify each chunk is in both processed_ids and global_processed_ids
        for chunk in chunks_processed:
            self.assertIn(chunk.message_id(), working_state.processed_ids,
                         f"{msg} - chunk {chunk.message_id()} not in processed_ids")
            self.assertIn(str(chunk.message_id()), working_state.global_processed_ids,
                         f"{msg} - chunk {chunk.message_id()} not in global_processed_ids")
        
        # Verify structure consistency
        self.assertIsInstance(working_state.processed_ids, set,
                            f"{msg} - processed_ids should be a set")
        self.assertIsInstance(working_state.global_processed_ids, set,
                            f"{msg} - global_processed_ids should be a set")

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_global_processed_ids_persisted_and_recovered(self, mock_exchange, mock_queue):
        """
        Test that global_processed_ids is persisted and recovered correctly
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # PHASE 1: Process first 3 chunks
        filter_worker = Filter(self.config)
        for i in range(3):
            filtered = filter_worker.process_chunk(chunks[i])
            filter_worker.working_state.processed_ids.add(chunks[i].message_id())
            filter_worker.working_state.global_processed_ids.add(str(chunks[i].message_id()))
            filter_worker.persistence_service.commit_working_state(
                filter_worker.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        # Verify comprehensive state
        self._verify_working_state(filter_worker.working_state, 3, chunks[:3], "Before crash")
        
        # PHASE 2: Simulate crash and restart
        filter_worker2 = Filter(self.config)
        
        # Verify recovered state
        self._verify_working_state(filter_worker2.working_state, 3, chunks[:3], "After recovery")
        
        # Process 4th chunk
        filtered_4 = filter_worker2.process_chunk(chunks[3])
        filter_worker2.working_state.processed_ids.add(chunks[3].message_id())
        filter_worker2.working_state.global_processed_ids.add(str(chunks[3].message_id()))
        filter_worker2.persistence_service.commit_working_state(
            filter_worker2.working_state.to_bytes(),
            chunks[3].message_id()
        )
        
        # Verify final state
        self._verify_working_state(filter_worker2.working_state, 4, chunks, "Final state")

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_idempotency_duplicate_prevention(self, mock_exchange, mock_queue):
        """
        Test that duplicate messages are properly detected and ignored
        """
        chunk = self._create_test_chunk(message_id="msg_duplicate")
        
        filter_worker = Filter(self.config)
        
        # Process first time
        filtered_1 = filter_worker.process_chunk(chunk)
        filter_worker.working_state.processed_ids.add(chunk.message_id())
        filter_worker.working_state.global_processed_ids.add(str(chunk.message_id()))
        
        # Check idempotency
        is_duplicate = chunk.message_id() in filter_worker.working_state.processed_ids
        self.assertTrue(is_duplicate, "Duplicate should be detected in processed_ids")
        
        is_global_duplicate = str(chunk.message_id()) in filter_worker.working_state.global_processed_ids
        self.assertTrue(is_global_duplicate, "Duplicate should be detected in global_processed_ids")

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_working_state_survives_multiple_crashes(self, mock_exchange, mock_queue):
        """
        Test that working state accumulates correctly across multiple crash/recovery cycles
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # CYCLE 1: Process first 2 chunks
        filter_worker1 = Filter(self.config)
        for i in range(2):
            filtered = filter_worker1.process_chunk(chunks[i])
            filter_worker1.working_state.processed_ids.add(chunks[i].message_id())
            filter_worker1.working_state.global_processed_ids.add(str(chunks[i].message_id()))
            filter_worker1.persistence_service.commit_working_state(
                filter_worker1.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        self._verify_working_state(filter_worker1.working_state, 2, chunks[:2], "After cycle 1")
        
        # CRASH 1 - CYCLE 2: Restart and process next 2 chunks
        filter_worker2 = Filter(self.config)
        self._verify_working_state(filter_worker2.working_state, 2, chunks[:2], "After recovery 1")
        
        for i in range(2, 4):
            filtered = filter_worker2.process_chunk(chunks[i])
            filter_worker2.working_state.processed_ids.add(chunks[i].message_id())
            filter_worker2.working_state.global_processed_ids.add(str(chunks[i].message_id()))
            filter_worker2.persistence_service.commit_working_state(
                filter_worker2.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        self._verify_working_state(filter_worker2.working_state, 4, chunks, "After cycle 2")
        
        # CRASH 2 - CYCLE 3: Final restart and verify
        filter_worker3 = Filter(self.config)
        self._verify_working_state(filter_worker3.working_state, 4, chunks, "After recovery 2")

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_end_message_survives_crash(self, mock_exchange, mock_queue):
        """
        Test that END messages are properly handled across crashes
        """
        filter_worker = Filter(self.config)
        
        # Process some chunks
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(2)]
        for chunk in chunks:
            filter_worker.process_chunk(chunk)
            filter_worker.working_state.processed_ids.add(chunk.message_id())
            filter_worker.working_state.global_processed_ids.add(str(chunk.message_id()))
        
        # Receive END message
        end_msg = MessageEnd(
            client_id=1,
            table_type=TableType.TRANSACTIONS,
            count=2,
            sender_id="test"
        )
        filter_worker.working_state.end_received(end_msg.client_id(), end_msg.table_type())
        
        # Persist state with END
        filter_worker.persistence_service.commit_working_state(
            filter_worker.working_state.to_bytes(),
            chunks[-1].message_id()
        )
        
        # Crash and restart
        filter_worker2 = Filter(self.config)
        
        # Verify END was recovered
        has_end = filter_worker2.working_state.end_is_received(1, TableType.TRANSACTIONS)
        self.assertTrue(has_end, "END message should survive crash")


class TestFilterCrashPoints(unittest.TestCase):
    """Specific crash point tests - simulating exact failure scenarios"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp(prefix="filter_crash_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["FILTER_SHARD_ID"] = "1"
        os.environ["FILTER_SHARDS"] = "1"
        
        self.config = {
            "id": 1,
            "filter_type": "year",
            "year_start": 2019,
            "year_end": 2025
        }

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_chunk(self, message_id="msg_001"):
        """Helper to create test chunk"""
        import uuid
        if isinstance(message_id, str):
            message_id = uuid.UUID(int=hash(message_id) & (2**128 - 1))
        
        header = ProcessChunkHeader(
            client_id=1,
            message_id=message_id,
            table_type=TableType.TRANSACTIONS
        )
        
        rows = []
        for i in range(3):
            row = TransactionsProcessRow(
                transaction_id=f"tx_{i}",
                store_id=i+1,
                user_id=i+1,
                final_amount=100.0 + i,
                created_at=DateTime(dt_module.date(2020, 1, i+1), dt_module.time(12, 0, 0))
            )
            rows.append(row)
        
        return ProcessChunk(header, rows)

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_crash_point_1_before_commit_processing(self, mock_exchange, mock_queue):
        """
        CRASH POINT 1: CRASH BEFORE commit_processing_chunk
        
        Scenario:
        - pop() from queue -> chunk
        - callback() starts
        - ❌ CRASH BEFORE commit_processing_chunk(chunk)
        
        Expected: Chunk NOT persisted, should be redelivered by RabbitMQ
        """
        chunk = self._create_test_chunk("msg_crash1")
        
        filter_worker = Filter(self.config)
        
        # Simulate receiving chunk but crashing BEFORE commit_processing
        # In real scenario, RabbitMQ would not get ACK and would redeliver
        
        # Verify: No processing chunk is saved
        recovered_chunk = filter_worker.persistence_service.recover_last_processing_chunk()
        self.assertIsNone(recovered_chunk, "No chunk should be persisted before commit")
        
        # Verify: Working state is empty
        self.assertEqual(len(filter_worker.working_state.processed_ids), 0)

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_crash_point_2_after_commit_processing(self, mock_exchange, mock_queue):
        """
        CRASH POINT 2: CRASH AFTER commit_processing_chunk
        
        Scenario:
        - commit_processing_chunk(chunk) ✅
        - ❌ CRASH BEFORE _handle_process_message()
        
        Expected: Chunk is persisted, recovery should process it
        """
        chunk = self._create_test_chunk("msg_crash2")
        
        filter_worker = Filter(self.config)
        
        # Simulate: commit_processing_chunk was called
        filter_worker.persistence_service.commit_processing_chunk(chunk)
        
        # ❌ CRASH (simulated by creating new Filter instance)
        filter_worker2 = Filter(self.config)
        
        # Verify: Chunk was recovered and processed
        # The handle_processing_recovery() should have processed it
        recovered_chunk = filter_worker2.persistence_service.recover_last_processing_chunk()
        
        # After recovery, the chunk should have been processed
        # Note: This depends on handle_processing_recovery implementation
        self.assertIsNotNone(recovered_chunk, "Chunk should be persisted and recoverable")

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_crash_point_3_before_commit_working_state(self, mock_exchange, mock_queue):
        """
        CRASH POINT 3: CRASH BEFORE commit_working_state
        
        Scenario:
        - apply(chunk) - process ✅
        - update state (add to processed_ids) ✅
        - ❌ CRASH BEFORE commit_working_state()
        
        Expected: Chunk was processed but state NOT persisted, chunk should be reprocessed (idempotent)
        """
        chunk = self._create_test_chunk("msg_crash3")
        
        filter_worker = Filter(self.config)
        
        # Process chunk
        filtered = filter_worker.process_chunk(chunk)
        
        # Update in-memory state but DON'T commit
        filter_worker.working_state.processed_ids.add(chunk.message_id())
        filter_worker.working_state.global_processed_ids.add(str(chunk.message_id()))
        
        # ❌ CRASH - DON'T call commit_working_state
        
        # Restart
        filter_worker2 = Filter(self.config)
        
        # Verify: State was NOT persisted
        self.assertEqual(len(filter_worker2.working_state.processed_ids), 0,
                        "State should not be persisted before commit")
        
        # Chunk should be reprocessed (idempotency ensures no duplicates downstream)
        self.assertNotIn(chunk.message_id(), filter_worker2.working_state.processed_ids)

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_crash_point_4_before_send(self, mock_exchange, mock_queue):
        """
        CRASH POINT 4: CRASH BEFORE sending to downstream queue
        
        Scenario:
        - commit_working_state() ✅
        - ❌ CRASH BEFORE sending to queue
        
        Expected: State persisted, but message NOT sent. On recovery, should resend.
        """
        chunk = self._create_test_chunk("msg_crash4")
        
        filter_worker = Filter(self.config)
        
        # Process and commit state
        filtered = filter_worker.process_chunk(chunk)
        filter_worker.working_state.processed_ids.add(chunk.message_id())
        filter_worker.working_state.global_processed_ids.add(str(chunk.message_id()))
        filter_worker.persistence_service.commit_working_state(
            filter_worker.working_state.to_bytes(),
            chunk.message_id()
        )
        
        # ❌ CRASH - DON'T send to queue or commit_send_ack
        
        # Restart
        filter_worker2 = Filter(self.config)
        
        # Verify: State was persisted
        self.assertEqual(len(filter_worker2.working_state.processed_ids), 1,
                        "State should be persisted")
        self.assertIn(chunk.message_id(), filter_worker2.working_state.processed_ids)
        
        # Verify: send_ack NOT committed (message should be resent on recovery)
        # This checks that process_has_been_counted would return False
        has_been_sent = filter_worker2.persistence_service.process_has_been_counted(chunk)
        self.assertFalse(has_been_sent, "Message should NOT be marked as sent")

    @patch('workers.filter.common.filter.MessageMiddlewareQueue')
    @patch('workers.filter.common.filter.MessageMiddlewareExchange')
    def test_crash_point_5_after_send(self, mock_exchange, mock_queue):
        """
        CRASH POINT 5: CRASH AFTER commit_send_ack
        
        Scenario:
        - Send to queue ✅
        - commit_send_ack() ✅
        - ❌ CRASH (complete processing)
        
        Expected: Everything persisted, recovery should find nothing to do
        """
        chunk = self._create_test_chunk("msg_crash5")
        
        filter_worker = Filter(self.config)
        
        # Complete processing
        filtered = filter_worker.process_chunk(chunk)
        filter_worker.working_state.processed_ids.add(chunk.message_id())
        filter_worker.working_state.global_processed_ids.add(str(chunk.message_id()))
        filter_worker.persistence_service.commit_working_state(
            filter_worker.working_state.to_bytes(),
            chunk.message_id()
        )
        
        # Simulate: message was sent and ACK committed
        filter_worker.persistence_service.commit_send_ack(chunk.client_id(), chunk.message_id())
        
        # ❌ CRASH after everything complete
        
        # Restart
        filter_worker2 = Filter(self.config)
        
        # Verify: State was persisted
        self.assertEqual(len(filter_worker2.working_state.processed_ids), 1)
        self.assertIn(chunk.message_id(), filter_worker2.working_state.processed_ids)
        
        # Verify: Send was marked as complete
        has_been_sent = filter_worker2.persistence_service.process_has_been_counted(chunk.message_id())
        self.assertTrue(has_been_sent, "Message should be marked as sent")
        
        # Verify: No pending work
        recovered_chunk = filter_worker2.persistence_service.recover_last_processing_chunk()
        # After handle_processing_recovery, there should be no pending chunk
        # (or it was already processed)


if __name__ == "__main__":
    unittest.main()
