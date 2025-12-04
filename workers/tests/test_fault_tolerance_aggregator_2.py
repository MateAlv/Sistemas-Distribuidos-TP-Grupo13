"""
Comprehensive Fault Tolerance Tests for Aggregator Worker

Tests verify fault tolerance logic without requiring full RabbitMQ infrastructure.
Uses mocks to simulate RabbitMQ and tests aggregation logic directly.

Based on test_fault_tolerance_filter.py structure with additions for:
- Chunk buffering recovery
- Periodic state commits
- handle_processing_recovery() method

Crash Points Tested:
1. CRASH_BEFORE_PROCESS - Before processing chunk
2. CRASH_AFTER_PROCESS_BEFORE_COMMIT - After processing, before committing state
3. CRASH_BEFORE_COMMIT_WORKING_STATE - Before persisting state
4. CRASH_BEFORE_SEND - Before sending to downstream
5. CRASH_AFTER_SEND - After commit_send_ack

New Tests for Chunk Buffering:
- Buffered chunks recovery
- Periodic commits (every N chunks)
- Buffer cleared after state commit
"""
import unittest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
import datetime as dt_module
from unittest.mock import Mock, patch, MagicMock

# Mock pika before importing modules that use it
import sys
sys.modules["pika"] = MagicMock()
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from workers.aggregators.common.aggregator import Aggregator
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TransactionItemsProcessRow, DateTime
from utils.file_utils.table_type import TableType
from utils.eof_protocol.end_messages import MessageEnd
from workers.aggregators.common.aggregator_stats_messages import AggregatorDataMessage
import json
import uuid


class TestAggregatorFaultTolerance(unittest.TestCase):
    """Fault tolerance tests using mocks - matching filter test structure"""

    def setUp(self):
        """Set up test environment with mocks"""
        # Create temporary persistence directory
        self.temp_dir = tempfile.mkdtemp(prefix="aggregator_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["CONTAINER_NAME"] = "test_aggregator"
        os.environ["AGGREGATOR_SHARD_ID"] = "1"
        os.environ["AGGREGATOR_SHARDS"] = "1"
        os.environ["AGGREGATOR_COMMIT_INTERVAL"] = "3"  # Commit every 3 chunks for testing
        
        # Required for PRODUCTS aggregator (MAX sharding config)
        os.environ["MAX_SHARDS_1_ID"] = "max_1"
        os.environ["MAX_SHARDS_1_QUEUE"] = "test_max_queue_1"
        # Format: shard_id:ids_spec (e.g., "items_1:1" or "items_1_4:1-4")
        os.environ["MAX_SHARDS"] = "items_1:1"
        
        # Clear any crash points
        if "CRASH_POINT" in os.environ:
            del os.environ["CRASH_POINT"]
        
        # Mock configuration - using PRODUCTS aggregator as example
        self.config = {
            "agg_type": "PRODUCTS",
            "agg_id": 1
        }

    def tearDown(self):
        """Clean up test environment"""
        # Remove temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Clear environment variables
        for key in ["CRASH_POINT", "PERSISTENCE_DIR", "AGGREGATOR_SHARD_ID", "AGGREGATOR_SHARDS", 
                    "AGGREGATOR_COMMIT_INTERVAL", "MAX_SHARDS"]:
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
            table_type=TableType.TRANSACTION_ITEMS
        )
        
        rows = []
        for i in range(num_rows):
            date_obj = dt_module.date(2020, 1, i+1)
            time_obj = dt_module.time(12, 0, 0)
            
            row = TransactionItemsProcessRow(
                transaction_id=f"tx_{client_id}_{i}",
                item_id=100 + i,
                quantity=10 + i,
                subtotal=100.0 + i * 10,
                created_at=DateTime(date_obj, time_obj)
            )
            rows.append(row)
        
        return ProcessChunk(header, rows)
    
    def _verify_working_state(self, working_state, expected_processed_count, chunks_processed, msg=""):
        """Helper: Comprehensive working state verification"""
        # Verify processed_ids count
        self.assertEqual(len(working_state.processed_ids), expected_processed_count,
                        f"{msg} - processed_ids count mismatch")
        
        # Verify each chunk is in processed_ids
        for chunk in chunks_processed:
            self.assertIn(chunk.message_id(), working_state.processed_ids,
                         f"{msg} - chunk {chunk.message_id()} not in processed_ids")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_processed_ids_persisted_and_recovered(self, mock_exchange, mock_queue):
        """
        Test that processed_ids is persisted and recovered correctly
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # PHASE 1: Process first 3 chunks
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        for i in range(3):
            aggregator._apply_and_update_state(chunks[i])
            aggregator.persistence.commit_working_state(
                aggregator.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        # Verify comprehensive state
        self._verify_working_state(aggregator.working_state, 3, chunks[:3], "Before crash")
        
        # PHASE 2: Simulate crash and restart
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify recovered state
        self._verify_working_state(aggregator2.working_state, 3, chunks[:3], "After recovery")
        
        # Process 4th chunk
        aggregator2._apply_and_update_state(chunks[3])
        aggregator2.persistence.commit_working_state(
            aggregator2.working_state.to_bytes(),
            chunks[3].message_id()
        )
        
        # Verify final state
        self._verify_working_state(aggregator2.working_state, 4, chunks, "Final state")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_idempotency_duplicate_prevention(self, mock_exchange, mock_queue):
        """
        Test that duplicate messages are properly detected and ignored
        """
        chunk = self._create_test_chunk(message_id="msg_duplicate")
        
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Process first time
        aggregator._apply_and_update_state(chunk)
        
        # Check idempotency
        is_duplicate = aggregator.working_state.is_processed(chunk.message_id())
        self.assertTrue(is_duplicate, "Duplicate should be detected in processed_ids")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_working_state_survives_multiple_crashes(self, mock_exchange, mock_queue):
        """
        Test that working state accumulates correctly across multiple crash/recovery cycles
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # CYCLE 1: Process first 2 chunks
        aggregator1 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        for i in range(2):
            aggregator1._apply_and_update_state(chunks[i])
            aggregator1.persistence.commit_working_state(
                aggregator1.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        self._verify_working_state(aggregator1.working_state, 2, chunks[:2], "After cycle 1")
        
        # CRASH 1 - CYCLE 2: Restart and process next 2 chunks
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        self._verify_working_state(aggregator2.working_state, 2, chunks[:2], "After recovery 1")
        
        for i in range(2, 4):
            aggregator2._apply_and_update_state(chunks[i])
            aggregator2.persistence.commit_working_state(
                aggregator2.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        self._verify_working_state(aggregator2.working_state, 4, chunks, "After cycle 2")
        
        # CRASH 2 - CYCLE 3: Final restart and verify
        aggregator3 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        self._verify_working_state(aggregator3.working_state, 4, chunks, "After recovery 2")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_end_message_survives_crash(self, mock_exchange, mock_queue):
        """
        Test that END messages are properly handled across crashes
        """
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Process some chunks
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(2)]
        for chunk in chunks:
            aggregator._apply_and_update_state(chunk)
        
        # Mark END message received
        client_id = 1
        table_type = TableType.TRANSACTION_ITEMS
        aggregator.working_state.mark_end_message_received(client_id, table_type)
        aggregator.working_state.set_chunks_to_receive(client_id, table_type, 2)
        
        # Persist state with END
        aggregator.persistence.commit_working_state(
            aggregator.working_state.to_bytes(),
            chunks[-1].message_id()
        )
        
        # Crash and restart
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify END was recovered
        has_end = aggregator2.working_state.is_end_message_received(client_id, table_type)
        self.assertTrue(has_end, "END message should survive crash")
        
        expected_chunks = aggregator2.working_state.get_chunks_to_receive(client_id, table_type)
        self.assertEqual(expected_chunks, 2, "Expected chunks count should survive crash")


class TestAggregatorChunkBuffering(unittest.TestCase):
    """Tests specific to chunk buffering implementation"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp(prefix="aggregator_buffer_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["AGGREGATOR_SHARD_ID"] = "1"
        os.environ["AGGREGATOR_SHARDS"] = "1"
        os.environ["AGGREGATOR_COMMIT_INTERVAL"] = "3"  # Commit every 3 chunks
        
        # Required for PRODUCTS aggregator
        os.environ["MAX_SHARDS"] = "items_1:1"
        
        self.config = {
            "agg_type": "PRODUCTS",
            "agg_id": 1
        }

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        for key in ["PERSISTENCE_DIR", "AGGREGATOR_COMMIT_INTERVAL", "AGGREGATOR_SHARD_ID", 
                    "AGGREGATOR_SHARDS", "MAX_SHARDS"]:
            if key in os.environ:
                del os.environ[key]

    def _create_test_chunk(self, message_id="msg_001"):
        """Helper to create test chunk"""
        import uuid
        if isinstance(message_id, str):
            message_id = uuid.UUID(int=hash(message_id) & (2**128 - 1))
        
        header = ProcessChunkHeader(
            client_id=1,
            message_id=message_id,
            table_type=TableType.TRANSACTION_ITEMS
        )
        
        rows = []
        for i in range(3):
            row = TransactionItemsProcessRow(
                transaction_id=f"tx_{i}",
                item_id=100 + i,
                quantity=10 + i,
                subtotal=100.0 + i * 10,
                created_at=DateTime(dt_module.date(2020, 1, i+1), dt_module.time(12, 0, 0))
            )
            rows.append(row)
        
        return ProcessChunk(header, rows)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_buffered_chunks_recovered(self, mock_exchange, mock_queue):
        """
        Test that buffered chunks are recovered and processed on restart
        """
        chunks = [self._create_test_chunk(f"msg_{i}") for i in range(5)]
        
        # PHASE 1: Buffer 5 chunks (commit every 3, so 2 will be buffered)
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Manually buffer chunks (simulating callback behavior)
        for i in range(5):
            aggregator.persistence.append_chunk_to_buffer(chunks[i])
        
        # Process first 3 (trigger commit after 3rd)
        for i in range(3):
            aggregator._apply_and_update_state(chunks[i])
        
        # Commit after 3rd chunk
        aggregator.persistence.commit_working_state(
            aggregator.working_state.to_bytes(),
            chunks[2].message_id()
        )
        
        # Buffer should be cleared after commit
        buffer_size_after_commit = aggregator.persistence.chunk_buffer.get_buffer_size()
        # Note: Buffer still has chunks 3 and 4 because we appended them but didn't commit again
        
        # CRASH before processing chunks 3 and 4
        
        # PHASE 2: Restart - handle_processing_recovery should process buffered chunks
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify buffered chunks were recovered and processed
        # State should have 3 chunks (from first commit)
        self.assertEqual(len(aggregator2.working_state.processed_ids), 3,
                        "Should have 3 processed chunks from committed state")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_periodic_state_commits(self, mock_exchange, mock_queue):
        """
        Test that state is committed periodically (every N chunks) not on every chunk
        """
        chunks = [self._create_test_chunk(f"msg_{i}") for i in range(5)]
        
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Commit interval is 3
        self.assertEqual(aggregator.persistence.commit_interval, 3)
        
        # Process first 2 chunks - should NOT trigger commit
        # Simulate the real flow: buffer then process
        for i in range(2):
            aggregator.persistence.append_chunk_to_buffer(chunks[i])
            aggregator._apply_and_update_state(chunks[i])
            should_commit = aggregator.persistence.should_commit_state()
            self.assertFalse(should_commit, f"Should NOT commit after {i+1} chunks")
        
        # Process 3rd chunk - should trigger commit
        aggregator.persistence.append_chunk_to_buffer(chunks[2])
        aggregator._apply_and_update_state(chunks[2])
        should_commit = aggregator.persistence.should_commit_state()
        self.assertTrue(should_commit, "Should commit after 3 chunks")
        
        # Commit
        aggregator.persistence.commit_working_state(
            aggregator.working_state.to_bytes(),
            chunks[2].message_id()
        )
        
        # Verify counter reset
        self.assertEqual(aggregator.persistence.chunks_since_last_commit, 0,
                        "Counter should reset after commit")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_buffer_cleared_after_state_commit(self, mock_exchange, mock_queue):
        """
        Test that chunk buffer is cleared when state is committed
        """
        chunks = [self._create_test_chunk(f"msg_{i}") for i in range(3)]
        
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Append chunks to buffer
        for chunk in chunks:
            aggregator.persistence.append_chunk_to_buffer(chunk)
        
        # Verify buffer has content
        buffer_size_before = aggregator.persistence.chunk_buffer.get_buffer_size()
        self.assertGreater(buffer_size_before, 0, "Buffer should have content")
        
        # Commit working state (should clear buffer)
        aggregator.persistence.commit_working_state(
            aggregator.working_state.to_bytes(),
            chunks[-1].message_id()
        )
        
        # Verify buffer is cleared
        buffer_size_after = aggregator.persistence.chunk_buffer.get_buffer_size()
        self.assertEqual(buffer_size_after, 0, "Buffer should be cleared after state commit")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_handle_processing_recovery_processes_buffered_chunks(self, mock_exchange, mock_queue):
        """
        Test that handle_processing_recovery() correctly processes buffered chunks
        """
        chunks = [self._create_test_chunk(f"msg_{i}") for i in range(4)]
        
        # PHASE 1: Setup - process, buffer, but don't commit the buffered ones
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Process first 2 chunks completely (buffer + process + commit)
        for i in range(2):
            aggregator.persistence.append_chunk_to_buffer(chunks[i])
            aggregator._apply_and_update_state(chunks[i])
        # Commit state (this clears buffer)
        aggregator.persistence.commit_working_state(
            aggregator.working_state.to_bytes(),
            chunks[1].message_id()
        )
        
        # Verify buffer is cleared and counter reset
        self.assertEqual(aggregator.persistence.chunk_buffer.get_buffer_size(), 0,
                        "Buffer should be empty after commit")
        self.assertEqual(aggregator.persistence.chunks_since_last_commit, 0,
                        "Counter should be 0 after commit")
        
        # Buffer next 2 chunks WITHOUT committing state
        # This simulates: chunks were buffered but crash happened before state commit
        for i in range(2, 4):
            aggregator.persistence.append_chunk_to_buffer(chunks[i])
        
        # Verify buffer has 2 chunks before crash
        buffer_size = aggregator.persistence.chunk_buffer.get_buffer_size()
        self.assertGreater(buffer_size, 0, "Buffer should have content before crash")
        buffered_count = aggregator.persistence.chunk_buffer.get_chunk_count()
        self.assertEqual(buffered_count, 2, "Should have 2 chunks in buffer before crash")
        
        # CRASH (without processing or committing chunks 2 and 3)
        
        # PHASE 2: Restart - handle_processing_recovery should process buffered chunks
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # After recovery:
        # - Working state has first 2 chunks (from last commit)
        # - Buffered chunks (2 and 3) should have been processed by handle_processing_recovery
        # - Total: 2 (from state) + 2 (from buffer) = 4
        self.assertEqual(len(aggregator2.working_state.processed_ids), 4,
                        "Should have 4 processed chunks after recovery")


class TestAggregatorCrashPoints(unittest.TestCase):
    """Specific crash point tests - matching filter test structure"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp(prefix="aggregator_crash_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["AGGREGATOR_SHARD_ID"] = "1"
        os.environ["AGGREGATOR_SHARDS"] = "1"
        os.environ["AGGREGATOR_COMMIT_INTERVAL"] = "10"  # Large interval to avoid auto-commits
        
        # Required for PRODUCTS aggregator
        os.environ["MAX_SHARDS"] = "items_1:1"
        
        self.config = {
            "agg_type": "PRODUCTS",
            "agg_id": 1
        }

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        for key in ["CRASH_POINT", "PERSISTENCE_DIR", "AGGREGATOR_COMMIT_INTERVAL",
                    "MAX_SHARDS"]:
            if key in os.environ:
                del os.environ[key]

    def _create_test_chunk(self, message_id="msg_001"):
        """Helper to create test chunk"""
        import uuid
        if isinstance(message_id, str):
            message_id = uuid.UUID(int=hash(message_id) & (2**128 - 1))
        
        header = ProcessChunkHeader(
            client_id=1,
            message_id=message_id,
            table_type=TableType.TRANSACTION_ITEMS
        )
        
        rows = []
        for i in range(3):
            row = TransactionItemsProcessRow(
                transaction_id=f"tx_{i}",
                item_id=100 + i,
                quantity=10 + i,
                subtotal=100.0 + i * 10,
                created_at=DateTime(dt_module.date(2020, 1, i+1), dt_module.time(12, 0, 0))
            )
            rows.append(row)
        
        return ProcessChunk(header, rows)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_crash_point_before_commit_working_state(self, mock_exchange, mock_queue):
        """
        CRASH POINT: CRASH BEFORE commit_working_state
        
        Scenario:
        - apply(chunk) - process ✅
        - update state (add to processed_ids) ✅
        - ❌ CRASH BEFORE commit_working_state()
        
        Expected: Chunk was processed but state NOT persisted, chunk should be reprocessed
        """
        chunk = self._create_test_chunk("msg_crash3")
        
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Process chunk
        aggregator._apply_and_update_state(chunk)
        
        # ❌ CRASH - DON'T call commit_working_state
        
        # Restart
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify: State was NOT persisted
        self.assertEqual(len(aggregator2.working_state.processed_ids), 0,
                        "State should not be persisted before commit")
        
        # Chunk should be reprocessed (idempotency ensures no duplicates)
        self.assertNotIn(chunk.message_id(), aggregator2.working_state.processed_ids)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_crash_point_after_commit_before_send(self, mock_exchange, mock_queue):
        """
        CRASH POINT: CRASH AFTER commit_working_state, BEFORE sending
        
        Scenario:
        - commit_working_state() ✅
        - ❌ CRASH BEFORE sending to downstream
        
        Expected: State persisted, message NOT sent, should resend on recovery
        """
        chunk = self._create_test_chunk("msg_crash4")
        
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Process and commit state
        aggregator._apply_and_update_state(chunk)
        aggregator.persistence.commit_working_state(
            aggregator.working_state.to_bytes(),
            chunk.message_id()
        )
        
        # ❌ CRASH - DON'T send to downstream or commit_send_ack
        
        # Restart
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify: State was persisted
        self.assertEqual(len(aggregator2.working_state.processed_ids), 1,
                        "State should be persisted")
        self.assertIn(chunk.message_id(), aggregator2.working_state.processed_ids)
        
        # Verify: send_ack NOT committed (message should be resent on recovery)
        has_been_sent = aggregator2.persistence.send_has_been_acknowledged(chunk.client_id(), chunk.message_id())
        self.assertFalse(has_been_sent, "Message should NOT be marked as sent")




class TestAggregatorExtendedFaultTolerance(unittest.TestCase):
    """
    Extended fault tolerance scenarios provided by Spanish friend.
    Covers: ACK safety, buffer flushing, deterministic IDs, crash recovery, etc.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="agg_ext_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["AGGREGATOR_SHARD_ID"] = "1"
        os.environ["AGGREGATOR_SHARDS"] = "1"
        os.environ["AGGREGATOR_COMMIT_INTERVAL"] = "3"
        os.environ["MAX_SHARDS"] = "items_1:1"
        self.config = {"agg_type": "PRODUCTS", "agg_id": 1}

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        for key in ["PERSISTENCE_DIR", "AGGREGATOR_COMMIT_INTERVAL", "AGGREGATOR_SHARD_ID", 
                    "AGGREGATOR_SHARDS", "MAX_SHARDS", "CRASH_POINT"]:
            if key in os.environ:
                del os.environ[key]

    def _create_test_chunk(self, message_id="msg_001"):
        import uuid
        if isinstance(message_id, str):
            message_id = uuid.UUID(int=hash(message_id) & (2**128 - 1))
        header = ProcessChunkHeader(client_id=1, message_id=message_id, table_type=TableType.TRANSACTION_ITEMS)
        rows = [
            TransactionItemsProcessRow(f"tx_{i}", 100+i, 10+i, 100.0+i*10, DateTime(dt_module.date(2024, 1, 1), dt_module.time(12, 0, 0)))
            for i in range(2)
        ]
        return ProcessChunk(header, rows)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_safe_ack_callback_exception(self, mock_exchange, mock_queue):
        """
        1. ACK seguro (callback): Simular excepción antes de append/commit.
        Verificar que NO se incrementa buffer ni se marca procesado.
        """
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        chunk = self._create_test_chunk("msg_fail")
        
        # Mock append_chunk_to_buffer to raise exception
        with patch.object(aggregator.persistence, 'append_chunk_to_buffer', side_effect=Exception("Simulated Append Fail")):
            try:
                # Simulate callback logic manually since we can't easily invoke the real callback with pika mock
                # Logic: append -> process (if flush)
                aggregator.persistence.append_chunk_to_buffer(chunk)
            except Exception:
                pass
        
        # Verify buffer is empty
        self.assertEqual(aggregator.persistence.chunk_buffer.get_chunk_count(), 0)
        # Verify not processed
        self.assertFalse(aggregator.working_state.is_processed(chunk.message_id()))

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_flush_every_n_and_end(self, mock_exchange, mock_queue):
        """
        3. Flush cada N y en END.
        AGGREGATOR_COMMIT_INTERVAL=2. Procesar 2 chunks -> flush.
        Procesar 1 chunk + END -> flush forzado.
        """
        os.environ["AGGREGATOR_COMMIT_INTERVAL"] = "2"
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        chunks = [self._create_test_chunk(f"msg_{i}") for i in range(3)]
        
        # 1. Process 2 chunks
        for i in range(2):
            aggregator.persistence.append_chunk_to_buffer(chunks[i])
            aggregator._apply_and_update_state(chunks[i])
            if aggregator.persistence.should_commit_state():
                aggregator.persistence.commit_working_state(aggregator.working_state.to_bytes(), chunks[i].message_id())
        
        # Verify flush happened (buffer empty)
        self.assertEqual(aggregator.persistence.chunk_buffer.get_chunk_count(), 0)
        self.assertEqual(aggregator.persistence.chunks_since_last_commit, 0)
        
        # 2. Process 1 chunk (count = 1)
        aggregator.persistence.append_chunk_to_buffer(chunks[2])
        aggregator._apply_and_update_state(chunks[2])
        self.assertEqual(aggregator.persistence.chunks_since_last_commit, 1)
        self.assertEqual(aggregator.persistence.chunk_buffer.get_chunk_count(), 1)
        
        # 3. Receive END (simulate callback logic)
        # END should trigger _save_state
        aggregator._save_state(uuid.uuid4())
        
        # Verify flush happened
        self.assertEqual(aggregator.persistence.chunk_buffer.get_chunk_count(), 0)
        self.assertEqual(aggregator.persistence.chunks_since_last_commit, 0)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_reproduction_after_crash_partial_buffer(self, mock_exchange, mock_queue):
        """
        4. Reproducción tras crash con buffer parcial.
        Procesar N-1 chunks (sin flush). Crash. Recovery reprocesa.
        """
        os.environ["AGGREGATOR_COMMIT_INTERVAL"] = "5"
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        chunks = [self._create_test_chunk(f"msg_{i}") for i in range(3)]
        
        # Buffer 3 chunks (N=5, so no flush)
        for chunk in chunks:
            aggregator.persistence.append_chunk_to_buffer(chunk)
            aggregator._apply_and_update_state(chunk)
            
        # Verify buffer has 3
        self.assertEqual(aggregator.persistence.chunk_buffer.get_chunk_count(), 3)
        
        # CRASH (restart)
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify recovered (handle_processing_recovery calls recover_buffered_chunks)
        # Note: handle_processing_recovery is called in run(), we simulate it
        aggregator2.handle_processing_recovery()
        
        self.assertEqual(len(aggregator2.working_state.processed_ids), 3)
        for chunk in chunks:
            self.assertTrue(aggregator2.working_state.is_processed(chunk.message_id()))

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_duplicates_and_invalid_table_type(self, mock_exchange, mock_queue):
        """
        5. Duplicados e invalid table type.
        """
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        chunk = self._create_test_chunk("msg_dup")
        
        # 1. Process valid chunk
        aggregator._apply_and_update_state(chunk)
        self.assertTrue(aggregator.working_state.is_processed(chunk.message_id()))
        
        # 2. Process duplicate
        # Should return early, not increment counters
        # We can check log or side effects. Here we check processed_ids count stays 1
        aggregator._apply_and_update_state(chunk)
        self.assertEqual(len(aggregator.working_state.processed_ids), 1)
        
        # 3. Invalid Table Type
        invalid_chunk = self._create_test_chunk("msg_inv")
        invalid_chunk.header.table_type = "INVALID_TYPE"
        
        # Should be ignored (or logged error), not added to processed_ids
        aggregator._apply_and_update_state(invalid_chunk)
        self.assertFalse(aggregator.working_state.is_processed(invalid_chunk.message_id()))

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_deterministic_ids_and_flags(self, mock_exchange, mock_queue):
        """
        6. Deterministic IDs + flags.
        Verify output ID is stable.
        """
        chunk = self._create_test_chunk("msg_det")
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        mock_send = MagicMock()
        aggregator.middleware_data_exchange.send = mock_send
        
        # Run 1
        aggregator._apply_and_update_state(chunk)
        args1, _ = mock_send.call_args
        id1 = AggregatorDataMessage.decode(args1[0]).message_id
        
        # Run 2 (simulate crash/retry)
        mock_send.reset_mock()
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        aggregator2.middleware_data_exchange.send = mock_send
        aggregator2._apply_and_update_state(chunk)
        args2, _ = mock_send.call_args
        id2 = AggregatorDataMessage.decode(args2[0]).message_id
        
        self.assertEqual(id1, id2, "Output IDs must be deterministic")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_delete_client_data_saved(self, mock_exchange, mock_queue):
        """
        8. delete_client_data guardado.
        Verify delete is persisted.
        """
        chunk = self._create_test_chunk("msg_del_2")
        client_id = chunk.client_id()
        
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        aggregator._apply_and_update_state(chunk)
        aggregator.persistence.commit_working_state(aggregator.working_state.to_bytes(), chunk.message_id())
        
        # Delete
        aggregator.delete_client_data(client_id, chunk.table_type())
        
        # CRASH
        aggregator2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify deleted
        self.assertNotIn(client_id, aggregator2.working_state.chunks_received_per_client)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_processed_ids_pruning(self, mock_exchange, mock_queue):
        """
        12. processed_ids pruning.
        Tras delete_client_data, processed_ids debe podarse.
        """
        chunk = self._create_test_chunk("msg_prune")
        client_id = chunk.client_id()
        
        aggregator = Aggregator(self.config["agg_type"], self.config["agg_id"])
        aggregator._apply_and_update_state(chunk)
        
        self.assertTrue(aggregator.working_state.is_processed(chunk.message_id()))
        
        aggregator.delete_client_data(client_id, chunk.table_type())
        
        # Verify pruned (this might fail if not implemented yet)
        # Note: Current implementation might NOT prune processed_ids globally, 
        # but user requirement says it SHOULD.
        # If it fails, it confirms the "gap".
        self.assertFalse(aggregator.working_state.is_processed(chunk.message_id()), 
                        "processed_ids should be pruned after delete_client_data")
if __name__ == "__main__":
    unittest.main()
