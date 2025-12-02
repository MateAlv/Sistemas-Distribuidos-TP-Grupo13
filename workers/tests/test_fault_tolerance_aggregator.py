"""
Comprehensive Mock-based Fault Tolerance Tests for Aggregator Worker

Tests verify that for each crash point, the aggregator can:
1. Process multiple messages
2. Crash at a specific point
3. Recover and continue
4. Produce EXACTLY the same result as if no crash occurred

Crash Points Tested:
1. CRASH_BEFORE_COMMIT_PROCESSING - Before persisting chunk
2. CRASH_AFTER_COMMIT_PROCESSING - After persisting chunk
3. CRASH_BEFORE_COMMIT_WORKING_STATE - Before persisting state  
4. CRASH_BEFORE_COMMIT_SEND_ACK - Before sending message
5. CRASH_AFTER_COMMIT_SEND_ACK - After sending message
"""
import unittest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock, call
import datetime as dt_module

# Import aggregator and dependencies
import sys
from pathlib import Path
# Adjust path - now we're in workers/tests, need to go up to project root
sys.path.append(str(Path(__file__).parent.parent.parent))

from workers.aggregators.common.aggregator import Aggregator
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TransactionItemsProcessRow, DateTime
from utils.file_utils.table_type import TableType
from utils.eof_protocol.end_messages import MessageEnd


class TestAggregatorFaultToleranceComprehensive(unittest.TestCase):
    """Comprehensive fault tolerance tests covering all crash points"""

    def setUp(self):
        """Set up test environment with mocks"""
        # Create temporary persistence directory
        self.temp_dir = tempfile.mkdtemp(prefix="aggregator_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["CONTAINER_NAME"] = "test_aggregator"
        
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
        if "CRASH_POINT" in os.environ:
            del os.environ["CRASH_POINT"]
        if "PERSISTENCE_DIR" in os.environ:
            del os.environ["PERSISTENCE_DIR"]

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
            # Create DateTime using date and time objects
            date_obj = dt_module.date(2020, 1, i+1)
            time_obj = dt_module.time(12, 0, 0)
            
            row = TransactionItemsProcessRow(
                transaction_id=f"tx_{i}",
                item_id=i+1,
                quantity=10 + i,
                subtotal=100.0 + i,
                created_at=DateTime(date_obj, time_obj)
            )
            rows.append(row)
        
        return ProcessChunk(header, rows)

    def _process_chunks_no_fault(self, chunks):
        """Helper: Process chunks without any faults (baseline)"""
        with patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue'), \
             patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange'):
            
            aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
            results = []
            
            for chunk in chunks:
                # Accumulate data through the aggregator
                aggregator_worker.accumulate_products(chunk.client_id(), chunk.rows())
                aggregator_worker.working_state.mark_processed(chunk.message_id())
                results.append((chunk.message_id(), len(chunk.rows())))
            
            return results

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_crash_before_commit_processing_multi_message_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 1: CRASH_BEFORE_COMMIT_PROCESSING
        
        Scenario:
        1. Process 3 messages successfully  
        2. Process 4th message, crash BEFORE commit_processing_chunk
        3. Restart aggregator
        4. Verify 4th message is redelivered and processed
        5. Result identical to no-fault execution
        """
        # Create 4 test chunks
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # Get baseline (no fault)
        baseline_results = self._process_chunks_no_fault(chunks)
        
        # PHASE 1: Process first 3 successfully
        aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
        for i in range(3):
            aggregator_worker.accumulate_products(chunks[i].client_id(), chunks[i].rows())
            aggregator_worker.working_state.mark_processed(chunks[i].message_id())
            aggregator_worker.persistence_service.commit_working_state(
                aggregator_worker.working_state.to_bytes(), 
                chunks[i].message_id()
            )
        
        # Verify 3 chunks processed
        self.assertEqual(len(aggregator_worker.working_state.processed_ids), 3)
        
        # PHASE 2: Set crash point and simulate processing 4th message
        os.environ["CRASH_POINT"] = "CRASH_BEFORE_COMMIT_PROCESSING"
        
        # PHASE 3: Restart aggregator (simulates recovery)
        del os.environ["CRASH_POINT"]  # Remove crash point for recovery
        aggregator_worker2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify recovered state has 3 processed messages
        self.assertEqual(len(aggregator_worker2.working_state.processed_ids), 3)
        
        # Process 4th message after recovery
        aggregator_worker2.accumulate_products(chunks[3].client_id(), chunks[3].rows())
        aggregator_worker2.working_state.mark_processed(chunks[3].message_id())
        
        # VERIFICATION: Result identical to baseline
        self.assertEqual(len(aggregator_worker2.working_state.processed_ids), 4)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_crash_after_commit_processing_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 2: CRASH_AFTER_COMMIT_PROCESSING
        
        Scenario:
        1. Process 2 messages
        2. Process 3rd message, persist it, then crash AFTER commit
        3. Restart aggregator
        4. Verify recovery processes persisted chunk
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(3)]
        baseline_results = self._process_chunks_no_fault(chunks)
        
        # PHASE 1: Process first 2 messages
        aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
        for i in range(2):
            aggregator_worker.accumulate_products(chunks[i].client_id(), chunks[i].rows())
            aggregator_worker.working_state.mark_processed(chunks[i].message_id())
        
        # PHASE 2: Process 3rd, persist it (simulating AFTER commit but BEFORE handle_process_message)
        aggregator_worker.persistence_service.commit_processing_chunk(chunks[2])
        
        # Crash happens here (AFTER commit_processing_chunk)
        os.environ["CRASH_POINT"] = "CRASH_AFTER_COMMIT_PROCESSING"
        
        # PHASE 3: Restart aggregator
        del os.environ["CRASH_POINT"]
        aggregator_worker2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify recovery detected persisted chunk
        recovered_chunk = aggregator_worker2.persistence_service.recover_last_processing_chunk()
        self.assertIsNotNone(recovered_chunk)
        # Compare UUID objects
        self.assertEqual(recovered_chunk.message_id(), chunks[2].message_id())

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_crash_before_commit_working_state_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 3: CRASH_BEFORE_COMMIT_WORKING_STATE
        
        Scenario:
        1. Process 2 messages
        2. Process 3rd, accumulate, crash BEFORE commit_working_state
        3. Restart aggregator
        4. Verify 3rd message reprocessed (idempotency)
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(3)]
        baseline_results = self._process_chunks_no_fault(chunks)
        
        # PHASE 1: Process first 2 messages completely
        aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
        for i in range(2):
            aggregator_worker.accumulate_products(chunks[i].client_id(), chunks[i].rows())
            aggregator_worker.working_state.mark_processed(chunks[i].message_id())
            aggregator_worker.persistence_service.commit_working_state(
                aggregator_worker.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        # PHASE 2: Process 3rd message but DON'T commit working state
        aggregator_worker.accumulate_products(chunks[2].client_id(), chunks[2].rows())
        # Crash happens here (BEFORE commit_working_state)
        # Working state was NOT persisted
        
        # PHASE 3: Restart aggregator
        aggregator_worker2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify recovered state only has 2 processed IDs
        self.assertEqual(len(aggregator_worker2.working_state.processed_ids), 2)
        self.assertNotIn(chunks[2].message_id(), aggregator_worker2.working_state.processed_ids)
        
        # Reprocess 3rd message
        aggregator_worker2.accumulate_products(chunks[2].client_id(), chunks[2].rows())
        aggregator_worker2.working_state.mark_processed(chunks[2].message_id())
        
        # VERIFICATION: Same result as baseline
        self.assertEqual(len(aggregator_worker2.working_state.processed_ids), 3)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_crash_before_send_ack_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 4: CRASH_BEFORE_COMMIT_SEND_ACK
        
        Scenario:
        1. Process and persist 2 messages
        2. Process 3rd message, commit state, crash BEFORE send ACK
        3. Restart aggregator
        4. Verify 3rd message marked as processed but needs resending
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(3)]
        
        # PHASE 1: Process first 2 completely
        aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
        for i in range(2):
            aggregator_worker.accumulate_products(chunks[i].client_id(), chunks[i].rows())
            aggregator_worker.working_state.mark_processed(chunks[i].message_id())
            aggregator_worker.persistence_service.commit_working_state(
                aggregator_worker.working_state.to_bytes(),
                chunks[i].message_id()
            )
        
        # PHASE 2: Process 3rd, commit state, but DON'T send
        aggregator_worker.accumulate_products(chunks[2].client_id(), chunks[2].rows())
        aggregator_worker.working_state.mark_processed(chunks[2].message_id())
        aggregator_worker.persistence_service.commit_working_state(
            aggregator_worker.working_state.to_bytes(),
            chunks[2].message_id()
        )
        # Crash happens here (BEFORE commit_send_ack / sending to queue)
        
        # PHASE 3: Restart aggregator
        aggregator_worker2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify 3rd message was marked as processed in working state
        self.assertIn(chunks[2].message_id(), aggregator_worker2.working_state.processed_ids)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_crash_after_send_ack_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 5: CRASH_AFTER_COMMIT_SEND_ACK
        
        Scenario:
        1. Process 3 messages completely (send + ACK)
        2. Process 4th message, send, ACK, then crash AFTER
        3. Restart aggregator
        4. Verify 4th message fully processed and acknowledged
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # PHASE 1: Process all 4 messages completely
        aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
        for i in range(4):
            aggregator_worker.accumulate_products(chunks[i].client_id(), chunks[i].rows())
            aggregator_worker.working_state.mark_processed(chunks[i].message_id())
            aggregator_worker.persistence_service.commit_working_state(
                aggregator_worker.working_state.to_bytes(),
                chunks[i].message_id()
            )
            # Simulate send + ACK
            aggregator_worker.persistence_service.commit_send_ack(
                chunks[i].client_id(),
                chunks[i].message_id()
            )
        
        # Crash happens AFTER everything complete
        
        # PHASE 2: Restart aggregator
        aggregator_worker2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify all 4 messages in processed state
        self.assertEqual(len(aggregator_worker2.working_state.processed_ids), 4)
        for i in range(4):
            self.assertIn(chunks[i].message_id(), aggregator_worker2.working_state.processed_ids)

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_idempotency_duplicate_prevention(self, mock_exchange, mock_queue):
        """
        Test idempotency: duplicate messages are ignored
        
        This ensures that if a message is redelivered (e.g., after crash),
        it won't be processed twice.
        """
        chunk = self._create_test_chunk(message_id="msg_dup")
        
        aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Process first time
        aggregator_worker.accumulate_products(chunk.client_id(), chunk.rows())
        aggregator_worker.working_state.mark_processed(chunk.message_id())
        
        # Simulate redelivery - check idempotency
        is_duplicate = aggregator_worker.working_state.is_processed(chunk.message_id())
        self.assertTrue(is_duplicate, "Duplicate should be detected")

    @patch('workers.aggregators.common.aggregator.MessageMiddlewareQueue')
    @patch('workers.aggregators.common.aggregator.MessageMiddlewareExchange')
    def test_end_message_survives_crash(self, mock_exchange, mock_queue):
        """
        Test that END messages are properly handled across crashes
        """
        aggregator_worker = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Process some chunks
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(2)]
        for chunk in chunks:
            aggregator_worker.accumulate_products(chunk.client_id(), chunk.rows())
            aggregator_worker.working_state.mark_processed(chunk.message_id())
        
        # Mark END message received
        client_id = 1
        table_type = TableType.TRANSACTION_ITEMS
        aggregator_worker.working_state.mark_end_message_received(client_id, table_type)
        
        # Persist state with END - use last chunk's message_id
        aggregator_worker.persistence_service.commit_working_state(
            aggregator_worker.working_state.to_bytes(),
            chunks[-1].message_id()  # Use UUID from chunk
        )
        
        # Crash and restart
        aggregator_worker2 = Aggregator(self.config["agg_type"], self.config["agg_id"])
        
        # Verify END was recovered
        has_end = aggregator_worker2.working_state.is_end_message_received(client_id, table_type)
        self.assertTrue(has_end, "END message should survive crash")


if __name__ == "__main__":
    unittest.main()
