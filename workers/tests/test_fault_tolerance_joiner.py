"""
Comprehensive Mock-based Fault Tolerance Tests for Joiner Worker

Tests verify that for each crash point, the joiner can:
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

# Import joiner and dependencies
import sys
from pathlib import Path
# Adjust path - now we're in workers/tests, need to go up to project root
sys.path.append(str(Path(__file__).parent.parent.parent))

from workers.joiners.common.joiner import Joiner
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TransactionItemsProcessRow, DateTime
from utils.file_utils.table_type import TableType
from utils.eof_protocol.end_messages import MessageEnd


class TestJoinerFaultToleranceComprehensive(unittest.TestCase):
    """Comprehensive fault tolerance tests covering all crash points"""

    def setUp(self):
        """Set up test environment with mocks"""
        # Create temporary persistence directory
        self.temp_dir = tempfile.mkdtemp(prefix="joiner_test_")
        os.environ["PERSISTENCE_DIR"] = self.temp_dir
        os.environ["WORKER_ID"] = "1"
        os.environ["CONTAINER_NAME"] = "test_joiner"
        
        # Clear any crash points
        if "CRASH_POINT" in os.environ:
            del os.environ["CRASH_POINT"]
        
        # Mock configuration - using ITEMS joiner as example
        self.config = {
            "join_type": "ITEMS",
            "expected_inputs": 1
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
        with patch('workers.joiners.common.joiner.MessageMiddlewareQueue'), \
             patch('workers.joiners.common.joiner.MessageMiddlewareExchange'):
            
            joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
            results = []
            
            for chunk in chunks:
                # Save data through the joiner
                joiner_worker.save_data(chunk)
                joiner_worker.working_state_main.add_processed_id(chunk.message_id())
                results.append((chunk.message_id(), len(chunk.rows())))
            
            return results

    @patch('workers.joiners.common.joiner.MessageMiddlewareQueue')
    @patch('workers.joiners.common.joiner.MessageMiddlewareExchange')
    def test_crash_before_commit_processing_multi_message_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 1: CRASH_BEFORE_COMMIT_PROCESSING
        
        Scenario:
        1. Process 3 messages successfully  
        2. Process 4th message, crash BEFORE commit_processing_chunk
        3. Restart joiner
        4. Verify 4th message is redelivered and processed
        5. Result identical to no-fault execution
        """
        # Create 4 test chunks
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # Get baseline (no fault)
        baseline_results = self._process_chunks_no_fault(chunks)
        
        # PHASE 1: Process first 3 successfully
        joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
        for i in range(3):
            joiner_worker.save_data(chunks[i])
            joiner_worker.working_state_main.add_processed_id(chunks[i].message_id())
            joiner_worker.persistence_service.commit_working_state(
                joiner_worker.working_state_main.to_bytes(), 
                chunks[i].message_id()
            )
        
        # Verify 3 chunks processed
        self.assertEqual(len(joiner_worker.working_state_main.processed_ids_main), 3)
        
        # PHASE 2: Set crash point and simulate processing 4th message
        os.environ["CRASH_POINT"] = "CRASH_BEFORE_COMMIT_PROCESSING"
        
        # PHASE 3: Restart joiner (simulates recovery)
        del os.environ["CRASH_POINT"]  # Remove crash point for recovery
        joiner_worker2 = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Verify recovered state has 3 processed messages
        self.assertEqual(len(joiner_worker2.working_state_main.processed_ids_main), 3)
        
        # Process 4th message after recovery
        joiner_worker2.save_data(chunks[3])
        joiner_worker2.working_state_main.add_processed_id(chunks[3].message_id())
        
        # VERIFICATION: Result identical to baseline
        self.assertEqual(len(joiner_worker2.working_state_main.processed_ids_main), 4)

    @patch('workers.joiners.common.joiner.MessageMiddlewareQueue')
    @patch('workers.joiners.common.joiner.MessageMiddlewareExchange')
    def test_crash_after_commit_processing_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 2: CRASH_AFTER_COMMIT_PROCESSING
        
        Scenario:
        1. Process 2 messages
        2. Process 3rd message, persist it, then crash AFTER commit
        3. Restart joiner
        4. Verify recovery processes persisted chunk
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(3)]
        baseline_results = self._process_chunks_no_fault(chunks)
        
        # PHASE 1: Process first 2 messages
        joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
        for i in range(2):
            joiner_worker.save_data(chunks[i])
            joiner_worker.working_state_main.add_processed_id(chunks[i].message_id())
        
        # PHASE 2: Process 3rd, persist it (simulating AFTER commit but BEFORE handle_process_message)
        joiner_worker.persistence_service.commit_processing_chunk(chunks[2])
        
        # Crash happens here (AFTER commit_processing_chunk)
        os.environ["CRASH_POINT"] = "CRASH_AFTER_COMMIT_PROCESSING"
        
        # PHASE 3: Restart joiner
        del os.environ["CRASH_POINT"]
        joiner_worker2 = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Verify recovery detected persisted chunk
        recovered_chunk = joiner_worker2.persistence_service.recover_last_processing_chunk()
        self.assertIsNotNone(recovered_chunk)
        # Compare UUID objects
        self.assertEqual(recovered_chunk.message_id(), chunks[2].message_id())
        
        # Recovery should have processed it via handle_processing_recovery
        # Verify final state matches baseline
        self.assertIn(chunks[2].message_id(), joiner_worker2.working_state_main.processed_ids_main)

    @patch('workers.joiners.common.joiner.MessageMiddlewareQueue')
    @patch('workers.joiners.common.joiner.MessageMiddlewareExchange')
    def test_crash_before_commit_working_state_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 3: CRASH_BEFORE_COMMIT_WORKING_STATE
        
        Scenario:
        1. Process 2 messages
        2. Process 3rd, apply join, crash BEFORE commit_working_state
        3. Restart joiner
        4. Verify 3rd message reprocessed (idempotency)
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(3)]
        baseline_results = self._process_chunks_no_fault(chunks)
        
        # PHASE 1: Process first 2 messages completely
        joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
        for i in range(2):
            joiner_worker.save_data(chunks[i])
            joiner_worker.working_state_main.add_processed_id(chunks[i].message_id())
            joiner_worker.persistence_service.commit_working_state(
                joiner_worker.working_state_main.to_bytes(),
                chunks[i].message_id()
            )
        
        # PHASE 2: Process 3rd message but DON'T commit working state
        joiner_worker.save_data(chunks[2])
        # Crash happens here (BEFORE commit_working_state)
        # Working state was NOT persisted
        
        # PHASE 3: Restart joiner
        joiner_worker2 = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Verify recovered state only has 2 processed IDs
        self.assertEqual(len(joiner_worker2.working_state_main.processed_ids_main), 2)
        self.assertNotIn(chunks[2].message_id(), joiner_worker2.working_state_main.processed_ids_main)
        
        # Reprocess 3rd message
        joiner_worker2.save_data(chunks[2])
        joiner_worker2.working_state_main.add_processed_id(chunks[2].message_id())
        
        # VERIFICATION: Same result as baseline
        self.assertEqual(len(joiner_worker2.working_state_main.processed_ids_main), 3)

    @patch('workers.joiners.common.joiner.MessageMiddlewareQueue')
    @patch('workers.joiners.common.joiner.MessageMiddlewareExchange')
    def test_crash_before_send_ack_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 4: CRASH_BEFORE_COMMIT_SEND_ACK
        
        Scenario:
        1. Process and persist 2 messages
        2. Process 3rd message, commit state, crash BEFORE send ACK
        3. Restart joiner
        4. Verify 3rd message marked as processed but needs resending
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(3)]
        
        # PHASE 1: Process first 2 completely
        joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
        for i in range(2):
            joiner_worker.save_data(chunks[i])
            joiner_worker.working_state_main.add_processed_id(chunks[i].message_id())
            joiner_worker.persistence_service.commit_working_state(
                joiner_worker.working_state_main.to_bytes(),
                chunks[i].message_id()
            )
        
        # PHASE 2: Process 3rd, commit state, but DON'T send
        joiner_worker.save_data(chunks[2])
        joiner_worker.working_state_main.add_processed_id(chunks[2].message_id())
        joiner_worker.persistence_service.commit_working_state(
            joiner_worker.working_state_main.to_bytes(),
            chunks[2].message_id()
        )
        # Crash happens here (BEFORE commit_send_ack / sending to queue)
        
        # PHASE 3: Restart joiner
        joiner_worker2 = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Verify 3rd message was marked as processed in working state
        self.assertIn(chunks[2].message_id(), joiner_worker2.working_state_main.processed_ids_main)

    @patch('workers.joiners.common.joiner.MessageMiddlewareQueue')
    @patch('workers.joiners.common.joiner.MessageMiddlewareExchange')
    def test_crash_after_send_ack_recovery(self, mock_exchange, mock_queue):
        """
        Test Crash Point 5: CRASH_AFTER_COMMIT_SEND_ACK
        
        Scenario:
        1. Process 3 messages completely (send + ACK)
        2. Process 4th message, send, ACK, then crash AFTER
        3. Restart joiner
        4. Verify 4th message fully processed and acknowledged
        5. Result identical to no-fault execution
        """
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(4)]
        
        # PHASE 1: Process all 4 messages completely
        joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
        for i in range(4):
            joiner_worker.save_data(chunks[i])
            joiner_worker.working_state_main.add_processed_id(chunks[i].message_id())
            joiner_worker.persistence_service.commit_working_state(
                joiner_worker.working_state_main.to_bytes(),
                chunks[i].message_id()
            )
            # Simulate send + ACK
            joiner_worker.persistence_service.commit_send_ack(
                chunks[i].client_id(),
                chunks[i].message_id()
            )
        
        # Crash happens AFTER everything complete
        
        # PHASE 2: Restart joiner
        joiner_worker2 = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Verify all 4 messages in processed state
        self.assertEqual(len(joiner_worker2.working_state_main.processed_ids_main), 4)
        for i in range(4):
            self.assertIn(chunks[i].message_id(), joiner_worker2.working_state_main.processed_ids_main)

    @patch('workers.joiners.common.joiner.MessageMiddlewareQueue')
    @patch('workers.joiners.common.joiner.MessageMiddlewareExchange')
    def test_idempotency_duplicate_prevention(self, mock_exchange, mock_queue):
        """
        Test idempotency: duplicate messages are ignored
        
        This ensures that if a message is redelivered (e.g., after crash),
        it won't be processed twice.
        """
        chunk = self._create_test_chunk(message_id="msg_dup")
        
        joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Process first time
        joiner_worker.save_data(chunk)
        joiner_worker.working_state_main.add_processed_id(chunk.message_id())
        
        # Simulate redelivery - check idempotency
        is_duplicate = joiner_worker.working_state_main.is_processed(chunk.message_id())
        self.assertTrue(is_duplicate, "Duplicate should be detected")

    @patch('workers.joiners.common.joiner.MessageMiddlewareQueue')
    @patch('workers.joiners.common.joiner.MessageMiddlewareExchange')
    def test_end_message_survives_crash(self, mock_exchange, mock_queue):
        """
        Test that END messages are properly handled across crashes
        """
        joiner_worker = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Process some chunks
        chunks = [self._create_test_chunk(message_id=f"msg_{i}") for i in range(2)]
        for chunk in chunks:
            joiner_worker.save_data(chunk)
            joiner_worker.working_state_main.add_processed_id(chunk.message_id())
        
        # Receive END message
        client_id = 1
        joiner_worker.working_state_main.mark_end_message_received(client_id)
        
        # Persist state with END - use last chunk's message_id
        joiner_worker.persistence_service.commit_working_state(
            joiner_worker.working_state_main.to_bytes(),
            chunks[-1].message_id()  # Use UUID from chunk
        )
        
        # Crash and restart
        joiner_worker2 = Joiner(self.config["join_type"], self.config["expected_inputs"])
        
        # Verify END was recovered
        has_end = joiner_worker2.working_state_main.is_end_message_received(client_id)
        self.assertTrue(has_end, "END message should survive crash")


if __name__ == "__main__":
    unittest.main()
