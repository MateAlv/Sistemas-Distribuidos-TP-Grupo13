import os
import sys
import datetime
import unittest
from unittest.mock import MagicMock, patch
from collections import defaultdict

# Add root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Mock pika before import
sys.modules["pika"] = MagicMock()

from workers.maximizers.common.maximizer import Maximizer
from workers.maximizers.common.maximizer_working_state import MaximizerWorkingState
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import (
    TransactionItemsProcessRow,
    TPVProcessRow,
)
from utils.file_utils.table_type import TableType
from utils.file_utils.file_table import DateTime
from utils.common.processing_types import MonthYear
from utils.eof_protocol.end_messages import MessageEnd


class TestMaximizerFaultTolerance(unittest.TestCase):
    def setUp(self):
        self.env_patcher = patch.dict(
            os.environ,
            {
                "AGGREGATOR_SHARDS": "1",
                "MAX_SHARDS": "1",
                "TOP3_SHARDS": "1",
                "TPV_SHARDS": "1",
            },
        )
        self.env_patcher.start()

        # PersistenceService mock (avoid disk)
        self.persistence_patcher = patch("workers.maximizers.common.maximizer.PersistenceService")
        self.MockPersistence = self.persistence_patcher.start()
        self.mock_persistence = self.MockPersistence.return_value
        self.mock_persistence.recover_working_state.return_value = None
        self.mock_persistence.recover_last_processing_chunk.return_value = None
        self.mock_persistence.commit_processing_chunk = MagicMock()
        self.mock_persistence.commit_working_state = MagicMock()
        self.mock_persistence.clear_processing_commit = MagicMock()

        # Middleware mocks
        self.queue_patcher = patch("workers.maximizers.common.maximizer.MessageMiddlewareQueue")
        self.exchange_patcher = patch("workers.maximizers.common.maximizer.MessageMiddlewareExchange")
        self.MockQueue = self.queue_patcher.start()
        self.MockExchange = self.exchange_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.persistence_patcher.stop()
        self.queue_patcher.stop()
        self.exchange_patcher.stop()

    def _make_tx_chunk(self, table_type=TableType.TRANSACTION_ITEMS, client_id=1, message_id=None):
        header = ProcessChunkHeader(client_id=client_id, table_type=table_type, message_id=message_id)
        if table_type == TableType.TRANSACTION_ITEMS:
            row = TransactionItemsProcessRow(
                transaction_id="tx1",
                item_id=100,
                quantity=5,
                subtotal=50.0,
                created_at=DateTime(datetime.date(2024, 1, 1), datetime.time(10, 0)),
            )
            return ProcessChunk(header, [row])
        elif table_type == TableType.TPV:
            row = TPVProcessRow(
                store_id=1,
                tpv=10.0,
                year_half="2024-H1",
                shard_id="1",
            )
            return ProcessChunk(header, [row])
        return ProcessChunk(header, [])

    def test_process_chunk_persists_and_clears_commit(self):
        maximizer = Maximizer("MAX", "absolute")
        chunk = self._make_tx_chunk()
        maximizer._handle_data_chunk(chunk.serialize())

        self.mock_persistence.commit_processing_chunk.assert_called_once()
        self.mock_persistence.commit_working_state.assert_called()
        self.mock_persistence.clear_processing_commit.assert_called_once()
        self.assertIn(chunk.message_id(), maximizer.working_state.processed_ids)

    def test_invalid_table_type_clears_commit(self):
        maximizer = Maximizer("MAX", "absolute")
        bad_chunk = self._make_tx_chunk(table_type=TableType.TPV)
        maximizer._handle_data_chunk(bad_chunk.serialize())

        self.mock_persistence.commit_processing_chunk.assert_called()
        self.mock_persistence.clear_processing_commit.assert_called()
        self.mock_persistence.commit_working_state.assert_not_called()

    def test_deterministic_ids_for_all_outputs(self):
        client_id = 1

        # MAX absolute
        max_abs = Maximizer("MAX", "absolute")
        max_abs.data_sender.send = MagicMock()
        max_abs.working_state.sellings_max[client_id][(1, MonthYear(1, 2024))] = 10
        max_abs.publish_absolute_max_results(client_id)
        id1 = ProcessChunkHeader.deserialize(max_abs.data_sender.send.call_args[0][0][:28]).message_id
        max_abs.publish_absolute_max_results(client_id)
        id2 = ProcessChunkHeader.deserialize(max_abs.data_sender.send.call_args[0][0][:28]).message_id
        self.assertEqual(id1, id2)

        # TOP3 absolute
        top_abs = Maximizer("TOP3", "absolute")
        top_abs.data_sender.send = MagicMock()
        top_abs.working_state.top3_by_store[client_id][1] = defaultdict(int, {10: 5})
        top_abs.publish_absolute_top3_results(client_id)
        t1 = ProcessChunkHeader.deserialize(top_abs.data_sender.send.call_args[0][0][:28]).message_id
        top_abs.publish_absolute_top3_results(client_id)
        t2 = ProcessChunkHeader.deserialize(top_abs.data_sender.send.call_args[0][0][:28]).message_id
        self.assertEqual(t1, t2)

        # TPV absolute
        tpv_abs = Maximizer("TPV", "absolute")
        tpv_abs.data_sender.send = MagicMock()
        tpv_abs.working_state.tpv_aggregated[client_id][(1, "2024-H1")] = 10.0
        tpv_abs.publish_tpv_results(client_id)
        p1 = ProcessChunkHeader.deserialize(tpv_abs.data_sender.send.call_args[0][0][:28]).message_id
        tpv_abs.publish_tpv_results(client_id)
        p2 = ProcessChunkHeader.deserialize(tpv_abs.data_sender.send.call_args[0][0][:28]).message_id
        self.assertEqual(p1, p2)

    def test_idempotent_results_and_end_skip_on_flags(self):
        maximizer = Maximizer("MAX", "absolute")
        client_id = 1
        label_end = f"end-{maximizer.stage}"
        maximizer.working_state.mark_results_sent(client_id, maximizer._results_label())
        maximizer.working_state.mark_end_sent(client_id, label_end)

        maximizer.data_sender.send = MagicMock()
        with patch.object(maximizer, "delete_client_data") as mock_del:
            maximizer.process_client_end(client_id, TableType.TRANSACTION_ITEMS)
            # No sends because flags already set
            maximizer.data_sender.send.assert_not_called()
            mock_del.assert_called_once()
            self.assertTrue(maximizer.working_state.is_client_end_processed(client_id))

    def test_resume_pending_finalization_resends(self):
        client_id = 1
        maximizer = Maximizer("TPV", "absolute")
        maximizer.expected_shards = 1
        maximizer.working_state.finished_senders[client_id].add("agg1")
        maximizer.working_state.tpv_aggregated[client_id][(1, "2024-H1")] = 5.0
        maximizer.data_sender.send = MagicMock()

        with patch.object(maximizer, "delete_client_data") as mock_del:
            maximizer._resume_pending_finalization()
            mock_del.assert_called_once()
            # After finalization (before delete), end was marked processed
            self.assertTrue(maximizer.working_state.is_client_end_processed(client_id))

    def test_process_client_end_marks_after_send(self):
        maximizer = Maximizer("MAX", "absolute")
        client_id = 1
        maximizer.working_state.sellings_max[client_id][(1, MonthYear(1, 2024))] = 5
        maximizer.data_sender.send = MagicMock()

        with patch.object(maximizer, "delete_client_data") as mock_del:
            maximizer.process_client_end(client_id, TableType.TRANSACTION_ITEMS)
            self.assertTrue(maximizer.working_state.is_client_end_processed(client_id))
            mock_del.assert_called_once()

    def test_retry_after_publish_failure(self):
        maximizer = Maximizer("MAX", "absolute")
        client_id = 1
        maximizer.working_state.sellings_max[client_id][(1, MonthYear(1, 2024))] = 5
        maximizer.data_sender.send = MagicMock()

        original_publish = maximizer.publish_absolute_max_results
        publish_state = {"first": True}

        def side_effect(*args, **kwargs):
            if publish_state["first"]:
                publish_state["first"] = False
                raise Exception("boom")
            return original_publish(*args, **kwargs)

        with patch.object(maximizer, "publish_absolute_max_results", side_effect=side_effect) as mock_pub, \
             patch.object(maximizer, "_send_end_message", wraps=maximizer._send_end_message) as mock_end, \
             patch.object(maximizer, "delete_client_data") as mock_del:
            with self.assertRaises(Exception):
                maximizer.process_client_end(client_id, TableType.TRANSACTION_ITEMS)
            self.assertFalse(maximizer.working_state.is_client_end_processed(client_id))

            maximizer.process_client_end(client_id, TableType.TRANSACTION_ITEMS)
            self.assertTrue(maximizer.working_state.is_client_end_processed(client_id))
            self.assertEqual(mock_pub.call_count, 2)
            mock_end.assert_called()
            mock_del.assert_called_once()

    def test_send_end_idempotent(self):
        maximizer = Maximizer("MAX", "absolute")
        maximizer.data_sender.send = MagicMock()
        client_id = 1
        maximizer._send_end_message(client_id, TableType.TRANSACTION_ITEMS, "joiner", 0, "abs")
        maximizer._send_end_message(client_id, TableType.TRANSACTION_ITEMS, "joiner", 0, "abs")
        self.assertEqual(maximizer.data_sender.send.call_count, 1)

    def test_resume_when_only_end_missing(self):
        client_id = 1
        maximizer = Maximizer("TPV", "absolute")
        maximizer.expected_shards = 1
        maximizer.working_state.finished_senders[client_id].add("agg1")
        maximizer.working_state.tpv_aggregated[client_id][(1, "2024-H1")] = 5.0
        maximizer.working_state.mark_results_sent(client_id, maximizer._results_label())
        maximizer.data_sender.send = MagicMock()

        with patch.object(maximizer, "delete_client_data") as mock_del:
            maximizer._resume_pending_finalization()
            mock_del.assert_called_once()
            self.assertTrue(maximizer.working_state.is_client_end_processed(client_id))

    def test_completion_despite_shard_tracking_loss(self):
        """
        Even if received_shards is lost on crash, END counts in working_state should drive completion.
        """
        client_id = 1
        maximizer = Maximizer("TPV", "absolute")
        maximizer.expected_shards = 1
        # Process data to populate state and in-memory received_shards
        chunk = self._make_tx_chunk(table_type=TableType.TPV, client_id=client_id)
        chunk.rows[0].shard_id = "shard_1"
        maximizer._handle_data_chunk(chunk.serialize())
        self.assertTrue(len(maximizer.received_shards[client_id]) > 0)

        # Simulate crash: persist working_state only
        state_bytes = maximizer.working_state.to_bytes()

        # Restart
        maximizer_rec = Maximizer("TPV", "absolute")
        maximizer_rec.expected_shards = 1
        maximizer_rec.working_state = MaximizerWorkingState.from_bytes(state_bytes)
        maximizer_rec.data_sender.send = MagicMock()
        maximizer_rec.middleware_coordination.send = MagicMock()

        # shard tracking in memory is empty
        self.assertEqual(len(maximizer_rec.received_shards[client_id]), 0)

        end_msg = MessageEnd(client_id, TableType.TPV, 1, "shard_1")

        with patch.object(maximizer_rec, "delete_client_data") as mock_del:
            maximizer_rec._handle_end_message(end_msg.encode())
            self.assertTrue(maximizer_rec.working_state.is_sender_finished(client_id, "shard_1"))
            self.assertTrue(maximizer_rec.working_state.is_client_end_processed(client_id))
            mock_del.assert_called_once()
            # END and results should have been sent
            self.assertGreaterEqual(maximizer_rec.data_sender.send.call_count, 1)

    def test_completion_despite_shard_tracking_loss(self):
        """
        Test that completion depends on persisted END counts, not ephemeral received_shards.
        Even if received_shards is lost on crash, receiving all ENDs should trigger completion.
        """
        client_id = 1
        # TPV Absolute waits for expected_shards
        maximizer = Maximizer("TPV", "absolute")
        maximizer.expected_shards = 1
        
        # 1. Process data (updates received_shards in memory)
        chunk = self._make_tx_chunk(table_type=TableType.TPV, client_id=client_id)
        chunk.rows[0].shard_id = "shard_1"
        maximizer._handle_data_chunk(chunk.serialize())
        
        # Verify in-memory tracking (may store a derived slug)
        self.assertGreater(len(maximizer.received_shards[client_id]), 0)
        
        # 2. Simulate Crash & Restart
        state_bytes = maximizer.working_state.to_bytes()
        
        maximizer_rec = Maximizer("TPV", "absolute")
        maximizer_rec.expected_shards = 1
        maximizer_rec.working_state = MaximizerWorkingState.from_bytes(state_bytes)
        
        # Verify shard tracking is LOST (as expected/accepted)
        self.assertEqual(len(maximizer_rec.received_shards[client_id]), 0)
        
        # 3. Send END message
        # This should trigger completion if logic relies on finished_senders (which is in working_state)
        # We need to mock dependencies for completion
        maximizer_rec.data_sender.send = MagicMock()
        maximizer_rec.middleware_coordination.send = MagicMock()
        
        # Construct END message
        end_msg = MessageEnd(client_id, TableType.TPV, 1, "shard_1")
        
        with patch.object(maximizer_rec, "process_client_end", wraps=maximizer_rec.process_client_end) as mock_process:
            with patch.object(maximizer_rec, "delete_client_data"):
                maximizer_rec._handle_end_message(end_msg.encode())
                
                # 4. Verify Completion
                # Should have marked sender finished
                self.assertTrue(maximizer_rec.working_state.is_sender_finished(client_id, "shard_1"))
                # Should have triggered process_client_end
                mock_process.assert_called_once()
                # Should have marked client end processed
                self.assertTrue(maximizer_rec.working_state.is_client_end_processed(client_id))


if __name__ == "__main__":
    unittest.main()
