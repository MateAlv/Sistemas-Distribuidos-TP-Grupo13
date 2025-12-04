import json
import uuid
from unittest.mock import Mock, patch

import pytest

from workers.filter.common.filter import Filter
from utils.file_utils.table_type import TableType
from utils.eof_protocol.end_messages import MessageEnd
from utils.protocol import STAGE_AGG_TPV, STAGE_AGG_PRODUCTS, STAGE_AGG_PURCHASES


def _make_filter(filter_type="hour", filter_id=1, tpv_shards=3, products_shards=3):
    cfg = {
        "id": filter_id,
        "filter_type": filter_type,
        "year_start": 2024,
        "year_end": 2025,
    }
    with patch("workers.filter.common.filter.MessageMiddlewareQueue") as mock_q, patch(
        "workers.filter.common.filter.MessageMiddlewareExchange"
    ) as mock_exch, patch("workers.filter.common.filter.PersistenceService") as mock_p:
        mock_p.return_value.recover_working_state.return_value = None
        mock_p.return_value.recover_last_processing_chunk.return_value = None
        f = Filter(cfg)
        f.persistence_service = Mock()
        f.persistence_service.commit_working_state = Mock()
        f.tpv_shards = tpv_shards
        f.products_shards = products_shards
        # coordination exchange mock send
        f.middleware_coordination = Mock()
        f.middleware_coordination.send = Mock()
        # end exchange mock send
        f.middleware_end_exchange = Mock()
        f.middleware_end_exchange.send = Mock()
        # queues dict with MagicMocks
        f.middleware_queue_sender = {}
        return f, mock_q


def _decode_end_sent(mock_queue_send):
    """Helper to decode MessageEnd from mock call."""
    args, _ = mock_queue_send.call_args
    return MessageEnd.decode(args[0])


class TestFilterEOFSynchronization:
    def test_hour_end_sends_zero_expected_for_empty_shard(self, monkeypatch):
        """Hour filter sends END and stats for all agg_tpv shards, even those with 0 chunks."""
        monkeypatch.setenv("TPV_SHARDS", "3")
        f, _ = _make_filter(filter_type="hour", tpv_shards=3)

        client_id = 1
        table_type = TableType.TRANSACTIONS
        total_expected = 4
        total_not_sent = 1
        # Simulate chunks sent only to shard 1
        f.shard_chunks_sent[(STAGE_AGG_TPV, client_id, table_type, 1)] = 3
        # shard 2/3 not present -> treated as 0

        # Act
        f._send_end_message(client_id, table_type, total_expected, total_not_sent)

        # Assert END sent to all three shard queues
        for shard_id, expected_chunks in [(1, 3), (2, 0), (3, 0)]:
            queue_name = f"to_agg_tpv_shard_{shard_id}"
            assert queue_name in f.middleware_queue_sender
            mock_queue = f.middleware_queue_sender[queue_name]
            mock_queue.send.assert_called_once()
            end_msg = _decode_end_sent(mock_queue.send)
            assert end_msg.total_chunks() == expected_chunks

        # Assert per-shard expected stats to monitor (coordination) for next stage
        # 3 shards -> 3 sends
        assert f.middleware_coordination.send.call_count == 4  # 1 barrier END + 3 per-shard stats
        per_shard_payloads = [
            json.loads(call.args[0].decode()) for call in f.middleware_coordination.send.call_args_list[1:]
        ]
        shards_expected = {(int(p["shard"]), p["expected"]) for p in per_shard_payloads}
        assert shards_expected == {(1, 3), (2, 0), (3, 0)}

    def test_year_items_end_sends_expected_per_product_shard(self, monkeypatch):
        """Year filter sends END and expected stats per agg_products shard."""
        monkeypatch.setenv("PRODUCTS_SHARDS", "2")
        f, _ = _make_filter(filter_type="year", products_shards=2)
        client_id = 2
        table_type = TableType.TRANSACTION_ITEMS
        total_expected = 5
        total_not_sent = 0
        f.shard_chunks_sent[(STAGE_AGG_PRODUCTS, client_id, table_type, 1)] = 5
        # shard 2 should be zero

        f._send_end_message(client_id, table_type, total_expected, total_not_sent)

        for shard_id, expected_chunks in [(1, 5), (2, 0)]:
            queue_name = f"to_agg_products_shard_{shard_id}"
            mock_queue = f.middleware_queue_sender[queue_name]
            end_msg = _decode_end_sent(mock_queue.send)
            assert end_msg.total_chunks() == expected_chunks

    def test_end_message_triggers_send_when_expected_met(self, monkeypatch):
        """_handle_end_message should call _send_end_message when expected == received + not_sent."""
        monkeypatch.setenv("TPV_SHARDS", "1")
        f, _ = _make_filter(filter_type="hour", tpv_shards=1)
        client_id = 3
        table_type = TableType.TRANSACTIONS
        total_expected = 2
        # Pretend we already processed 2 chunks
        f.working_state.increase_received_chunks(client_id, table_type, f.id, 2)
        f.working_state.set_total_chunks_expected(client_id, table_type, total_expected)
        f.working_state.end_received(client_id, table_type)

        with patch.object(f, "_send_end_message") as mock_send_end:
            end_msg = MessageEnd(client_id, table_type, total_expected, "sender")
            f._handle_end_message(end_msg.encode())
            mock_send_end.assert_called_once()

    def test_year_transactions_end_sends_expected_per_purchases_shard(self, monkeypatch):
        """Year filter sends END and expected stats per agg_purchases shard (including zeros)."""
        monkeypatch.setenv("PURCHASES_SHARDS", "3")
        f, _ = _make_filter(filter_type="year", products_shards=1)
        client_id = 5
        table_type = TableType.TRANSACTIONS
        total_expected = 4
        total_not_sent = 1
        # Only shard 2 got chunks
        f.shard_chunks_sent[(STAGE_AGG_PURCHASES, client_id, table_type, 2)] = 3

        f._send_end_message(client_id, table_type, total_expected, total_not_sent)

        for shard_id, expected_chunks in [(1, 0), (2, 3), (3, 0)]:
            queue_name = f"to_agg_purchases_shard_{shard_id}"
            assert queue_name in f.middleware_queue_sender
            mock_queue = f.middleware_queue_sender[queue_name]
            end_msg = _decode_end_sent(mock_queue.send)
            assert end_msg.total_chunks() == expected_chunks

        # Coordination stats: 1 barrier END + 3 per-shard expected stats
        assert f.middleware_coordination.send.call_count == 4
        per_shard_payloads = [
            json.loads(call.args[0].decode()) for call in f.middleware_coordination.send.call_args_list[1:]
        ]
        shards_expected = {(int(p["shard"]), p["expected"]) for p in per_shard_payloads}
        assert shards_expected == {(1, 0), (2, 3), (3, 0)}

    def test_amount_end_creates_merge_queue_and_sends_query_end(self, monkeypatch):
        """Amount filter should create merge queue on END even if no chunks were sent."""
        f, mock_q = _make_filter(filter_type="amount")
        client_id = 7
        table_type = TableType.TRANSACTIONS
        total_expected = 0
        total_not_sent = 0

        # Ensure merge queue not present
        assert f.middleware_queue_sender == {}

        f._send_end_message(client_id, table_type, total_expected, total_not_sent)

        queue_name = f"to_merge_data_{client_id}"
        assert queue_name in f.middleware_queue_sender
        mock_queue = f.middleware_queue_sender[queue_name]
        mock_queue.send.assert_called_once()
        sent = mock_queue.send.call_args[0][0]
        # For amount, it sends MessageQueryEnd; just ensure bytes were sent
        assert sent

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
