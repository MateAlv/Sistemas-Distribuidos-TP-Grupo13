import pytest
import uuid
from unittest.mock import Mock, MagicMock, patch
from collections import defaultdict

from workers.filter.common.filter import Filter
from workers.filter.common.filter_working_state import FilterWorkingState
from utils.eof_protocol.end_messages import MessageForceEnd
from utils.file_utils.table_type import TableType


class TestFilterForceEnd:
    """Tests for Filter force-end functionality"""
    
    @pytest.fixture
    def filter_config(self):
        """Basic filter configuration for year filter"""
        return {
            "id": 1,
            "filter_type": "year",
            "year_start": 2024,
            "year_end": 2025,
        }
    
    @pytest.fixture
    def filter_instance(self, filter_config):
        """Create filter instance with mocked middleware"""
        with patch('workers.filter.common.filter.MessageMiddlewareQueue'), \
             patch('workers.filter.common.filter.MessageMiddlewareExchange'), \
             patch('workers.filter.common.filter.PersistenceService') as mock_persistence:
            
            # Mock persistence service to avoid real file I/O
            mock_persistence_instance = Mock()
            mock_persistence_instance.recover_working_state.return_value = None
            mock_persistence_instance.recover_last_processing_chunk.return_value = None
            mock_persistence_instance.commit_working_state = Mock()
            mock_persistence.return_value = mock_persistence_instance
            
            filter_obj = Filter(filter_config)
            # Replace persistence service with a proper mock after initialization
            filter_obj.persistence_service = Mock()
            filter_obj.persistence_service.commit_working_state = Mock()
            
            return filter_obj
    
    def test_delete_specific_client(self, filter_instance):
        """Test deleting data for a specific client"""
        # Setup: Add data for multiple clients
        filter_instance.working_state.increase_received_chunks(1, TableType.TRANSACTIONS, 1, 5)
        filter_instance.working_state.increase_received_chunks(2, TableType.TRANSACTIONS, 1, 3)
        filter_instance.chunk_counters[("agg_products", 1, TableType.TRANSACTION_ITEMS)] = 10
        filter_instance.chunk_counters[("agg_products", 2, TableType.TRANSACTION_ITEMS)] = 5
        filter_instance.shard_chunks_sent[("STAGE", 1, TableType.TRANSACTIONS, 1)] = 8
        filter_instance.shard_chunks_sent[("STAGE", 2, TableType.TRANSACTIONS, 1)] = 4
        
        # Action: Delete client 1
        filter_instance.delete_client_data(1)
        
        # Assert: Client 1 data deleted, client 2 remains
        assert filter_instance.working_state.get_total_chunks_received(1, TableType.TRANSACTIONS) == 0
        assert filter_instance.working_state.get_total_chunks_received(2, TableType.TRANSACTIONS) == 3
        
        # Check chunk_counters cleaned for client 1
        remaining_counters = [k for k in filter_instance.chunk_counters.keys() if k[1] == 1]
        assert len(remaining_counters) == 0
        remaining_counters_client2 = [k for k in filter_instance.chunk_counters.keys() if k[1] == 2]
        assert len(remaining_counters_client2) == 1
        
        # Check shard_chunks_sent cleaned for client 1
        remaining_shards = [k for k in filter_instance.shard_chunks_sent.keys() if k[1] == 1]
        assert len(remaining_shards) == 0
        remaining_shards_client2 = [k for k in filter_instance.shard_chunks_sent.keys() if k[1] == 2]
        assert len(remaining_shards_client2) == 1
        
        # Assert: Persistence called
        filter_instance.persistence_service.commit_working_state.assert_called_once()
    
    def test_delete_all_clients(self, filter_instance):
        """Test deleting data for all clients (client_id=-1)"""
        # Setup: Add data for multiple clients
        filter_instance.working_state.increase_received_chunks(1, TableType.TRANSACTIONS, 1, 5)
        filter_instance.working_state.increase_received_chunks(2, TableType.TRANSACTIONS, 1, 3)
        filter_instance.chunk_counters[("agg_products", 1, TableType.TRANSACTION_ITEMS)] = 10
        filter_instance.chunk_counters[("agg_products", 2, TableType.TRANSACTION_ITEMS)] = 5
        filter_instance.shard_chunks_sent[("STAGE", 1, TableType.TRANSACTIONS, 1)] = 8
        filter_instance.shard_chunks_sent[("STAGE", 2, TableType.TRANSACTIONS, 1)] = 4
        
        # Action: Delete all clients (client_id=-1)
        filter_instance.delete_client_data(-1)
        
        # Assert: All data cleared
        assert filter_instance.working_state.get_total_chunks_received(1, TableType.TRANSACTIONS) == 0
        assert filter_instance.working_state.get_total_chunks_received(2, TableType.TRANSACTIONS) == 0
        
        # Check all counters cleared
        assert len(filter_instance.chunk_counters) == 0
        assert len(filter_instance.shard_chunks_sent) == 0
        
        # Assert: Persistence called
        filter_instance.persistence_service.commit_working_state.assert_called_once()
    
    def test_force_end_message_decode(self):
        """Test MessageForceEnd encoding and decoding"""
        # Test specific client
        msg = MessageForceEnd(5)
        encoded = msg.encode()
        decoded = MessageForceEnd.decode(encoded)
        assert decoded.client_id() == 5
        
        # Test all clients signal
        msg_all = MessageForceEnd(-1)
        encoded_all = msg_all.encode()
        decoded_all = MessageForceEnd.decode(encoded_all)
        assert decoded_all.client_id() == -1
    
    def test_filter_working_state_delete_client_data(self):
        """Test FilterWorkingState.delete_client_data method"""
        state = FilterWorkingState()
        
        # Setup: Add data for multiple clients
        state.increase_received_chunks(1, TableType.TRANSACTIONS, 1, 5)
        state.increase_received_chunks(2, TableType.TRANSACTIONS, 1, 3)
        state.increase_not_sent_chunks(1, TableType.TRANSACTIONS, 1, 2)
        state.set_total_chunks_expected(1, TableType.TRANSACTIONS, 10)
        state.set_total_chunks_expected(2, TableType.TRANSACTIONS, 8)
        state.end_received(1, TableType.TRANSACTIONS)
        state.mark_stats_sent(1, TableType.TRANSACTIONS, 5, 2)
        
        # Action: Delete client 1
        state.delete_client_data(1)
        
        # Assert: Client 1 data deleted
        assert 1 not in state.chunks_received_per_filter
        assert 1 not in state.chunks_not_sent_per_filter
        assert 1 not in state.number_of_chunks_to_receive
        assert 1 not in state.end_message_received
        assert (1, TableType.TRANSACTIONS) not in state.already_sent_stats
        
        # Assert: Client 2 data remains
        assert state.get_total_chunks_received(2, TableType.TRANSACTIONS) == 3
        assert state.get_total_chunks_to_receive(2, TableType.TRANSACTIONS) == 8


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
