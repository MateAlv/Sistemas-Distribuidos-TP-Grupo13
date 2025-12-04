import pytest
import uuid
from unittest.mock import Mock, MagicMock, patch
from collections import defaultdict

from workers.maximizers.common.maximizer import Maximizer
from workers.maximizers.common.maximizer_working_state import MaximizerWorkingState
from utils.eof_protocol.end_messages import MessageForceEnd
from utils.file_utils.table_type import TableType
from utils.processing.process_table import TransactionItemsProcessRow, PurchasesPerUserStoreRow
from utils.common.processing_types import MonthYear

class TestMaximizerForceEnd:
    """Tests for Maximizer force-end functionality"""
    
    @pytest.fixture
    def maximizer_instance(self):
        """Create maximizer instance with mocked middleware"""
        with patch('workers.maximizers.common.maximizer.MessageMiddlewareQueue'), \
             patch('workers.maximizers.common.maximizer.MessageMiddlewareExchange'), \
             patch('workers.maximizers.common.maximizer.PersistenceService') as mock_persistence:
            
            # Mock persistence service to avoid real file I/O
            mock_persistence_instance = Mock()
            mock_persistence_instance.recover_working_state.return_value = None
            mock_persistence_instance.recover_last_processing_chunk.return_value = None
            mock_persistence_instance.commit_working_state = Mock()
            mock_persistence.return_value = mock_persistence_instance
            
            maximizer_obj = Maximizer("MAX", "absolute", expected_inputs=2)
            
            # Replace persistence service with a proper mock after initialization
            maximizer_obj.persistence = Mock()
            maximizer_obj.persistence.commit_working_state = Mock()
            
            return maximizer_obj
    
    def test_delete_specific_client_max(self, maximizer_instance):
        """Test deleting data for a specific client (MAX type)"""
        # Setup: Add data for multiple clients
        # Client 1
        maximizer_instance.working_state.sellings_max[1][(101, MonthYear(1, 2024))] = 50
        maximizer_instance.working_state.profit_max[1][(101, MonthYear(1, 2024))] = 500.0
        maximizer_instance.working_state.mark_client_end_processed(1)
        maximizer_instance.working_state.mark_results_sent(1, "max-partial")
        maximizer_instance.received_shards[1].add("shard1")
        
        # Client 2
        maximizer_instance.working_state.sellings_max[2][(102, MonthYear(1, 2024))] = 30
        maximizer_instance.working_state.profit_max[2][(102, MonthYear(1, 2024))] = 300.0
        maximizer_instance.received_shards[2].add("shard2")
        
        # Action: Delete client 1
        maximizer_instance.delete_client_data(1)
        
        # Assert: Client 1 data deleted
        assert 1 not in maximizer_instance.working_state.sellings_max
        assert 1 not in maximizer_instance.working_state.profit_max
        assert not maximizer_instance.working_state.is_client_end_processed(1)
        assert not maximizer_instance.working_state.results_already_sent(1, "max-partial")
        assert 1 not in maximizer_instance.received_shards
        
        # Assert: Client 2 data remains
        assert 2 in maximizer_instance.working_state.sellings_max
        assert 2 in maximizer_instance.working_state.profit_max
        assert 2 in maximizer_instance.received_shards
        
        # Assert: Persistence called
        maximizer_instance.persistence.commit_working_state.assert_called_once()
    
    def test_delete_specific_client_top3(self, maximizer_instance):
        """Test deleting data for a specific client (TOP3 type)"""
        maximizer_instance.maximizer_type = "TOP3"
        
        # Setup: Add data for multiple clients
        # Client 1
        maximizer_instance.working_state.top3_by_store[1][1001] = defaultdict(int)
        maximizer_instance.working_state.top3_by_store[1][1001][501] = 10
        maximizer_instance.working_state.mark_client_end_processed(1)
        
        # Client 2
        maximizer_instance.working_state.top3_by_store[2][1002] = defaultdict(int)
        maximizer_instance.working_state.top3_by_store[2][1002][502] = 5
        
        # Action: Delete client 1
        maximizer_instance.delete_client_data(1)
        
        # Assert: Client 1 data deleted
        assert 1 not in maximizer_instance.working_state.top3_by_store
        assert not maximizer_instance.working_state.is_client_end_processed(1)
        
        # Assert: Client 2 data remains
        assert 2 in maximizer_instance.working_state.top3_by_store
    
    def test_delete_specific_client_tpv(self, maximizer_instance):
        """Test deleting data for a specific client (TPV type)"""
        maximizer_instance.maximizer_type = "TPV"
        
        # Setup: Add data for multiple clients
        # Client 1
        maximizer_instance.working_state.update_tpv(1, 1001, "2024-H1", 1000.0)
        
        # Client 2
        maximizer_instance.working_state.update_tpv(2, 1002, "2024-H1", 500.0)
        
        # Action: Delete client 1
        maximizer_instance.delete_client_data(1)
        
        # Assert: Client 1 data deleted
        assert 1 not in maximizer_instance.working_state.tpv_aggregated
        
        # Assert: Client 2 data remains
        assert 2 in maximizer_instance.working_state.tpv_aggregated
    
    def test_delete_all_clients(self, maximizer_instance):
        """Test deleting data for all clients (client_id=-1)"""
        # Setup: Add data for multiple clients
        maximizer_instance.working_state.sellings_max[1][(101, MonthYear(1, 2024))] = 50
        maximizer_instance.working_state.sellings_max[2][(102, MonthYear(1, 2024))] = 30
        maximizer_instance.received_shards[1].add("shard1")
        maximizer_instance.received_shards[2].add("shard2")
        
        # Action: Delete all clients
        maximizer_instance.delete_client_data(-1)
        
        # Assert: All data deleted
        assert len(maximizer_instance.working_state.sellings_max) == 0
        assert len(maximizer_instance.received_shards) == 0
        
        # Assert: Persistence called
        maximizer_instance.persistence.commit_working_state.assert_called_once()
    
    def test_maximizer_working_state_get_all_client_ids(self):
        """Test get_all_client_ids method"""
        state = MaximizerWorkingState()
        
        # Setup: Add data in different structures
        state.sellings_max[1][(101, MonthYear(1, 2024))] = 50
        state.top3_by_store[2][1001] = defaultdict(int)
        state.top3_by_store[2][1001][501] = 10
        state.update_tpv(3, 1002, "2024-H1", 1000.0)
        state.mark_client_end_processed(4)
        
        # Action: Get all IDs
        ids = state.get_all_client_ids()
        
        # Assert
        assert ids == {1, 2, 3, 4}

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
