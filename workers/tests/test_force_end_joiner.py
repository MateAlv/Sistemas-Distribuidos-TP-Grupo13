import pytest
import uuid
from unittest.mock import Mock, MagicMock, patch
from collections import defaultdict

from workers.joiners.common.joiner import Joiner, ITEMS_JOINER
from workers.joiners.common.joiner_working_state import JoinerMainWorkingState, JoinerJoinWorkingState
from utils.eof_protocol.end_messages import MessageForceEnd


class TestJoinerForceEnd:
    """Tests for Joiner force-end functionality"""
    
    @pytest.fixture
    def joiner_instance(self):
        """Create joiner instance with mocked middleware"""
        with patch('workers.joiners.common.joiner.MessageMiddlewareExchange') as mock_exchange, \
             patch('workers.joiners.common.joiner.PersistenceService') as mock_persistence:
            
            # Mock persistence service to avoid real file I/O
            mock_persistence_instance = Mock()
            mock_persistence_instance.recover_working_state.return_value = None
            mock_persistence_instance.recover_last_processing_chunk.return_value = None
            mock_persistence_instance.commit_working_state = Mock()
            mock_persistence.return_value = mock_persistence_instance
            
            # Create joiner - it will call define_queues which is abstract but mocked
            with patch.object(Joiner, 'define_queues', return_value=None):
                joiner_obj = Joiner(ITEMS_JOINER, expected_inputs=2)
                
                # Replace persistence services with proper mocks after initialization
                joiner_obj.persistence_main = Mock()
                joiner_obj.persistence_main.commit_working_state = Mock()
                joiner_obj.persistence_join = Mock()
                joiner_obj.persistence_join.commit_working_state = Mock()
                
                return joiner_obj
    
    def test_delete_specific_client(self, joiner_instance):
        """Test deleting data for a specific client"""
        # Setup: Add data for multiple clients
        joiner_instance.working_state_main.add_data(1, ["row1", "row2"])
        joiner_instance.working_state_main.add_data(2, ["row3"])
        joiner_instance.working_state_main.mark_end_message_received(1)
        joiner_instance.working_state_main.mark_end_message_received(2)
        joiner_instance.working_state_main.set_ready_to_join(1)
        joiner_instance.working_state_main.set_ready_to_join(2)
        
        joiner_instance.working_state_join.add_join_data(1, "item1", "Product 1")
        joiner_instance.working_state_join.add_join_data(2, "item2", "Product 2")
        
        # Action: Delete client 1
        joiner_instance.delete_client_data(1)
        
        # Assert: Client 1 data deleted, client 2 remains
        assert 1 not in joiner_instance.working_state_main.data
        assert 2 in joiner_instance.working_state_main.data
        assert not joiner_instance.working_state_main.is_end_message_received(1)
        assert joiner_instance.working_state_main.is_end_message_received(2)
        assert not joiner_instance.working_state_main.is_ready_flag_set(1)
        assert joiner_instance.working_state_main.is_ready_flag_set(2)
        
        # Check join state
        assert 1 not in joiner_instance.working_state_join.joiner_data
        assert 2 in joiner_instance.working_state_join.joiner_data
        
        # Assert: Both persistence services called
        joiner_instance.persistence_main.commit_working_state.assert_called_once()
        joiner_instance.persistence_join.commit_working_state.assert_called_once()
    
    def test_delete_all_clients(self, joiner_instance):
        """Test deleting data for all clients (client_id=-1)"""
        # Setup: Add data for multiple clients
        joiner_instance.working_state_main.add_data(1, ["row1", "row2"])
        joiner_instance.working_state_main.add_data(2, ["row3"])
        joiner_instance.working_state_main.mark_end_message_received(1)
        joiner_instance.working_state_main.mark_end_message_received(2)
        
        joiner_instance.working_state_join.add_join_data(1, "item1", "Product 1")
        joiner_instance.working_state_join.add_join_data(2, "item2", "Product 2")
        
        # Action: Delete all clients (client_id=-1)
        joiner_instance.delete_client_data(-1)
        
        # Assert: All data cleared from main state
        assert len(joiner_instance.working_state_main.data) == 0
        assert len(joiner_instance.working_state_main.client_end_messages_received) == 0
        
        # Assert: All data cleared from join state
        assert len(joiner_instance.working_state_join.joiner_data) == 0
        
        # Assert: Both persistence services called
        joiner_instance.persistence_main.commit_working_state.assert_called_once()
        joiner_instance.persistence_join.commit_working_state.assert_called_once()
    
    def test_joiner_main_working_state_delete_client_data(self):
        """Test JoinerMainWorkingState.delete_client_data method"""
        state = JoinerMainWorkingState()
        
        # Setup: Add data for multiple clients
        state.add_data(1, [Mock(), Mock()])
        state.add_data(2, [Mock()])
        state.mark_end_message_received(1)
        state.mark_end_message_received(2)
        state.set_ready_to_join(1)
        state.set_ready_to_join(2)
        state.mark_client_completed(1)
        state.mark_sender_finished(1, "sender1")
        state.mark_sender_finished(2, "sender1")
        state.add_expected_chunks(1, 10)
        state.add_expected_chunks(2, 5)
        
        # Action: Delete client 1
        state.delete_client_data(1)
        
        # Assert: Client 1 data deleted
        assert 1 not in state.data
        assert 1 not in state.ready_to_join
        assert 1 not in state.finished_senders
        assert 1 not in state.total_expected_chunks
        assert 1 not in state.client_end_messages_received
        assert 1 not in state.completed_clients
        
        # Assert: Client 2 data remains
        assert 2 in state.data
        assert state.is_end_message_received(2)
        assert state.is_ready_flag_set(2)
        assert state.get_total_expected_chunks(2) == 5
    
    def test_joiner_join_working_state_delete_client_data(self):
        """Test JoinerJoinWorkingState.delete_client_data method"""
        state = JoinerJoinWorkingState()
        
        # Setup: Add join data for multiple clients
        state.add_join_data(1, "item1", "Product 1")
        state.add_join_data(1, "item2", "Product 2")
        state.add_join_data(2, "item3", "Product 3")
        
        # Action: Delete client 1
        state.delete_client_data(1)
        
        # Assert: Client 1 data deleted
        assert 1 not in state.joiner_data
        assert state.get_join_data_count(1) == 0
        
        # Assert: Client 2 data remains
        assert 2 in state.joiner_data
        assert state.get_join_data_count(2) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
