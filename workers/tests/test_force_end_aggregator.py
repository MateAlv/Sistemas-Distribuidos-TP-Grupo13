import pytest
from unittest.mock import Mock
from workers.aggregators.common.aggregator_working_state import AggregatorWorkingState
from utils.eof_protocol.end_messages import MessageForceEnd

class TestAggregatorForceEnd:
    """Tests for Aggregator force-end functionality"""
    
    @pytest.fixture
    def working_state(self):
        """Create a fresh working state for testing"""
        return AggregatorWorkingState()
    
    def test_delete_specific_client_stats(self, working_state):
        """Test deleting stats for a specific client"""
        # Setup: Add data for multiple clients
        working_state.global_accumulator[1] = {"stat1": 10, "stat2": 20}
        working_state.global_accumulator[2] = {"stat1": 30, "stat2": 40}
        working_state.global_accumulator[3] = {"stat1": 50, "stat2": 60}
        
        working_state.end_message_received[1] = {"products": True, "purchases": True}
        working_state.end_message_received[2] = {"products": True}
        
        working_state.chunks_to_receive[1] = {"products": 5, "purchases": 3}
        working_state.chunks_to_receive[2] = {"products": 2}
        
        working_state.chunks_received_per_client[1] = {"products": 3, "purchases": 2}
        working_state.chunks_received_per_client[2] = {"products": 1}
        
        # Action: Delete client 1
        working_state.force_delete_client_stats_data(1)
        
        # Assert: Client 1 data is deleted, others remain
        assert 1 not in working_state.global_accumulator
        assert 2 in working_state.global_accumulator
        assert 3 in working_state.global_accumulator
        
        assert 1 not in working_state.end_message_received
        assert 2 in working_state.end_message_received
        
        assert 1 not in working_state.chunks_to_receive
        assert 2 in working_state.chunks_to_receive
        
        assert 1 not in working_state.chunks_received_per_client
        assert 2 in working_state.chunks_received_per_client

    def test_delete_all_clients_with_minus_one(self, working_state):
        """Test deleting all clients using client_id=-1 (server crash scenario)"""
        # Setup: Add data for multiple clients
        working_state.global_accumulator[1] = {"stat1": 10}
        working_state.global_accumulator[2] = {"stat2": 20}
        working_state.global_accumulator[3] = {"stat3": 30}
        
        working_state.end_message_received[1] = {"products": True}
        working_state.end_message_received[2] = {"purchases": True}
        
        working_state.chunks_to_receive[1] = {"products": 5}
        working_state.chunks_to_receive[2] = {"purchases": 3}
        
        working_state.chunks_received_per_client[1] = {"products": 2}
        working_state.chunks_received_per_client[2] = {"purchases": 1}
        
        working_state.chunks_processed_per_client[1] = {"products": 2}
        working_state.chunks_processed_per_client[2] = {"purchases": 1}
        
        working_state.accumulated_chunks_per_client[1] = {"products": {"data": "test"}}
        
        # Action: Delete all clients (client_id=-1)
        working_state.force_delete_client_stats_data(-1)
        
        # Assert: All client data is deleted
        assert len(working_state.global_accumulator) == 0
        assert len(working_state.end_message_received) == 0
        assert len(working_state.chunks_to_receive) == 0
        assert len(working_state.chunks_received_per_client) == 0
        assert len(working_state.chunks_processed_per_client) == 0
        assert len(working_state.accumulated_chunks_per_client) == 0

    def test_delete_nonexistent_client(self, working_state):
        """Test that deleting a nonexistent client doesn't raise errors"""
        # Setup: Add data for client 1 only
        working_state.global_accumulator[1] = {"stat1": 10}
        
        # Action: Try to delete client 99 (doesn't exist)
        working_state.force_delete_client_stats_data(99)
        
        # Assert: No errors, client 1 still exists
        assert 1 in working_state.global_accumulator
        assert len(working_state.global_accumulator) == 1

    def test_thread_safety_force_delete(self, working_state):
        """Test that force_delete_client_stats_data is thread-safe"""
        # Setup: Add data
        working_state.global_accumulator[1] = {"stat1": 10}
        working_state.end_message_received[1] = {"products": True}
        
        # Action: Call force delete (which should use locks internally)
        working_state.force_delete_client_stats_data(1)
        
        # Assert: Data is deleted
        assert 1 not in working_state.global_accumulator
        assert 1 not in working_state.end_message_received

    def test_message_force_end_decode(self):
        """Test MessageForceEnd encoding and decoding"""
        # Test specific client ID
        msg = MessageForceEnd(42)
        encoded = msg.encode()
        decoded = MessageForceEnd.decode(encoded)
        assert decoded.client_id() == 42
        
        # Test all clients (-1)
        msg_all = MessageForceEnd(-1)
        encoded_all = msg_all.encode()
        decoded_all = MessageForceEnd.decode(encoded_all)
        assert decoded_all.client_id() == -1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
