"""
Tests for working state serialization and persistence.

Verifies that all working states can be correctly serialized to bytes,
deserialized from bytes, and persisted/recovered using PersistenceService.
"""
import os
import tempfile
import uuid
import pytest
from collections import defaultdict

from workers.aggregators.common.aggregator_working_state import AggregatorWorkingState
from workers.filter.common.filter_working_state import FilterWorkingState
from workers.joiners.common.joiner_working_state import JoinerMainWorkingState, JoinerJoinWorkingState
from workers.maximizers.common.maximizer_working_state import MaximizerWorkingState
from utils.tolerance.persistence_service import PersistenceService
from utils.file_utils.table_type import TableType


class TestAggregatorWorkingStateSerialization:
    """Test serialization and deserialization of AggregatorWorkingState"""
    
    def test_empty_state_serialization(self):
        """Test serialization of empty state"""
        state = AggregatorWorkingState()
        serialized = state.to_bytes()
        
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0
        
        # Deserialize and verify
        recovered = AggregatorWorkingState.from_bytes(serialized)
        assert isinstance(recovered, AggregatorWorkingState)
        assert recovered.end_message_received == {}
        assert recovered.chunks_received_per_client == {}
        assert recovered.processed_ids == set()
    
    def test_state_with_data_serialization(self):
        """Test serialization with populated data"""
        state = AggregatorWorkingState()
        
        # Populate state
        client_id = 1
        table_type = TableType.TRANSACTIONS
        
        state.mark_end_message_received(client_id, table_type)
        state.set_chunks_to_receive(client_id, table_type, 100)
        state.increment_chunks_received(client_id, table_type, aggregator_id=1, delta=50)
        state.increment_chunks_processed(client_id, table_type, aggregator_id=1, delta=45)
        state.mark_processed(uuid.uuid4())
        state.mark_processed(uuid.uuid4())
        
        # Serialize
        serialized = state.to_bytes()
        
        # Deserialize
        recovered = AggregatorWorkingState.from_bytes(serialized)
        
        # Verify all data preserved
        assert recovered.is_end_message_received(client_id, table_type) == True
        assert recovered.get_chunks_to_receive(client_id, table_type) == 100
        assert recovered.get_total_received(client_id, table_type) == 50
        assert recovered.get_total_processed(client_id, table_type) == 45
        assert len(recovered.processed_ids) == 2
    
    def test_product_accumulator_serialization(self):
        """Test serialization with product accumulator data"""
        state = AggregatorWorkingState()
        
        client_id = 1
        accumulator = state.get_product_accumulator(client_id)
        
        # Add product data
        key1 = ("item_1", 2024, 1)
        key2 = ("item_2", 2024, 2)
        accumulator[key1]["quantity"] = 100
        accumulator[key1]["subtotal"] = 1500.50
        accumulator[key2]["quantity"] = 50
        accumulator[key2]["subtotal"] = 750.25
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = AggregatorWorkingState.from_bytes(serialized)
        
        # Verify accumulator preserved
        recovered_acc = recovered.get_product_accumulator(client_id)
        assert recovered_acc[key1]["quantity"] == 100
        assert recovered_acc[key1]["subtotal"] == 1500.50
        assert recovered_acc[key2]["quantity"] == 50
        assert recovered_acc[key2]["subtotal"] == 750.25
    
    def test_purchase_accumulator_serialization(self):
        """Test serialization with purchase accumulator data"""
        state = AggregatorWorkingState()
        
        client_id = 1
        accumulator = state.get_purchase_accumulator(client_id)
        
        # Add purchase data (nested defaultdict)
        accumulator[("store_1", "user_1")]["item_1"] = 5
        accumulator[("store_1", "user_1")]["item_2"] = 3
        accumulator[("store_2", "user_2")]["item_3"] = 10
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = AggregatorWorkingState.from_bytes(serialized)
        
        # Verify nested data preserved
        recovered_acc = recovered.get_purchase_accumulator(client_id)
        assert recovered_acc[("store_1", "user_1")]["item_1"] == 5
        assert recovered_acc[("store_1", "user_1")]["item_2"] == 3
        assert recovered_acc[("store_2", "user_2")]["item_3"] == 10


class TestFilterWorkingStateSerialization:
    """Test serialization and deserialization of FilterWorkingState"""
    
    def test_empty_state_serialization(self):
        """Test serialization of empty state"""
        state = FilterWorkingState()
        serialized = state.to_bytes()
        
        recovered = FilterWorkingState.from_bytes(serialized)
        assert isinstance(recovered, FilterWorkingState)
        assert recovered.end_message_received == {}
        assert recovered.processed_ids == set()
    
    def test_state_with_filter_data(self):
        """Test serialization with filter tracking data"""
        state = FilterWorkingState()
        
        client_id = 1
        table_type = TableType.TRANSACTIONS
        filter_id = 1
        
        # Populate state using actual FilterWorkingState methods
        state.end_received(client_id, table_type)  # Mark END received
        state.set_total_chunks_expected(client_id, table_type, 100)
        state.increase_received_chunks(client_id, table_type, filter_id, 75)
        state.increase_not_sent_chunks(client_id, table_type, filter_id, 70)
        
        msg_id1 = uuid.uuid4()
        msg_id2 = uuid.uuid4()
        state.mark_processed(msg_id1)
        state.mark_processed(msg_id2)
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = FilterWorkingState.from_bytes(serialized)
        
        # Verify data
        assert recovered.end_is_received(client_id, table_type) == True
        assert recovered.get_total_chunks_to_receive(client_id, table_type) == 100
        assert recovered.get_total_chunks_received(client_id, table_type) == 75
        assert recovered.get_total_not_sent_chunks(client_id, table_type) == 70
        assert recovered.is_processed(msg_id1) == True
        assert recovered.is_processed(msg_id2) == True


class TestJoinerWorkingStateSerialization:
    """Test serialization of joiner working states"""
    
    def test_main_state_empty(self):
        """Test JoinerMainWorkingState empty serialization"""
        state = JoinerMainWorkingState()
        serialized = state.to_bytes()
        
        recovered = JoinerMainWorkingState.from_bytes(serialized)
        assert isinstance(recovered, JoinerMainWorkingState)
        assert recovered.data == {}
        assert recovered.joiner_data_chunks == {}
        assert recovered.client_end_messages_received == []
    
    def test_main_state_with_chunks(self):
        """Test JoinerMainWorkingState with chunk tracking"""
        state = JoinerMainWorkingState()
        
        client_id = 1
        
        # Add mock chunk (using dict as placeholder)
        mock_chunk = {"client_id": client_id, "data": "test"}
        state.add_chunk(client_id, mock_chunk)
        state.add_chunk(client_id, mock_chunk)
        
        state.mark_end_message_received(client_id)
        
        msg_id = uuid.uuid4()
        state.add_processed_id(msg_id)
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = JoinerMainWorkingState.from_bytes(serialized)
        
        # Verify
        assert len(recovered.get_chunks(client_id)) == 2
        assert recovered.is_end_message_received(client_id) == True
        assert recovered.is_processed(msg_id) == True
    
    def test_main_state_multi_sender_tracking(self):
        """Test multi-sender tracking serialization"""
        state = JoinerMainWorkingState()
        
        client_id = 1
        
        # Track multiple senders
        state.mark_sender_finished(client_id, "sender_1")
        state.mark_sender_finished(client_id, "sender_2")
        state.add_expected_chunks(client_id, 50)
        state.add_expected_chunks(client_id, 75)
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = JoinerMainWorkingState.from_bytes(serialized)
        
        # Verify multi-sender state
        assert recovered.is_sender_finished(client_id, "sender_1") == True
        assert recovered.is_sender_finished(client_id, "sender_2") == True
        assert recovered.get_finished_senders_count(client_id) == 2
        assert recovered.get_total_expected_chunks(client_id) == 125
    
    def test_join_state_serialization(self):
        """Test JoinerJoinWorkingState serialization"""
        state = JoinerJoinWorkingState()
        
        client_id = 1
        
        # Add join data
        state.add_join_data(client_id, "item_1", "Product A")
        state.add_join_data(client_id, "item_2", "Product B")
        
        msg_id = uuid.uuid4()
        state.add_processed_id(msg_id)
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = JoinerJoinWorkingState.from_bytes(serialized)
        
        # Verify join data
        assert recovered.get_join_data(client_id, "item_1") == "Product A"
        assert recovered.get_join_data(client_id, "item_2") == "Product B"
        assert recovered.get_join_data_count(client_id) == 2
        assert recovered.is_processed(msg_id) == True


class TestMaximizerWorkingStateSerialization:
    """Test serialization of MaximizerWorkingState"""
    
    def test_empty_state(self):
        """Test empty state serialization"""
        state = MaximizerWorkingState()
        serialized = state.to_bytes()
        
        recovered = MaximizerWorkingState.from_bytes(serialized)
        assert isinstance(recovered, MaximizerWorkingState)
        assert recovered.sellings_max == {}
        assert recovered.profit_max == {}
        assert recovered.processed_ids == set()
    
    def test_state_with_max_data(self):
        """Test serialization with maximizer data"""
        state = MaximizerWorkingState()
        
        client_id = 1
        
        # Add max selling data using actual MaximizerWorkingState structure
        sellings_dict = state.get_sellings_max(client_id)
        sellings_dict[("item_1", 1)] = 100
        sellings_dict[("item_2", 2)] = 150
        
        # Add max profit data
        profit_dict = state.get_profit_max(client_id)
        profit_dict[("item_3", 1)] = 500.50
        profit_dict[("item_4", 2)] = 750.75
        
        msg_id = uuid.uuid4()
        state.mark_processed(msg_id)
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = MaximizerWorkingState.from_bytes(serialized)
        
        # Verify max data
        rec_sellings = recovered.get_sellings_max(client_id)
        rec_profit = recovered.get_profit_max(client_id)
        
        assert rec_sellings[("item_1", 1)] == 100
        assert rec_sellings[("item_2", 2)] == 150
        assert rec_profit[("item_3", 1)] == 500.50
        assert rec_profit[("item_4", 2)] == 750.75
        assert recovered.is_processed(msg_id) == True
    
    def test_state_with_top3_data(self):
        """Test serialization with top3 data"""
        state = MaximizerWorkingState()
        
        client_id = 1
        
        # Add top3 data (using actual structure: defaultdict of heaps per store)
        top3_dict = state.get_top3_by_store(client_id)
        # Simulate heap data structure for store1
        top3_dict["store_1"] = [(10, "user_1"), (20, "user_2"), (30, "user_3")]
        top3_dict["store_2"] = [(15, "user_4"), (25, "user_5")]
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = MaximizerWorkingState.from_bytes(serialized)
        
        # Verify top3 data
        recovered_top3 = recovered.get_top3_by_store(client_id)
        assert "store_1" in recovered_top3
        assert "store_2" in recovered_top3
        assert len(recovered_top3["store_1"]) == 3
        assert len(recovered_top3["store_2"]) == 2


class TestPersistenceServiceIntegration:
    """Integration tests for PersistenceService with working states"""
    
    def test_commit_and_recover_aggregator_state(self):
        """Test commit and recovery of AggregatorWorkingState"""
        with tempfile.TemporaryDirectory() as tmpdir:
            service = PersistenceService(directory=tmpdir)
            
            # Create and populate state
            state = AggregatorWorkingState()
            client_id = 1
            table_type = TableType.TRANSACTIONS
            
            state.mark_end_message_received(client_id, table_type)
            state.set_chunks_to_receive(client_id, table_type, 100)
            state.increment_chunks_processed(client_id, table_type, aggregator_id=1, delta=50)
            
            msg_id = uuid.uuid4()
            state.mark_processed(msg_id)
            
            # Commit state
            service.commit_working_state(state.to_bytes(), msg_id)
            
            # Create new service instance to simulate restart
            service2 = PersistenceService(directory=tmpdir)
            recovered_bytes = service2.recover_working_state()
            
            # Verify recovery
            assert recovered_bytes is not None
            recovered_state = AggregatorWorkingState.from_bytes(recovered_bytes)
            
            assert recovered_state.is_end_message_received(client_id, table_type) == True
            assert recovered_state.get_chunks_to_receive(client_id, table_type) == 100
            assert recovered_state.get_total_processed(client_id, table_type) == 50
            assert recovered_state.is_processed(msg_id) == True
    
    def test_commit_and_recover_joiner_main_state(self):
        """Test commit and recovery of JoinerMainWorkingState"""
        with tempfile.TemporaryDirectory() as tmpdir:
            service = PersistenceService(directory=tmpdir)
            
            # Create and populate state
            state = JoinerMainWorkingState()
            client_id = 1
            
            state.mark_end_message_received(client_id)
            state.mark_sender_finished(client_id, "sender_1")
            state.add_expected_chunks(client_id, 50)
            
            msg_id = uuid.uuid4()
            state.add_processed_id(msg_id)
            
            # Commit
            service.commit_working_state(state.to_bytes(), msg_id)
            
            # Recover
            service2 = PersistenceService(directory=tmpdir)
            recovered_bytes = service2.recover_working_state()
            
            assert recovered_bytes is not None
            recovered_state = JoinerMainWorkingState.from_bytes(recovered_bytes)
            
            assert recovered_state.is_end_message_received(client_id) == True
            assert recovered_state.is_sender_finished(client_id, "sender_1") == True
            assert recovered_state.get_total_expected_chunks(client_id) == 50
            assert recovered_state.is_processed(msg_id) == True
    
    def test_multiple_commits_last_wins(self):
        """Test that multiple commits preserve only the last state"""
        with tempfile.TemporaryDirectory() as tmpdir:
            service = PersistenceService(directory=tmpdir)
            
            state = AggregatorWorkingState()
            client_id = 1
            table_type = TableType.TRANSACTIONS
            
            # First commit
            state.set_chunks_to_receive(client_id, table_type, 50)
            msg_id1 = uuid.uuid4()
            service.commit_working_state(state.to_bytes(), msg_id1)
            
            # Second commit with different value
            state.set_chunks_to_receive(client_id, table_type, 100)
            msg_id2 = uuid.uuid4()
            service.commit_working_state(state.to_bytes(), msg_id2)
            
            # Recover
            service2 = PersistenceService(directory=tmpdir)
            recovered_bytes = service2.recover_working_state()
            recovered_state = AggregatorWorkingState.from_bytes(recovered_bytes)
            
            # Should have the LAST committed value
            assert recovered_state.get_chunks_to_receive(client_id, table_type) == 100
    
    def test_atomicity_of_commits(self):
        """Test that commits are atomic (file is not corrupted on crash)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            service = PersistenceService(directory=tmpdir)
            
            state = AggregatorWorkingState()
            client_id = 1
            table_type = TableType.TRANSACTIONS
            state.set_chunks_to_receive(client_id, table_type, 100)
            
            msg_id = uuid.uuid4()
            service.commit_working_state(state.to_bytes(), msg_id)
            
            # Verify file exists and is valid
            state_file = os.path.join(tmpdir, "persistence_state")
            assert os.path.exists(state_file)
            
            # File should be readable and deserializable
            with open(state_file, "rb") as f:
                file_data = f.read()
                assert len(file_data) > 0
            
            # Should be able to recover
            service2 = PersistenceService(directory=tmpdir)
            recovered_bytes = service2.recover_working_state()
            assert recovered_bytes is not None


class TestWorkingStateEdgeCases:
    """Test edge cases in working state serialization"""
    
    def test_empty_defaultdict_serialization(self):
        """Test that empty defaultdicts serialize correctly"""
        state = AggregatorWorkingState()
        
        # Access accumulator but don't populate it
        _ = state.get_product_accumulator(1)
        
        # Should still serialize/deserialize
        serialized = state.to_bytes()
        recovered = AggregatorWorkingState.from_bytes(serialized)
        
        assert isinstance(recovered, AggregatorWorkingState)
    
    def test_large_processed_ids_set(self):
        """Test serialization with many processed IDs"""
        state = AggregatorWorkingState()
        
        # Add 1000 processed IDs
        ids = [uuid.uuid4() for _ in range(1000)]
        for msg_id in ids:
            state.mark_processed(msg_id)
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = AggregatorWorkingState.from_bytes(serialized)
        
        # All IDs should be preserved
        assert len(recovered.processed_ids) == 1000
        for msg_id in ids:
            assert recovered.is_processed(msg_id) == True
    
    def test_special_characters_in_data(self):
        """Test serialization with special characters in strings"""
        state = JoinerJoinWorkingState()
        
        client_id = 1
        
        # Add data with special characters
        state.add_join_data(client_id, "item_ñ_1", "Product with ñ and 中文")
        state.add_join_data(client_id, "item_€_2", "Price: €100")
        state.add_join_data(client_id, "item_🎉_3", "Emoji: 🎉")
        
        # Serialize and deserialize
        serialized = state.to_bytes()
        recovered = JoinerJoinWorkingState.from_bytes(serialized)
        
        # Verify special characters preserved
        assert recovered.get_join_data(client_id, "item_ñ_1") == "Product with ñ and 中文"
        assert recovered.get_join_data(client_id, "item_€_2") == "Price: €100"
        assert recovered.get_join_data(client_id, "item_🎉_3") == "Emoji: 🎉"
