"""
Basic test to verify chunk buffer functionality.
Run with: python3 -m pytest utils/tolerance/tests/test_chunk_buffer_basic.py -v
"""

import os
import tempfile
import uuid
from utils.tolerance.chunk_buffer import ChunkBuffer
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.file_utils.table_type import TableType
from utils.processing.process_table import TransactionItemsProcessRow
from utils.file_utils.file_table import DateTime
import datetime


def test_append_and_recover_single_chunk():
    """Test basic append and recovery of a single chunk."""
    with tempfile.TemporaryDirectory() as tmpdir:
        buffer_path = os.path.join(tmpdir, "test_buffer")
        buffer = ChunkBuffer(buffer_path)
        
        # Create a test chunk
        header = ProcessChunkHeader(client_id=1, table_type=TableType.TRANSACTION_ITEMS)
        row = TransactionItemsProcessRow(
            transaction_id="test_tx_1",
            item_id=100,
            quantity=5,
            subtotal=25.50,
            created_at=DateTime(datetime.date(2024, 1, 1), datetime.time(12, 0))
        )
        chunk = ProcessChunk(header, [row])
        
        # Append chunk
        buffer.append_chunk(chunk)
        
        # Verify buffer size is non-zero
        assert buffer.get_buffer_size() > 0
        
        # Recover chunks
        recovered = buffer.read_all_chunks()
        
        # Verify we recovered 1 chunk
        assert len(recovered) == 1
        assert recovered[0].client_id() == 1
        assert recovered[0].table_type() == TableType.TRANSACTION_ITEMS
        assert len(recovered[0].rows) == 1
        assert recovered[0].rows[0].transaction_id == "test_tx_1"
        
        print("✓ Single chunk append and recovery test passed")


def test_append_multiple_chunks():
    """Test appending and recovering multiple chunks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        buffer_path = os.path.join(tmpdir, "test_buffer")
        buffer = ChunkBuffer(buffer_path)
        
        # Create and append 5 chunks
        num_chunks = 5
        for i in range(num_chunks):
            header = ProcessChunkHeader(client_id=i, table_type=TableType.TRANSACTION_ITEMS)
            row = TransactionItemsProcessRow(
                transaction_id=f"test_tx_{i}",
                item_id=100 + i,
                quantity=i,
                subtotal=10.0 * i,
                created_at=DateTime(datetime.date(2024, 1, i+1), datetime.time(12, 0))
            )
            chunk = ProcessChunk(header, [row])
            buffer.append_chunk(chunk)
        
        # Recover all chunks
        recovered = buffer.read_all_chunks()
        
        # Verify count
        assert len(recovered) == num_chunks
        
        # Verify order is preserved
        for i, chunk in enumerate(recovered):
            assert chunk.client_id() == i
            assert chunk.rows[0].transaction_id == f"test_tx_{i}"
        
        print(f"✓ Multiple chunks ({num_chunks}) append and recovery test passed")


def test_clear_buffer():
    """Test buffer clearing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        buffer_path = os.path.join(tmpdir, "test_buffer")
        buffer = ChunkBuffer(buffer_path)
        
        # Append a chunk
        header = ProcessChunkHeader(client_id=1, table_type=TableType.TRANSACTION_ITEMS)
        row = TransactionItemsProcessRow(
            transaction_id="test_tx",
            item_id=100,
            quantity=5,
            subtotal=25.50,
            created_at=DateTime(datetime.date(2024, 1, 1), datetime.time(12, 0))
        )
        chunk = ProcessChunk(header, [row])
        buffer.append_chunk(chunk)
        
        # Verify buffer is not empty
        assert buffer.get_buffer_size() > 0
        
        # Clear buffer
        buffer.clear()
        
        # Verify buffer is empty
        assert buffer.get_buffer_size() == 0
        
        # Verify no chunks are recovered
        recovered = buffer.read_all_chunks()
        assert len(recovered) == 0
        
        print("✓ Buffer clear test passed")


def test_empty_buffer_recovery():
    """Test recovering from an empty buffer."""
    with tempfile.TemporaryDirectory() as tmpdir:
        buffer_path = os.path.join(tmpdir, "test_buffer")
        buffer = ChunkBuffer(buffer_path)
        
        # Don't append anything
        recovered = buffer.read_all_chunks()
        
        # Verify empty list is returned
        assert len(recovered) == 0
        assert buffer.get_buffer_size() == 0
        
        print("✓ Empty buffer recovery test passed")


if __name__ == "__main__":
    print("Running chunk buffer tests...\n")
    test_append_and_recover_single_chunk()
    test_append_multiple_chunks()
    test_clear_buffer()
    test_empty_buffer_recovery()
    print("\n✅ All tests passed!")
