import logging
import os
import struct
from typing import List

from utils.processing.process_batch_reader import ProcessBatchReader
from utils.processing.process_chunk import ProcessChunk


class ChunkBuffer:
    """
    Manages a buffer file for chunks using length-prefixed format.
    Format: [4-byte length in big-endian][chunk bytes][4-byte length][chunk bytes]...
    
    This allows appending chunks efficiently and reading them back during recovery.
    """
    
    def __init__(self, buffer_path: str):
        """
        Initialize the chunk buffer.
        
        Args:
            buffer_path: Absolute path to the buffer file
        """
        self.buffer_path = buffer_path
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Create the buffer file if it doesn't exist."""
        if not os.path.exists(self.buffer_path):
            try:
                os.makedirs(os.path.dirname(self.buffer_path), exist_ok=True)
                with open(self.buffer_path, 'wb') as f:
                    f.write(b'')
                logging.debug(f"Created chunk buffer file: {self.buffer_path}")
            except Exception as e:
                logging.error(f"Failed to create chunk buffer file: {e}")
                raise
    
    def append_chunk(self, chunk: ProcessChunk):
        """
        Append a chunk to the buffer file atomically.
        
        Uses length-prefixed format: [4-byte length][chunk bytes]
        
        Args:
            chunk: ProcessChunk to append
        """
        try:
            chunk_bytes = chunk.serialize()
            chunk_length = len(chunk_bytes)
            
            # Create entry: [length][data]
            length_prefix = struct.pack('>I', chunk_length)  # 4 bytes, big-endian
            entry = length_prefix + chunk_bytes
            
            # Append to file
            with open(self.buffer_path, 'ab') as f:
                f.write(entry)
                f.flush()
                os.fsync(f.fileno())
            
            logging.debug(f"Appended chunk to buffer | msg_id:{chunk.message_id()} | size:{chunk_length}")
        
        except Exception as e:
            logging.error(f"Failed to append chunk to buffer: {e}")
            raise
    
    def read_all_chunks(self) -> List[ProcessChunk]:
        """
        Read all chunks from the buffer file.
        
        Returns:
            List of ProcessChunk objects in order they were appended
        """
        chunks = []
        
        try:
            if not os.path.exists(self.buffer_path):
                logging.debug("Chunk buffer file does not exist, returning empty list")
                return chunks
            
            file_size = os.path.getsize(self.buffer_path)
            if file_size == 0:
                logging.debug("Chunk buffer is empty")
                return chunks
            
            with open(self.buffer_path, 'rb') as f:
                offset = 0
                while offset < file_size:
                    # Read length prefix
                    length_bytes = f.read(4)
                    if len(length_bytes) < 4:
                        logging.warning(f"Incomplete length prefix at offset {offset}, stopping read")
                        break
                    
                    chunk_length = struct.unpack('>I', length_bytes)[0]
                    
                    # Read chunk data
                    chunk_bytes = f.read(chunk_length)
                    if len(chunk_bytes) < chunk_length:
                        logging.warning(f"Incomplete chunk data at offset {offset}, expected {chunk_length} bytes, got {len(chunk_bytes)}")
                        break
                    
                    # Deserialize chunk
                    try:
                        chunk = ProcessBatchReader.from_bytes(chunk_bytes)
                        chunks.append(chunk)
                        offset += 4 + chunk_length
                    except Exception as e:
                        logging.error(f"Failed to deserialize chunk at offset {offset}: {e}")
                        # Skip this chunk and continue
                        offset += 4 + chunk_length
                        continue
            
            logging.info(f"Read {len(chunks)} chunks from buffer | file_size:{file_size}")
            return chunks
        
        except Exception as e:
            logging.error(f"Failed to read chunks from buffer: {e}")
            raise
    
    def clear(self):
        """
        Clear the buffer file atomically by truncating it.
        """
        try:
            with open(self.buffer_path, 'wb') as f:
                f.write(b'')
                f.flush()
                os.fsync(f.fileno())
            
            logging.debug("Cleared chunk buffer")
        
        except Exception as e:
            logging.error(f"Failed to clear chunk buffer: {e}")
            raise
    
    def get_chunk_count(self) -> int:
        """
        Get the number of chunks currently in the buffer.
        
        Returns:
            Number of buffered chunks
        """
        try:
            chunks = self.read_all_chunks()
            return len(chunks)
        except Exception as e:
            logging.error(f"Failed to get chunk count: {e}")
            return 0
    
    def get_buffer_size(self) -> int:
        """
        Get the size of the buffer file in bytes.
        
        Returns:
            Size in bytes
        """
        try:
            if os.path.exists(self.buffer_path):
                return os.path.getsize(self.buffer_path)
            return 0
        except Exception as e:
            logging.error(f"Failed to get buffer size: {e}")
            return 0
