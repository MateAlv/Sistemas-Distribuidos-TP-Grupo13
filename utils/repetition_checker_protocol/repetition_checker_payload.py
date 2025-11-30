import uuid

PROCESSING = 1
BEING_PROCESSED_BY = 2
PROCESSED = 3
PROCESSED_ACK = 4

class RepetitionCheckerMessagePayload:
    def serialize(self) -> bytes:
        raise NotImplementedError

    @staticmethod
    def deserialize(data: bytes):
        raise NotImplementedError
    
class ProcessingMessage(RepetitionCheckerMessagePayload):
    def __init__(self, processor_id: int, message_id: uuid.UUID):
        self.processor_id = processor_id
        self.message_id = message_id

    def serialize(self) -> bytes:
        return (
            self.processor_id.to_bytes(4, byteorder="big") +
            self.message_id.bytes
        )
    
    @staticmethod
    def deserialize(data: bytes):
        processor_id = int.from_bytes(data[0:4], byteorder="big")
        message_id = uuid.UUID(bytes=data[4:20])
        return ProcessingMessage(processor_id, message_id)

class BeingProcessedByMessage(RepetitionCheckerMessagePayload):
    def __init__(self, processor_id: int):
        self.processor_id = processor_id

    def serialize(self) -> bytes:
        return self.processor_id.to_bytes(4, byteorder="big")
    
    @staticmethod
    def deserialize(data: bytes):
        processor_id = int.from_bytes(data[0:4], byteorder="big")
        return BeingProcessedByMessage(processor_id)

class ProcessedMessage(RepetitionCheckerMessagePayload):
    def __init__(self, message_id: uuid.UUID):
        self.message_id = message_id

    def serialize(self) -> bytes:
        return self.message_id.bytes
    
    @staticmethod
    def deserialize(data: bytes):
        message_id = uuid.UUID(bytes=data[0:16])
        return ProcessedMessage(message_id)
    
class ProcessedAckMessage(RepetitionCheckerMessagePayload):
    def __init__(self, message_id: uuid.UUID):
        self.message_id = message_id

    def serialize(self) -> bytes:
        return self.message_id.bytes
    
    @staticmethod
    def deserialize(data: bytes):
        message_id = uuid.UUID(bytes=data[0:16])
        return ProcessedAckMessage(message_id)
    
def define_class_by_type(message_type: int):
    if message_type == PROCESSING:
        return ProcessingMessage
    elif message_type == BEING_PROCESSED_BY:
        return BeingProcessedByMessage
    elif message_type == PROCESSED:
        return ProcessedMessage
    elif message_type == PROCESSED_ACK:
        return ProcessedAckMessage
    else:
        raise ValueError(f"Tipo de mensaje desconocido: {message_type}")