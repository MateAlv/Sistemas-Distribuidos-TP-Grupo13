from utils.repetition_checker_protocol.repetition_checker_payload import RepetitionCheckerMessagePayload, define_class_by_type

class RepetitionCheckerMessageHeader:

    HEADER_SIZE = 8  # 4 bytes for message_type + 4 bytes for size
    
    def __init__(self, message_type: int, size: int = 0):
        self.message_type = message_type
        self.size = size
    
    def serialize(self) -> bytes:
        return (
            self.message_type.to_bytes(4, byteorder="big") +
            self.size.to_bytes(4, byteorder="big")
        )
    
    @staticmethod
    def deserialize(data: bytes):
        message_type = int.from_bytes(data[0:4], byteorder="big")
        size = int.from_bytes(data[4:8], byteorder="big")
        return RepetitionCheckerMessageHeader(message_type, size)
    
class RepetitionCheckerMessage:
    def __init__(self, header: RepetitionCheckerMessageHeader, payload: RepetitionCheckerMessagePayload):
        self.header = header
        self.header.size = len(payload.serialize())

    def header(self):
        return self.header
    
    def payload(self) -> RepetitionCheckerMessagePayload:
        return self.payload

    def serialize(self) -> bytes:
        return self.header.serialize() + self.payload.serialize()

    @staticmethod
    def deserialize(header: RepetitionCheckerMessageHeader, data: bytes):
        payload_class = define_class_by_type(header.message_type)
        payload = payload_class.deserialize(data)
        return RepetitionCheckerMessage(header, payload)

