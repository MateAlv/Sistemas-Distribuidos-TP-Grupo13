import pickle

class WorkingState:
    def __init__(self):
        pass

    def to_bytes(self) -> bytes:
        return pickle.dumps(self.__dict__)

    @classmethod
    def from_bytes(cls, b: bytes):
        state_dict = pickle.loads(b)
        inst = cls()
        inst.__dict__.update(state_dict)
        return inst