import pickle

class WorkingState:
    def __init__(self):
        pass

    def to_bytes(self) -> bytes:
        state_dict = {}
        for k, v in self.__dict__.items():
            # Skip attributes that are known to be unpicklable
            if k == '_lock' or str(type(v)) == "<class 'thread.lock'>" or str(type(v)) == "<class '_thread.lock'>":
                continue
            state_dict[k] = v
        return pickle.dumps(state_dict)

    @classmethod
    def from_bytes(cls, b: bytes):
        state_dict = pickle.loads(b)
        inst = cls()
        inst.__dict__.update(state_dict)
        return inst