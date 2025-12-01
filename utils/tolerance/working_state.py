import pickle

class WorkingState:
    def __init__(self):
        pass

    def to_bytes(self) -> bytes:
        """Serialize to bytes, excluding unpicklable objects like locks"""
        # Filter out attributes that cannot be pickled (e.g., threading.Lock)
        state_dict = {}
        for k, v in self.__dict__.items():
            # Skip attributes that are known to be unpicklable
            if k == '_lock' or str(type(v)) == "<class 'thread.lock'>" or str(type(v)) == "<class '_thread.lock'>":
                continue
            state_dict[k] = v
        return pickle.dumps(state_dict)

    @classmethod
    def from_bytes(cls, b: bytes):
        """Deserialize from bytes and reinitialize the instance properly"""
        state_dict = pickle.loads(b)
        inst = cls()
        inst.__dict__.update(state_dict)
        return inst