import json

def encode_dict_value(value):
    """Recursively encode values, preserving dict key types."""
    if isinstance(value, dict):
        return {
            "__type__": "dict",
            "items": [{"k": k, "v": encode_dict_value(v)} for k, v in value.items()]
        }
    elif isinstance(value, list):
        return {
            "__type__": "list",
            "items": [encode_dict_value(v) for v in value]
        }
    else:
        # primitive value (number, string, bool, None)
        return value


def decode_dict_value(obj):
    """Recursively decode JSON structure back into Python objects."""
    if isinstance(obj, dict) and "__type__" in obj:
        if obj["__type__"] == "dict":
            return {item["k"]: decode_dict_value(item["v"]) for item in obj["items"]}
        if obj["__type__"] == "list":
            return [decode_dict_value(v) for v in obj["items"]]
    return obj  # primitive


class WorkingState:
    def __init__(self):
        pass

    def to_bytes(self) -> bytes:
        data = encode_dict_value(self.__dict__)
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_bytes(cls, b: bytes):
        raw = json.loads(b.decode("utf-8"))
        decoded = decode_dict_value(raw)
        inst = cls()
        inst.__dict__.update(decoded)
        return inst