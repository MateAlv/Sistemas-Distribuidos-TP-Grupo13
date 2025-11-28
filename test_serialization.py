
import json
from utils.tolerance.working_state import WorkingState

class TestState(WorkingState):
    def __init__(self):
        super().__init__()
        self.data = {}

state = TestState()
# Use a tuple key
key = (1, "test")
state.data[key] = "value"

print(f"Original: {state.data}")

# Serialize
encoded = state.to_bytes()
print(f"Encoded: {encoded}")

# Deserialize
restored = TestState.from_bytes(encoded)
print(f"Restored: {restored.data}")

# Check type
first_key = list(restored.data.keys())[0]
print(f"Restored key type: {type(first_key)}")
print(f"Is equal? {key == first_key}")
