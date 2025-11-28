import uuid

from utils.tolerance.persistence_service import PersistenceService
from utils.tolerance.working_state import WorkingState


class ExampleWorkingState(WorkingState):
    def __init__(self, dict_simple=None, dict_of_dicts=None):
        super().__init__()
        self.dict_simple = dict_simple
        self.dict_of_dicts = dict_of_dicts


def test_working_state_serialization():
    original = ExampleWorkingState(
        dict_simple={1: 1, 2: 2},
        dict_of_dicts={
            1: {1: 5, 2: True},
            2: {1: 3, 2: False},
        },
    )

    serialized = original.to_bytes()
    deserialized = ExampleWorkingState.from_bytes(serialized)

    assert deserialized.dict_simple == original.dict_simple
    assert deserialized.dict_of_dicts == original.dict_of_dicts


def test_integrated_with_persistence_service():
    original = ExampleWorkingState(
        dict_simple={1: 10, 2: 20},
        dict_of_dicts={
            1: {1: 100, 2: False},
            2: {1: 200, 2: True},
        },
    )

    directory = "/tmp"
    # Simulate persistence service
    service = PersistenceService(directory)
    last_uuid = uuid.uuid4()
    service.commit_working_state(original.to_bytes(), last_uuid)

    # Simulate shutdown and recovery
    service = PersistenceService(directory)
    persisted_data = service.recover_working_state()
    data_recovered = ExampleWorkingState.from_bytes(persisted_data)

    assert data_recovered.dict_simple == original.dict_simple
    assert data_recovered.dict_of_dicts == original.dict_of_dicts

    service.clean_persisted_data()