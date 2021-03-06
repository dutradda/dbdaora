import pytest

from dbdaora.exceptions import RequiredClassAttributeError
from dbdaora.repository import MemoryRepository


def test_should_raise_validation_error_without_attributes():
    with pytest.raises(RequiredClassAttributeError) as exc_info:

        class FakeRepository(MemoryRepository):
            ...

    assert exc_info.value.args == (
        'FakeRepository',
        'entity_cls or get_entity_type',
    )


def test_should_create_class_with_entity_cls():
    class FakeRepository(MemoryRepository):
        entity_cls = dict

    assert FakeRepository


def test_should_create_class_with_get_entity_type():
    class FakeRepository(MemoryRepository):
        def get_entity_type(self):
            ...

    assert FakeRepository
