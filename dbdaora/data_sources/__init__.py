from typing import Any, Protocol


class DataSource(Protocol):
    def make_key(self, *key_parts: Any) -> Any:
        raise NotImplementedError()  # pragma: no cover
