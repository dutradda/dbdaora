from typing import Any, Dict, Iterable, Optional, Protocol


class FallbackDataSource(Protocol):
    def make_key(self, *key_parts: Any) -> Any:
        ...

    def get(self, key: Any) -> Optional[Dict[str, Any]]:
        ...

    def put(
        self,
        key: Any,
        data: Dict[str, Any],
        exclude_from_indexes: Iterable[str] = (),
        **kwargs: Any,
    ) -> None:
        ...

    def delete(self, key: Any) -> None:
        ...
