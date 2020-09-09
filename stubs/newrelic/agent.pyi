from typing import Any, Optional


def wrap_datastore_trace(
    module: Any,
    object_path: str,
    product: str,
    target: Optional[str],
    operation: str,
) -> Any: ...
