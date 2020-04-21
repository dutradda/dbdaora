from typing import Any, Dict, List, Optional, Sequence, Union


class Pipeline:
    async def execute(
        self, *, return_exceptions: bool = False
    ) -> List[Any]:
        ...

    def hmget(
        self, key: str, field: Union[str, bytes], *fields: Union[str, bytes]
    ) -> Sequence[Optional[bytes]]: ...
    def hgetall(self, key: str) -> Dict[bytes, bytes]: ...
