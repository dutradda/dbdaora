from typing import Any, AsyncGenerator, Dict, Optional


class AsyncIOMotorCollection:
    async def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        upsert: bool = False,
        bypass_document_validation: bool = False,
        collation: Any = None,
        session: Any = None,
    ) -> Dict[str, Any]: ...

    async def delete_one(
        self,
        filter: Dict[str, Any],
        collation: Any = None,
        session: Any = None,
    ) -> Dict[str, Any]: ...

    async def find_one(
        self,
        filter: Optional[Dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]: ...

    def find(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncGenerator[Dict[str, Any], None]: ...

    async def create_index(
        self, name: str, expireAfterSeconds: Optional[int],
    ) -> None: ...

    async def drop_index(self, name: str) -> None: ...

    def list_indexes(self) -> AsyncGenerator[Dict[str, Any], None]: ...


class AsyncIOMotorDatabase:
    def __getitem__(self, key: str) -> AsyncIOMotorCollection: ...


class AsyncIOMotorClient:
    def __getitem__(self, key: str) -> AsyncIOMotorDatabase: ...
