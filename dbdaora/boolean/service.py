from typing import Any

from dbdaora.service import Service

from ..keys import FallbackKey


class BooleanService(Service[Any, bool, FallbackKey]):
    ...
