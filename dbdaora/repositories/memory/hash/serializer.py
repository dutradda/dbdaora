from typing import Any, Dict, Union

from dbdaora.exceptions import InvalidHashAttribute


DeserializableValues = Union[bool, int, float, str]


def serialize(values: Dict[str, Any]) -> Dict[str, Any]:
    data: Dict[str, Any] = {}

    for key, value in values.items():
        if isinstance(value, bool):
            skey = f'b:{key}'
            data[skey] = int(value)

        elif isinstance(value, int):
            skey = f'i:{key}'
            data[skey] = value

        elif isinstance(value, float):
            skey = f'n:{key}'
            data[skey] = value

        elif isinstance(value, str) or isinstance(value, bytes):
            data[key] = value

        elif value is None:
            continue

        else:
            raise InvalidHashAttribute(
                f'key={key} value={value} value-type={type(value)}'
            )

    return data


def deserialize(values: Dict[bytes, bytes]) -> Dict[str, DeserializableValues]:
    data: Dict[str, DeserializableValues] = {}

    for key_, value in values.items():
        _set_field(data, key_.decode(), value)

    return data


def _set_field(
    data: Dict[str, DeserializableValues], key: str, value: bytes
) -> None:
    if key.startswith('b:'):
        data[key.replace('b:', '')] = bool(int(value))

    elif key.startswith('i:'):
        data[key.replace('i:', '')] = int(value)

    elif key.startswith('n:'):
        data[key.replace('n:', '')] = float(value)

    else:
        data[key] = value.decode()
