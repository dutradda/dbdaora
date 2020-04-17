from dataclasses import dataclass
from typing import Dict


@dataclass
class HashEntity:
    id: str
    data: Dict[bytes, bytes]
