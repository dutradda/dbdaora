from hashlib import md5
from typing import Generic, Optional, Sequence, TypeVar


DataSource = TypeVar('DataSource')


class HashRing(Generic[DataSource]):
    def __init__(
        self, nodes: Sequence[DataSource], nodes_size: Optional[int] = None
    ):
        self.nodes = nodes

        if nodes_size is None:
            nodes_size = len(nodes)

        self.nodes_size = nodes_size

    def get_index(self, key: str) -> int:
        return (
            int(md5(str(key).encode('utf-8')).hexdigest(), 16)
            % self.nodes_size
        )

    def get_node(self, key: str) -> DataSource:
        return self.nodes[self.get_index(key)]
