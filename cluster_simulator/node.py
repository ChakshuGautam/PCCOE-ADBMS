from typing import List, Dict
from dataclasses import dataclass, field

@dataclass
class File:
    name: str
    size: int
    content: str = ""

@dataclass
class Directory:
    name: str
    files: List[File] = field(default_factory=list)
    subdirectories: List['Directory'] = field(default_factory=list)

class Node:
    def __init__(self, node_id: str, compute: float, memory: int, storage: int):
        self.id: str = node_id
        self.files: List[File] = []
        self.directories: List[Directory] = []
        self.neighbors: Dict[str, 'Node'] = {}
        self.compute: float = compute  # in GHz
        self.memory: int = memory  # in MB
        self.storage: int = storage  # in MB
        self.available_storage: int = storage
        self.network_address: str = f"192.168.0.{node_id}"  # Simple network address assignment
        self.is_active: bool = True  # To simulate node availability

    def ping(self) -> bool:
        """Simulate a ping response."""
        return self.is_active

    def pong(self) -> str:
        """Simulate a pong response with node information."""
        return f"Pong from {self.id} at {self.network_address}"

    def add_file(self, file: File) -> bool:
        if file.size <= self.available_storage:
            self.files.append(file)
            self.available_storage -= file.size
            return True
        return False

    def add_directory(self, directory: Directory) -> None:
        self.directories.append(directory)

    def connect_to(self, node: 'Node') -> None:
        self.neighbors[node.id] = node
        node.neighbors[self.id] = self

    def disconnect_from(self, node: 'Node') -> None:
        if node.id in self.neighbors:
            del self.neighbors[node.id]
            del node.neighbors[self.id]

    def __str__(self) -> str:
        return f"Node(id={self.id}, compute={self.compute}GHz, memory={self.memory}MB, storage={self.storage}MB)"

