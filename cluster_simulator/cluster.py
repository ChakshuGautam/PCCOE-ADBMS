from typing import List, Dict
from cluster_simulator.node import Node
import random

class Cluster:
    def __init__(self, name: str):
        """
        Initialize a new cluster.

        Args:
            name (str): The name of the cluster.
        """
        self.name: str = name
        self.nodes: Dict[str, Node] = {}
        self.network_topology: Dict[str, List[str]] = {}

    def add_node(self, node: Node) -> None:
        """
        Add a new node to the cluster.

        Args:
            node (Node): The node to be added.
        """
        self.nodes[node.id] = node
        self.network_topology[node.id] = []

    def remove_node(self, node_id: str) -> None:
        """
        Remove a node from the cluster.

        Args:
            node_id (str): The ID of the node to be removed.
        """
        if node_id in self.nodes:
            del self.nodes[node_id]
            del self.network_topology[node_id]
            for connections in self.network_topology.values():
                if node_id in connections:
                    connections.remove(node_id)

    def connect_nodes(self, node1_id: str, node2_id: str) -> None:
        """
        Connect two nodes in the cluster.

        Args:
            node1_id (str): The ID of the first node.
            node2_id (str): The ID of the second node.
        """
        if node1_id in self.nodes and node2_id in self.nodes:
            self.nodes[node1_id].connect_to(self.nodes[node2_id])
            self.network_topology[node1_id].append(node2_id)
            self.network_topology[node2_id].append(node1_id)

    def generate_random_cluster(self, num_nodes: int, min_connections: int = 2, max_connections: int = 5) -> None:
        """
        Generate a random cluster with the specified number of nodes.

        Args:
            num_nodes (int): The number of nodes to generate.
            min_connections (int): The minimum number of connections per node.
            max_connections (int): The maximum number of connections per node.
        """
        # Create nodes
        for i in range(num_nodes):
            node = Node(
                node_id=str(i),
                compute=random.uniform(2.0, 4.0),  # Random compute power between 2.0 and 4.0 GHz
                memory=random.randint(4096, 16384),  # Random memory between 4 and 16 GB
                storage=random.randint(100000, 1000000)  # Random storage between 100 GB and 1 TB
            )
            self.add_node(node)

        # Connect nodes
        if num_nodes > 1:
            for node_id in self.nodes:
                num_connections = random.randint(min(min_connections, num_nodes - 1), 
                                                 min(max_connections, num_nodes - 1))
                possible_connections = list(set(self.nodes.keys()) - {node_id} - set(self.network_topology[node_id]))
                connections = random.sample(possible_connections, min(num_connections, len(possible_connections)))
                for connection in connections:
                    self.connect_nodes(node_id, connection)

    def __str__(self) -> str:
        return f"Cluster(name={self.name}, nodes={len(self.nodes)})"

# Example usage
if __name__ == "__main__":
    cluster = Cluster("DataCenter1")
    cluster.generate_random_cluster(10)
    print(cluster)
    print("Network Topology:")
    for node_id, connections in cluster.network_topology.items():
        print(f"Node {node_id}: {connections}")