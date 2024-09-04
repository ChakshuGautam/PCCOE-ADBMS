from typing import List, Dict, Tuple
from cluster_simulator.node import Node
from cluster_simulator.cluster import Cluster
import random
import time
import matplotlib.pyplot as plt
import numpy as np
import os

def range_partition_sort(cluster: Cluster, relation: List[Tuple], sort_attribute: int) -> List[Tuple]:
    """
    Perform range-partitioning sort on a relation across a cluster.

    Args:
        cluster (Cluster): The cluster to perform the sort on.
        relation (List[Tuple]): The relation to be sorted.
        sort_attribute (int): The index of the attribute to sort on.

    Returns:
        List[Tuple]: The sorted relation.
    """
    # Step 1: Range partition the relation
    partitions = range_partition(cluster, relation, sort_attribute)

    # Step 2: Sort each partition locally
    sorted_partitions = []
    for node_id, partition in partitions.items():
        sorted_partition = cluster.nodes[node_id].local_sort(partition, sort_attribute)
        sorted_partitions.append(sorted_partition)

    # Concatenate the sorted partitions
    return [tuple for partition in sorted_partitions for tuple in partition]

def range_partition(cluster: Cluster, relation: List[Tuple], sort_attribute: int) -> Dict[str, List[Tuple]]:
    """
    Range partition the relation across the nodes in the cluster.

    Args:
        cluster (Cluster): The cluster to partition the relation across.
        relation (List[Tuple]): The relation to be partitioned.
        sort_attribute (int): The index of the attribute to partition on.

    Returns:
        Dict[str, List[Tuple]]: A dictionary mapping node IDs to their partitions.
    """
    num_nodes = len(cluster.nodes)
    min_val = min(tuple[sort_attribute] for tuple in relation)
    max_val = max(tuple[sort_attribute] for tuple in relation)
    range_size = (max_val - min_val) / num_nodes

    partitions = {node_id: [] for node_id in cluster.nodes}

    for tuple in relation:
        value = tuple[sort_attribute]
        partition_index = min(int((value - min_val) / range_size), num_nodes - 1)
        node_id = list(cluster.nodes.keys())[partition_index]
        partitions[node_id].append(tuple)

    return partitions

# Add a method to the Node class for local sorting
def local_sort(self, partition: List[Tuple], sort_attribute: int) -> List[Tuple]:
    """
    Sort the partition locally on this node.

    Args:
        partition (List[Tuple]): The partition to be sorted.
        sort_attribute (int): The index of the attribute to sort on.

    Returns:
        List[Tuple]: The sorted partition.
    """
    return sorted(partition, key=lambda x: x[sort_attribute])

# Monkey patch the Node class to add the local_sort method
Node.local_sort = local_sort

def run_sorting_experiment(num_nodes: int, num_data_points: int) -> Tuple[float, Dict[str, List[Tuple]]]:
    cluster = Cluster(f"SortCluster_{num_nodes}")
    cluster.generate_random_cluster(num_nodes)
    relation = [(random.randint(1, 1000000), f"data_{i}") for i in range(num_data_points)]
    start_time = time.time()
    partitions = range_partition(cluster, relation, sort_attribute=0)
    for node_id, partition in partitions.items():
        cluster.nodes[node_id].local_sort(partition, sort_attribute=0)
    end_time = time.time()
    return end_time - start_time, partitions

def plot_range_sort_visualizations():
    num_nodes = 5
    num_data_points = 10000

    # Run the experiment
    time_taken, partitions = run_sorting_experiment(num_nodes, num_data_points)

    # Create a 2x2 subplot
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 20))
    fig.suptitle(f"Range Partitioning Sort Visualization\n{num_data_points} data points, {num_nodes} nodes", fontsize=16)

    # 1. Partition Distribution
    partition_sizes = [len(partition) for partition in partitions.values()]
    ax1.bar(range(num_nodes), partition_sizes)
    ax1.set_title("Partition Size Distribution")
    ax1.set_xlabel("Node ID")
    ax1.set_ylabel("Number of Elements")

    # 2. Value Range per Partition
    for i, (node_id, partition) in enumerate(partitions.items()):
        if partition:
            min_val, max_val = min(t[0] for t in partition), max(t[0] for t in partition)
            ax2.plot([i, i], [min_val, max_val], 'bo-', linewidth=2, markersize=8)
    ax2.set_title("Value Range per Partition")
    ax2.set_xlabel("Node ID")
    ax2.set_ylabel("Value Range")

    # 3. Sorting Progress Simulation
    total_elements = sum(partition_sizes)
    sorted_elements = np.zeros(num_nodes)
    times = np.linspace(0, time_taken, 100)
    progress = []
    for _ in times:
        sorted_elements += np.random.rand(num_nodes) * (np.array(partition_sizes) - sorted_elements) * 0.1
        progress.append(sorted_elements.sum() / total_elements * 100)
    ax3.plot(times, progress)
    ax3.set_title("Sorting Progress Simulation")
    ax3.set_xlabel("Time (seconds)")
    ax3.set_ylabel("Percentage Sorted")

    # 4. Parallel Efficiency
    single_node_time = run_sorting_experiment(1, num_data_points)[0]
    speedups = [single_node_time / run_sorting_experiment(n, num_data_points)[0] for n in range(1, num_nodes + 1)]
    ax4.plot(range(1, num_nodes + 1), speedups, marker='o')
    ax4.plot([1, num_nodes], [1, num_nodes], 'r--', label="Ideal Speedup")
    ax4.set_title("Parallel Efficiency")
    ax4.set_xlabel("Number of Nodes")
    ax4.set_ylabel("Speedup")
    ax4.legend()

    plt.tight_layout()
    
    # Save the figure as JPEG in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "range_sort_visualization.jpg")
    plt.savefig(file_path, format='jpg', dpi=300)
    plt.close()  # Close the figure to free up memory

if __name__ == "__main__":
    plot_range_sort_visualizations()
    print("Visualization saved as 'range_sort_visualization.jpg' in the script directory")