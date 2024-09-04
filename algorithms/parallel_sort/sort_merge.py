import random
import matplotlib.pyplot as plt
import numpy as np
from typing import List, Tuple
from datetime import datetime, timedelta
from cluster_simulator.node import Node
from cluster_simulator.cluster import Cluster

def generate_time_based_data(points_per_node: int, num_nodes: int) -> List[Tuple[datetime, datetime, str, int]]:
    data = []
    now = datetime.now()
    total_time_range = timedelta(days=365)  # Total time range of 1 year

    for node in range(num_nodes):
        start_date = now - total_time_range + (node * total_time_range / num_nodes)
        end_date = start_date + (total_time_range / num_nodes)
        
        for i in range(points_per_node):
            timestamp = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
            delivery_time = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
            data.append((timestamp, delivery_time, f"data_{node}_{i}", node))
    
    return data

def local_sort(partition: List[Tuple[datetime, datetime, str, int]]) -> List[Tuple[datetime, datetime, str, int]]:
    return sorted(partition, key=lambda x: x[0])

def parallel_external_sort_merge(cluster: Cluster, relation: List[Tuple[datetime, datetime, str, int]]) -> List[Tuple[datetime, datetime, str, int]]:
    num_nodes = len(cluster.nodes)
    
    # Distribute data to nodes based on the preassigned node
    node_partitions = [[] for _ in range(num_nodes)]
    for item in relation:
        node_partitions[item[3]].append(item)
    
    # Local sort on each node
    locally_sorted = [local_sort(partition) for partition in node_partitions]
    
    # Merge sorted partitions
    return sorted([item for partition in locally_sorted for item in partition], key=lambda x: x[0])

def visualize_sort_merge(num_nodes: int, points_per_node: int):
    cluster = Cluster(f"SortCluster_{num_nodes}")
    cluster.generate_random_cluster(num_nodes)
    relation = generate_time_based_data(points_per_node, num_nodes)

    fig, axs = plt.subplots(3, 1, figsize=(12, 15))
    fig.suptitle(f"Parallel External Sort-Merge with Timestamp-Based Sorting\n{points_per_node} points per node, {num_nodes} nodes", fontsize=16)

    colors = plt.cm.viridis(np.linspace(0, 1, num_nodes))

    # Calculate global time range for opacity scaling
    global_start = min(item[0] for item in relation)
    global_end = max(item[0] for item in relation)
    global_range = (global_end - global_start).total_seconds()

    # Step 1: Initial time-based data distribution
    axs[0].set_title("1. Initial Time-Based Data Distribution")
    for timestamp, delivery_time, _, node in relation:
        opacity = (timestamp - global_start).total_seconds() / global_range
        axs[0].scatter(delivery_time, node, c=[colors[node]], alpha=opacity, s=100)
    axs[0].set_yticks(range(num_nodes))
    axs[0].set_yticklabels([f"Node {i+1}" for i in range(num_nodes)])
    axs[0].set_xlabel("Delivery Time")
    axs[0].set_ylabel("Assigned Node")

    # Step 2: Local sorting on each node
    axs[1].set_title("2. Local Sorting on Each Node (by Timestamp)")
    node_partitions = [[] for _ in range(num_nodes)]
    for item in relation:
        node_partitions[item[3]].append(item)
    locally_sorted = [local_sort(partition) for partition in node_partitions]
    for i, partition in enumerate(locally_sorted):
        for timestamp, _, _, _ in partition:
            opacity = (timestamp - global_start).total_seconds() / global_range
            axs[1].scatter(timestamp, i, c=[colors[i]], alpha=opacity, s=100)
    axs[1].set_yticks(range(num_nodes))
    axs[1].set_yticklabels([f"Node {i+1}" for i in range(num_nodes)])
    axs[1].set_xlabel("Timestamp")
    axs[1].set_ylabel("Node")

    # Step 3: Final merge
    axs[2].set_title("3. Final Merged Result")
    final_sorted = parallel_external_sort_merge(cluster, relation)
    for timestamp, _, _, node in final_sorted:
        opacity = (timestamp - global_start).total_seconds() / global_range
        axs[2].scatter(timestamp, 0, c=[colors[node]], alpha=opacity, s=100)
    axs[2].set_yticks([])
    axs[2].set_xlabel("Timestamp")

    # Add a color bar
    sm = plt.cm.ScalarMappable(cmap=plt.cm.viridis, norm=plt.Normalize(vmin=0, vmax=num_nodes-1))
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=axs[2], ticks=range(num_nodes))
    cbar.set_label('Node')
    cbar.set_ticklabels([f"Node {i+1}" for i in range(num_nodes)])

    plt.tight_layout()
    plt.savefig("parallel_external_sort_merge_visualization.jpg", format='jpg', dpi=300)
    plt.close()

if __name__ == "__main__":
    visualize_sort_merge(5, 10)  # 5 nodes, 10 points per node
    print("Visualization saved as 'parallel_external_sort_merge_visualization.jpg' in the script directory")