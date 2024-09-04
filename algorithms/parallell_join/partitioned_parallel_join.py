from typing import List, Dict, Tuple
from cluster_simulator.cluster import Cluster
import hashlib

class PartitionedParallelJoin:
    def __init__(self, cluster: Cluster):
        """
        Initialize the PartitionedParallelJoin with a given cluster.

        Args:
            cluster (Cluster): The cluster on which the join operation will be performed.
        """
        self.cluster = cluster
        self.num_partitions = len(cluster.nodes)  # Number of partitions equals number of nodes

    def range_partition(self, table: List[Tuple[int, any]]) -> Dict[int, List[Tuple[int, any]]]:
        """
        Range partitioning function based on the join key.

        Args:
            table (List[Tuple[int, any]]): The table to be partitioned.

        Returns:
            Dict[int, List[Tuple[int, any]]]: A dictionary of partitions.
        """
        partitions = {i: [] for i in range(self.num_partitions)}
        if not table:
            return partitions

        # Determine range boundaries for partitioning based on the join key
        min_key = min(row[0] for row in table)
        max_key = max(row[0] for row in table)
        range_size = (max_key - min_key + 1) / self.num_partitions

        for row in table:
            key = row[0]
            partition_id = int((key - min_key) // range_size)
            partition_id = min(partition_id, self.num_partitions - 1)  # Ensure it's within bounds
            partitions[partition_id].append(row)

        return partitions

    def hash_partition(self, table: List[Tuple[int, any]]) -> Dict[int, List[Tuple[int, any]]]:
        """
        Hash partitioning function based on the join key.

        Args:
            table (List[Tuple[int, any]]): The table to be partitioned.

        Returns:
            Dict[int, List[Tuple[int, any]]]: A dictionary of partitions.
        """
        partitions = {i: [] for i in range(self.num_partitions)}
        if not table:
            return partitions

        for row in table:
            key = row[0]
            # Compute hash of the key and mod by number of partitions to assign to a partition
            partition_id = int(hashlib.md5(str(key).encode()).hexdigest(), 16) % self.num_partitions
            partitions[partition_id].append(row)

        return partitions

    def join(self, table_r: List[Tuple[int, any]], table_s: List[Tuple[int, any]], partition_type: str = 'range') -> List[Tuple[any, any]]:
        """
        Perform a partitioned parallel join using either range or hash partitioning.

        Args:
            table_r (List[Tuple[int, any]]): The first table to join.
            table_s (List[Tuple[int, any]]): The second table to join.
            partition_type (str): The type of partitioning to use ('range' or 'hash').

        Returns:
            List[Tuple[any, any]]: The result of the join operation.
        """
        if partition_type == 'range':
            # Partition both tables using range partitioning
            r_partitions = self.range_partition(table_r)
            s_partitions = self.range_partition(table_s)
        elif partition_type == 'hash':
            # Partition both tables using hash partitioning
            r_partitions = self.hash_partition(table_r)
            s_partitions = self.hash_partition(table_s)
        else:
            raise ValueError("Invalid partition_type. Use 'range' or 'hash'.")

        # Join the partitions in parallel
        results = []
        for i in range(self.num_partitions):
            r_partition = r_partitions[i]
            s_partition = s_partitions[i]

            # Perform a nested loop join on the partitions
            for r_row in r_partition:
                for s_row in s_partition:
                    if r_row[0] == s_row[0]:  # Join based on the key
                        results.append((r_row, s_row))

        return results



## Usage (uncomment to run)
# # Sample tables
# table_r = [(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (6, 'F')]
# table_s = [(2, 'X'), (4, 'Y'), (5, 'Z'), (6, 'W')]

# # Create a cluster
# cluster = Cluster("DataCenter1")
# cluster.generate_random_cluster(5)

# # Perform Range Partitioned Parallel Join
# range_join = PartitionedParallelJoin(cluster)
# range_result = range_join.join(table_r, table_s, partition_type='range')
# print("Range Partitioned Join Result:")
# for row in range_result:
#     print(row)

# # Perform Hash Partitioned Parallel Join
# hash_result = range_join.join(table_r, table_s, partition_type='hash')
# print("\nHash Partitioned Join Result:")
# for row in hash_result:
#     print(row)
