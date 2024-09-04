import random
from collections import defaultdict
from itertools import chain

# Sample tables
table_r = [(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')]
table_s = [(2, 'X'), (4, 'Y'), (5, 'Z')]

# Hash function for partitioning
def hash_function(key, num_partitions):
    return key % num_partitions

# Partition tables
def partition_table(table, num_partitions):
    partitions = defaultdict(list)
    for row in table:
        key = row[0]
        partition_id = hash_function(key, num_partitions)
        partitions[partition_id].append(row)
    return partitions

# 1. Partitioned Parallel Hash Join
def partitioned_parallel_hash_join(table_r, table_s, num_partitions=2):
    print("\n1. Partitioned Parallel Hash Join:")
    
    # Partition tables using hash function
    r_partitions = partition_table(table_r, num_partitions)
    s_partitions = partition_table(table_s, num_partitions)

    results = []
    
    for i in range(num_partitions):
        r_partition = r_partitions[i]
        s_partition = s_partitions[i]
        
        # Build a hash table on r_partition (smaller relation)
        hash_table = {row[0]: row for row in r_partition}
        
        # Probe with s_partition
        for row in s_partition:
            key = row[0]
            if key in hash_table:
                results.append((hash_table[key], row))
    
    print(results)
    return results

# 2. Hybrid Hash Join Optimization
def hybrid_hash_join_optimization(table_r, table_s, num_partitions=2):
    print("\n2. Hybrid Hash Join Optimization:")
    
    # Partition tables using hash function
    r_partitions = partition_table(table_r, num_partitions)
    s_partitions = partition_table(table_s, num_partitions)

    results = []
    
    # Retain the first partition of R in memory
    r0 = r_partitions[0]
    s0 = s_partitions[0]
    
    # Build a hash table on r0 (smaller relation)
    hash_table = {row[0]: row for row in r0}
    
    # Probe with s0 directly
    for row in s0:
        key = row[0]
        if key in hash_table:
            results.append((hash_table[key], row))
    
    print(results)
    return results

# 3. Partitioned Parallel Merge Join
def partitioned_parallel_merge_join(table_r, table_s, num_partitions=2):
    print("\n3. Partitioned Parallel Merge Join:")
    
    # Partition tables using hash function
    r_partitions = partition_table(table_r, num_partitions)
    s_partitions = partition_table(table_s, num_partitions)
    
    results = []
    
    for i in range(num_partitions):
        r_partition = sorted(r_partitions[i])
        s_partition = sorted(s_partitions[i])
        
        # Perform merge join
        r_idx = s_idx = 0
        while r_idx < len(r_partition) and s_idx < len(s_partition):
            r_key = r_partition[r_idx][0]
            s_key = s_partition[s_idx][0]
            if r_key == s_key:
                results.append((r_partition[r_idx], s_partition[s_idx]))
                r_idx += 1
                s_idx += 1
            elif r_key < s_key:
                r_idx += 1
            else:
                s_idx += 1
    
    print(results)
    return results

# 4. Partitioned Parallel Nested-Loop Join
def partitioned_parallel_nested_loop_join(table_r, table_s, num_partitions=2):
    print("\n4. Partitioned Parallel Nested-Loop Join:")
    
    # Partition tables using hash function
    r_partitions = partition_table(table_r, num_partitions)
    s_partitions = partition_table(table_s, num_partitions)
    
    results = []
    
    for i in range(num_partitions):
        r_partition = r_partitions[i]
        s_partition = s_partitions[i]
        
        # Perform nested-loop join
        for r_row in r_partition:
            for s_row in s_partition:
                if r_row[0] == s_row[0]:
                    results.append((r_row, s_row))
    
    print(results)
    return results

# 5. Partitioned Parallel Indexed Nested-Loops Join
def partitioned_parallel_indexed_nested_loop_join(table_r, table_s, num_partitions=2):
    print("\n5. Partitioned Parallel Indexed Nested-Loops Join:")
    
    # Partition tables using hash function
    r_partitions = partition_table(table_r, num_partitions)
    s_partitions = partition_table(table_s, num_partitions)
    
    results = []
    
    for i in range(num_partitions):
        r_partition = r_partitions[i]
        s_partition = s_partitions[i]
        
        # Build an index on s_partition
        index = {row[0]: row for row in s_partition}
        
        # Perform indexed nested-loop join
        for r_row in r_partition:
            if r_row[0] in index:
                results.append((r_row, index[r_row[0]]))
    
    print(results)
    return results

# Run the join algorithms
partitioned_parallel_hash_join(table_r, table_s)
hybrid_hash_join_optimization(table_r, table_s)
partitioned_parallel_merge_join(table_r, table_s)
partitioned_parallel_nested_loop_join(table_r, table_s)
partitioned_parallel_indexed_nested_loop_join(table_r, table_s)
