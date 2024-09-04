# Parallel External Sort-Merge Algorithm with Timestamp Opacity Visualization

## 1. Algorithm Overview

The Parallel External Sort-Merge algorithm efficiently sorts datasets across multiple nodes in a distributed system. This implementation focuses on timestamp-based data distribution and sorting, using opacity to represent the timestamp's position within the global time range.

Key steps:
1. **Time-Based Data Distribution**: Data is distributed across nodes based on predefined, non-overlapping time ranges.
2. **Local Sorting**: Each node sorts its local data independently based on timestamps.
3. **Merging**: The sorted data from all nodes is merged to produce the final sorted output.

## 2. Graph Interpretation

The visualization consists of three graphs, each representing a key stage in the sorting process:

### Graph 1: Initial Time-Based Data Distribution

- **X-axis**: Represents the delivery time of data points.
- **Y-axis**: Represents the assigned node (from Node 1 to Node N, where Node 1 handles the oldest data and Node N the newest).
- **Data Points**: Each point represents a single data tuple. There are 10 points per node.
- **Color**: Each node has a distinct color, with cooler colors (e.g., purples, blues) for older data nodes and warmer colors (e.g., yellows, reds) for newer data nodes.
- **Opacity**: Represents how early or late the timestamp is within the global time range. More transparent points have earlier timestamps, while more opaque points have later timestamps.
- **Interpretation**: This graph shows how data is initially distributed across nodes based on delivery time. You should observe:
  - Horizontal bands of color, each representing a node's specific time range.
  - Random distribution of points along the x-axis within each node's time range, representing varying delivery times.
  - Varying opacity of points within each node, representing the actual timestamps of the data.

### Graph 2: Local Sorting on Each Node

- **X-axis**: Represents the timestamp of data points.
- **Y-axis**: Represents the node (from Node 1 to Node N).
- **Data Points**: Each point represents a single data tuple.
- **Color**: Each node retains its distinct color from Graph 1.
- **Opacity**: Maintained from Graph 1, representing the timestamp's position within the global time range.
- **Interpretation**: This graph shows the result after each node has sorted its local data by timestamp. You should observe:
  - Within each node (row), the data points are now ordered from left to right based on their timestamps.
  - The opacity of points generally increases from left to right within each node, reflecting the sorting by timestamp.

### Graph 3: Final Merged Result

- **X-axis**: Represents the timestamp of data points.
- **Y-axis**: No significant meaning (all points are on one line).
- **Data Points**: Each point represents a single data tuple.
- **Color**: Colors are maintained from previous graphs, indicating the original node assignment.
- **Opacity**: Maintained from previous graphs, representing the timestamp's position within the global time range.
- **Color Bar**: A color bar is added to help identify which colors correspond to which nodes.
- **Interpretation**: This graph shows the final, globally sorted data. You should observe:
  - A continuous progression of timestamps from left to right.
  - Interleaving of colors, representing how data from different nodes is merged.
  - A general trend of increasing opacity from left to right, reflecting the global sorting by timestamp.

## 3. Tuple Distribution and Examples

### Tuple Structure

Each data tuple in our implementation has the following structure:

```python
(timestamp, delivery_time, data_id, assigned_node)
```

- `timestamp`: A datetime object representing the actual time of the data point.
- `delivery_time`: A datetime object representing when the data point was delivered to the node.
- `data_id`: A string identifier for the data point.
- `assigned_node`: An integer indicating which node the tuple is initially assigned to.

### Example Tuples

Here are some example tuples:

```python
(datetime(2023, 6, 1, 14, 30), datetime(2023, 6, 2, 10, 15), "data_4_2", 4)  # Timestamp: June 1, Delivered: June 2, Node 4
(datetime(2023, 1, 7, 9, 15), datetime(2023, 1, 7, 9, 20), "data_2_5", 2)    # Timestamp: Jan 7, 9:15, Delivered: Jan 7, 9:20, Node 2
(datetime(2022, 8, 25, 0, 0), datetime(2022, 8, 26, 12, 0), "data_0_8", 0)   # Timestamp: Aug 25, Delivered: Aug 26, Node 0
```

### Node Distribution

Each node is assigned a specific, non-overlapping time range within a total range of one year. Exactly 10 data points are generated for each node, with timestamps randomly distributed within the node's assigned time range. The delivery time for each point is also randomly generated within the same range, simulating real-world scenarios where data might not be processed immediately upon creation.

This distribution method allows for a clear visualization of:
- How data is initially distributed across nodes based on delivery time.
- The relationship between a data point's actual timestamp and its delivery time.
- The effect of local sorting within each node based on actual timestamps.
- The final merged sort across all nodes, maintaining the global timestamp order.

The use of opacity to represent the timestamp's position within the global time range provides an additional dimension of information, making it easier to understand the temporal relationships between data points, both within and across nodes.