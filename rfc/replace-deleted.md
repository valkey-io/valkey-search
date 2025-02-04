# RFC: Versioning-Based Lazy (and Optional Synchronous) Repair in HNSW

**Author(s):** Vitaly Arbuzov 
**Created:** 02/04/2025 
**Status:** Draft / Proposed

---

## 1. Overview and Motivation

### 1.1 Background

Hierarchical Navigable Small World (HNSW) graphs are widely used for Approximate Nearest Neighbor (ANN) search due to their efficient graph-based structure. However, in production systems that support dynamic updates (e.g., new data points arriving or existing ones being removed), deleted nodes are sometimes reused (via `allow_replace_deleted_` in hnswlib). This reuse introduces two major problems:

- **Stale Inbound Links**: When a node is deleted and subsequently reused, any existing inbound links from other nodes’ adjacency lists point to a different “state.” These stale references mislead search queries.
- **Recall Degradation**: Searches that traverse stale edges can produce suboptimal or incorrect nearest-neighbor results—an issue with potentially severe impact on high-recall vector search applications.

### 1.2 Motivation for a Versioning-Based Repair Mechanism

To mitigate the impact of stale links without fully rebuilding the index, this RFC proposes a lightweight versioning system that:

- **Tracks Node Reuse**: Each node has a version counter incremented every time the node is reused (after being deleted).
- **Detects Stale Edges**: Adjacency lists store both an identifier and the version of the target node at link creation time. During search, edges whose stored version doesn’t match the node’s current version are treated as invalid (stale).
- **Supports Lazy Repair**: An asynchronous background process progressively removes or updates invalid edges, preventing them from repeatedly impacting query results.
- **Enables Optional Synchronous Repair**: For environments where immediate consistency is essential (and some write latency is acceptable), stale edges can be repaired on the spot—trading off query-time locking for guaranteed consistency.

This solution aims to maintain high recall over time, accommodating frequent updates with minimal overhead. While a 16-bit version counter can theoretically wrap around, the probability of such a collision causing an incorrect nearest neighbor is extremely low. Even in the rare event of a collision, the system behaves no worse than the current (stale-link) approach.

---

## 2. Research and Industry Context

- **Xiao et al. (2024).** *Enhancing HNSW Index for Real-Time Updates (arXiv)*. Describes how frequent deletions and replacements degrade search performance in HNSW. [Link](https://arxiv.org/abs/2407.07871)

- **Pinecone Engineering (2023).** Blog post discussing differences between hard vs. soft deletes and how stale links can accumulate in HNSW. [Link](https://www.pinecone.io/blog/hnsw-not-enough/)

- **QuantumTrail (2023).** Industry discussions on the pitfalls of managing real-time updates in HNSW-based systems. [Link](https://dev.to/quantumtrail/hnsw-in-vector-database-13kk)

- **FreshDiskANN (Singh et al., 2021).** Demonstrates how incremental link repair enhances ANN performance in streaming scenarios. [Link](https://arxiv.org/abs/2105.09613)

The need for a structured fix to stale inbound links is broadly recognized. Our version-based proposal combines simple checks (to skip stale edges) with a practical mechanism for gradual or immediate repair.

---

## 3. Proposed Solution: Versioning-Based Repair

### 3.1 Core Idea

Each node maintains a small version counter (e.g., `uint16_t` or `uint32_t`). Adjacency entries then include:

- `neighbor_id`: The unique ID of the neighbor.
- `neighbor_version`: The version of the neighbor at the time this link was created.

Whenever a node is deleted and reused:

1. The node’s version is incremented, making old edges pointing to the old version immediately recognizable as stale.
2. Queries compare an edge’s stored version to the node’s current version; mismatches indicate a stale link and are skipped.

#### Addressing Version Wraparound

With a 16-bit counter, wraparound occurs only after 65,536 increments of the same node’s version. Because the chance of collision aligning with a genuinely “near” neighbor in high-dimensional space is minimal, the probability of a noticeable negative impact is extremely small. Even if a collision leads to following a stale link, this case is no worse than the uncorrected scenario.

### 3.2 How It Works

- **Node Replacement**:  
  When reusing a deleted node, do:
  ```cpp
  node_versions_[node_id].fetch_add(1, std::memory_order_relaxed);
  ```
  Any newly formed connections then record the updated node version.

- **Query Traversal**:  
  During search (e.g., within `searchBaseLayerST` in HNSWlib), compare each edge’s stored version against the current node version:
  ```cpp
  auto entry = neighbor_list[j];
  uint16_t stored_version = entry.version;
  uint16_t current_version = node_versions_[entry.neighbor_id].load(std::memory_order_relaxed);

  if (stored_version != current_version) {
      // Edge is stale—skip it
      continue;
  }
  // Otherwise, process the neighbor
  ```
  This ensures search logic avoids stale edges on-the-fly.

- **Lazy Repair (Asynchronous)**:  
  As soon as an edge is identified as stale, it can be placed into a repair queue. A background thread later acquires locks and removes (or updates) these stale links in batches. This way, queries remain fast, while stale edges are eventually cleared.

- **Synchronous Repair (Optional)**:  
  For environments with stricter consistency needs or offline maintenance windows, edges can be repaired as soon as they are discovered to be stale. Although this approach increases write-lock contention, it ensures that the graph remains fully consistent at all times—potentially useful for high-accuracy use cases.

---

## 4. Detailed Changes and Implementation Outline

### 4.1 Data Structure Modifications

- **Node Version Array**:
  ```cpp
  std::vector<std::atomic<uint16_t>> node_versions_;
  ```
  Each node’s version is stored here. This counter increments each time a node is reused.

- **Adjacency List Storage**:
  ```cpp
  struct NeighborEntry {
      tableint neighbor_id;
      uint16_t version;
  };
  // Each adjacency list is effectively an array of NeighborEntry
  ```
  An additional 2 bytes per adjacency entry are needed for the version. (Optionally, `uint32_t` could be used if extremely high churn is expected.)

### 4.2 Overhead Calculations

**Per-Edge Overhead:**  
Each edge (adjacency) in the graph stores a version value. Using a 16-bit counter adds 2 bytes per edge, while a 32-bit counter requires 4 bytes per edge.

**Per-Node Overhead:**  
Each node maintains its own version counter, adding an additional 2 bytes (for a 16-bit counter) or 4 bytes (for a 32-bit counter) per node.

**Example Calculation:**

Suppose we have an HNSW graph with the following parameters:
- **Average number of neighbors per node (M):** 16  
- **Average number of layers per node:** 1.2  
- **Total number of nodes:** 100 million (i.e., 100 x 10^6)

1. **Total Number of Edges:**  
   The approximate number of edges in the graph is calculated as follows:  
   Edges = M × (layers per node) × (total nodes)  
         = 16 × 1.2 × 100 million  
         = 1.92 billion edges

2. **Extra Memory for Versioning (using a 16-bit counter):**  
   With 2 bytes per edge, the extra memory required is:  
   Extra Memory = 1.92 billion edges × 2 bytes per edge  
                = 3.84 GB

*Note:* Although this additional memory overhead is significant, it is often acceptable in large-scale systems that already allocate substantial memory for vector data. The actual overhead will vary based on the exact layer distribution and on whether a 16-bit or 32-bit version counter is used.
It's also possible to use fewer bits in the spin counter reducing total memory requirements, as in case of accidental match between node and edge version (after a full spin) connection will simply stay a little longer and be repaired after the next update and encounter.

### 4.3 Repair Queue and Batch Processing

- **Repair Queue**:  
  Each encounter with a stale edge enqueues a repair task (e.g., `(owner_node_id, edge_index)` or `(neighbor_id, stored_version)`).

- **Batch Repairs**:  
  A background process periodically locks node adjacency lists, removes or fixes stale entries, and moves on.

- **Overload Protection**:  
  If the queue grows beyond a set limit, new repair tasks can be dropped to prevent memory bloat. If too many tasks are dropped, a “bulk repair” pass (e.g., a full traversal) can be triggered to ensure consistent cleanup.

### 4.4 Concurrency

- **Locking in HNSWlib**:  
  Typically, HNSWlib ensures no writes happen concurrently with queries by employing top-level locking (plus finer-grained adjacency-list locks). For asynchronous repair, the background repair thread would acquire locks on adjacency lists just as normal writes do.  
  - In high-throughput scenarios, the frequency of updates (and repairs) should be balanced to avoid long queues or excessive lock contention.
  - Synchronous repair will perform these adjacency-list modifications at query time, guaranteeing consistency but at the cost of higher write latency.

---

## 5. Testing and Evaluation Plan

### 5.1 Experimental Setup

- **Datasets**: Use standard ANN benchmarks like SIFT1M and DEEP1B.
- **Index Configurations**:
  1. Baseline HNSW (`allow_replace_deleted_ = true`), no versioning.
  2. Versioned HNSW with asynchronous repair.
  3. Versioned HNSW with synchronous repair.

### 5.2 Metrics

- **Recall**: Compare recall@k to assess if stale edges are effectively skipped or repaired.
- **Query Latency**: Measure average and tail (p95/p99) latencies.
- **Update Throughput**: Gauge insertion and node replacement rates.
- **Stale Edge Count**: Track how many stale edges are discovered and how quickly they are removed.
- **Memory Usage**: Evaluate overhead compared to baseline in large-scale or realistic scenarios.

### 5.3 Expected Outcomes

- **Stable Recall**: By skipping links with mismatched versions, versioning should limit stale edge impact.
- **Low Latency Impact**: Checking a 16-bit version is minor overhead per adjacency.
- **Effective Repair**: Both asynchronous and synchronous modes address stale edges, with the former favoring minimal query-time cost and the latter favoring immediate consistency.

---

## 6. Additional Considerations

- **Version Wraparound**: With a 16-bit version counter, the rarity of wraparound means collisions are highly unlikely. If one occurs, it simply behaves like a non-versioned stale link—statistically tolerable.
- **Synchronous vs. Asynchronous**:
  - *Asynchronous* repair amortizes cleanup over time and keeps query latencies low, at the cost of a short window where stale edges remain.
  - *Synchronous* repair eliminates stale edges immediately but increases write-lock contention, which may spike latencies for inserts or updates.
- **Migration Path**:  
  - Adding version fields to adjacency lists and nodes requires changes to on-disk and in-memory structures. An in-place upgrade may be complex, so a rebuild or partial reindex is likely.  
  - For backward compatibility, handling older index files without version metadata is necessary (e.g., by providing a conversion routine or a fallback behavior).

- **High-Churn Environments**: If node reuse is extremely frequent (e.g., tens of thousands of reuses for the same node ID), consider using a 32-bit version counter to further reduce the risk of wraparound.

---

## 7. Implementation Roadmap

1. **Phase 1: Prototype**  
   - Implement node version storage (`node_versions_`).  
   - Modify adjacency lists to store `(neighbor_id, neighbor_version)`.  
   - Increment version on node reuse.

2. **Phase 2: Core Integration**  
   - Update search logic to skip edges with stale versions.  
   - Implement both asynchronous and immediate link repair modes.

3. **Phase 3: Testing and Benchmarking**  
   - Evaluate performance on SIFT1M, DEEP1B with various update loads.  
   - Compare recall, latency, and overhead to baseline HNSW.

4. **Phase 4: Optimization**  
   - Tune memory usage, concurrency approach, and background repair intervals.  
   - Address edge cases or performance bottlenecks discovered in testing.

5. **Phase 5: Documentation & Release**  
   - Provide clear documentation, including configuration options (enable versioning, queue thresholds, repair mode).  
   - Gather user feedback and finalize.

---

## 8. Conclusion

This RFC proposes a versioning-based approach to address the stale inbound link problem in HNSW:

- **Immediate Identification of Stale Links**: By embedding a version counter into both nodes and their adjacency references, the search process can detect and skip outdated links in real time.
- **Flexible Repair**: A background (lazy) repair process removes invalid links with minimal interference, while an optional synchronous approach achieves immediate consistency at higher lock cost.
- **Manageable Overhead**: Although storing versions increases memory usage (2–4 bytes per edge), this overhead is typically acceptable for large vector indices that already prioritize recall and throughput.
- **Statistically Safe**: With small version counters (e.g., 16-bit), wraparound collisions are exceedingly rare, and even then the worst case is merely a reversion to the existing stale-link scenario.

Implementing this incremental version-based fix can significantly stabilize recall in HNSW-based ANN services. Pending final approval, the next steps involve prototyping, followed by integration, testing, and iterative refinement as outlined in the roadmap.
