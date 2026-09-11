# CacheX

> **Design, Implementation and Performance Evaluation of a Distributed In-Memory Cache**

## Overview

CacheX is a distributed in-memory key-value cache written in Rust. It is designed as both a systems engineering project and an experimental platform for studying trade-offs in distributed caching systems.

The goal is not to reproduce Redis Cluster. Instead, CacheX focuses on building a smaller, modular cache and experimentally evaluating **data partitioning, replication, scalability, memory management, persistence, and fault tolerance**.

## Project Information

| Item | Details |
|---|---|
| Project | CacheX |
| Domain | Distributed Systems, Computer Networks, Operating Systems |
| Language | Rust |
| Duration | Approximately 4–5 months |
| Students | Tirth Patel (202301023), Jill Chhagnani (202301273) |

## Motivation

Modern applications use distributed caches to reduce database load and improve latency. Production systems such as Redis Cluster and Memcached involve important trade-offs related to scalability, replication, memory management, and fault tolerance.

CacheX aims to expose these concepts through a modular implementation that can also be used to experimentally answer questions such as:

- How does consistent hashing compare with modulo-based partitioning?
- What overhead does replication introduce?
- How does performance change as nodes and clients increase?
- How do TTL and LRU eviction affect cache efficiency?
- How quickly can failures be detected and handled?

## Goals

- Build a modular in-memory key-value cache.
- Implement a custom TCP-based protocol.
- Support concurrent client connections.
- Extend the system from a single node to a multi-node cluster.
- Implement modulo and consistent-hashing partitioning.
- Support primary-replica replication.
- Implement TTL expiration and LRU eviction.
- Add Append-Only File (AOF) persistence and recovery.
- Detect node failures using heartbeats.
- Collect runtime metrics.
- Benchmark and evaluate different system configurations.

# High-Level Architecture

```text
                         +----------------------+
                         |      Dashboard       |
                         |  Metrics / Charts    |
                         +----------+-----------+
                                    |
                              HTTP / WebSocket
                                    |
                                    v
+---------------------------------------------------------------+
|                         CacheX Cluster                        |
|                                                               |
|   +-------------+       +-------------+       +-------------+ |
|   |   Node A    |       |   Node B    |       |   Node C    | |
|   | TCP Server  |       | TCP Server  |       | TCP Server  | |
|   | Storage     |       | Storage     |       | Storage     | |
|   | AOF         |       | AOF         |       | AOF         | |
|   | Metrics     |       | Metrics     |       | Metrics     | |
|   +------+------+       +------+------+       +------+------+ |
+----------|---------------------|---------------------|----------+
           |                     |                     |
           +----------+----------+----------+----------+
                      |
                      v
              +---------------+
              | CacheX Client |
              | Request Router|
              +---------------+
                      ^
                      |
                 Application
```

# Core Components

## Protocol

CacheX uses a custom application-level protocol over TCP.

Supported operations:

```text
GET
SET
DELETE
PING
INFO
```

Requests and responses are serialized using:

```text
Serde + Bincode
```

Since TCP is a byte stream and does not preserve message boundaries, CacheX will use **length-prefixed framing**:

```text
+--------------------+-------------------------+
| Message Length     | Serialized Message      |
| 4 bytes            | N bytes                 |
+--------------------+-------------------------+
```

## Single-Node Cache

The project starts with a standalone cache server:

```text
cachex-cli
     |
     | TCP
     v
+-------------------+
| cachex-server     |
|                   |
| TCP Listener      |
|       |           |
| Request Handler   |
|       |           |
| In-Memory Store   |
+-------------------+
```

Example:

```text
> SET name Tirth
OK

> GET name
VALUE Tirth

> DELETE name
OK

> PING
PONG
```

## Storage Engine

The storage engine manages:

```text
Key -> Entry

Entry
 ├── Value
 ├── Creation Time
 ├── Expiration Time
 └── Access Metadata
```

Initial storage:

```text
HashMap<Key, Entry>
```

Concurrent access will initially use:

```text
Arc<RwLock<HashMap<...>>>
```

`DashMap` may later be evaluated as an alternative implementation.

## Concurrent Request Handling

CacheX will use Tokio for asynchronous networking:

```text
                     Tokio Runtime
                           |
            +--------------+--------------+
            |              |              |
         Client 1       Client 2       Client N
            |              |              |
            +--------------+--------------+
                           |
                           v
                    Shared Storage
```

## TTL

Keys may have a Time-To-Live:

```text
SET session123 abc TTL 60
```

Expired keys can be removed:

1. Lazily during reads.
2. By a periodic background cleanup task.

## LRU Eviction

When the configured cache capacity is reached, CacheX will evict entries using the **Least Recently Used (LRU)** policy.

```text
Most Recently Used

[A] <-> [B] <-> [C] <-> [D]

                    Least Recently Used
```

## Persistence

Mutating operations will be stored in an Append-Only File:

```text
SET user1 Alice
SET user2 Bob
DELETE user1
```

On restart:

```text
Start Server
     |
     v
Read AOF
     |
     v
Replay Operations
     |
     v
Reconstruct In-Memory State
```

# Distributed Architecture

After the single-node implementation is stable, multiple CacheX instances will form a cluster.

```text
              CacheX Client
                    |
              Partitioner
                    |
          +---------+---------+
          |         |         |
          v         v         v
        Node A    Node B    Node C
```

Nodes can initially be configured statically:

```toml
[[nodes]]
id = "node-a"
address = "127.0.0.1:7001"

[[nodes]]
id = "node-b"
address = "127.0.0.1:7002"

[[nodes]]
id = "node-c"
address = "127.0.0.1:7003"
```

# Data Partitioning

Partitioning will be implemented as an abstraction:

```text
                  Partitioner
                       |
            +----------+----------+
            |                     |
            v                     v
     Modulo Partitioning    Consistent Hashing
```

This allows the same workload to be evaluated against multiple partitioning strategies.

### Modulo Partitioning

```text
node_index = hash(key) % number_of_nodes
```

### Consistent Hashing

Nodes are placed on a logical hash ring:

```text
                    Node A
                      *
               *             *

         Node C                   Node B
```

A key is assigned to the next node clockwise on the ring.

# Replication

CacheX will implement primary-replica replication.

```text
Client
   |
   v
Primary Node
   |
   +---- Store Locally
   |
   +---- Replicate ----> Replica Node
```

Initial configurations:

```text
Replication Factor = 1
Replication Factor = 2
```

These configurations will be used to measure the trade-off between redundancy and performance.

# Failure Detection

Nodes will exchange heartbeat messages:

```text
Node A ------ PING ------> Node B
Node A <----- PONG ------- Node B
```

Possible node states:

```text
ALIVE
  |
  v
SUSPECT
  |
  v
DEAD
```

Full consensus-based failover is outside the initial project scope.

# Metrics and Dashboard

Nodes will collect metrics such as:

- Total requests.
- Cache hits and misses.
- Active connections.
- Memory usage.
- Network bytes.
- Replication success/failure.
- Average latency.
- P50, P95, and P99 latency.

A lightweight dashboard will display:

```text
Cluster Status
Node Health
Memory Usage
Requests / Second
Request Latency
Cache Hit Ratio
Active Connections
Replication Status
```

The dashboard is a supporting tool; the main contribution remains the distributed system and its experimental evaluation.

# Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Rust |
| Async Runtime | Tokio |
| Networking | TCP Sockets |
| Serialization | Serde + Bincode |
| Concurrent Storage | `RwLock<HashMap>` / DashMap |
| Persistence | Append-Only File |
| Backend API | Axum |
| Frontend | React + Tailwind CSS |
| Real-Time Updates | WebSockets |
| Charts | Chart.js / Recharts |
| Benchmarking | Criterion + Custom Workload Generator |
| Testing | Rust Test Framework + Integration Tests |

# Proposed Repository Structure

```text
cachex/
|
├── Cargo.toml
├── crates/
│   ├── cachex-protocol/
│   ├── cachex-server/
│   ├── cachex-client/
│   └── cachex-cli/
├── benchmarks/
├── dashboard/
│   ├── backend/
│   └── frontend/
├── configs/
├── scripts/
├── docs/
└── README.md
```

The repository should grow gradually. Not every directory needs to exist from day one.

# Development Phases

## Phase 1 — Single-Node Cache and Networking

Build:

- TCP server.
- TCP client / CLI.
- Length-prefixed framing.
- Serde + Bincode serialization.
- `GET`, `SET`, `DELETE`, `PING`, and `INFO`.
- Concurrent client handling.
- Thread-safe in-memory storage.
- Unit and integration tests.

**Definition of Done:** A client can perform concurrent cache operations against a real TCP server.

## Phase 2 — Storage Features

Add:

```text
TTL
LRU Eviction
Memory Limits
AOF Persistence
Crash Recovery
```

## Phase 3 — Distributed Cluster

Add:

```text
Multiple Nodes
Client-Side Routing
Partitioner Abstraction
Modulo Partitioning
Consistent Hashing
```

Phase 3 uses static client-side routing. The preferred command-line form is:

```powershell
cargo run -p cachex-cli -- `
  --nodes "node-a=127.0.0.1:7001,node-b=127.0.0.1:7002,node-c=127.0.0.1:7003" `
  --partitioner consistent
```

The equivalent environment variables remain supported. `--partitioner`
accepts `consistent` (the default) or `modulo`. The client validates the
complete topology at startup, including empty IDs,
duplicate IDs, duplicate addresses, and invalid socket addresses. `GET`,
`SET`, and `DELETE` are routed by key; servers remain unaware of the cluster.
`PING` and `INFO` use the first configured node. Single-node behavior remains
available through `CACHEX_ADDR` without setting `CACHEX_NODES`.

Phase 3 intentionally opens and closes one TCP connection per command. It has
no connection pool, retry, failover, or failure detection; connection reuse is
reserved for a later performance comparison.

See [commands.md](commands.md) for the preferred command-line workflow.

## Phase 4 — Replication and Fault Tolerance

Add:

```text
Primary-Replica Replication
Replication Factor 1 and 2
Heartbeats
Failure Detection
Basic Recovery / Rerouting
```

Phase 4 uses the existing static topology. The preferred startup and manual
testing commands are documented in [commands.md](commands.md). Environment
variables remain supported for compatibility, but are no longer required.

Mutations are written locally and sent to the replica using internal framed
protocol commands. Replica application does not fan out again, preventing
replication loops. Nodes exchange `Heartbeat` messages every second by
default and log `SUSPECT`, `DEAD` (after three missed probes), and `RECOVERED`
transitions. The client retries key operations against the replica candidates
when the primary connection fails, providing basic rerouting during failure.
Set `CACHEX_HEARTBEAT_INTERVAL_MS` to tune the probe interval.

## Phase 5 — Evaluation and Monitoring

The first Phase 5 deliverable is implemented: a React/Vite monitoring
dashboard with a lightweight HTTP/JSON bridge exposed by each CacheX server.
It provides live node overview data, health/connection status, local key and
memory metrics, replication-factor visibility, and GET/SET/DELETE operations.
See [commands.md](commands.md) for startup instructions.

The workload generator, benchmark suite, and full experimental evaluation
remain future Phase 5 work.

# Experimental Evaluation

## Data Distribution

Compare:

```text
Modulo Partitioning
        vs
Consistent Hashing
```

Metrics:

- Load distribution.
- Key redistribution during node addition/removal.
- Lookup latency.

## Replication

Compare:

```text
Replication Factor = 1
        vs
Replication Factor = 2
```

Metrics:

- Read throughput.
- Write throughput.
- Request latency.
- Storage overhead.
- Network overhead.

## Scalability

Increase the number of:

- Cache nodes.
- Concurrent clients.

Measure:

- Throughput.
- Average latency.
- P95/P99 latency.
- CPU utilization.
- Memory utilization.

## Memory Management

Evaluate:

- Different cache capacities.
- TTL behavior.
- LRU eviction.

Measure:

- Cache hit ratio.
- Memory usage.
- Number of evictions.

## Fault Tolerance

Simulate node failures and measure:

- Failure detection time.
- Recovery time.
- Request success rate.
- System availability.

# Research Questions

1. How does consistent hashing compare with modulo-based partitioning in terms of load balancing and key redistribution?
2. What overhead does primary-replica replication introduce compared to a non-replicated cache?
3. How does increasing the replication factor affect throughput, latency, and storage overhead?
4. How well does the cache scale with increasing numbers of nodes and concurrent clients?
5. How do TTL expiration and LRU eviction influence cache hit ratio and memory utilization?
6. How effectively can the system detect and handle simulated node failures?

# Expected Outputs

- A modular distributed in-memory cache written in Rust.
- A custom TCP-based protocol.
- Modulo and consistent-hashing partitioners.
- Primary-replica replication.
- TTL and LRU memory management.
- Append-only persistence and recovery.
- Heartbeat-based failure detection.
- Runtime metrics and monitoring.
- Benchmarking tools and reproducible workloads.
- Experimental results and performance analysis.
- Technical documentation.

# Non-Goals

To keep the project achievable, the initial implementation will not include:

- Raft or Paxos consensus.
- Strongly consistent distributed transactions.
- Full automatic cluster membership.
- Multi-region deployment.
- Kubernetes orchestration.
- Redis protocol compatibility.
- Authentication and ACLs.

These can be explored as future work.

# Development Principles

### Build incrementally

```text
Single Node
    |
    v
Storage Features
    |
    v
Multiple Nodes
    |
    v
Partitioning
    |
    v
Replication
    |
    v
Failure Detection
    |
    v
Benchmarking
```

### Correctness before optimization

```text
Correct
   |
   v
Measurable
   |
   v
Optimized
```

### Keep modules independent

The architecture should keep these components as separate as possible:

```text
Networking
Protocol
Storage
Partitioning
Replication
Persistence
Cluster Management
Metrics
```

# First Milestone

> A CacheX client can connect to a CacheX server over TCP and perform concurrent `GET`, `SET`, `DELETE`, `PING`, and `INFO` operations against a thread-safe in-memory key-value store.

This is where development begins.

# Future Extensions

Potential future work:

- Raft-based metadata management.
- Dynamic cluster membership.
- Automatic failover.
- Asynchronous replication.
- Adaptive replication.
- Redis-compatible RESP protocol.
- Authentication and ACLs.
- Pub/Sub.
- Prometheus and Grafana.
- Docker and Kubernetes deployment.

# License

This project is being developed as an academic Bachelor Minor Project (BMP). License details will be decided later.
