# HyDFS

HyDFS is a distributed, fault-tolerant file storage system inspired by HDFS and Dynamo-style key–value stores. It provides scalable, replicated storage using consistent hashing, quorum-based reads and writes, and a gossip/SWIM membership layer for node discovery and failure detection.

The system is designed to remain available under node churn, automatically rebalance data when nodes join or leave, and expose a simple client API for file operations.

---

## Architecture Overview

### Membership and Failure Detection

HyDFS uses a gossip-style failure detector to maintain a consistent view of live nodes:

- Each node periodically exchanges heartbeats with a small random subset of peers.
- Failure suspicion is propagated via gossip until the cluster converges.
- Join/leave events trigger automatic updates to the hash ring.
- Storage and replication adapt dynamically to membership changes.

This decentralized design eliminates single points of failure and enables horizontal scalability.

---

### Consistent Hashing Ring

Data placement is managed through a consistent hashing ring:

- Each node owns one or more virtual positions on the ring.
- Keys are hashed to positions on the ring, determining the primary replica.
- HyDFS uses a replication factor of 3, storing data on the primary node and its next two successors.
- Ring changes only require nearby keys to move, minimizing data migration.

This ensures stable, predictable data distribution even as nodes join or leave.

---

### Replication and Quorum Operations

HyDFS provides strong consistency through quorum reads and writes:

- Writes are sent to all replicas; the client completes after receiving acknowledgments from a quorum.
- Reads query multiple replicas to ensure the latest version is returned.
- Versioning through timestamps or vector metadata ensures conflicts are detected and resolved.
- Background repair processes maintain full replication when failures occur.

---

### Storage Engine

Each node maintains local storage:

- Files are stored in a node-specific storage directory.
- Metadata such as ownership and version information is kept in memory.
- Background tasks handle re-replication and cleanup of outdated versions.

---

### Client API

The HyDFS client (`hyclient`) communicates with any node in the cluster.  
Requests are automatically routed to the correct replicas based on the consistent hashing ring and quorum logic.

Supported commands:

- `create <localfile> <HyDFSfilename>`  
  Uploads a new file into HyDFS. The file is replicated and versioned across the appropriate nodes.

- `get <HyDFSfilename> <localfile>`  
  Fetches a file from HyDFS and writes it to a local path.  
  Includes logs showing replica timing and quorum completion.

- `append <localfile> <HyDFSfilename>`  
  Appends local file contents to an existing HyDFS file.

- `merge <HyDFSfilename>`  
  Forces a merge of all replica versions of a file.

- `ls <prefix>`  
  Lists file keys stored on the cluster that match the prefix.

- `liststore`  
  Lists the contents stored on the current node.

- `list_mem_ids`  
  Returns all node IDs known in the membership list.

- `getfromreplica <vm> <HyDFSfilename> <localfile>`  
  Retrieves a file directly from a specific replica node.

- `multiappend <HyDFSfilename> <vm1,vm2,...> <file1,file2,...>`  
  Performs concurrent append operations pulled from different VMs.

All commands internally perform routed RPC calls and honor read/write quorum semantics.

---

## Running HyDFS

### Starting a Server Node

Run a HyDFS server on each machine:

```bash
./bin/server
```
