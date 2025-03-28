# Raft Consensus Chat System: Engineering Notebook

## Overview

This document details the design and implementation of a distributed messaging application built on top of the Raft consensus algorithm. The system provides fault-tolerant messaging services with guarantees of consistency even in the presence of node failures.

## 1. System Architecture

### 1.1 Component Overview

The system consists of several key components:

- **Raft Cluster**: A group of server nodes that implement the Raft consensus protocol.
- **MessageService**: Built on top of Raft to provide chat functionality.
- **Chat Client**: A PyQt6-based GUI for end users to interact with the system.

```
┌───────────────┐      ┌───────────────────────────┐
│               │      │                           │
│  Chat Client  │◄─────┤   Raft Server Cluster     │
│    (PyQt6)    │      │                           │
│               │─────►│  ┌─────────────────────┐  │
└───────────────┘      │  │  MessageService     │  │
                       │  │                     │  │
                       │  └─────────────────────┘  │
                       │  ┌─────────────────────┐  │
                       │  │  Raft Consensus     │  │
                       │  │  Algorithm          │  │
                       │  └─────────────────────┘  │
                       │  ┌─────────────────────┐  │
                       │  │  Persistent Storage │  │
                       │  │                     │  │
                       │  └─────────────────────┘  │
                       └───────────────────────────┘
```

### 1.2 Communication Protocol

All inter-component communication is handled through gRPC, with service definitions in `replication.proto`:

- **NodeCommunication**: For inter-node Raft protocol messages
- **DataService**: For key-value operations on the state machine
- **MessageService**: For chat-specific operations
- **MonitoringService**: For health checks and status monitoring

## 2. Raft Consensus Implementation

### 2.1 Core Concepts

Our implementation follows the Raft paper (Ongaro & Ousterhout, 2014) with:

- **Term-based Leadership**: Each term has at most one leader
- **Log Replication**: Commands are replicated across nodes
- **Safety**: If a node has applied a log entry at a given index, no other node will apply a different entry for the same index
- **Liveness**: As long as a majority of nodes are operational, the system continues to function

### 2.2 Server States

Each server exists in one of three states:

1. **Follower**: Passive state, responds to requests from leaders and candidates
2. **Candidate**: Initiates elections to become a leader
3. **Leader**: Handles client requests and manages log replication

```python
# State is managed as an enum in raft_node.py
class NodeState(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2
```

### 2.3 Log Structure

Each log entry contains:
- Term number when the entry was created
- Index position in the log
- Command data (operation to be applied to the state machine)

```python
class LogEntry:
    def __init__(self, term, data, index=None):
        self.term = term
        self.data = data
        self.index = index
```

## 3. Leadership Election

### 3.1 Election Process

Elections are triggered by:
- System initialization
- Timeout when no heartbeat is received from a leader

#### Election Algorithm:

1. Follower increments its term and transitions to candidate state
2. Candidate votes for itself and requests votes from other nodes
3. If a candidate receives votes from a majority of nodes, it becomes leader
4. If a candidate discovers a higher term, it reverts to follower state
5. If election timeout elapses with no winner, a new election begins

```python
def start_election(self):
    """Start a leader election"""
    with self.state_lock:
        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        
        # Reset election timeout
        self.last_heartbeat = time.time()
        self.election_timeout = self.compute_election_timeout()
        
        self.logger.info(f"Starting election for term {self.current_term}")
        
        # Request votes from all other nodes
        votes_received = 1  # Vote for self
        
        last_log_index = len(self.log) - 1
        last_log_term = 0
        if last_log_index >= 0:
            last_log_term = self.log[last_log_index].term
```

### 3.2 Leader Responsibilities

Once elected, a leader:
- Sends heartbeats (empty AppendEntries) to maintain authority
- Manages client requests
- Replicates log entries to followers
- Commits entries when safely replicated to a majority

## 4. Data Replication

### 4.1 Log Replication Process

```
   Client    Leader                             Followers
      │         │                                   │
      ├────────►│                                   │
      │         │                                   │
      │         ├───AppendEntries(log entries)─────►│
      │         │                                   │
      │         ◄─────────Response────────────────┤
      │         │                                   │
      │         │─────────────────────────────────►│
      │         │                                   │
      │         ◄─────────Response────────────────┤
      │         │                                   │
      │         │(After majority of confirmations)  │
      │         │(Leader commits entry)             │
      │         │                                   │
      │         ├────AppendEntries(commit index)───►│
      │         │                                   │
      ◄─────────┤                                   │
```

### 4.2 Consistency Guarantees

The implementation maintains the following consistency properties:

- **Election Safety**: At most one leader per term
- **Leader Append-Only**: Leaders never overwrite or delete entries
- **Log Matching**: If two logs contain an entry with the same index and term, they are identical up to that point
- **Leader Completeness**: If an entry is committed, it will be present in the logs of all future leaders
- **State Machine Safety**: If a node applies a command to its state machine, no other node will apply a different command for the same log index

### 4.3 Next Index and Match Index Tracking

Leaders maintain two critical metadata arrays to track replication status:

- **next_index[]**: Index of the next log entry to send to each follower
- **match_index[]**: Index of highest log entry known to be replicated on each follower

```python
def become_leader(self):
    """Transition to leader state"""
    with self.state_lock:
        if self.state != NodeState.CANDIDATE:
            return False
            
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        
        # Initialize leader state
        self.next_index = {node['id']: len(self.log) for node in self.nodes if node['id'] != self.node_id}
        self.match_index = {node['id']: 0 for node in self.nodes if node['id'] != self.node_id}
```

## 5. Persistent Storage

### 5.1 Persisted State

To recover from crashes, critical state is persisted to disk:

- **Log Entries**: Complete command history
- **Current Term**: Latest term server has seen
- **Voted For**: Candidate that received vote in current term
- **State Machine**: Latest applied state

### 5.2 Persistence Implementation

Data is stored in a structured directory hierarchy:

```
data/
├── node1/
│   ├── log.json
│   ├── metadata.json
│   └── state_machine.json
├── node2/
│   ├── ...
...
```

Persistence operations:

```python
def save_state(self):
    """Save state to disk"""
    # Ensure data directory exists
    os.makedirs(self.data_dir, exist_ok=True)
    
    # Save log
    log_file = os.path.join(self.data_dir, "log.json")
    with open(log_file, 'w') as f:
        log_data = [entry.to_dict() for entry in self.log]
        json.dump(log_data, f)
    
    # Save metadata
    metadata_file = os.path.join(self.data_dir, "metadata.json")
    with open(metadata_file, 'w') as f:
        metadata = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied
        }
        json.dump(metadata, f)
    
    # Save state machine
    state_machine_file = os.path.join(self.data_dir, "state_machine.json")
    with open(state_machine_file, 'w') as f:
        json.dump(self.state_machine, f)
```

### 5.3 Recovery Process

On startup, each node:

1. Loads the log entries
2. Restores metadata (term, voted_for)
3. Rebuilds state machine
4. Starts in follower state

## 6. Fault Tolerance

### 6.1 Theoretical Guarantees

With N nodes, the system can tolerate F failures where:

```
F = (N - 1) / 2
```

For our 5-node cluster, this means the system can tolerate up to 2 node failures.

### 6.2 Leader Failover

When a leader fails:

1. Followers stop receiving heartbeats
2. After election timeout, followers become candidates
3. A new leader is elected
4. System continues operation with new leader

### 6.3 Handling Node Rejoins

When a failed node rejoins the cluster:

1. It starts as a follower
2. The current leader sends AppendEntries RPCs to help it catch up
3. Log inconsistencies are resolved by the leader (overwriting divergent entries)
4. Once caught up, the node participates normally in the cluster

### 6.4 Partial Network Failures

The system handles network partitions by:

- Ensuring only one partition (with majority) can elect a leader
- Minority partitions remain in candidate or follower state
- When partitions heal, nodes in minority partition detect higher term and follow the established leader

## 7. MessageService Implementation

### 7.1 Architecture

The MessageService is built on top of the Raft consensus layer, using the replicated state machine for:

- User account management
- Message storage and retrieval
- Online status tracking

```python
class MessageService(pb2_grpc.MessageServiceServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node
        self.session_tokens = {}  # Map of session tokens to user IDs
        
        # Initialize data structures in the Raft state machine
        self._initialize_data_structures()
```

### 7.2 State Machine Data Model

The state machine maintains structured data for:

```
users: {
    "user_id1": {
        "username": "alice",
        "password_hash": "...",
        "display_name": "Alice",
        "created_at": 1678124567
    },
    ...
}

messages: {
    "message_id1": {
        "sender_id": "user_id1",
        "receiver_id": "user_id2", 
        "content": "Hello there!",
        "timestamp": 1678125567,
        "read": false
    },
    ...
}

online_users: ["user_id1", "user_id3", ...]
```

### 7.3 Interaction with Raft

Operations flow:

1. Client invokes MessageService RPC
2. MessageService processes request and creates necessary state machine commands
3. Commands are proposed to the Raft cluster via _set_value()
4. Raft replicates the commands to all nodes
5. Once committed, the state machine is updated
6. Response is returned to client

## 8. Performance Considerations

### 8.1 Consensus Latency

Raft requires at least one round-trip to a majority of nodes before committing, resulting in:

- Minimum latency = 1 RTT (Round Trip Time)
- Typical latency = 1-2 RTTs

### 8.2 Optimizations

Several optimizations are implemented:

- **Batching**: Multiple client requests can be batched in a single AppendEntries
- **Parallel AppendEntries**: Sent to all followers simultaneously
- **Client Connection Caching**: Maintain open connections to reduce setup overhead

### 8.3 Scaling Considerations

The current implementation has some scaling limitations:

- All nodes store the complete state machine
- All nodes process all commands
- O(N²) communication complexity for the Raft protocol

Potential improvements:
- Read-only queries could be served directly from followers
- Sharding across multiple Raft groups for horizontal scaling

## 9. Testing Fault Tolerance

### 9.1 Test Methodology

The system's fault tolerance can be tested by:

1. Starting with all 5 nodes
2. Verifying normal operation
3. Killing nodes one by one (up to 2)
4. Confirming continued operation with 3 nodes
5. Killing a third node to break quorum
6. Observing system behavior without consensus

### 9.2 Observed Results

When testing with our 5-node cluster:

- System operates normally with 5 nodes
- System continues functioning with 4 nodes (1 failure)
- System continues functioning with 3 nodes (2 failures)
- System becomes unavailable with only 2 nodes (3 failures)
- When failed nodes are restarted, the system resumes normal operation

## 10. Conclusion

This implementation demonstrates a robust distributed system using the Raft consensus algorithm. Key strengths include:

- Strong consistency guarantees
- Ability to tolerate minority node failures
- Automatic leader election and failover
- Persistent storage for crash recovery
- Clean separation between consensus and application layers

Future work could focus on:
- Improving read scalability
- Adding more sophisticated monitoring
- Implementing snapshotting for log compaction
- Supporting dynamic membership changes

## References

1. Ongaro, D., & Ousterhout, J. (2014). In search of an understandable consensus algorithm. USENIX Annual Technical Conference.
2. The Raft Consensus Algorithm. https://raft.github.io/
3. Lamport, L. (2001). Paxos made simple. ACM SIGACT News. 