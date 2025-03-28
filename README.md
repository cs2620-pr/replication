# Raft Consensus-based Distributed Server System

This project implements a distributed server replication system with leader election using the Raft consensus algorithm. It provides a fault-tolerant key-value store that can withstand multiple node failures.

## Architecture

The system consists of the following components:

1. **Raft Node**: The core component implementing the Raft consensus algorithm
2. **Server**: Runs a Raft node with gRPC services for communication
3. **Client**: Provides a simple interface to interact with the distributed system
4. **Configuration**: Defines the cluster topology and system parameters

### Fault Tolerance

This implementation provides 2+ fault tolerance by:
- Using the Raft consensus algorithm which ensures safety with 2f+1 nodes (where f is the number of tolerable failures)
- The default configuration has 5 nodes, so the system can tolerate 2 node failures
- Leader election automatically occurs if the leader fails
- Log replication ensures all nodes eventually have consistent state

## Project Structure

```
.
├── proto/                  # Protocol buffer definitions
│   └── replication.proto   # gRPC service and message definitions
├── generated/              # Auto-generated gRPC code (created by generate_proto.sh)
├── data/                   # Persistent state storage
├── logs/                   # Server logs
├── config.json             # Cluster configuration 
├── raft_node.py            # Raft consensus implementation
├── server.py               # Server executable
├── client.py               # Client library and CLI
├── run_cluster.py          # Script to run multiple nodes
└── generate_proto.sh       # Script to generate gRPC code
```

## Setup and Installation

1. Ensure you have Python 3.6+ installed
2. Activate the virtual environment:
   ```
   source venv/bin/activate
   ```
3. Install dependencies:
   ```
   pip install grpcio grpcio-tools
   ```
4. Generate the gRPC code:
   ```
   ./generate_proto.sh
   ```

## Usage

### Running a Cluster

USE THIS TO RUN SERVERS:
 
```
python run_cluster.py --config config.json --nodes all --no-auto-restart
```

Start a cluster of servers:

```
python run_cluster.py --config config.json
```

To start specific nodes only:

```
python run_cluster.py --config config.json --nodes 1,2,3
```

### Running the Server Monitor
 
```
python raft_monitor.py
```

### Using the Full Raft Client

```
python full_raft_client.py
```

### IGNORE: Using the Client

Set a key-value pair:

```
python client.py --config config.json --operation set --key mykey --value myvalue
```

Get a value:

```
python client.py --config config.json --operation get --key mykey
```

### Running Individual Nodes

Start a single node:

```
python server.py --id 1 --config config.json
```

## Configuration

The `config.json` file defines the cluster:

```json
{
    "nodes": [
        {"id": 1, "host": "localhost", "port": 50051},
        {"id": 2, "host": "localhost", "port": 50052},
        {"id": 3, "host": "localhost", "port": 50053},
        {"id": 4, "host": "localhost", "port": 50054},
        {"id": 5, "host": "localhost", "port": 50055}
    ],
    "election_timeout_ms": {
        "min": 150,
        "max": 300
    },
    "heartbeat_interval_ms": 50,
    "data_directory": "./data"
}
```

## Key Components of the Implementation

### Raft Consensus Algorithm

The implementation includes:
- Leader Election: Nodes vote to elect a leader
- Log Replication: Leader replicates state changes to followers
- Safety: Ensures all nodes agree on the same sequence of state changes

### gRPC Communication

The system uses gRPC for communication between nodes:
- `NodeCommunication` service for inter-node communication
- `DataService` service for client-to-node communication

### Fault Handling

- Automatic leader election when a leader fails
- Persistence of state to recover from crashes
- Leader redirections for clients
- Retry mechanisms for transient failures

## Testing the System

1. Start the cluster:
   ```
   python run_cluster.py
   ```

2. Set some values:
   ```
   python client.py --operation set --key color --value blue
   ```

3. Get values:
   ```
   python client.py --operation get --key color
   ```

4. Test fault tolerance by killing the leader node (the system will elect a new leader)

## Limitations

- This is a simplified implementation of Raft for educational purposes
- No security features (authentication, encryption)
- Limited to key-value store operations

## Future Enhancements

- Add cluster membership changes
- Implement snapshotting for log compaction
- Add authentication and encryption
- Improve client interface with more operations
