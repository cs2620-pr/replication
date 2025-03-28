import os
import json
import time
import random
import threading
import logging
import pickle
from concurrent import futures
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import grpc

# Import the generated gRPC code
from generated import replication_pb2
from generated import replication_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class NodeState(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()

class LogEntry:
    def __init__(self, term: int, command: Dict, index: int):
        self.term = term
        self.command = command
        self.index = index
    
    def to_pb(self):
        """Convert to protobuf LogEntry"""
        pb_entry = replication_pb2.LogEntry()
        pb_entry.term = self.term
        pb_entry.index = self.index
        pb_entry.data = pickle.dumps(self.command)
        return pb_entry
    
    @classmethod
    def from_pb(cls, pb_entry):
        """Create LogEntry from protobuf LogEntry"""
        command = pickle.loads(pb_entry.data)
        return cls(pb_entry.term, command, pb_entry.index)

class RaftNode(replication_pb2_grpc.NodeCommunicationServicer, 
               replication_pb2_grpc.DataServiceServicer,
               replication_pb2_grpc.MonitoringServiceServicer):
    def __init__(self, node_id: int, config_path: str):
        self.node_id = node_id
        self.config_path = config_path
        self.load_config()
        
        # Initialize logger first
        self.logger = logging.getLogger(f"Node-{self.node_id}")
        self.logger.info(f"Initializing node {self.node_id}")
        
        # Node state
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        
        # Log and state machine
        self.log: List[LogEntry] = []
        self.commit_index = -1
        self.last_applied = -1
        
        # Leader state (reinitialized after election)
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}
        
        # Persistent state
        self.state_machine: Dict[str, str] = {}
        self.load_persistent_state()
        
        # Volatile state
        self.election_timeout = self.generate_election_timeout()
        self.last_heartbeat = time.time()
        
        # Locks
        self.state_lock = threading.RLock()
        self.state_machine_lock = threading.RLock()
        
        # Start threads
        self.running = True
        self.election_thread = threading.Thread(target=self.run_election_timeout)
        self.election_thread.daemon = True
        self.election_thread.start()
        
        self.apply_thread = threading.Thread(target=self.apply_committed_entries)
        self.apply_thread.daemon = True
        self.apply_thread.start()
        
        # Leader heartbeat thread (started when becoming leader)
        self.heartbeat_thread = None
        
        # Track peer connection status and last contact time
        self.peer_connection_status = {node['id']: False for node in self.nodes if node['id'] != self.node_id}
        self.peer_last_contact = {node['id']: 0 for node in self.nodes if node['id'] != self.node_id}
        
        # Record start time for uptime measurement
        self.start_time = time.time()
        
        self.logger.info(f"Initialized node {self.node_id} as FOLLOWER")
    
    def load_config(self):
        """Load configuration from config file"""
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        
        self.nodes = config['nodes']
        self.node_map = {node['id']: (node['host'], node['port']) for node in self.nodes}
        self.election_timeout_range = (
            config['election_timeout_ms']['min'] / 1000,
            config['election_timeout_ms']['max'] / 1000
        )
        self.heartbeat_interval = config['heartbeat_interval_ms'] / 1000
        self.data_dir = config['data_directory']
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(f"{self.data_dir}/node{self.node_id}", exist_ok=True)
    
    def generate_election_timeout(self) -> float:
        """Generate a random election timeout"""
        return random.uniform(*self.election_timeout_range)
    
    def load_persistent_state(self):
        """Load persistent state from disk"""
        state_path = os.path.join(
            self.data_dir,
            f"node{self.node_id}",
        )
        
        # Create directory if it doesn't exist
        os.makedirs(state_path, exist_ok=True)
        
        log_path = os.path.join(state_path, "log.json")
        state_machine_path = os.path.join(state_path, "state_machine.json")
        metadata_path = os.path.join(state_path, "metadata.json")
        
        # Load log
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r') as f:
                    log_data = json.load(f)
                    self.log = [
                        LogEntry(entry["term"], entry["command"], entry["index"])
                        for entry in log_data
                    ]
                    if self.log:
                        self.logger.info(f"Loaded {len(self.log)} log entries from disk")
            except (json.JSONDecodeError, IOError) as e:
                self.logger.error(f"Error loading log: {e}")
                self.log = []
        
        # Load state machine
        if os.path.exists(state_machine_path):
            try:
                with open(state_machine_path, 'r') as f:
                    self.state_machine = json.load(f)
                    self.logger.info(f"Loaded state machine with {len(self.state_machine)} items from disk")
            except (json.JSONDecodeError, IOError) as e:
                self.logger.error(f"Error loading state machine: {e}")
                self.state_machine = {}
        
        # Load metadata
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                    self.current_term = metadata.get("current_term", 0)
                    self.voted_for = metadata.get("voted_for")
                    self.commit_index = metadata.get("commit_index", -1)
                    self.last_applied = metadata.get("last_applied", -1)
                    self.logger.info(f"Loaded metadata from disk: term={self.current_term}, commit_index={self.commit_index}, last_applied={self.last_applied}")
            except (json.JSONDecodeError, IOError) as e:
                self.logger.error(f"Error loading metadata: {e}")
    
    def save_persistent_state(self):
        """Save persistent state to disk"""
        state_path = os.path.join(
            self.data_dir,
            f"node{self.node_id}",
        )
        
        # Create directory if it doesn't exist
        os.makedirs(state_path, exist_ok=True)
        
        log_path = os.path.join(state_path, "log.json")
        state_machine_path = os.path.join(state_path, "state_machine.json")
        metadata_path = os.path.join(state_path, "metadata.json")
        
        # Save log
        try:
            with open(log_path, 'w') as f:
                log_data = [
                    {"term": entry.term, "command": entry.command, "index": entry.index}
                    for entry in self.log
                ]
                json.dump(log_data, f, indent=2)
        except IOError as e:
            self.logger.error(f"Error saving log: {e}")
        
        # Save state machine
        try:
            with open(state_machine_path, 'w') as f:
                json.dump(self.state_machine, f, indent=2)
        except IOError as e:
            self.logger.error(f"Error saving state machine: {e}")
        
        # Save metadata
        try:
            with open(metadata_path, 'w') as f:
                metadata = {
                    "current_term": self.current_term,
                    "voted_for": self.voted_for,
                    "commit_index": self.commit_index,
                    "last_applied": self.last_applied
                }
                json.dump(metadata, f, indent=2)
        except IOError as e:
            self.logger.error(f"Error saving metadata: {e}")
    
    def run_election_timeout(self):
        """Monitor election timeout and start election if no heartbeat received"""
        while self.running:
            time.sleep(0.05)  # Sleep for 50ms to prevent busy waiting
            
            with self.state_lock:
                if self.state == NodeState.LEADER:
                    continue  # Leaders don't timeout
                
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
                    self.last_heartbeat = time.time()
                    self.election_timeout = self.generate_election_timeout()
    
    def start_election(self):
        """Start an election and request votes"""
        with self.state_lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id  # Vote for self
            self.save_persistent_state()
            
            self.logger.info(f"Starting election for term {self.current_term}")
            
            # Create vote request
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index].term if last_log_index >= 0 else 0
            
            request = replication_pb2.VoteRequest(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term
            )
            
        # Count votes (vote for self)
        votes_received = 1
        votes_needed = (len(self.nodes) // 2) + 1
        
        # Request votes from all other nodes
        for node in self.nodes:
            node_id = node['id']
            if node_id == self.node_id:
                continue  # Skip self
            
            host, port = node['host'], node['port']
            try:
                with grpc.insecure_channel(f"{host}:{port}") as channel:
                    stub = replication_pb2_grpc.NodeCommunicationStub(channel)
                    response = stub.RequestVote(request, timeout=0.3)  # 300ms timeout
                    
                    with self.state_lock:
                        # If response term is higher, revert to follower
                        if response.term > self.current_term:
                            self.current_term = response.term
                            self.state = NodeState.FOLLOWER
                            self.voted_for = None
                            self.save_persistent_state()
                            return
                        
                        # Count vote if granted
                        if response.vote_granted:
                            votes_received += 1
            except Exception as e:
                self.logger.debug(f"Failed to get vote from node {node_id}: {e}")
        
        # Check if won election
        with self.state_lock:
            if self.state == NodeState.CANDIDATE and votes_received >= votes_needed:
                self.become_leader()
    
    def become_leader(self):
        """Transition to leader state"""
        with self.state_lock:
            if self.state != NodeState.CANDIDATE:
                return
            
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            
            # Initialize leader state
            last_log_index = len(self.log) - 1
            # Set nextIndex to the index just after our last log entry 
            self.next_index = {node['id']: last_log_index + 1 for node in self.nodes}
            # Initialize matchIndex to 0 for all nodes except self
            self.match_index = {node['id']: 0 for node in self.nodes}
            # Set our own match_index to our last log entry
            self.match_index[self.node_id] = last_log_index
            
            self.logger.info(f"Became LEADER for term {self.current_term}")
            
            # Start heartbeat thread
            if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
                self.heartbeat_thread = threading.Thread(target=self.send_heartbeats)
                self.heartbeat_thread.daemon = True
                self.heartbeat_thread.start()
            
            # Append empty entry to establish leadership and force synchronization
            noop_success = self.append_entry({'type': 'noop'})
            if noop_success:
                self.logger.info("No-op entry successfully appended to log, leadership established")
            else:
                self.logger.warning("Failed to append no-op entry as new leader")
    
    def send_heartbeats(self):
        """Send heartbeats/AppendEntries to all followers"""
        while self.running:
            with self.state_lock:
                if self.state != NodeState.LEADER:
                    return
                
                # Send AppendEntries RPCs to each follower
                for node in self.nodes:
                    node_id = node['id']
                    if node_id == self.node_id:
                        continue  # Skip self
                    
                    self.send_append_entries(node_id)
            
            # Sleep before next heartbeat
            time.sleep(self.heartbeat_interval)
    
    def send_append_entries(self, follower_id: int, retry_count: int = 0) -> bool:
        """Send AppendEntries RPC to a follower"""
        if retry_count >= 5:
            self.logger.warning(f"Too many retries for node {follower_id}, giving up for now")
            return False
        
        try:
            # Get follower's host and port
            host, port = self.node_map[follower_id]
            
            # Skip if not leader
            if self.state != NodeState.LEADER:
                return False
            
            # Get next index for this follower
            next_idx = self.next_index.get(follower_id, 0)
            if next_idx < 0:
                next_idx = 0
            
            # Get prev log index and term
            prev_log_index = next_idx - 1
            prev_log_term = 0
            if prev_log_index >= 0 and prev_log_index < len(self.log):
                prev_log_term = self.log[prev_log_index].term
            
            # Get entries to send (from nextIndex to end of log)
            entries = []
            if next_idx < len(self.log):
                entries = [entry.to_pb() for entry in self.log[next_idx:]]
                self.logger.info(f"Sending {len(entries)} entries to node {follower_id} starting at index {next_idx}")
            
            # Create channel and stub
            with grpc.insecure_channel(f"{host}:{port}") as channel:
                stub = replication_pb2_grpc.NodeCommunicationStub(channel)
                
                # Send AppendEntries RPC using the correct message type from the generated protobuf
                request = replication_pb2.AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index
                )
                # Using 1.0 seconds timeout
                response = stub.AppendEntries(request, timeout=1.0)
                
                # If successful, update nextIndex and matchIndex
                if response.success:
                    # Update peer connection status and last contact time 
                    self.peer_connection_status[follower_id] = True
                    self.peer_last_contact[follower_id] = time.time()
                    
                    if entries:
                        # Convert to int to fix the type error
                        next_index_new = int(next_idx + len(entries))
                        match_index_new = int(next_idx + len(entries) - 1)
                        self.next_index[follower_id] = next_index_new
                        self.match_index[follower_id] = match_index_new
                        self.logger.info(f"AppendEntries successful to node {follower_id}, updated nextIndex to {self.next_index[follower_id]}")
                        
                        # Update commit index if possible
                        self.update_commit_index()
                    return True
                else:
                    # If not successful due to term, become follower
                    if response.term > self.current_term:
                        self.current_term = response.term
                        self.state = NodeState.FOLLOWER
                        self.voted_for = None
                        self.save_persistent_state()
                        self.logger.info(f"Found higher term {response.term} from node {follower_id}, becoming follower")
                        return False
                    
                    # If not successful due to log consistency, decrement nextIndex and retry
                    if next_idx > 0:
                        self.next_index[follower_id] = max(0, next_idx - 1)
                        self.logger.info(f"AppendEntries failed to node {follower_id}, decremented nextIndex from {next_idx} to {self.next_index[follower_id]}")
                        return self.send_append_entries(follower_id, retry_count + 1)
                    else:
                        self.logger.warning(f"AppendEntries failed to node {follower_id} at index 0, cannot decrement further")
                        return False
                
        except (grpc.RpcError, Exception) as e:
            self.logger.error(f"Error sending AppendEntries to node {follower_id}: {e}")
            return False
    
    def update_commit_index(self):
        """Update commit_index if there's a majority of matchIndex at a given index"""
        if self.state != NodeState.LEADER:
            return
        
        # For each log entry, count replicas (including self)
        old_commit_index = self.commit_index
        
        # Find the highest index that has been replicated to a majority
        for i in range(self.commit_index + 1, len(self.log)):
            # Only commit entries from current term (Raft safety property)
            if self.log[i].term != self.current_term:
                continue
                
            replica_count = 1  # Count self
            for node_id in self.match_index:
                if self.match_index[node_id] >= i:
                    replica_count += 1
            
            # If majority of nodes have replicated this entry, commit it
            if replica_count > len(self.nodes) // 2:
                self.commit_index = i
                self.logger.info(f"Updated commit index to {i}")
            else:
                # No need to check further entries
                break
        
        # If commit index changed, save state and notify apply thread
        if self.commit_index > old_commit_index:
            self.logger.info(f"Commit index advanced from {old_commit_index} to {self.commit_index}")
            # Save the updated commit index
            self.save_persistent_state()
    
    def apply_committed_entries(self):
        """Apply committed log entries to state machine"""
        while self.running:
            try:
                time.sleep(0.05)  # Sleep for 50ms to prevent busy waiting
                
                with self.state_lock:
                    if self.commit_index > self.last_applied:
                        self.logger.info(f"Applying entries from index {self.last_applied + 1} to {self.commit_index}")
                        
                        # Apply entries to state machine
                        entries_applied = False
                        for i in range(self.last_applied + 1, self.commit_index + 1):
                            if i < len(self.log):
                                entry = self.log[i]
                                self.apply_to_state_machine(entry.command)
                                self.last_applied = i
                                entries_applied = True
                            else:
                                self.logger.error(f"Cannot apply entry at index {i}, log length is {len(self.log)}")
                                break
                        
                        # Only save state after applying entries (reduces disk I/O)
                        if entries_applied:
                            self.save_persistent_state()
            except Exception as e:
                self.logger.error(f"Error in apply_committed_entries: {e}")
    
    def apply_to_state_machine(self, command: Dict):
        """Apply a command to the state machine"""
        with self.state_machine_lock:
            cmd_type = command.get('type')
            
            if cmd_type == 'set':
                key, value = command.get('key'), command.get('value')
                if key and value:  # Make sure we have valid key and value
                    self.state_machine[key] = value
                    self.logger.info(f"Applied SET {key}={value} to state machine, state now: {self.state_machine}")
            elif cmd_type == 'delete':
                key = command.get('key')
                if key in self.state_machine:
                    del self.state_machine[key]
                    self.logger.info(f"Applied DELETE {key}")
            elif cmd_type == 'noop':
                self.logger.debug("Applied NOOP command")
            # Add other command types as needed
    
    def append_entry(self, command: Dict) -> bool:
        """Append an entry to the log (leader only)"""
        with self.state_lock:
            if self.state != NodeState.LEADER:
                return False
            
            # Create log entry
            index = len(self.log)
            entry = LogEntry(self.current_term, command, index)
            self.log.append(entry)
            
            # Update own matchIndex and nextIndex
            self.match_index[self.node_id] = index
            self.next_index[self.node_id] = index + 1
            
            # Save log
            self.save_persistent_state()
            
            # Try to replicate to majority immediately for client operations
            cmd_type = command.get('type')
            if cmd_type and cmd_type != 'noop':
                # Send AppendEntries to all followers
                for node in self.nodes:
                    node_id = node['id']
                    if node_id == self.node_id:
                        continue  # Skip self
                    self.send_append_entries(node_id)
                
                # Update commit index if possible
                self.update_commit_index()
            
            return True
    
    def get_value(self, key: str) -> Tuple[bool, str]:
        """Get a value from the state machine"""
        with self.state_machine_lock:
            self.logger.info(f"GET request for key '{key}', state machine has keys: {list(self.state_machine.keys())}")
            if key in self.state_machine:
                value = self.state_machine[key]
                return True, value
            return False, ""
    
    # gRPC service methods for NodeCommunication
    def RequestVote(self, request, context):
        with self.state_lock:
            # If request term is lower, reject
            if request.term < self.current_term:
                return replication_pb2.VoteResponse(
                    term=self.current_term,
                    vote_granted=False
                )
            
            # Update peer connection status for the candidate
            candidate_id = request.candidate_id
            if candidate_id in self.peer_connection_status:
                self.peer_connection_status[candidate_id] = True
                self.peer_last_contact[candidate_id] = time.time()
            
            # If request term is higher, update current term and become follower
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = NodeState.FOLLOWER
                self.voted_for = None
                self.save_persistent_state()
            
            # Check if candidate's log is at least as up-to-date as ours
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index].term if last_log_index >= 0 else 0
            
            log_ok = (request.last_log_term > last_log_term or 
                     (request.last_log_term == last_log_term and 
                      request.last_log_index >= last_log_index))
            
            # Vote if we haven't voted for someone else and candidate's log is ok
            vote_granted = (self.voted_for is None or self.voted_for == request.candidate_id) and log_ok
            
            if vote_granted:
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()  # Reset election timeout
                self.save_persistent_state()
                self.logger.info(f"Granted vote to node {request.candidate_id} for term {request.term}")
            
            return replication_pb2.VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted
            )
    
    def AppendEntries(self, request, context):
        with self.state_lock:
            # If request term is lower, reject
            if request.term < self.current_term:
                return replication_pb2.AppendEntriesResponse(
                    term=self.current_term,
                    success=False
                )
            
            # Update peer connection status for the leader
            leader_id = request.leader_id
            if leader_id in self.peer_connection_status:
                self.peer_connection_status[leader_id] = True
                self.peer_last_contact[leader_id] = time.time()
            
            # If request term is higher or equal, update current term and become follower
            if request.term >= self.current_term:
                if self.state != NodeState.FOLLOWER or request.term > self.current_term:
                    self.state = NodeState.FOLLOWER
                    self.current_term = request.term
                    self.voted_for = None
                    self.save_persistent_state()
                
                self.leader_id = request.leader_id
                self.last_heartbeat = time.time()  # Reset election timeout
            
            # Special case for empty log and follower log is also empty
            log_ok = False
            if request.prev_log_index == -1:
                log_ok = True
            elif request.prev_log_index >= len(self.log):
                # We don't have the required previous log entry
                log_ok = False
            else:
                # Check if previous log entry matches
                prev_log_index = request.prev_log_index
                log_ok = self.log[prev_log_index].term == request.prev_log_term
            
            if not log_ok:
                self.logger.info(f"Log consistency check failed at index {request.prev_log_index}, my log length: {len(self.log)}")
                return replication_pb2.AppendEntriesResponse(
                    term=self.current_term,
                    success=False
                )
            
            # Process log entries
            if request.entries:
                self.logger.info(f"Received {len(request.entries)} entries from leader")
                # Convert protobuf entries to LogEntry objects
                new_entries = [LogEntry.from_pb(entry) for entry in request.entries]
                
                # Find conflicting entries and truncate log if necessary
                next_idx = request.prev_log_index + 1
                if next_idx < len(self.log):
                    for i, entry in enumerate(new_entries):
                        log_idx = next_idx + i
                        if log_idx < len(self.log):
                            # Check for conflict
                            if self.log[log_idx].term != entry.term:
                                # Delete conflicting entry and all that follow
                                self.logger.info(f"Conflict found at index {log_idx}, truncating log")
                                self.log = self.log[:log_idx]
                                break
                        else:
                            # We've reached the end of our log
                            break
                
                # Append new entries not already in log
                for i, entry in enumerate(new_entries):
                    log_idx = next_idx + i
                    if log_idx >= len(self.log):
                        self.log.append(entry)
                        self.logger.info(f"Appended log entry at index {log_idx}: {entry.command}")
                
                # Save log changes
                self.save_persistent_state()
            
            # Update commit index
            if request.leader_commit > self.commit_index:
                old_commit_index = self.commit_index
                self.commit_index = min(request.leader_commit, len(self.log) - 1)
                if self.commit_index > old_commit_index:
                    self.logger.info(f"Updated commit index from {old_commit_index} to {self.commit_index}")
            
            return replication_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=True
            )
    
    # gRPC service methods for DataService
    def Set(self, request, context):
        key, value = request.key, request.value
        
        with self.state_lock:
            # If not leader, redirect to leader
            if self.state != NodeState.LEADER:
                if self.leader_id is not None and self.leader_id != self.node_id:
                    leader_host, leader_port = self.node_map[self.leader_id]
                    context.set_details(f"Not leader. Leader is at {leader_host}:{leader_port}")
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                else:
                    context.set_details("Not leader. Leader unknown.")
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return replication_pb2.SetResponse(success=False, message="Not leader")
            
            # Create command and append to log
            command = {'type': 'set', 'key': key, 'value': value}
            self.logger.info(f"Appending SET command: {command}")
            if self.append_entry(command):
                # Wait for the entry to be committed
                entry_index = len(self.log) - 1
                attempts = 0
                max_attempts = 10
                
                while attempts < max_attempts:
                    if self.commit_index >= entry_index:
                        return replication_pb2.SetResponse(success=True, message="Value set successfully")
                    time.sleep(0.1)  # Wait 100ms
                    attempts += 1
                
                # If we timed out waiting for commit
                return replication_pb2.SetResponse(success=True, message="Value set but may not be durable yet")
            else:
                return replication_pb2.SetResponse(success=False, message="Failed to set value")
    
    def Get(self, request, context):
        key = request.key
        
        # Read from state machine
        success, value = self.get_value(key)
        
        if success:
            return replication_pb2.GetResponse(
                success=True,
                value=value,
                message="Value retrieved successfully"
            )
        else:
            return replication_pb2.GetResponse(
                success=False,
                value="",
                message=f"Key '{key}' not found"
            )
    
    def stop(self):
        """Stop the node"""
        self.running = False
        if self.election_thread.is_alive():
            self.election_thread.join(timeout=1.0)
        if self.apply_thread.is_alive():
            self.apply_thread.join(timeout=1.0)
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=1.0)

    def GetPeerStatus(self, request, context):
        """Return the status of all peers"""
        peer_status = []
        for node_id, connected in self.peer_connection_status.items():
            peer_status.append(replication_pb2.PeerStatus(node_id=node_id, connected=connected))
        return replication_pb2.GetPeerStatusResponse(peer_status=peer_status)

    def GetNodeStatus(self, request, context):
        """Return the status of this node"""
        state = self.state.name
        term = self.current_term
        leader_id = self.leader_id
        uptime = time.time() - self.start_time
        return replication_pb2.GetNodeStatusResponse(state=state, term=term, leader_id=leader_id, uptime=uptime)

    def HealthCheck(self, request, context):
        """Implementation of the HealthCheck RPC method"""
        status = replication_pb2.HealthCheckResponse.ServingStatus.SERVING
        version = "1.0.0"  # Version of the system
        uptime = int(time.time() - self.start_time)
        
        self.logger.info(f"Health check requested, reporting status: {status}")
        
        return replication_pb2.HealthCheckResponse(
            status=status,
            version=version,
            uptime_seconds=uptime
        )
    
    def NodeStatus(self, request, context):
        """Implementation of the NodeStatus RPC method"""
        with self.state_lock:
            # Convert NodeState enum to integer values expected by proto
            state_map = {
                NodeState.FOLLOWER: 0,
                NodeState.CANDIDATE: 1,
                NodeState.LEADER: 2
            }
            
            # Prepare peer status information
            peers = []
            for peer_id, is_connected in self.peer_connection_status.items():
                last_contact_ms = 0
                if self.peer_last_contact[peer_id] > 0:
                    last_contact_ms = int((time.time() - self.peer_last_contact[peer_id]) * 1000)
                
                # Get next_index and match_index for this peer if we're the leader
                next_index = 0
                match_index = 0
                if self.state == NodeState.LEADER and peer_id in self.next_index:
                    next_index = self.next_index[peer_id]
                    match_index = self.match_index[peer_id]
                
                peer_status = replication_pb2.NodeStatusResponse.PeerStatus(
                    node_id=peer_id,
                    is_connected=is_connected,
                    last_contact_ms=last_contact_ms,
                    next_index=next_index,
                    match_index=match_index
                )
                peers.append(peer_status)
            
            # Build full response
            response = replication_pb2.NodeStatusResponse(
                node_id=self.node_id,
                state=state_map.get(self.state, 0),
                current_term=self.current_term,
                current_leader=self.leader_id if self.leader_id is not None else 0,
                committed_index=self.commit_index,
                last_applied=self.last_applied,
                log_size=len(self.log),
                peers=peers
            )
            
            # Include log entries if requested
            if request.include_log:
                for entry in self.log:
                    response.log.append(entry.to_pb())
            
            # Include state machine data if requested
            if request.include_state_machine:
                with self.state_machine_lock:
                    for key, value in self.state_machine.items():
                        response.state_machine[key] = value
            
            self.logger.info(f"Node status requested, reporting state: {self.state}")
            return response
    
    def ClusterStatus(self, request, context):
        """Implementation of the ClusterStatus RPC method"""
        with self.state_lock:
            # Only leaders can provide accurate cluster status
            if self.state != NodeState.LEADER:
                self.logger.warning(f"Cluster status requested but node is not leader, state: {self.state}")
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("Node is not the leader, cannot provide cluster status")
                return replication_pb2.ClusterStatusResponse()
            
            # Count healthy nodes (nodes that responded to recent AppendEntries)
            healthy_nodes = 1  # Count self as healthy
            for peer_id, is_connected in self.peer_connection_status.items():
                if is_connected:
                    healthy_nodes += 1
            
            # Build basic response
            response = replication_pb2.ClusterStatusResponse(
                leader_id=self.node_id,
                current_term=self.current_term,
                total_nodes=len(self.nodes),
                healthy_nodes=healthy_nodes
            )
            
            # Include all node details if requested
            if request.include_all_node_details:
                # Add self as a node
                self_status = self.NodeStatus(
                    replication_pb2.NodeStatusRequest(
                        include_log=False,
                        include_state_machine=True
                    ), 
                    context
                )
                response.node_details.append(self_status)
                
                # Try to get status from all peers
                for node in self.nodes:
                    peer_id = node['id']
                    if peer_id == self.node_id:
                        continue  # Skip self
                    
                    if self.peer_connection_status[peer_id]:
                        try:
                            # Connect to the peer and request its status
                            with grpc.insecure_channel(f"{node['host']}:{node['port']}") as channel:
                                stub = replication_pb2_grpc.MonitoringServiceStub(channel)
                                peer_status = stub.NodeStatus(
                                    replication_pb2.NodeStatusRequest(
                                        include_log=False,
                                        include_state_machine=False
                                    ),
                                    timeout=1.0
                                )
                                response.node_details.append(peer_status)
                        except Exception as e:
                            self.logger.error(f"Failed to get status from peer {peer_id}: {e}")
            
            self.logger.info(f"Cluster status requested, reporting {healthy_nodes}/{len(self.nodes)} healthy nodes")
            return response
