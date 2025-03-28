#!/usr/bin/env python3
import os
import sys
import time
import json
import logging
import argparse
import random
import grpc
from typing import Dict, List, Optional, Tuple

# Make sure generated directory is in the path
generated_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import generated protocol buffers - use the direct imports
from generated import replication_pb2
from generated import replication_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('RaftClient')

class RaftClient:
    """Client for interacting with a Raft cluster"""
    
    def __init__(self, config_path: str, server_id: Optional[int] = None):
        """Initialize the Raft client
        
        Args:
            config_path: Path to the config file with server information
            server_id: Optional specific server ID to connect to, otherwise random
        """
        self.config_path = config_path
        self.config = self.load_config()
        self.nodes = self.config['nodes']
        self.server_id = server_id
        self.current_leader_id = None
        self.stubs: Dict[int, Tuple[replication_pb2_grpc.NodeCommunicationStub, replication_pb2_grpc.MonitoringStub, str]] = {}  # Cache for gRPC stubs
        
    def load_config(self) -> Dict:
        """Load configuration from file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"nodes": []}
    
    def get_node_by_id(self, node_id: int) -> Dict:
        """Get node configuration by ID"""
        for node in self.nodes:
            if node['id'] == node_id:
                return node
        raise ValueError(f"Node ID {node_id} not found in configuration")
    
    def get_stub(self, node_id: int) -> Tuple[replication_pb2_grpc.NodeCommunicationStub, replication_pb2_grpc.MonitoringStub, str]:
        """Get or create a gRPC stub for the specified node"""
        if node_id in self.stubs:
            return self.stubs[node_id]
        
        node = self.get_node_by_id(node_id)
        addr = f"{node['host']}:{node['port']}"
        channel = grpc.insecure_channel(addr)
        stub = replication_pb2_grpc.NodeCommunicationStub(channel)
        
        # Also create a MonitoringStub for checking status
        monitoring_stub = replication_pb2_grpc.MonitoringStub(channel)
        
        # Store both stubs and the address for later use
        self.stubs[node_id] = (stub, monitoring_stub, addr)
        return stub, monitoring_stub, addr
    
    def find_leader(self) -> Optional[int]:
        """Try to find the current leader by querying each node"""
        logger.info("Searching for current leader...")
        
        # Try nodes in random order to distribute load
        node_ids = [node['id'] for node in self.nodes]
        random.shuffle(node_ids)
        
        for node_id in node_ids:
            try:
                stub, monitoring_stub, addr = self.get_stub(node_id)
                
                # Check the node's state
                response = monitoring_stub.NodeStatus(replication_pb2.NodeStatusRequest())
                if response.state == "LEADER":
                    logger.info(f"Found leader: Node {node_id} at {addr}")
                    self.current_leader_id = node_id
                    return node_id
                elif response.leader_id:
                    # If node knows the leader, use that
                    leader_id = response.leader_id
                    logger.info(f"Node {node_id} reports leader is Node {leader_id}")
                    self.current_leader_id = leader_id
                    return leader_id
            except Exception as e:
                logger.debug(f"Error connecting to Node {node_id}: {e}")
        
        logger.warning("No leader found in the cluster")
        return None
        
    def get_target_node(self) -> Optional[int]:
        """Determine which node to send the request to"""
        if self.server_id is not None:
            # User specified a specific server
            return self.server_id
        
        # Try to use the current known leader
        if self.current_leader_id is not None:
            return self.current_leader_id
        
        # Try to find the leader
        leader_id = self.find_leader()
        if leader_id is not None:
            return leader_id
        
        # No leader found, pick a random node
        logger.warning("No leader found, will try a random node")
        return random.choice([node['id'] for node in self.nodes])
    
    def set_value(self, key: str, value: str) -> Tuple[bool, str]:
        """Set a key-value pair in the Raft cluster"""
        node_id = self.get_target_node()
        if node_id is None:
            return False, "No available nodes found"
        
        try:
            stub, monitoring_stub, addr = self.get_stub(node_id)
            
            # Create request
            request = replication_pb2.SetRequest(key=key, value=value)
            
            # Send request
            start_time = time.time()
            response = stub.Set(request, timeout=5.0)
            end_time = time.time()
            
            # Process response
            if response.success:
                return True, f"Success via Node {node_id} ({addr}) in {(end_time - start_time)*1000:.1f}ms"
            else:
                # Check if there's a redirect to the leader
                if response.leader_id:
                    self.current_leader_id = response.leader_id
                    return self.set_value(key, value)  # Retry with the correct leader
                return False, f"Failed via Node {node_id} ({addr}): not the leader"
        except Exception as e:
            logger.error(f"Error setting value via Node {node_id}: {e}")
            return False, f"Error via Node {node_id}: {str(e)}"
    
    def get_value(self, key: str) -> Tuple[bool, str, str]:
        """Get a value by key from the Raft cluster"""
        node_id = self.get_target_node()
        if node_id is None:
            return False, "", "No available nodes found"
        
        try:
            stub, monitoring_stub, addr = self.get_stub(node_id)
            
            # Create request
            request = replication_pb2.GetRequest(key=key)
            
            # Send request
            start_time = time.time()
            response = stub.Get(request, timeout=5.0)
            end_time = time.time()
            
            # Process response
            if response.success:
                return True, response.value, f"Success via Node {node_id} ({addr}) in {(end_time - start_time)*1000:.1f}ms"
            else:
                return False, "", f"Not found via Node {node_id} ({addr})"
        except Exception as e:
            logger.error(f"Error getting value via Node {node_id}: {e}")
            return False, "", f"Error via Node {node_id}: {str(e)}"

def print_help():
    """Print help message"""
    print("\nRaft Client Commands:")
    print("  set <key> <value>   - Store a key-value pair")
    print("  get <key>           - Retrieve a value by key")
    print("  leader              - Find and display the current leader")
    print("  connect <node_id>   - Connect to a specific node")
    print("  auto                - Connect to any node (auto-discover leader)")
    print("  exit/quit           - Exit the client")
    print("  help                - Show this help message")
    print()

def main():
    """Main entry point for the client"""
    parser = argparse.ArgumentParser(description="Raft cluster client")
    parser.add_argument("--config", type=str, default="config.json", help="Path to config file")
    parser.add_argument("--node", type=int, help="Connect to a specific node ID")
    args = parser.parse_args()
    
    # Create client
    client = RaftClient(args.config, args.node)
    
    # Print welcome message
    print("\nRaft Cluster Client")
    print("=================")
    if args.node:
        print(f"Connected to Node {args.node}")
    else:
        print("Auto-discovering leader nodes")
    print('Type "help" for a list of commands')
    
    # Main loop
    while True:
        try:
            cmd = input("\n> ").strip()
            
            if cmd.lower() in ("exit", "quit", "q"):
                break
                
            elif cmd.lower() in ("help", "h", "?"):
                print_help()
                
            elif cmd.lower() == "leader":
                leader_id = client.find_leader()
                if leader_id:
                    node = client.get_node_by_id(leader_id)
                    print(f"Current leader: Node {leader_id} ({node['host']}:{node['port']})")
                else:
                    print("No leader found. The cluster may be in an election state.")
                
            elif cmd.lower().startswith("connect "):
                try:
                    node_id = int(cmd.split(" ")[1])
                    client.server_id = node_id
                    print(f"Now connected to Node {node_id}")
                except (ValueError, IndexError):
                    print("Invalid node ID. Use 'connect <node_id>'")
                    
            elif cmd.lower() == "auto":
                client.server_id = None
                print("Now auto-discovering leaders")
                
            elif cmd.lower().startswith("set "):
                parts = cmd.split(" ", 2)
                if len(parts) < 3:
                    print("Invalid set command. Use 'set <key> <value>'")
                    continue
                    
                key = parts[1]
                value = parts[2]
                success, message = client.set_value(key, value)
                if success:
                    print(f"SET {key}={value}: {message}")
                else:
                    print(f"SET failed: {message}")
                    
            elif cmd.lower().startswith("get "):
                parts = cmd.split(" ")
                if len(parts) < 2:
                    print("Invalid get command. Use 'get <key>'")
                    continue
                    
                key = parts[1]
                success, value, message = client.get_value(key)
                if success:
                    print(f"GET {key}: {value}")
                    print(f"Request details: {message}")
                else:
                    print(f"GET failed: {message}")
                    
            else:
                print(f"Unknown command: {cmd}")
                print_help()
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
    
    print("\nExiting Raft client.")

if __name__ == "__main__":
    main()
