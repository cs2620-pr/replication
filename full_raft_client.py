#!/usr/bin/env python3
import os
import sys
import json
import time
import random
import logging
import argparse
import grpc
import re
from typing import List, Dict, Optional, Tuple, Any

# Add the generated directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated"))

# Import generated protocol buffers
from generated import replication_pb2
from generated import replication_pb2_grpc

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RaftClient')

class RaftClient:
    """Client for interacting with a Raft cluster"""
    
    def __init__(self, config_path: str, server_id: Optional[int] = None):
        """Initialize the client
        
        Args:
            config_path: Path to the config file with server information
            server_id: Optional specific server ID to connect to
        """
        self.config_path = config_path
        self.config = self.load_config()
        self.nodes = self.config.get('nodes', [])
        self.server_id = server_id
        self.leader_id = None
        self.leader_addr = None
        self.max_retries = 10
        
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
            if node.get('id') == node_id:
                return node
        raise ValueError(f"Node ID {node_id} not found in configuration")
    
    def get_node_by_addr(self, addr: str) -> Optional[Dict]:
        """Get node configuration by address"""
        host, port_str = addr.split(':')
        port = int(port_str)
        for node in self.nodes:
            if node.get('host') == host and node.get('port') == port:
                return node
        return None
    
    def get_target_address(self) -> str:
        """Get the address to connect to"""
        # If we know the leader, use that
        if self.leader_addr:
            return self.leader_addr
        
        # If specific server requested, use that    
        if self.server_id is not None:
            node = self.get_node_by_id(self.server_id)
            host = node.get('host')
            port = node.get('port')
            return f"{host}:{port}"
        
        # If no specific server, pick a random one
        node = random.choice(self.nodes)
        host = node.get('host')
        port = node.get('port')
        return f"{host}:{port}"
    
    def extract_leader_addr(self, error_message: str) -> Optional[str]:
        """Extract leader address from error message"""
        # Try to match the leader address pattern in the error message
        match = re.search(r"Leader is at ([^\"]+)", error_message)
        if match:
            return match.group(1)
        return None
        
    def find_leader(self) -> bool:
        """Try to find the leader by querying nodes"""
        for node in self.nodes:
            node_id = node.get('id')
            host = node.get('host')
            port = node.get('port')
            addr = f"{host}:{port}"
            
            try:
                print(f"Checking if Node {node_id} ({addr}) is the leader...")
                with grpc.insecure_channel(addr) as channel:
                    # Create data service stub since MonitoringStub doesn't exist
                    stub = replication_pb2_grpc.DataServiceStub(channel)
                    
                    # Try a Get operation and check if it succeeds directly
                    # or gives us a leader redirect
                    try:
                        # Try with a non-existent key that should return quickly
                        response = stub.Get(replication_pb2.GetRequest(key="__leader_check__"))
                        # If we got here without error, this might be the leader
                        print(f"Node {node_id} appears to be the leader (responded to request)")
                        self.leader_id = node_id
                        self.leader_addr = addr
                        return True
                    except grpc.RpcError as e:
                        error_str = str(e)
                        if "Not leader" in error_str:
                            # Extract leader from error
                            leader_addr = self.extract_leader_addr(error_str)
                            if leader_addr:
                                print(f"Node {node_id} reports leader is at {leader_addr}")
                                self.leader_addr = leader_addr
                                # Try to get node ID from the leader address
                                for n in self.nodes:
                                    n_host = n.get('host')
                                    n_port = n.get('port')
                                    if f"{n_host}:{n_port}" == leader_addr:
                                        self.leader_id = n.get('id')
                                        break
                                return True
            except Exception as e:
                print(f"Error querying Node {node_id}: {e}")
                
        print("No leader found")
        return False
            
    def set_value(self, key: str, value: str, retry_count: int = 0) -> None:
        """Set a key-value pair in the cluster"""
        if retry_count >= self.max_retries:
            print(f"Maximum retries ({self.max_retries}) exceeded. Cannot set {key}={value}")
            return
        
        # If we don't know the leader, try to find it first
        if not self.leader_addr and retry_count == 0:
            self.find_leader()
            
        addr = self.get_target_address()
        node = self.get_node_by_addr(addr)
        node_id = node.get('id') if node else "unknown"
        
        print(f"Trying to set {key}={value} via Node {node_id} ({addr})")
        
        try:
            with grpc.insecure_channel(addr) as channel:
                # Create data service stub
                stub = replication_pb2_grpc.DataServiceStub(channel)
                
                # Create set request
                request = replication_pb2.SetRequest(key=key, value=value)
                
                # Send request
                start_time = time.time()
                response = stub.Set(request)
                end_time = time.time()
                
                # Process response
                if response.success:
                    print(f"SET {key}={value} successful")
                    print(f"Request handled by Node {node_id} ({addr}) in {(end_time - start_time)*1000:.1f}ms")
                    
                    if hasattr(response, 'message') and response.message:
                        print(f"Message: {response.message}")
                else:
                    print(f"SET {key}={value} failed")
                    print(f"Message: {response.message if hasattr(response, 'message') else 'No message'}")
                    
                    # If there's a leader redirect in the message
                    leader_addr = self.extract_leader_addr(response.message if hasattr(response, 'message') else "")
                    if leader_addr:
                        print(f"Redirecting to leader at {leader_addr}")
                        self.leader_addr = leader_addr
                        self.set_value(key, value, retry_count + 1)
        
        except grpc.RpcError as e:
            error_str = str(e)
            print(f"Error: {e}")
            
            # Check if it's a "Not leader" error
            if "Not leader" in error_str:
                # Try to extract the leader address
                leader_addr = self.extract_leader_addr(error_str)
                if leader_addr:
                    print(f"Redirecting to leader at {leader_addr}")
                    self.leader_addr = leader_addr
                    self.set_value(key, value, retry_count + 1)
                else:
                    print(f"Could not extract leader address from error: {error_str}")
            else:
                print(f"Failed to set value via {addr}")
                
                # If we have a channel error and this is the first retry, try to find the leader
                if retry_count == 0 and ("Failed to connect" in error_str or 
                                         "Socket closed" in error_str or
                                         "Connection refused" in error_str):
                    print("Connection failed. Trying to find the leader...")
                    if self.find_leader():
                        self.set_value(key, value, retry_count + 1)
    
    def get_value(self, key: str, retry_count: int = 0) -> None:
        """Get a value by key from the cluster"""
        if retry_count >= self.max_retries:
            print(f"Maximum retries ({self.max_retries}) exceeded. Cannot get value for {key}")
            return
        
        # If we don't know the leader, try to find it first
        if not self.leader_addr and retry_count == 0:
            self.find_leader()
            
        addr = self.get_target_address()
        node = self.get_node_by_addr(addr)
        node_id = node.get('id') if node else "unknown"
        
        print(f"Trying to get value for {key} from Node {node_id} ({addr})")
        
        try:
            with grpc.insecure_channel(addr) as channel:
                # Create data service stub
                stub = replication_pb2_grpc.DataServiceStub(channel)
                
                # Create get request
                request = replication_pb2.GetRequest(key=key)
                
                # Send request
                start_time = time.time()
                response = stub.Get(request)
                end_time = time.time()
                
                # Process response
                if response.success:
                    print(f"GET {key} successful")
                    print(f"Value: {response.value}")
                    print(f"Request handled by Node {node_id} ({addr}) in {(end_time - start_time)*1000:.1f}ms")
                else:
                    print(f"GET {key} failed")
                    print(f"Message: {response.message if hasattr(response, 'message') else 'No message'}")
                    
                    # If there's a leader redirect in the message
                    leader_addr = self.extract_leader_addr(response.message if hasattr(response, 'message') else "")
                    if leader_addr:
                        print(f"Redirecting to leader at {leader_addr}")
                        self.leader_addr = leader_addr
                        self.get_value(key, retry_count + 1)
        
        except grpc.RpcError as e:
            error_str = str(e)
            print(f"Error: {e}")
            
            # Check if it's a "Not leader" error
            if "Not leader" in error_str:
                # Try to extract the leader address
                leader_addr = self.extract_leader_addr(error_str)
                if leader_addr:
                    print(f"Redirecting to leader at {leader_addr}")
                    self.leader_addr = leader_addr
                    self.get_value(key, retry_count + 1)
                else:
                    print(f"Could not extract leader address from error: {error_str}")
            else:
                print(f"Failed to get value via {addr}")
                
                # If we have a channel error and this is the first retry, try to find the leader
                if retry_count == 0 and ("Failed to connect" in error_str or 
                                         "Socket closed" in error_str or
                                         "Connection refused" in error_str):
                    print("Connection failed. Trying to find the leader...")
                    if self.find_leader():
                        self.get_value(key, retry_count + 1)

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
    parser = argparse.ArgumentParser(description="Raft client")
    parser.add_argument("--config", type=str, default="config.json", 
                       help="Path to config file (default: config.json)")
    parser.add_argument("--node", type=int, help="Connect to a specific node ID")
    args = parser.parse_args()
    
    # Create client
    client = RaftClient(args.config, args.node)
    
    # Print welcome message
    print("\nRaft Cluster Client")
    print("=================")
    if args.node:
        node = client.get_node_by_id(args.node)
        addr = f"{node.get('host')}:{node.get('port')}"
        print(f"Connected to Node {args.node} ({addr})")
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
                if client.find_leader():
                    print(f"Current leader: Node {client.leader_id} ({client.leader_addr})")
                else:
                    print("No leader found. The cluster may be in an election state.")
                
            elif cmd.lower().startswith("connect "):
                try:
                    node_id = int(cmd.split(" ")[1])
                    node = client.get_node_by_id(node_id)
                    addr = f"{node.get('host')}:{node.get('port')}"
                    client.server_id = node_id
                    client.leader_addr = None  # Reset leader knowledge
                    print(f"Now connected to Node {node_id} ({addr})")
                except (ValueError, IndexError):
                    print("Invalid node ID. Use 'connect <node_id>'")
                    
            elif cmd.lower() == "auto":
                client.server_id = None
                client.leader_addr = None
                print("Now auto-discovering leaders")
                
            elif cmd.lower().startswith("set "):
                parts = cmd.split(" ", 2)
                if len(parts) < 3:
                    print("Invalid set command. Use 'set <key> <value>'")
                    continue
                    
                key = parts[1]
                value = parts[2]
                client.set_value(key, value)
                    
            elif cmd.lower().startswith("get "):
                parts = cmd.split(" ")
                if len(parts) < 2:
                    print("Invalid get command. Use 'get <key>'")
                    continue
                    
                key = parts[1]
                client.get_value(key)
                    
            else:
                print(f"Unknown command: {cmd}")
                print_help()
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            
    print("\nExiting client.")

if __name__ == "__main__":
    main()
