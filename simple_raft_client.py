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
from typing import List, Dict, Optional, Tuple

# Setup logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SimpleRaftClient')

class SimpleRaftClient:
    """A simple client for interacting with the Raft cluster"""
    
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
        self.current_leader = None
        self.leader_addr = None
        
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
        
    def get_target_server(self) -> Tuple[str, int]:
        """Get the address and port of a server to connect to"""
        # If we know the leader, use that
        if self.leader_addr:
            host, port_str = self.leader_addr.split(':')
            return host, int(port_str)
            
        # If specific server requested, use that
        if self.server_id is not None:
            # Use specified server
            node = self.get_node_by_id(self.server_id)
            return node.get('host'), node.get('port')
        
        # If no specific server, pick a random one
        node = random.choice(self.nodes)
        return node.get('host'), node.get('port')
    
    def extract_leader_addr(self, error_message: str) -> Optional[str]:
        """Extract leader address from error message"""
        # Try to match the leader address pattern in the error message
        match = re.search(r"Leader is at ([^\"]+)", error_message)
        if match:
            return match.group(1)
        return None
        
    def set_value(self, key: str, value: str, retry_count: int = 0) -> None:
        """Set a key-value pair in the cluster"""
        if retry_count >= 10:
            print(f"Maximum retries (10) exceeded. Cannot set {key}={value}")
            return
            
        host, port = self.get_target_server()
        addr = f"{host}:{port}"
        node_id = next((n.get('id') for n in self.nodes if n.get('host') == host and n.get('port') == port), None)
        
        print(f"Trying to set {key}={value} via Node {node_id} ({addr})")
        
        # Prepare gRPC call with manually-constructed messages
        with grpc.insecure_channel(addr) as channel:
            try:
                start_time = time.time()
                
                # Call the Set method using low-level gRPC
                stub = channel.unary_unary(
                    '/replication.DataService/Set',
                    request_serializer=lambda req: req,
                    response_deserializer=lambda res: res
                )
                
                # Construct request manually (this is a simplified approach)
                request = f'{{"key": "{key}", "value": "{value}"}}'.encode('utf-8')
                
                # Make the call
                response = stub(request)
                end_time = time.time()
                
                print(f"SET {key}={value}")
                print(f"Request handled by Node {node_id} ({addr}) in {(end_time - start_time)*1000:.1f}ms")
                
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
                        # Retry with the leader
                        self.set_value(key, value, retry_count + 1)
                    else:
                        print(f"Could not extract leader address from error: {error_str}")
                else:
                    print(f"Failed to set value via {addr}")
            except Exception as e:
                print(f"Unexpected error: {e}")
                
    def get_value(self, key: str, retry_count: int = 0) -> None:
        """Get a value by key from the cluster"""
        if retry_count >= 10:
            print(f"Maximum retries (10) exceeded. Cannot get value for {key}")
            return
            
        host, port = self.get_target_server()
        addr = f"{host}:{port}"
        node_id = next((n.get('id') for n in self.nodes if n.get('host') == host and n.get('port') == port), None)
        
        print(f"Trying to get value for {key} from Node {node_id} ({addr})")
        
        # Prepare gRPC call with manually-constructed messages
        with grpc.insecure_channel(addr) as channel:
            try:
                start_time = time.time()
                
                # Call the Get method using low-level gRPC
                stub = channel.unary_unary(
                    '/replication.DataService/Get',
                    request_serializer=lambda req: req,
                    response_deserializer=lambda res: res
                )
                
                # Construct request manually
                request = f'{{"key": "{key}"}}'.encode('utf-8')
                
                # Make the call
                response = stub(request)
                end_time = time.time()
                
                print(f"GET {key}")
                print(f"Request handled by Node {node_id} ({addr}) in {(end_time - start_time)*1000:.1f}ms")
                print(f"Response: {response}")
                
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
                        # Retry with the leader
                        self.get_value(key, retry_count + 1)
                    else:
                        print(f"Could not extract leader address from error: {error_str}")
                else:
                    print(f"Failed to get value via {addr}")
            except Exception as e:
                print(f"Unexpected error: {e}")
                
def print_help():
    """Print help message"""
    print("\nSimple Raft Client Commands:")
    print("  set <key> <value>   - Store a key-value pair")
    print("  get <key>           - Retrieve a value by key")
    print("  connect <node_id>   - Connect to a specific node")
    print("  random              - Connect to a random node")
    print("  quit/exit           - Exit the client")
    print("  help                - Show this help message")
    print()

def main():
    """Main entry point for the client"""
    parser = argparse.ArgumentParser(description="Simple Raft client")
    parser.add_argument("--config", type=str, default="config.json", 
                       help="Path to config file (default: config.json)")
    parser.add_argument("--node", type=int, help="Connect to a specific node ID")
    args = parser.parse_args()
    
    # Create client
    client = SimpleRaftClient(args.config, args.node)
    
    # Print welcome message
    print("\nSimple Raft Client")
    print("=================")
    if args.node:
        node = client.get_node_by_id(args.node)
        print(f"Connected to Node {args.node} ({node.get('host')}:{node.get('port')})")
    else:
        print("Will connect to random nodes")
    print('Type "help" for a list of commands')
    
    # Main loop
    while True:
        try:
            cmd = input("\n> ").strip()
            
            if cmd.lower() in ("exit", "quit", "q"):
                break
                
            elif cmd.lower() in ("help", "h", "?"):
                print_help()
                
            elif cmd.lower().startswith("connect "):
                try:
                    node_id = int(cmd.split(" ")[1])
                    client.server_id = node_id
                    node = client.get_node_by_id(node_id)
                    print(f"Now connected to Node {node_id} ({node.get('host')}:{node.get('port')})")
                except (ValueError, IndexError):
                    print("Invalid node ID. Use 'connect <node_id>'")
                    
            elif cmd.lower() == "random":
                client.server_id = None
                print("Now connecting to random nodes")
                
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
