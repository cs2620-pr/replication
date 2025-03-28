import json
import random
import argparse
import logging
from typing import Optional, Tuple

import grpc

from generated import replication_pb2
from generated import replication_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RaftClient")

class RaftClient:
    def __init__(self, config_path: str):
        """Initialize the Raft client with a configuration file"""
        self.config_path = config_path
        self.nodes = []
        self.leader_id = None
        self.leader_address = None
        
        # Load the configuration
        self.load_config()
    
    def load_config(self):
        """Load nodes configuration from config file"""
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        
        self.nodes = config['nodes']
        self.node_map = {node['id']: (node['host'], node['port']) for node in self.nodes}
    
    def find_leader(self) -> bool:
        """Find the current leader by trying each node"""
        # Shuffle nodes to distribute load
        nodes = list(self.nodes)
        random.shuffle(nodes)
        
        for node in nodes:
            node_id = node['id']
            host, port = node['host'], node['port']
            address = f"{host}:{port}"
            
            try:
                with grpc.insecure_channel(address) as channel:
                    # Try to set a dummy value to identify the leader
                    stub = replication_pb2_grpc.DataServiceStub(channel)
                    response = stub.Set(
                        replication_pb2.SetRequest(key="_find_leader", value="ping"),
                        timeout=1.0
                    )
                    
                    # If successful, this is the leader
                    if response.success:
                        self.leader_id = node_id
                        self.leader_address = address
                        logger.info(f"Found leader: Node {node_id} at {address}")
                        return True
            except grpc.RpcError as e:
                # Check if the error contains leader information
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    details = e.details()
                    if "Leader is at" in details:
                        # Extract leader address from error message
                        leader_info = details.split("Leader is at ")[1]
                        self.leader_address = leader_info
                        logger.info(f"Leader identified at {leader_info}")
                        
                        # Find the leader ID
                        for n in self.nodes:
                            h, p = n['host'], n['port']
                            if f"{h}:{p}" == leader_info:
                                self.leader_id = n['id']
                                break
                        
                        return True
            except Exception as e:
                logger.debug(f"Error contacting node {node_id}: {e}")
                continue
        
        logger.warning("No leader found")
        return False
    
    def get_leader_channel(self) -> Optional[grpc.Channel]:
        """Get a gRPC channel to the leader, finding the leader if necessary"""
        # If we don't know the leader or the connection fails, try to find the leader
        if not self.leader_address or not self._test_connection(self.leader_address):
            if not self.find_leader():
                logger.error("Could not find a leader")
                return None
        
        return grpc.insecure_channel(self.leader_address)
    
    def _test_connection(self, address: str) -> bool:
        """Test if a connection to a server is working"""
        try:
            with grpc.insecure_channel(address) as channel:
                stub = replication_pb2_grpc.DataServiceStub(channel)
                channel_ready = grpc.channel_ready_future(channel)
                channel_ready.result(timeout=1.0)
                return True
        except Exception:
            return False
    
    def set(self, key: str, value: str) -> Tuple[bool, str]:
        """Set a key-value pair"""
        channel = self.get_leader_channel()
        if not channel:
            return False, "No leader available"
        
        try:
            stub = replication_pb2_grpc.DataServiceStub(channel)
            response = stub.Set(
                replication_pb2.SetRequest(key=key, value=value),
                timeout=3.0
            )
            return response.success, response.message
        except Exception as e:
            logger.error(f"Error setting key {key}: {e}")
            self.leader_address = None  # Reset leader to force re-discovery
            return False, str(e)
        finally:
            channel.close()
    
    def get(self, key: str) -> Tuple[bool, str, str]:
        """Get a value for a key (can be from any node, not just leader)"""
        # We can read from any node, try a random one first
        nodes = list(self.nodes)
        random.shuffle(nodes)
        
        for node in nodes:
            host, port = node['host'], node['port']
            address = f"{host}:{port}"
            
            try:
                with grpc.insecure_channel(address) as channel:
                    stub = replication_pb2_grpc.DataServiceStub(channel)
                    response = stub.Get(
                        replication_pb2.GetRequest(key=key),
                        timeout=1.0
                    )
                    return response.success, response.value, response.message
            except Exception as e:
                logger.debug(f"Error getting key {key} from {address}: {e}")
                continue
        
        # If all nodes failed, try to find the leader
        logger.warning("All nodes failed for get operation, trying leader")
        channel = self.get_leader_channel()
        if not channel:
            return False, "", "No nodes available"
        
        try:
            stub = replication_pb2_grpc.DataServiceStub(channel)
            response = stub.Get(
                replication_pb2.GetRequest(key=key),
                timeout=3.0
            )
            return response.success, response.value, response.message
        except Exception as e:
            logger.error(f"Error getting key {key} from leader: {e}")
            return False, "", str(e)
        finally:
            channel.close()

def main():
    parser = argparse.ArgumentParser(description="Raft consensus client")
    parser.add_argument("--config", type=str, default="config.json", help="Path to config file")
    parser.add_argument("--operation", type=str, choices=["get", "set"], required=True, help="Operation to perform")
    parser.add_argument("--key", type=str, required=True, help="Key to get or set")
    parser.add_argument("--value", type=str, help="Value to set (required for set operation)")
    
    args = parser.parse_args()
    
    # Create the client
    client = RaftClient(args.config)
    
    # Perform the requested operation
    if args.operation == "set":
        if not args.value:
            print("Error: --value is required for set operation")
            return
        
        success, message = client.set(args.key, args.value)
        if success:
            print(f"Successfully set {args.key}={args.value}")
        else:
            print(f"Failed to set value: {message}")
    
    elif args.operation == "get":
        success, value, message = client.get(args.key)
        if success:
            print(f"Key: {args.key}, Value: {value}")
        else:
            print(f"Failed to get value: {message}")

if __name__ == "__main__":
    main()
