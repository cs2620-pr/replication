import os
import sys
import time
import signal
import argparse
import logging
from concurrent import futures

import grpc

from raft_node import RaftNode
from generated import replication_pb2
from generated import replication_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RaftServer")

def run_server(node_id, config_path):
    """Run a Raft node server"""
    # Create and initialize the Raft node
    node = RaftNode(node_id, config_path)
    
    # Load node configuration
    node.load_config()
    host, port = None, None
    for n in node.nodes:
        if n['id'] == node_id:
            host, port = n['host'], n['port']
            break
    
    if host is None or port is None:
        logger.error(f"Node {node_id} not found in configuration")
        return
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Register services
    replication_pb2_grpc.add_NodeCommunicationServicer_to_server(node, server)
    replication_pb2_grpc.add_DataServiceServicer_to_server(node, server)
    replication_pb2_grpc.add_MonitoringServiceServicer_to_server(node, server)
    
    # Start server
    server_address = f"{host}:{port}"
    server.add_insecure_port(server_address)
    server.start()
    
    logger.info(f"Raft server node {node_id} started on {server_address}")
    
    # Handle graceful shutdown
    def handle_signal(sig, frame):
        logger.info(f"Shutting down node {node_id}...")
        node.stop()
        server.stop(grace=2.0)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    try:
        # Keep server running
        while True:
            time.sleep(3600)  # Sleep for an hour (or until interrupted)
    except KeyboardInterrupt:
        node.stop()
        server.stop(grace=2.0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Raft consensus node")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    parser.add_argument("--config", type=str, default="config.json", help="Path to config file")
    
    args = parser.parse_args()
    run_server(args.id, args.config)
