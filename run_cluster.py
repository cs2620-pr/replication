import os
import time
import signal
import subprocess
import argparse
import json
from typing import List, Dict

# Global list of server processes
server_processes: List[subprocess.Popen] = []

def load_config(config_path: str) -> Dict:
    """Load the configuration file"""
    with open(config_path, 'r') as f:
        return json.load(f)

def start_node(node_id: int, config_path: str) -> subprocess.Popen:
    """Start a single Raft node"""
    # Get the path to the virtual environment Python interpreter
    venv_python = os.path.join(os.path.dirname(os.path.abspath(__file__)), "venv", "bin", "python")
    
    cmd = [
        venv_python, 'server.py',
        '--id', str(node_id),
        '--config', config_path
    ]
    
    # Redirect stdout and stderr to node-specific log files
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    stdout_file = open(f"{log_dir}/node{node_id}.log", 'w')
    
    # Start the process
    process = subprocess.Popen(
        cmd,
        stdout=stdout_file,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    
    print(f"Started node {node_id}, PID: {process.pid}")
    return process

def stop_cluster():
    """Stop all server processes"""
    for process in server_processes:
        if process.poll() is None:  # If process is still running
            process.send_signal(signal.SIGINT)
            print(f"Sent SIGINT to process {process.pid}")
    
    # Wait for all processes to terminate
    for process in server_processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            print(f"Forcefully killed process {process.pid}")

def run_cluster(node_ids: List[int], config_path: str, auto_restart: bool = True):
    """Run a cluster of Raft nodes"""
    global server_processes
    
    # Start each node
    for node_id in node_ids:
        process = start_node(node_id, config_path)
        server_processes.append(process)
        time.sleep(0.5)  # Small delay between starts
    
    print(f"Cluster started with {len(node_ids)} nodes")
    if not auto_restart:
        print("Auto-restart is disabled. Failed nodes will not be restarted automatically.")
    
    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("Shutting down cluster...")
        stop_cluster()
        exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Keep the script running until interrupted
    try:
        while True:
            # Check if any processes have died
            for i, process in enumerate(server_processes):
                if process.poll() is not None:
                    node_id = node_ids[i]
                    print(f"Node {node_id} (PID: {process.pid}) has exited with code {process.returncode}")
                    
                    if auto_restart:
                        # Restart the node
                        print(f"Restarting node {node_id}...")
                        new_process = start_node(node_id, config_path)
                        server_processes[i] = new_process
                    else:
                        print(f"Auto-restart is disabled. Node {node_id} will not be restarted.")
            
            time.sleep(5)  # Check every 5 seconds
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Shutting down cluster...")
        stop_cluster()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a cluster of Raft nodes")
    parser.add_argument("--config", type=str, default="config.json", help="Path to config file")
    parser.add_argument("--nodes", type=str, default="all", 
                        help="Comma-separated list of node IDs to start, or 'all' for all nodes")
    parser.add_argument("--no-auto-restart", action="store_true", 
                        help="Disable auto-restart of failed nodes")
    
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    
    # Determine which nodes to start
    if args.nodes.lower() == 'all':
        node_ids = [node['id'] for node in config['nodes']]
    else:
        node_ids = [int(x.strip()) for x in args.nodes.split(',')]
    
    # Create logs directory
    os.makedirs("logs", exist_ok=True)
    
    # Run the cluster
    run_cluster(node_ids, args.config, not args.no_auto_restart)
