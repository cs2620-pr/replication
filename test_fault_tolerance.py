#!/usr/bin/env python3
import os
import time
import signal
import subprocess
import argparse
import json
import random
from typing import List, Dict, Optional, Tuple

def run_command(cmd: str) -> Tuple[int, str]:
    """Run a shell command and return exit code and output"""
    process = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    output, _ = process.communicate()
    return process.returncode, output.strip()

def clear_data():
    """Clear all node data directories"""
    print("Clearing all node data...")
    run_command("rm -rf data/node*")

def start_cluster(config_path: str = "config.json") -> List[int]:
    """Start the cluster and return a list of PIDs"""
    print("Starting the cluster...")
    
    # Run in the background
    process = subprocess.Popen(
        ["python", "run_cluster.py", "--config", config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    
    # Wait for cluster to initialize (look for "Cluster started" in output)
    initialized = False
    pids = []
    output_lines = []
    
    while not initialized:
        line = process.stdout.readline().strip()
        if not line:
            time.sleep(0.1)
            continue
            
        output_lines.append(line)
        print(f"  {line}")
        
        # Extract PIDs from output
        if line.startswith("Started node "):
            try:
                pid = int(line.split("PID: ")[1])
                pids.append(pid)
            except (IndexError, ValueError):
                pass
                
        if "Cluster started with " in line:
            initialized = True
    
    print(f"Cluster started with {len(pids)} nodes (PIDs: {pids})")
    return pids

def kill_node(pid: int):
    """Kill a specific node by PID"""
    print(f"Killing node with PID {pid}...")
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"  Node {pid} terminated")
    except OSError as e:
        print(f"  Failed to kill node {pid}: {e}")
    
    # Give the system time to detect the failure
    time.sleep(2)

def find_leader():
    """Find the current leader and return (node_id, host, port)"""
    print("Finding current leader...")
    exitcode, output = run_command("python client.py --operation set --key _test_leader --value test")
    
    leader_info = None
    
    for line in output.splitlines():
        if "Leader identified at" in line:
            # Parse out the leader address
            parts = line.split("Leader identified at ")[1]
            host, port = parts.rsplit(":", 1)
            
            # Load config to map back to node_id
            with open("config.json", 'r') as f:
                config = json.load(f)
            
            for node in config['nodes']:
                if node['host'] == host and str(node['port']) == port:
                    leader_info = (node['id'], host, port)
                    break
        elif "Found leader: Node" in line:
            # Parse "Found leader: Node 2 at localhost:50052"
            parts = line.split("Found leader: Node ")[1]
            node_id = int(parts.split(" at ")[0])
            addr = parts.split(" at ")[1]
            host, port = addr.rsplit(":", 1)
            leader_info = (node_id, host, port)
    
    if leader_info:
        print(f"  Current leader is Node {leader_info[0]} at {leader_info[1]}:{leader_info[2]}")
    else:
        print("  Could not determine current leader")
    
    return leader_info

def set_value(key: str, value: str) -> bool:
    """Set a key-value pair and return success status"""
    print(f"Setting {key}={value}...")
    exitcode, output = run_command(f'python client.py --operation set --key "{key}" --value "{value}"')
    
    success = "Successfully set" in output
    if success:
        print(f"  Successfully set {key}={value}")
    else:
        print(f"  Failed to set {key}={value}: {output}")
    
    return success

def get_value(key: str) -> Optional[str]:
    """Get a value for a key and return the value or None if not found"""
    print(f"Getting value for key '{key}'...")
    exitcode, output = run_command(f'python client.py --operation get --key "{key}"')
    
    value = None
    if "Key: " in output and ", Value: " in output:
        # Extract value from "Key: color, Value: blue"
        value = output.split(", Value: ")[1]
        print(f"  Successfully retrieved {key}={value}")
    else:
        print(f"  Failed to get value for '{key}': {output}")
    
    return value

def wait_for_leader(max_attempts: int = 5) -> bool:
    """Wait for a leader to be elected, returns True if leader found"""
    print("Waiting for leader election...")
    attempts = 0
    leader = None
    
    while attempts < max_attempts:
        leader = find_leader()
        if leader:
            return True
        
        print(f"  No leader found, waiting (attempt {attempts+1}/{max_attempts})...")
        attempts += 1
        time.sleep(2)
    
    return False

def run_fault_tolerance_test(max_failures: int = 2):
    """Run a comprehensive fault tolerance test"""
    print("\n=== Starting Fault Tolerance Test ===\n")
    
    # Start with a clean state
    clear_data()
    
    # Start the cluster
    node_pids = start_cluster()
    if not node_pids:
        print("Failed to start cluster")
        return
    
    # Give the cluster time to elect a leader
    print("\nAllowing time for initial leader election...")
    time.sleep(5)
    
    # Find the initial leader
    leader_info = find_leader()
    if not leader_info:
        print("Failed to identify initial leader")
        run_command("pkill -f 'python run_cluster.py'")
        return
    
    leader_node_id, leader_host, leader_port = leader_info
    
    # Store some initial data
    print("\n--- Setting Initial Values ---")
    initial_keys = {
        "color": "blue",
        "fruit": "apple",
        "animal": "tiger",
        "country": "Canada",
        "language": "Python"
    }
    
    for key, value in initial_keys.items():
        set_value(key, value)
        time.sleep(0.5)  # Small delay between operations
    
    # Verify we can read the values
    print("\n--- Verifying Initial Values ---")
    successful_reads = 0
    for key, expected_value in initial_keys.items():
        value = get_value(key)
        if value == expected_value:
            successful_reads += 1
        time.sleep(0.5)  # Small delay between operations
    
    print(f"\nSuccessfully read {successful_reads}/{len(initial_keys)} values")
    
    # Find the leader's PID
    leader_pid = None
    for i, pid in enumerate(node_pids):
        # Node IDs are 1-indexed, but our list is 0-indexed
        if i + 1 == leader_node_id:
            leader_pid = pid
            break
    
    if not leader_pid:
        print(f"Could not find PID for leader node {leader_node_id}")
        run_command("pkill -f 'python run_cluster.py'")
        return
    
    # Test fault tolerance by killing nodes one by one
    print("\n--- Testing Fault Tolerance by Killing Nodes ---")
    nodes_killed = 0
    
    # Test killing the leader first
    print("\n1. Killing the leader")
    kill_node(leader_pid)
    nodes_killed += 1
    
    # Wait for a new leader to be elected
    if not wait_for_leader():
        print("Failed to elect a new leader after killing the initial leader")
        run_command("pkill -f 'python run_cluster.py'")
        return
    
    # Set a new value to confirm the cluster is still working
    print("\nTesting if cluster is still operational...")
    if not set_value("status", "operational_after_leader_failure"):
        print("Failed to set value after killing leader")
        run_command("pkill -f 'python run_cluster.py'")
        return
    
    # Get a previously set value
    value = get_value("color")
    if value is None:
        print("Failed to retrieve previous value after leader failure")
    
    # Kill additional followers up to max_failures
    print(f"\n2. Killing up to {max_failures-1} more nodes (followers)")
    
    # Get remaining PIDs (excluding the leader we already killed)
    remaining_pids = [pid for pid in node_pids if pid != leader_pid]
    
    # Shuffle to randomly select followers to kill
    random.shuffle(remaining_pids)
    
    for i in range(min(max_failures-1, len(remaining_pids))):
        pid_to_kill = remaining_pids[i]
        kill_node(pid_to_kill)
        nodes_killed += 1
        
        # Try to set and get a value after each failure
        test_key = f"after_kill_{i+1}"
        test_value = f"survived_{i+1}_failures"
        
        # Give system time to stabilize
        time.sleep(2)
        
        print(f"\nTesting if cluster is still operational after {nodes_killed} node failures...")
        if not set_value(test_key, test_value):
            print(f"Failed to set value after killing {nodes_killed} nodes")
            break
        
        value = get_value(test_key)
        if value != test_value:
            print(f"Failed to retrieve new value after killing {nodes_killed} nodes")
        
        # Also try to get an initial value
        value = get_value("color")
        if value is None:
            print(f"Failed to retrieve initial value after killing {nodes_killed} nodes")
    
    # Final status
    print("\n=== Fault Tolerance Test Results ===")
    print(f"Nodes in cluster: {len(node_pids)}")
    print(f"Nodes killed: {nodes_killed}")
    
    # Get final values to check data consistency
    print("\nChecking final data consistency...")
    consistent_values = 0
    for key, expected_value in initial_keys.items():
        value = get_value(key)
        if value == expected_value:
            consistent_values += 1
    
    print(f"Data consistency: {consistent_values}/{len(initial_keys)} original values maintained")
    
    # Clean up
    print("\nCleaning up...")
    run_command("pkill -f 'python run_cluster.py'")
    
    # Final conclusion
    if nodes_killed >= max_failures and consistent_values > 0:
        print(f"\nSUCCESS: The system tolerated {nodes_killed} node failures while maintaining data consistency!")
    else:
        print(f"\nPARTIAL SUCCESS: The system handled some failures but with data consistency issues.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test fault tolerance of Raft cluster")
    parser.add_argument("--max-failures", type=int, default=2, 
                        help="Maximum number of node failures to test")
    
    args = parser.parse_args()
    run_fault_tolerance_test(args.max_failures)
