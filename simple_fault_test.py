#!/usr/bin/env python3
import os
import time
import signal
import subprocess
import json
from typing import Tuple, Optional, List

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

def start_cluster():
    """Start the cluster in the background"""
    print("Starting the cluster in the background...")
    subprocess.Popen(
        ["python", "run_cluster.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    # Give time for cluster to start
    time.sleep(10)
    print("Cluster started")

def kill_cluster():
    """Kill the cluster"""
    print("Killing the cluster...")
    run_command("pkill -f 'python run_cluster.py'")
    time.sleep(2)

def clear_data():
    """Clear all node data"""
    print("Clearing all node data...")
    run_command("rm -rf data/node*")

def get_node_pids() -> List[int]:
    """Get PIDs of running server processes"""
    exitcode, output = run_command("ps -ef | grep 'python server.py' | grep -v grep")
    pids = []
    
    for line in output.splitlines():
        parts = line.split()
        if len(parts) > 1:
            try:
                pid = int(parts[1])
                pids.append(pid)
            except (ValueError, IndexError):
                continue
    
    print(f"Found {len(pids)} running nodes with PIDs: {pids}")
    return pids

def find_leader() -> Optional[Tuple[int, str, str]]:
    """Find the current leader"""
    print("Finding current leader...")
    exitcode, output = run_command("python client.py --operation set --key _test_leader --value test")
    
    leader_info = None
    
    for line in output.splitlines():
        if "Leader identified at" in line:
            parts = line.split("Leader identified at ")[1]
            host, port = parts.rsplit(":", 1)
            
            with open("config.json", 'r') as f:
                config = json.load(f)
            
            for node in config['nodes']:
                if node['host'] == host and str(node['port']) == port:
                    leader_info = (node['id'], host, port)
                    break
        elif "Found leader: Node" in line:
            parts = line.split("Found leader: Node ")[1]
            node_id = int(parts.split(" at ")[0])
            addr = parts.split(" at ")[1]
            host, port = addr.rsplit(":", 1)
            leader_info = (node_id, host, port)
    
    if leader_info:
        print(f"Current leader is Node {leader_info[0]} at {leader_info[1]}:{leader_info[2]}")
    else:
        print("Could not determine current leader")
    
    return leader_info

def kill_node(pid: int):
    """Kill a specific node by PID"""
    print(f"Killing node with PID {pid}...")
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Node {pid} terminated")
    except OSError as e:
        print(f"Failed to kill node {pid}: {e}")
    
    # Give the system time to detect the failure
    time.sleep(2)

def set_value(key: str, value: str) -> bool:
    """Set a key-value pair and return success status"""
    print(f"Setting {key}={value}...")
    exitcode, output = run_command(f'python client.py --operation set --key "{key}" --value "{value}"')
    
    success = "Successfully set" in output
    if success:
        print(f"Successfully set {key}={value}")
    else:
        print(f"Failed to set {key}={value}: {output}")
    
    return success

def get_value(key: str) -> Optional[str]:
    """Get a value for a key and return the value or None if not found"""
    print(f"Getting value for key '{key}'...")
    exitcode, output = run_command(f'python client.py --operation get --key "{key}"')
    
    value = None
    if "Key: " in output and ", Value: " in output:
        value = output.split(", Value: ")[1]
        print(f"Successfully retrieved {key}={value}")
    else:
        print(f"Failed to get value for '{key}': {output}")
    
    return value

def run_simple_test():
    """Run a simple fault tolerance test"""
    try:
        print("\n=== Starting Simple Fault Tolerance Test ===\n")
        
        # Clean up from previous runs
        kill_cluster()
        clear_data()
        
        # Start the cluster and wait for it to initialize
        start_cluster()
        
        # Store some initial data
        print("\n--- Setting Initial Values ---")
        set_value("color", "blue")
        set_value("fruit", "apple")
        
        # Verify we can read the values
        print("\n--- Verifying Initial Values ---")
        color = get_value("color")
        fruit = get_value("fruit")
        
        # Get the leader and running nodes
        leader_info = find_leader()
        node_pids = get_node_pids()
        
        if not leader_info or not node_pids:
            print("Failed to get necessary information")
            return
        
        leader_node_id = leader_info[0]
        
        # Find leader PID
        leader_pid = None
        for pid in node_pids:
            exitcode, output = run_command(f"ps -p {pid} -o command=")
            if f"--id {leader_node_id}" in output:
                leader_pid = pid
                break
        
        if not leader_pid:
            print(f"Could not find PID for leader node {leader_node_id}")
            return
        
        print(f"\n--- Killing Leader (Node {leader_node_id}, PID {leader_pid}) ---")
        kill_node(leader_pid)
        
        # Wait for election
        print("Waiting for new leader election...")
        time.sleep(5)
        
        # Try setting a new value
        print("\n--- Testing After Leader Failure ---")
        set_value("status", "after_leader_failure")
        
        # See if we can still get the old values
        print("\nTrying to get values set before leader failure:")
        color = get_value("color")
        fruit = get_value("fruit")
        
        # Kill a follower node
        remaining_pids = [pid for pid in node_pids if pid != leader_pid]
        if remaining_pids:
            follower_pid = remaining_pids[0]
            print(f"\n--- Killing Follower Node (PID {follower_pid}) ---")
            kill_node(follower_pid)
            
            # Try setting another value
            print("\n--- Testing After Follower Failure ---")
            set_value("temperature", "25C")
            
            # See if we can still get all values
            print("\nTrying to get all values:")
            color = get_value("color")
            fruit = get_value("fruit")
            status = get_value("status")
            temp = get_value("temperature")
        
        print("\n=== Test Completed ===")
        
    finally:
        # Clean up
        print("\nCleaning up...")
        kill_cluster()

if __name__ == "__main__":
    run_simple_test()
