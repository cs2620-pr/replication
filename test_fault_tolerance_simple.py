#!/usr/bin/env python3
import os
import time
import signal
import subprocess
import logging
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FaultTest")

def run_command(cmd, blocking=True):
    """Run a shell command"""
    logger.info(f"Running command: {cmd}")
    if blocking:
        return subprocess.run(cmd, shell=True, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    else:
        return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def clear_data():
    """Clear all node data"""
    logger.info("Clearing all node data...")
    run_command("rm -rf data/node*")
    
def start_cluster():
    """Start the Raft cluster"""
    logger.info("Starting the Raft cluster...")
    proc = run_command("python run_cluster.py", blocking=False)
    # Wait for cluster to initialize
    time.sleep(10)
    return proc
    
def kill_node(pid):
    """Kill a node by PID"""
    logger.info(f"Killing node with PID {pid}")
    try:
        os.kill(pid, signal.SIGTERM)
        logger.info(f"Successfully killed node {pid}")
        time.sleep(2)  # Give time for detection
        return True
    except OSError as e:
        logger.error(f"Failed to kill node {pid}: {e}")
        return False

def get_node_pids():
    """Get PIDs of all running server processes"""
    result = run_command("ps -ef | grep 'python server.py' | grep -v grep")
    pids = []
    
    for line in result.stdout.decode('utf-8').splitlines():
        parts = line.split()
        if len(parts) > 1:
            try:
                pid = int(parts[1])
                node_id = None
                # Extract node ID from command
                for i, part in enumerate(parts):
                    if part == "--id" and i + 1 < len(parts):
                        node_id = int(parts[i + 1])
                        break
                
                if node_id:
                    pids.append((pid, node_id))
            except (ValueError, IndexError):
                continue
    
    logger.info(f"Found {len(pids)} running nodes: {pids}")
    return pids

def set_value(key, value):
    """Set a key-value pair"""
    logger.info(f"Setting {key}={value}")
    result = run_command(f'python client.py --operation set --key "{key}" --value "{value}"')
    output = result.stdout.decode('utf-8')
    logger.info(f"Set result: {output}")
    return "Successfully set" in output

def get_value(key):
    """Get a value for a key"""
    logger.info(f"Getting value for key '{key}'")
    result = run_command(f'python client.py --operation get --key "{key}"')
    output = result.stdout.decode('utf-8')
    logger.info(f"Get result: {output}")
    
    if "Key: " in output and ", Value: " in output:
        value = output.split(", Value: ")[1]
        return value
    return None

def run_test():
    """Run a simple fault tolerance test"""
    try:
        # Setup
        logger.info("==== Starting Fault Tolerance Test ====")
        clear_data()
        
        # Start cluster
        cluster_proc = start_cluster()
        
        # Initial data operations
        logger.info("==== Setting Initial Values ====")
        set_value("color", "blue")
        set_value("fruit", "apple")
        
        # Verify data
        logger.info("==== Verifying Initial Values ====")
        color = get_value("color")
        fruit = get_value("fruit")
        
        if color == "blue" and fruit == "apple":
            logger.info("Initial values set and retrieved successfully!")
        else:
            logger.warning(f"Initial values not retrieved correctly: color={color}, fruit={fruit}")
        
        # Get node processes
        node_pids = get_node_pids()
        if not node_pids:
            logger.error("No nodes found running!")
            return
        
        # Kill leader node (assuming first node is leader)
        leader_pid, leader_id = node_pids[0]
        logger.info(f"==== Killing Leader Node {leader_id} (PID {leader_pid}) ====")
        killed = kill_node(leader_pid)
        
        if killed:
            # Wait for election
            logger.info("Waiting for new leader election...")
            time.sleep(5)
            
            # Test if system works without the leader
            logger.info("==== Testing After Leader Failure ====")
            set_value("status", "after_leader_failure")
            status = get_value("status")
            
            if status == "after_leader_failure":
                logger.info("Leader failure handled successfully!")
            else:
                logger.warning(f"Leader failure test failed: status={status}")
            
            # Kill a follower node
            remaining_nodes = get_node_pids()
            if len(remaining_nodes) > 1:
                follower_pid, follower_id = remaining_nodes[1]
                logger.info(f"==== Killing Follower Node {follower_id} (PID {follower_pid}) ====")
                killed = kill_node(follower_pid)
                
                if killed:
                    # Test if system works with a follower failure
                    logger.info("==== Testing After Follower Failure ====")
                    set_value("temperature", "25")
                    temp = get_value("temperature")
                    
                    if temp == "25":
                        logger.info("Follower failure handled successfully!")
                    else:
                        logger.warning(f"Follower failure test failed: temp={temp}")
                    
                    # Check if original values are still available
                    logger.info("==== Checking Data Persistence ====")
                    color = get_value("color")
                    fruit = get_value("fruit")
                    status = get_value("status")
                    
                    if color == "blue" and fruit == "apple" and status == "after_leader_failure":
                        logger.info("Data persistence confirmed!")
                    else:
                        logger.warning(f"Data persistence test failed: color={color}, fruit={fruit}, status={status}")
        
        logger.info("==== Test Complete ====")
    
    finally:
        # Clean up
        logger.info("Cleaning up...")
        run_command("pkill -f 'python run_cluster.py' || true")
        run_command("pkill -f 'python server.py' || true")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple fault tolerance test for Raft implementation")
    
    args = parser.parse_args()
    run_test()
