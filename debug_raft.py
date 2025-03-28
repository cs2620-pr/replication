import os
import json
import pickle
import logging
import time
from typing import Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RaftDebugger")

def inspect_node_state(node_id: int):
    """Inspect the state of a node"""
    logger.info(f"Inspecting node {node_id}")
    
    data_dir = f"./data/node{node_id}"
    
    # Check if node directory exists
    if not os.path.exists(data_dir):
        logger.error(f"Node {node_id} directory not found")
        return
    
    # Read state.json
    state_path = f"{data_dir}/state.json"
    if os.path.exists(state_path):
        with open(state_path, 'r') as f:
            state = json.load(f)
        logger.info(f"Node {node_id} state: {state}")
    else:
        logger.warning(f"Node {node_id} state.json not found")
    
    # Read log.pickle
    log_path = f"{data_dir}/log.pickle"
    log_entries = []
    if os.path.exists(log_path):
        with open(log_path, 'rb') as f:
            try:
                log_entries = pickle.load(f)
                logger.info(f"Node {node_id} log has {len(log_entries)} entries")
                
                # Print details of each log entry
                for i, entry in enumerate(log_entries):
                    logger.info(f"  Entry {i}: term={entry.term}, index={entry.index}, command={entry.command}")
            except Exception as e:
                logger.error(f"Error reading log file: {e}")
    else:
        logger.warning(f"Node {node_id} log.pickle not found")
    
    # Read data.json
    data_path = f"{data_dir}/data.json"
    if os.path.exists(data_path):
        with open(data_path, 'r') as f:
            try:
                data = json.load(f)
                logger.info(f"Node {node_id} data: {data}")
            except Exception as e:
                logger.error(f"Error reading data file: {e}")
    else:
        logger.warning(f"Node {node_id} data.json not found")
    
    return log_entries

def manually_apply_logs(node_id: int, log_entries):
    """Manually apply log entries to state machine"""
    if not log_entries:
        logger.info(f"No log entries to apply for node {node_id}")
        return
    
    data_dir = f"./data/node{node_id}"
    data_path = f"{data_dir}/data.json"
    
    # Load current state machine
    state_machine: Dict[str, str] = {}
    if os.path.exists(data_path):
        with open(data_path, 'r') as f:
            try:
                state_machine = json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in data file, starting with empty state")
    
    # Apply each log entry
    applied_count = 0
    for entry in log_entries:
        command = entry.command
        cmd_type = command.get('type')
        
        if cmd_type == 'set':
            key, value = command.get('key'), command.get('value')
            state_machine[key] = value
            applied_count += 1
            logger.info(f"Applied SET {key}={value}")
        elif cmd_type == 'delete':
            key = command.get('key')
            if key in state_machine:
                del state_machine[key]
                applied_count += 1
                logger.info(f"Applied DELETE {key}")
        elif cmd_type == 'noop':
            # No operation, just a heartbeat
            logger.info(f"Skipped NOOP operation")
    
    # Save updated state machine
    with open(data_path, 'w') as f:
        json.dump(state_machine, f)
    
    logger.info(f"Applied {applied_count} operations to node {node_id}")
    logger.info(f"Updated state machine: {state_machine}")

def main():
    """Main function"""
    # Inspect all nodes
    for node_id in range(1, 4):  # Assuming 3 nodes
        log_entries = inspect_node_state(node_id)
        
        # Ask if we should apply logs
        response = input(f"Apply log entries to node {node_id}? (y/n): ")
        if response.lower() == 'y':
            manually_apply_logs(node_id, log_entries)

if __name__ == "__main__":
    main()
