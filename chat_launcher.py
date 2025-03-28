#!/usr/bin/env python3
import sys
import os
import argparse
import logging
import subprocess

def check_requirements():
    """Check if all requirements are installed"""
    try:
        import PyQt6
        import grpc
        return True
    except ImportError as e:
        print(f"Missing requirement: {e}")
        print("Please install required packages:")
        print("  pip install PyQt6 grpcio grpcio-tools")
        return False

def generate_proto():
    """Generate protocol buffer code"""
    proto_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proto")
    generated_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated")
    
    # Create generated directory if it doesn't exist
    if not os.path.exists(generated_dir):
        os.makedirs(generated_dir)
    
    # Generate the Python code from the proto file
    proto_file = os.path.join(proto_dir, "replication.proto")
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={generated_dir}",
        f"--grpc_python_out={generated_dir}",
        proto_file
    ]
    
    try:
        subprocess.check_call(cmd)
        print("Protocol buffer code generated successfully")
        
        # Create an __init__.py file in the generated directory
        init_file = os.path.join(generated_dir, "__init__.py")
        with open(init_file, 'w') as f:
            f.write("# Generated code package\n")
        print("Created __init__.py in generated directory")
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error generating protocol buffer code: {e}")
        return False

def launch_client(config_path):
    """Launch the chat client"""
    # Make sure the current directory is in the Python path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    
    # Also add the parent directory to handle imports
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    # Import the chat client UI
    try:
        from chat_client_ui import main
        main()
    except ImportError as e:
        print(f"Error importing chat client: {e}")
        # Try to diagnose the import error
        try:
            import chat_client
            print("chat_client module imported successfully")
        except ImportError as e2:
            print(f"Error importing chat_client: {e2}")
        
        try:
            import generated.replication_pb2
            print("generated.replication_pb2 module imported successfully")
        except ImportError as e3:
            print(f"Error importing generated.replication_pb2: {e3}")
        
        return False
    
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raft Chat Client Launcher")
    parser.add_argument("--config", "-c", type=str, default="config.json", help="Path to config file")
    parser.add_argument("--skip-checks", action="store_true", help="Skip requirement checks")
    parser.add_argument("--regenerate-proto", action="store_true", help="Force regeneration of protocol buffer code")
    
    args = parser.parse_args()
    
    # Check requirements
    if not args.skip_checks and not check_requirements():
        sys.exit(1)
    
    # Generate protocol buffer code if needed or forced
    generated_marker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated", "replication_pb2.py")
    if args.regenerate_proto or not os.path.exists(generated_marker):
        if not generate_proto():
            sys.exit(1)
    
    # Launch the client
    if not launch_client(args.config):
        sys.exit(1) 