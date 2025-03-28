#!/usr/bin/env python3
import os
import sys
import importlib

# Add the current directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Try importing the modules and print the results
modules_to_test = [
    'PyQt6',
    'grpc',
    'generated',
    'generated.replication_pb2',
    'generated.replication_pb2_grpc',
    'chat_client',
    'chat_client_ui'
]

for module_name in modules_to_test:
    try:
        module = importlib.import_module(module_name)
        print(f"✅ Successfully imported {module_name}")
        if hasattr(module, '__file__'):
            print(f"   Path: {module.__file__}")
    except ImportError as e:
        print(f"❌ Failed to import {module_name}: {e}")

print("\nPython path:")
for path in sys.path:
    print(f"  - {path}") 