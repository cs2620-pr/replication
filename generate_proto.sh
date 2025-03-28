#!/bin/bash

# Ensure script exits on first error
set -e

# Activate virtual environment
source venv/bin/activate

# Create directory for generated files if it doesn't exist
mkdir -p generated

# Generate Python code from proto file
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./generated \
    --grpc_python_out=./generated \
    ./proto/replication.proto

# Create __init__.py file to make the directory a proper package
touch ./generated/__init__.py

# Fix import paths in the generated files
sed -i.bak 's/import replication_pb2/from . import replication_pb2/' ./generated/replication_pb2_grpc.py
rm -f ./generated/replication_pb2_grpc.py.bak

echo "Proto stubs successfully generated in ./generated directory"
