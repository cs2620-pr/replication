#!/bin/bash

# Exit on any error
set -e

# Create the output directories if they don't exist
mkdir -p server/chat
mkdir -p server/replication

# Compile the proto files with mypy support
python -m grpc_tools.protoc \
    -I./protos \
    --python_out=./server/chat \
    --grpc_python_out=./server/chat \
    --mypy_out=./server/chat \
    ./protos/chat.proto

python -m grpc_tools.protoc \
    -I./protos \
    --python_out=./server/replication \
    --grpc_python_out=./server/replication \
    --mypy_out=./server/replication \
    ./protos/replication.proto

# Create __init__.py files if they don't exist
touch server/__init__.py
touch server/chat/__init__.py
touch server/replication/__init__.py

# Create py.typed files to mark the packages as typed
touch server/chat/py.typed
touch server/replication/py.typed

# Fix imports in generated files
sed -i '' 's/import chat_pb2/from . import chat_pb2/' server/chat/chat_pb2_grpc.py
sed -i '' 's/import replication_pb2/from . import replication_pb2/' server/replication/replication_pb2_grpc.py

echo "Proto compilation completed successfully!"
