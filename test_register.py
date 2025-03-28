#!/usr/bin/env python3
import grpc
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TestRegister')

# Import the generated protobuf and grpc code
try:
    from generated import replication_pb2 as pb2
    from generated import replication_pb2_grpc as pb2_grpc
    logger.info("Successfully imported pb2 and pb2_grpc")
except ImportError as e:
    logger.error(f"Import error: {e}")
    sys.exit(1)

def test_register():
    """Test registering a user"""
    try:
        # Create a channel to the server
        channel = grpc.insecure_channel('localhost:50051')
        logger.info("Channel created")
        
        # Create a stub
        stub = pb2_grpc.MessageServiceStub(channel)
        logger.info("Stub created")
        
        # Create a request
        request = pb2.RegisterRequest(
            username='testuser', 
            password='password', 
            display_name='Test User'
        )
        logger.info("Request created")
        
        # Call the service method
        logger.info("Attempting to call Register...")
        response = stub.Register(request, timeout=5.0)
        
        # Print the response
        logger.info(f"Response received: {response}")
        
    except grpc.RpcError as e:
        logger.error(f"RPC error: {e.code()}: {e.details()}")
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == "__main__":
    test_register() 