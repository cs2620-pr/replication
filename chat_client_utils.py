import json
import time
import hashlib
import threading
import logging
from typing import Optional, List, Dict, Any, Callable

import grpc
from PyQt5.QtCore import QObject, pyqtSignal, QThread

from generated import replication_pb2
from generated import replication_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ChatClient")

class AsyncOperation(QThread):
    """Thread for executing asynchronous client operations"""
    
    # Signal to emit the result of the operation
    resultReady = pyqtSignal(object)
    
    def __init__(self, target_func, *args, **kwargs):
        super().__init__()
        self.target_func = target_func
        self.args = args
        self.kwargs = kwargs
        
    def run(self):
        try:
            result = self.target_func(*self.args, **self.kwargs)
            self.resultReady.emit(result)
        except Exception as e:
            logger.error(f"Error in async operation: {e}")
            self.resultReady.emit({"success": False, "message": f"Operation failed: {str(e)}"})


class ChatClientAPI(QObject):
    """API client for interacting with the chat service"""
    
    # Signals for async operations
    userListUpdated = pyqtSignal(list)
    messageReceived = pyqtSignal(object)
    userStatusChanged = pyqtSignal(object)
    messageRead = pyqtSignal(object)
    messageDeleted = pyqtSignal(object)
    connectionError = pyqtSignal(str)
    
    def __init__(self, config_path: str):
        """Initialize the client with a configuration file"""
        super().__init__()
        self.config_path = config_path
        self.nodes = []
        self.leader_id = None
        self.leader_address = None
        
        # User authentication state
        self.user_id = None
        self.session_token = None
        self.username = None
        
        # Active async operations
        self.async_operations = []
        
        # Load the configuration
        self.load_config()
        
        # Update thread
        self.update_subscriber = None
    
    def load_config(self):
        """Load nodes configuration from config file"""
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        
        self.nodes = config['nodes']
        self.node_map = {node['id']: (node['host'], node['port']) for node in self.nodes}
    
    def find_leader(self) -> bool:
        """Find the current leader by trying each node"""
        # Shuffle nodes to distribute load
        import random
        nodes = list(self.nodes)
        random.shuffle(nodes)
        
        for node in nodes:
            node_id = node['id']
            host, port = node['host'], node['port']
            address = f"{host}:{port}"
            
            try:
                with grpc.insecure_channel(address) as channel:
                    # Try to get node status to identify the leader
                    stub = replication_pb2_grpc.MonitoringServiceStub(channel)
                    response = stub.NodeStatus(
                        replication_pb2.NodeStatusRequest(include_log=False, include_state_machine=False),
                        timeout=1.0
                    )
                    
                    # Check if this is the leader
                    if response.state == 2:  # LEADER state
                        self.leader_id = node_id
                        self.leader_address = address
                        logger.info(f"Found leader: Node {node_id} at {address}")
                        return True
                    
                    # If not the leader, check if it knows who is
                    if response.current_leader > 0:
                        leader_id = response.current_leader
                        if leader_id in self.node_map:
                            host, port = self.node_map[leader_id]
                            self.leader_address = f"{host}:{port}"
                            self.leader_id = leader_id
                            logger.info(f"Leader identified: Node {leader_id} at {self.leader_address}")
                            return True
            except Exception as e:
                logger.debug(f"Error contacting node {node_id}: {e}")
                continue
        
        logger.warning("No leader found")
        return False
    
    def get_leader_channel(self) -> Optional[grpc.Channel]:
        """Get a gRPC channel to the leader, finding the leader if necessary"""
        # If we don't know the leader or the connection fails, try to find the leader
        if not self.leader_address or not self._test_connection(self.leader_address):
            if not self.find_leader():
                logger.error("Could not find a leader")
                self.connectionError.emit("Could not connect to any server in the cluster")
                return None
        
        return grpc.insecure_channel(self.leader_address)
    
    def _test_connection(self, address: str) -> bool:
        """Test if a connection to a server is working"""
        try:
            with grpc.insecure_channel(address) as channel:
                stub = replication_pb2_grpc.MonitoringServiceStub(channel)
                channel_ready = grpc.channel_ready_future(channel)
                channel_ready.result(timeout=1.0)
                return True
        except Exception:
            return False
    
    def _hash_password(self, password: str) -> str:
        """Hash a password for secure transmission"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def register_user(self, username: str, password: str, display_name: str) -> Dict[str, Any]:
        """Register a new user"""
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            # Hash password before sending
            hashed_password = self._hash_password(password)
            
            response = stub.RegisterUser(
                replication_pb2.RegisterUserRequest(
                    username=username,
                    password=hashed_password,
                    display_name=display_name
                ),
                timeout=5.0
            )
            
            return {
                "success": response.success,
                "message": response.message,
                "user_id": response.user_id if response.success else None
            }
            
        except Exception as e:
            logger.error(f"Error registering user: {e}")
            return {"success": False, "message": str(e)}
        finally:
            channel.close()
    
    def login(self, username: str, password: str) -> Dict[str, Any]:
        """Login a user"""
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            # Hash password before sending
            hashed_password = self._hash_password(password)
            
            response = stub.Login(
                replication_pb2.LoginRequest(
                    username=username,
                    password=hashed_password
                ),
                timeout=5.0
            )
            
            if response.success:
                # Save authentication state
                self.user_id = response.user_id
                self.session_token = response.session_token
                self.username = username
                
                # Start update subscriber if login successful
                self._start_update_subscriber()
            
            return {
                "success": response.success,
                "message": response.message,
                "user_id": response.user_id if response.success else None,
                "session_token": response.session_token if response.success else None
            }
            
        except Exception as e:
            logger.error(f"Error logging in: {e}")
            return {"success": False, "message": str(e)}
        finally:
            channel.close()
    
    def logout(self):
        """Logout the current user"""
        if self.update_subscriber:
            self.update_subscriber.stop()
            self.update_subscriber = None
        
        # Reset authentication state
        self.user_id = None
        self.session_token = None
        self.username = None
    
    def update_status(self, status: int) -> Dict[str, Any]:
        """Update the user's status"""
        if not self.user_id or not self.session_token:
            return {"success": False, "message": "Not logged in"}
        
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            response = stub.UpdateStatus(
                replication_pb2.UpdateStatusRequest(
                    user_id=self.user_id,
                    session_token=self.session_token,
                    status=status
                ),
                timeout=5.0
            )
            
            return {
                "success": response.success,
                "message": response.message
            }
            
        except Exception as e:
            logger.error(f"Error updating status: {e}")
            return {"success": False, "message": str(e)}
        finally:
            channel.close()
    
    def get_users_list(self):
        """Get a list of all users"""
        # Start an async operation for this request
        self._run_async(self._get_users_list_sync, self._on_users_list_received)
    
    def _get_users_list_sync(self):
        """Synchronous implementation of get_users_list"""
        if not self.user_id or not self.session_token:
            return {"success": False, "message": "Not logged in"}
        
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            response = stub.GetUsersList(
                replication_pb2.GetUsersListRequest(
                    user_id=self.user_id,
                    session_token=self.session_token
                ),
                timeout=5.0
            )
            
            if response.success:
                users = []
                for user in response.users:
                    users.append({
                        "user_id": user.user_id,
                        "username": user.username,
                        "display_name": user.display_name,
                        "status": user.status,
                        "last_active_timestamp": user.last_active_timestamp,
                        "unread_message_count": user.unread_message_count
                    })
                
                return {"success": True, "users": users}
            else:
                return {"success": False, "message": response.message}
            
        except Exception as e:
            logger.error(f"Error getting users list: {e}")
            return {"success": False, "message": str(e)}
        finally:
            channel.close()
    
    def _on_users_list_received(self, result):
        """Handle the result of an async get_users_list operation"""
        if result["success"]:
            self.userListUpdated.emit(result["users"])
        else:
            logger.warning(f"Failed to get users list: {result['message']}")
    
    def get_messages(self, other_user_id: str, limit: int = 50, before_timestamp: int = None):
        """Get messages between the current user and another user"""
        # Start an async operation for this request
        self._run_async(
            self._get_messages_sync, 
            self._on_messages_received,
            other_user_id, limit, before_timestamp
        )
        return {"success": True, "message": "Fetching messages..."}
    
    def _get_messages_sync(self, other_user_id: str, limit: int = 50, before_timestamp: int = None):
        """Synchronous implementation of get_messages"""
        if not self.user_id or not self.session_token:
            return {"success": False, "message": "Not logged in"}
        
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            request = replication_pb2.GetMessagesRequest(
                user_id=self.user_id,
                session_token=self.session_token,
                other_user_id=other_user_id,
                limit=limit
            )
            
            if before_timestamp:
                request.before_timestamp = before_timestamp
            
            response = stub.GetMessages(request, timeout=5.0)
            
            if response.success:
                messages = []
                for msg in response.messages:
                    messages.append({
                        "message_id": msg.message_id,
                        "sender_id": msg.sender_id,
                        "recipient_id": msg.recipient_id,
                        "content": msg.content,
                        "timestamp": msg.timestamp,
                        "read": msg.read,
                        "deleted": msg.deleted
                    })
                
                return {"success": True, "messages": messages, "other_user_id": other_user_id}
            else:
                return {"success": False, "message": response.message}
            
        except Exception as e:
            logger.error(f"Error getting messages: {e}")
            return {"success": False, "message": str(e), "other_user_id": other_user_id}
        finally:
            channel.close()
    
    def _on_messages_received(self, result):
        """Handle the result of an async get_messages operation"""
        if not result["success"]:
            logger.warning(f"Failed to get messages: {result.get('message')}")
            # We don't emit a signal for failure, the UI will remain unchanged
            return
        
        # For successful message retrieval, emit a signal with the messages data
        # The ChatWidget objects will filter and use only relevant messages
        self.messageReceived.emit(result)
    
    def _run_async(self, target_func, callback, *args, **kwargs):
        """Run an operation asynchronously and call the callback with the result"""
        thread = AsyncOperation(target_func, *args, **kwargs)
        thread.resultReady.connect(callback)
        
        # Store a reference to prevent garbage collection
        self.async_operations.append(thread)
        
        # Connect to the finished signal to remove the thread from our list
        thread.finished.connect(lambda: self._cleanup_thread(thread))
        
        # Start the thread
        thread.start()
    
    def _cleanup_thread(self, thread):
        """Remove a finished thread from our list of operations"""
        if thread in self.async_operations:
            self.async_operations.remove(thread)
    
    def send_message(self, recipient_id: str, content: str) -> Dict[str, Any]:
        """Send a message to another user"""
        if not self.user_id or not self.session_token:
            return {"success": False, "message": "Not logged in"}
        
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            response = stub.SendMessage(
                replication_pb2.SendMessageRequest(
                    user_id=self.user_id,
                    session_token=self.session_token,
                    recipient_id=recipient_id,
                    content=content
                ),
                timeout=5.0
            )
            
            message_data = None
            if response.success and response.sent_message:
                message_data = {
                    "message_id": response.sent_message.message_id,
                    "sender_id": response.sent_message.sender_id,
                    "recipient_id": response.sent_message.recipient_id,
                    "content": response.sent_message.content,
                    "timestamp": response.sent_message.timestamp,
                    "read": response.sent_message.read,
                    "deleted": response.sent_message.deleted
                }
                
                # Emit signal with the new message
                self.messageReceived.emit(message_data)
            
            return {
                "success": response.success,
                "message": response.message,
                "sent_message": message_data
            }
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return {"success": False, "message": str(e)}
        finally:
            channel.close()
    
    def delete_message(self, message_id: str) -> Dict[str, Any]:
        """Delete a message"""
        if not self.user_id or not self.session_token:
            return {"success": False, "message": "Not logged in"}
        
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            response = stub.DeleteMessage(
                replication_pb2.DeleteMessageRequest(
                    user_id=self.user_id,
                    session_token=self.session_token,
                    message_id=message_id
                ),
                timeout=5.0
            )
            
            return {
                "success": response.success,
                "message": response.message
            }
            
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            return {"success": False, "message": str(e)}
        finally:
            channel.close()
    
    def mark_messages_read(self, other_user_id: str, message_ids: List[str] = None) -> Dict[str, Any]:
        """Mark messages as read"""
        if not self.user_id or not self.session_token:
            return {"success": False, "message": "Not logged in"}
        
        channel = self.get_leader_channel()
        if not channel:
            return {"success": False, "message": "No leader available"}
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            response = stub.MarkMessagesAsRead(
                replication_pb2.MarkMessagesAsReadRequest(
                    user_id=self.user_id,
                    session_token=self.session_token,
                    other_user_id=other_user_id,
                    message_ids=message_ids if message_ids else []
                ),
                timeout=5.0
            )
            
            return {
                "success": response.success,
                "message": response.message,
                "marked_count": response.marked_count if response.success else 0
            }
            
        except Exception as e:
            logger.error(f"Error marking messages as read: {e}")
            return {"success": False, "message": str(e)}
        finally:
            channel.close()
    
    def _start_update_subscriber(self):
        """Start a thread to subscribe to real-time updates"""
        if self.update_subscriber:
            self.update_subscriber.stop()
        
        self.update_subscriber = UpdateSubscriber(self)
        self.update_subscriber.start()


class UpdateSubscriber(QThread):
    """Background thread for subscribing to real-time updates"""
    
    def __init__(self, client: ChatClientAPI):
        super().__init__()
        self.client = client
        self.running = True
    
    def run(self):
        """Run the update subscriber thread"""
        while self.running:
            try:
                self._subscribe_to_updates()
            except Exception as e:
                logger.error(f"Error in update subscriber: {e}")
                # Wait a bit before reconnecting
                time.sleep(5)
    
    def _subscribe_to_updates(self):
        """Subscribe to real-time updates from the server"""
        if not self.client.user_id or not self.client.session_token:
            return
        
        channel = self.client.get_leader_channel()
        if not channel:
            return
        
        try:
            stub = replication_pb2_grpc.ChatServiceStub(channel)
            
            request = replication_pb2.SubscribeRequest(
                user_id=self.client.user_id,
                session_token=self.client.session_token
            )
            
            # Start streaming updates
            for update in stub.SubscribeToUpdates(request):
                if not self.running:
                    break
                
                # Process update based on type
                if update.type == replication_pb2.UpdateType.MESSAGE_RECEIVED:
                    # New message received
                    message_data = {
                        "message_id": update.message.message_id,
                        "sender_id": update.message.sender_id,
                        "recipient_id": update.message.recipient_id,
                        "content": update.message.content,
                        "timestamp": update.message.timestamp,
                        "read": update.message.read,
                        "deleted": update.message.deleted
                    }
                    self.client.messageReceived.emit(message_data)
                
                elif update.type == replication_pb2.UpdateType.USER_STATUS_CHANGED:
                    # User status changed
                    user_data = {
                        "user_id": update.user_info.user_id,
                        "username": update.user_info.username,
                        "display_name": update.user_info.display_name,
                        "status": update.user_info.status,
                        "last_active_timestamp": update.user_info.last_active_timestamp
                    }
                    self.client.userStatusChanged.emit(user_data)
                
                elif update.type == replication_pb2.UpdateType.MESSAGE_READ:
                    # Message marked as read
                    message_data = {
                        "message_id": update.message.message_id,
                        "sender_id": update.message.sender_id,
                        "recipient_id": update.message.recipient_id,
                        "read": True
                    }
                    self.client.messageRead.emit(message_data)
                
                elif update.type == replication_pb2.UpdateType.MESSAGE_DELETED:
                    # Message deleted
                    message_data = {
                        "message_id": update.message.message_id,
                        "sender_id": update.message.sender_id,
                        "recipient_id": update.message.recipient_id,
                        "deleted": True
                    }
                    self.client.messageDeleted.emit(message_data)
                
                # Update users list periodically
                self.client.get_users_list()
                
        except Exception as e:
            logger.error(f"Error in update subscriber: {e}")
            self.client.connectionError.emit(str(e))
        finally:
            channel.close()
    
    def stop(self):
        """Stop the update subscriber thread"""
        self.running = False
        self.wait()
