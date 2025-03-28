import os
import time
import uuid
import json
import logging
import threading
import hashlib
import secrets
from typing import Dict, List, Set, Optional, Tuple

import grpc

from generated import replication_pb2
from generated import replication_pb2_grpc
from generated.replication_pb2 import UpdateEvent, UpdateType, UserStatus, ChatMessage, UserInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ChatService(replication_pb2_grpc.ChatServiceServicer):
    def __init__(self, raft_node):
        """Initialize the chat service with a reference to the Raft node"""
        self.raft_node = raft_node
        self.logger = logging.getLogger("ChatService")
        
        # Active sessions - maps session_token to user_id and expiry time
        self.active_sessions = {}
        self.session_lock = threading.RLock()
        
        # Active subscribers - maps user_id to list of active streams
        self.subscribers = {}
        self.subscribers_lock = threading.RLock()
        
        # Session cleanup thread
        self.session_cleanup_thread = threading.Thread(target=self._cleanup_expired_sessions)
        self.session_cleanup_thread.daemon = True
        self.session_cleanup_thread.start()
        
        self.logger.info("Chat service initialized")
    
    def _cleanup_expired_sessions(self):
        """Periodically clean up expired sessions"""
        while True:
            time.sleep(60)  # Check every minute
            
            with self.session_lock:
                current_time = time.time()
                expired_tokens = [token for token, (_, expiry) in self.active_sessions.items() 
                                 if current_time > expiry]
                
                for token in expired_tokens:
                    self.logger.info(f"Session {token[:8]}... expired and removed")
                    del self.active_sessions[token]
    
    def _authenticate(self, user_id: str, session_token: str) -> bool:
        """Authenticate a user with their session token"""
        with self.session_lock:
            if session_token in self.active_sessions:
                stored_user_id, expiry = self.active_sessions[session_token]
                
                if time.time() > expiry:
                    # Session expired
                    del self.active_sessions[session_token]
                    return False
                
                if stored_user_id == user_id:
                    # Update expiry time - extend session by 1 hour
                    self.active_sessions[session_token] = (user_id, time.time() + 3600)
                    return True
            
            return False

    def _process_command(self, command: Dict) -> Tuple[bool, str, Optional[Dict]]:
        """Process a command by appending it to the Raft log"""
        result = self.raft_node.append_entry(command)
        
        if not result:
            return False, "Failed to process command", None
        
        # Wait for the command to be committed and applied
        command_index = len(self.raft_node.log) - 1
        attempts = 0
        max_attempts = 10
        result_data = None
        
        while attempts < max_attempts:
            # Check if command has been applied
            if self.raft_node.last_applied >= command_index:
                # Get the result from the state machine if available
                result_key = f"result:{command['command_id']}"
                with self.raft_node.state_machine_lock:
                    if result_key in self.raft_node.state_machine:
                        result_data = json.loads(self.raft_node.state_machine[result_key])
                        # Clean up result data to avoid state machine bloat
                        del self.raft_node.state_machine[result_key]
                        self.raft_node.save_persistent_state()
                        return True, "Command processed successfully", result_data
                    else:
                        # Command was applied but no result was stored
                        return True, "Command processed without specific result", None
            
            time.sleep(0.1)  # Wait 100ms
            attempts += 1
        
        # Command was committed but we timed out waiting for application
        return True, "Command committed but may not be fully applied yet", None

    def _hash_password(self, password: str) -> str:
        """Hash a password for secure storage"""
        # In a real application, you would use a more secure method like bcrypt
        return hashlib.sha256(password.encode()).hexdigest()
    
    def _generate_session_token(self) -> str:
        """Generate a secure random session token"""
        return secrets.token_hex(32)
    
    def _generate_id(self) -> str:
        """Generate a unique ID"""
        return str(uuid.uuid4())

    def _broadcast_update(self, event: UpdateEvent, exclude_user_id: Optional[str] = None):
        """Broadcast an update event to all subscribers except the excluded user"""
        with self.subscribers_lock:
            for user_id, streams in list(self.subscribers.items()):
                if user_id == exclude_user_id:
                    continue
                
                for stream in list(streams):
                    try:
                        stream.put(event)
                    except Exception as e:
                        self.logger.error(f"Error sending update to {user_id}: {e}")
                        # Remove failed stream
                        if stream in streams:
                            streams.remove(stream)
                
                # Clean up empty subscriber entries
                if not streams:
                    del self.subscribers[user_id]
    
    def RegisterUser(self, request, context):
        """Register a new user"""
        username = request.username
        password = request.password  # Should already be hashed by client
        display_name = request.display_name or username
        
        # Create a unique command ID
        command_id = self._generate_id()
        
        # Create a unique user ID
        user_id = self._generate_id()
        
        # Hash password for storage
        hashed_password = self._hash_password(password)
        
        # Create command for the state machine
        command = {
            'type': 'register_user',
            'command_id': command_id,
            'user_id': user_id,
            'username': username,
            'password_hash': hashed_password,
            'display_name': display_name,
            'timestamp': int(time.time())
        }
        
        # Process command through Raft consensus
        success, message, result = self._process_command(command)
        
        if not success:
            return replication_pb2.RegisterUserResponse(
                success=False,
                message=f"Failed to register user: {message}"
            )
        
        if result and not result.get('success', False):
            return replication_pb2.RegisterUserResponse(
                success=False,
                message=result.get('message', "Registration failed")
            )
        
        return replication_pb2.RegisterUserResponse(
            success=True,
            message="User registered successfully",
            user_id=user_id
        )
    
    def Login(self, request, context):
        """Login a user and create a session"""
        username = request.username
        password = request.password  # Should already be hashed by client
        
        # Hash password for comparison
        hashed_password = self._hash_password(password)
        
        # Create command for the state machine
        command_id = self._generate_id()
        command = {
            'type': 'login',
            'command_id': command_id,
            'username': username,
            'password_hash': hashed_password,
            'timestamp': int(time.time())
        }
        
        # Process command through Raft consensus
        success, message, result = self._process_command(command)
        
        if not success or not result:
            return replication_pb2.LoginResponse(
                success=False,
                message=f"Login failed: {message}"
            )
        
        if not result.get('success', False):
            return replication_pb2.LoginResponse(
                success=False,
                message=result.get('message', "Invalid credentials")
            )
        
        user_id = result.get('user_id')
        
        # Create a new session token
        session_token = self._generate_session_token()
        
        # Store session with expiry (1 hour)
        with self.session_lock:
            self.active_sessions[session_token] = (user_id, time.time() + 3600)
        
        # Update user status to online
        status_command = {
            'type': 'update_status',
            'command_id': self._generate_id(),
            'user_id': user_id,
            'status': UserStatus.ONLINE,
            'timestamp': int(time.time())
        }
        
        # Process status update through Raft consensus
        self._process_command(status_command)
        
        return replication_pb2.LoginResponse(
            success=True,
            message="Login successful",
            user_id=user_id,
            session_token=session_token
        )
    
    def UpdateStatus(self, request, context):
        """Update a user's status"""
        user_id = request.user_id
        session_token = request.session_token
        status = request.status
        
        # Authenticate user
        if not self._authenticate(user_id, session_token):
            return replication_pb2.UpdateStatusResponse(
                success=False,
                message="Authentication failed"
            )
        
        # Create command for the state machine
        command = {
            'type': 'update_status',
            'command_id': self._generate_id(),
            'user_id': user_id,
            'status': status,
            'timestamp': int(time.time())
        }
        
        # Process command through Raft consensus
        success, message, _ = self._process_command(command)
        
        if not success:
            return replication_pb2.UpdateStatusResponse(
                success=False,
                message=f"Failed to update status: {message}"
            )
        
        return replication_pb2.UpdateStatusResponse(
            success=True,
            message="Status updated successfully"
        )
    
    def GetUsersList(self, request, context):
        """Get a list of all users and their status"""
        user_id = request.user_id
        session_token = request.session_token
        
        # Authenticate user
        if not self._authenticate(user_id, session_token):
            return replication_pb2.GetUsersListResponse(
                success=False,
                message="Authentication failed"
            )
        
        # Create command to retrieve user list
        command = {
            'type': 'get_users_list',
            'command_id': self._generate_id(),
            'user_id': user_id,
            'timestamp': int(time.time())
        }
        
        # Process command through Raft consensus
        success, message, result = self._process_command(command)
        
        if not success or not result:
            return replication_pb2.GetUsersListResponse(
                success=False,
                message=f"Failed to get users list: {message}"
            )
        
        # Build list of UserInfo messages
        user_info_list = []
        for user in result.get('users', []):
            user_info = UserInfo(
                user_id=user['user_id'],
                username=user['username'],
                display_name=user['display_name'],
                status=user['status'],
                last_active_timestamp=user['last_active_timestamp'],
                unread_message_count=user['unread_message_count']
            )
            user_info_list.append(user_info)
        
        return replication_pb2.GetUsersListResponse(
            success=True,
            message="Users list retrieved successfully",
            users=user_info_list
        )
    
    def SendMessage(self, request, context):
        """Send a message to another user"""
        user_id = request.user_id
        session_token = request.session_token
        recipient_id = request.recipient_id
        content = request.content
        
        # Authenticate user
        if not self._authenticate(user_id, session_token):
            return replication_pb2.SendMessageResponse(
                success=False,
                message="Authentication failed"
            )
        
        # Generate a message ID
        message_id = self._generate_id()
        timestamp = int(time.time())
        
        # Create command for the state machine
        command = {
            'type': 'send_message',
            'command_id': self._generate_id(),
            'message_id': message_id,
            'sender_id': user_id,
            'recipient_id': recipient_id,
            'content': content,
            'timestamp': timestamp,
            'read': False,
            'deleted': False
        }
        
        # Process command through Raft consensus
        success, message, result = self._process_command(command)
        
        if not success:
            return replication_pb2.SendMessageResponse(
                success=False,
                message=f"Failed to send message: {message}"
            )
        
        # Create chat message to return
        chat_message = ChatMessage(
            message_id=message_id,
            sender_id=user_id,
            recipient_id=recipient_id,
            content=content,
            timestamp=timestamp,
            read=False,
            deleted=False
        )
        
        # Broadcast message to recipient if they're subscribed
        update_event = UpdateEvent(
            type=UpdateType.MESSAGE_RECEIVED,
            initiator_id=user_id,
            message=chat_message
        )
        
        self._broadcast_update(update_event, exclude_user_id=user_id)
        
        return replication_pb2.SendMessageResponse(
            success=True,
            message="Message sent successfully",
            sent_message=chat_message
        )
    
    def GetMessages(self, request, context):
        """Get messages between the current user and another user"""
        user_id = request.user_id
        session_token = request.session_token
        other_user_id = request.other_user_id
        limit = request.limit or 50  # Default to 50 messages
        before_timestamp = request.before_timestamp or int(time.time() * 1000)
        
        # Authenticate user
        if not self._authenticate(user_id, session_token):
            return replication_pb2.GetMessagesResponse(
                success=False,
                message="Authentication failed"
            )
        
        # Create command for the state machine
        command = {
            'type': 'get_messages',
            'command_id': self._generate_id(),
            'user_id': user_id,
            'other_user_id': other_user_id,
            'limit': limit,
            'before_timestamp': before_timestamp,
            'timestamp': int(time.time())
        }
        
        # Process command through Raft consensus
        success, message, result = self._process_command(command)
        
        if not success or not result:
            return replication_pb2.GetMessagesResponse(
                success=False,
                message=f"Failed to get messages: {message}"
            )
        
        # Create chat messages list
        chat_messages = []
        for msg in result.get('messages', []):
            chat_message = ChatMessage(
                message_id=msg['message_id'],
                sender_id=msg['sender_id'],
                recipient_id=msg['recipient_id'],
                content=msg['content'],
                timestamp=msg['timestamp'],
                read=msg['read'],
                deleted=msg['deleted']
            )
            chat_messages.append(chat_message)
        
        return replication_pb2.GetMessagesResponse(
            success=True,
            message="Messages retrieved successfully",
            messages=chat_messages
        )
    
    def DeleteMessage(self, request, context):
        """Delete a message sent by the user"""
        user_id = request.user_id
        session_token = request.session_token
        message_id = request.message_id
        
        # Authenticate user
        if not self._authenticate(user_id, session_token):
            return replication_pb2.DeleteMessageResponse(
                success=False,
                message="Authentication failed"
            )
        
        # Create command for the state machine
        command = {
            'type': 'delete_message',
            'command_id': self._generate_id(),
            'user_id': user_id,
            'message_id': message_id,
            'timestamp': int(time.time())
        }
        
        # Process command through Raft consensus
        success, message, result = self._process_command(command)
        
        if not success:
            return replication_pb2.DeleteMessageResponse(
                success=False,
                message=f"Failed to delete message: {message}"
            )
        
        if result and not result.get('success', False):
            return replication_pb2.DeleteMessageResponse(
                success=False,
                message=result.get('message', "Failed to delete message")
            )
        
        # If there's message details in the result, broadcast deletion
        if result and result.get('message_details'):
            msg_details = result['message_details']
            
            # Create chat message for the update event
            chat_message = ChatMessage(
                message_id=message_id,
                sender_id=msg_details['sender_id'],
                recipient_id=msg_details['recipient_id'],
                content=msg_details['content'],
                timestamp=msg_details['timestamp'],
                read=msg_details['read'],
                deleted=True  # Now deleted
            )
            
            # Create update event
            update_event = UpdateEvent(
                type=UpdateType.MESSAGE_DELETED,
                initiator_id=user_id,
                message=chat_message
            )
            
            # Broadcast to all subscribers
            self._broadcast_update(update_event)
        
        return replication_pb2.DeleteMessageResponse(
            success=True,
            message="Message deleted successfully"
        )
    
    def MarkMessagesAsRead(self, request, context):
        """Mark messages as read"""
        user_id = request.user_id
        session_token = request.session_token
        other_user_id = request.other_user_id
        message_ids = list(request.message_ids) if request.message_ids else []
        
        # Authenticate user
        if not self._authenticate(user_id, session_token):
            return replication_pb2.MarkMessagesAsReadResponse(
                success=False,
                message="Authentication failed"
            )
        
        # Create command for the state machine
        command = {
            'type': 'mark_messages_read',
            'command_id': self._generate_id(),
            'user_id': user_id,
            'other_user_id': other_user_id,
            'message_ids': message_ids,
            'timestamp': int(time.time())
        }
        
        # Process command through Raft consensus
        success, message, result = self._process_command(command)
        
        if not success:
            return replication_pb2.MarkMessagesAsReadResponse(
                success=False,
                message=f"Failed to mark messages as read: {message}"
            )
        
        marked_count = result.get('marked_count', 0) if result else 0
        
        # If there are marked messages, broadcast updates
        if result and result.get('marked_messages'):
            for msg_data in result['marked_messages']:
                # Create chat message for the update event
                chat_message = ChatMessage(
                    message_id=msg_data['message_id'],
                    sender_id=msg_data['sender_id'],
                    recipient_id=msg_data['recipient_id'],
                    content=msg_data['content'],
                    timestamp=msg_data['timestamp'],
                    read=True,  # Now read
                    deleted=msg_data['deleted']
                )
                
                # Create update event
                update_event = UpdateEvent(
                    type=UpdateType.MESSAGE_READ,
                    initiator_id=user_id,
                    message=chat_message
                )
                
                # Broadcast to sender
                self._broadcast_update(update_event, exclude_user_id=user_id)
        
        return replication_pb2.MarkMessagesAsReadResponse(
            success=True,
            message="Messages marked as read successfully",
            marked_count=marked_count
        )
    
    def SubscribeToUpdates(self, request, context):
        """Subscribe to real-time updates"""
        user_id = request.user_id
        session_token = request.session_token
        
        # Authenticate user
        if not self._authenticate(user_id, session_token):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Authentication failed")
        
        # Create a queue for this subscriber
        queue = _AsyncQueue()
        
        # Register subscriber
        with self.subscribers_lock:
            if user_id not in self.subscribers:
                self.subscribers[user_id] = set()
            self.subscribers[user_id].add(queue)
        
        # Set up cleanup when the connection is closed
        def on_rpc_done():
            with self.subscribers_lock:
                if user_id in self.subscribers:
                    if queue in self.subscribers[user_id]:
                        self.subscribers[user_id].remove(queue)
                    if not self.subscribers[user_id]:
                        del self.subscribers[user_id]
            
            # Also update user status to offline when they disconnect
            status_command = {
                'type': 'update_status',
                'command_id': self._generate_id(),
                'user_id': user_id,
                'status': UserStatus.OFFLINE,
                'timestamp': int(time.time())
            }
            
            # Process status update through Raft consensus
            self._process_command(status_command)
        
        context.add_callback(on_rpc_done)
        
        # Yield updates as they come in
        while context.is_active():
            try:
                event = queue.get(timeout=3600)  # 1 hour timeout
                yield event
            except _QueueTimeout:
                # Send a keep-alive event
                continue
            except Exception as e:
                self.logger.error(f"Error in subscription stream for {user_id}: {e}")
                break


class _QueueTimeout(Exception):
    pass


class _AsyncQueue:
    """A simple async queue for streaming updates to clients"""
    
    def __init__(self):
        self.queue = []
        self.cv = threading.Condition()
    
    def put(self, item):
        with self.cv:
            self.queue.append(item)
            self.cv.notify()
    
    def get(self, timeout=None):
        with self.cv:
            if not self.queue:
                if not self.cv.wait(timeout=timeout):
                    raise _QueueTimeout()
            
            if not self.queue:
                raise _QueueTimeout()
            
            return self.queue.pop(0)
