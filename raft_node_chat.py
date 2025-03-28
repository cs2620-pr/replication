import os
import json
import time
import uuid
import logging
import hashlib
import sqlite3
from sqlite3 import Connection
from typing import Dict, List, Set, Optional, Tuple, Any

from raft_node import RaftNode, NodeState
from generated import replication_pb2
from generated.replication_pb2 import UserStatus

class ChatRaftNode(RaftNode):
    """Extended RaftNode with chat application functionality in the state machine"""
    
    def __init__(self, node_id, config_path):
        super().__init__(node_id, config_path)
        # Initialize database
        self.db_path = os.path.join(self.data_dir, f"chat_node_{node_id}.db")
        self.initialize_database()
        
    def initialize_database(self):
        """Initialize SQLite database with required tables"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Create users table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_login INTEGER,
                status INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        
        # Create messages table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp BIGINT NOT NULL,
                delivered BOOLEAN NOT NULL DEFAULT FALSE,
                unread BOOLEAN NOT NULL DEFAULT TRUE,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                FOREIGN KEY (sender) REFERENCES users(username),
                FOREIGN KEY (recipient) REFERENCES users(username)
            )
            """
        )
        
        # Create sessions table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                FOREIGN KEY (username) REFERENCES users(username)
            )
            """
        )
        
        conn.commit()
        conn.close()
    
    def get_db_connection(self) -> Connection:
        """Get a connection to the SQLite database"""
        return sqlite3.connect(self.db_path)

    def apply_to_state_machine(self, command: Dict):
        """Apply a command to the state machine"""
        command_type = command.get('type', '')
        
        # First, handle the standard key-value store commands
        if command_type == 'set':
            key = command['key']
            value = command['value']
            
            with self.state_machine_lock:
                self.state_machine[key] = value
                self.save_persistent_state()
                
            self.logger.info(f"Applied SET command: {key}={value}")
            return
        
        elif command_type == 'noop':
            self.logger.debug("Applied no-op command")
            return
        
        # Handle chat-specific commands
        elif command_type == 'register_user':
            self._handle_register_user(command)
        
        elif command_type == 'login':
            self._handle_login(command)
        
        elif command_type == 'update_status':
            self._handle_update_status(command)
        
        elif command_type == 'get_users_list':
            self._handle_get_users_list(command)
        
        elif command_type == 'send_message':
            self._handle_send_message(command)
        
        elif command_type == 'get_messages':
            self._handle_get_messages(command)
        
        elif command_type == 'delete_message':
            self._handle_delete_message(command)
        
        elif command_type == 'mark_messages_read':
            self._handle_mark_messages_read(command)
        
        else:
            self.logger.warning(f"Unknown command type: {command_type}")
    
    def _store_command_result(self, command_id: str, result: Dict):
        """Store command result in the state machine"""
        with self.state_machine_lock:
            self.state_machine[f"result:{command_id}"] = json.dumps(result)
            self.save_persistent_state()
    
    def _get_chat_data(self, key: str, default: Any = None) -> Any:
        """Get chat data from the state machine"""
        with self.state_machine_lock:
            json_str = self.state_machine.get(f"chat:{key}")
            if json_str:
                return json.loads(json_str)
            return default
    
    def _set_chat_data(self, key: str, data: Any):
        """Set chat data in the state machine"""
        with self.state_machine_lock:
            self.state_machine[f"chat:{key}"] = json.dumps(data)
            self.save_persistent_state()
    
    def _handle_register_user(self, command: Dict):
        """Handle user registration"""
        user_id = command['user_id']
        username = command['username']
        password_hash = command['password_hash']
        display_name = command['display_name']
        timestamp = command['timestamp']
        command_id = command['command_id']
        
        # Get users dictionary
        users = self._get_chat_data('users', {})
        
        # Check if username already exists
        username_to_id = self._get_chat_data('username_to_id', {})
        
        result = {}
        
        if username in username_to_id:
            result = {
                'success': False,
                'message': f"Username '{username}' already exists"
            }
        else:
            # Create new user
            user_data = {
                'user_id': user_id,
                'username': username,
                'password_hash': password_hash,
                'display_name': display_name,
                'created_at': timestamp,
                'last_active_timestamp': timestamp,
                'status': UserStatus.OFFLINE
            }
            
            # Store user
            users[user_id] = user_data
            username_to_id[username] = user_id
            
            # Save to state machine
            self._set_chat_data('users', users)
            self._set_chat_data('username_to_id', username_to_id)
            
            result = {
                'success': True,
                'message': "User registered successfully"
            }
        
        # Store result
        self._store_command_result(command_id, result)
        
        self.logger.info(f"Applied REGISTER_USER command for {username}")
    
    def _handle_login(self, command: Dict):
        """Handle user login"""
        username = command['username']
        password_hash = command['password_hash']
        command_id = command['command_id']
        
        # Get username to ID mapping
        username_to_id = self._get_chat_data('username_to_id', {})
        
        result = {}
        
        if username not in username_to_id:
            result = {
                'success': False,
                'message': "Invalid username or password"
            }
        else:
            user_id = username_to_id[username]
            
            # Get user data
            users = self._get_chat_data('users', {})
            user_data = users.get(user_id, {})
            
            # Check password
            if user_data.get('password_hash') != password_hash:
                result = {
                    'success': False,
                    'message': "Invalid username or password"
                }
            else:
                # Update last active timestamp
                user_data['last_active_timestamp'] = command['timestamp']
                users[user_id] = user_data
                self._set_chat_data('users', users)
                
                result = {
                    'success': True,
                    'message': "Login successful",
                    'user_id': user_id
                }
        
        # Store result
        self._store_command_result(command_id, result)
        
        self.logger.info(f"Applied LOGIN command for {username}")
    
    def _handle_update_status(self, command: Dict):
        """Handle status update"""
        user_id = command['user_id']
        status = command['status']
        timestamp = command['timestamp']
        
        # Get user data
        users = self._get_chat_data('users', {})
        
        if user_id in users:
            # Update status
            users[user_id]['status'] = UserStatus(status)
            users[user_id]['last_active_timestamp'] = timestamp
            
            # Save to state machine
            self._set_chat_data('users', users)
            
            self.logger.info(f"Updated status for user {user_id} to {status}")
        else:
            self.logger.warning(f"Cannot update status: User {user_id} not found")
    
    def _handle_get_users_list(self, command: Dict):
        """Handle getting all users list"""
        requesting_user_id = command['user_id']
        command_id = command['command_id']
        
        # Get all users
        users = self._get_chat_data('users', {})
        
        # Get unread message counts
        messages = self._get_chat_data('messages', {})
        
        # Calculate unread counts for each user
        unread_counts = {}
        for msg_id, msg in messages.items():
            if msg['recipient_id'] == requesting_user_id and not msg['read'] and not msg['deleted']:
                sender_id = msg['sender_id']
                if sender_id not in unread_counts:
                    unread_counts[sender_id] = 0
                unread_counts[sender_id] += 1
        
        # Format user list
        user_list = []
        for user_id, user_data in users.items():
            # Skip the requesting user
            if user_id == requesting_user_id:
                continue
                
            user_info = {
                'user_id': user_id,
                'username': user_data['username'],
                'display_name': user_data['display_name'],
                'status': user_data['status'],
                'last_active_timestamp': user_data['last_active_timestamp'],
                'unread_message_count': unread_counts.get(user_id, 0)
            }
            user_list.append(user_info)
        
        # Store result
        result = {
            'success': True,
            'users': user_list
        }
        self._store_command_result(command_id, result)
        
        self.logger.info(f"Applied GET_USERS_LIST command for user {requesting_user_id}")
    
    def _handle_send_message(self, command: Dict):
        """Handle sending a message"""
        message_id = command['message_id']
        sender_id = command['sender_id']
        recipient_id = command['recipient_id']
        content = command['content']
        timestamp = command['timestamp']
        
        # Get messages
        messages = self._get_chat_data('messages', {})
        
        # Store the message
        message_data = {
            'message_id': message_id,
            'sender_id': sender_id,
            'recipient_id': recipient_id,
            'content': content,
            'timestamp': timestamp,
            'read': False,
            'deleted': False
        }
        
        messages[message_id] = message_data
        
        # Save to state machine
        self._set_chat_data('messages', messages)
        
        # We could also update conversation metadata here if needed
        
        self.logger.info(f"Applied SEND_MESSAGE command: {sender_id} -> {recipient_id}")
    
    def _handle_get_messages(self, command: Dict):
        """Handle getting messages between two users"""
        user_id = command['user_id']
        other_user_id = command['other_user_id']
        limit = command['limit']
        before_timestamp = command['before_timestamp']
        command_id = command['command_id']
        
        # Get all messages
        all_messages = self._get_chat_data('messages', {})
        
        # Filter messages between these two users
        relevant_messages = []
        for msg_id, msg in all_messages.items():
            # Message must be between the two users
            is_relevant = (
                (msg['sender_id'] == user_id and msg['recipient_id'] == other_user_id) or
                (msg['sender_id'] == other_user_id and msg['recipient_id'] == user_id)
            )
            
            # Message must be before the specified timestamp
            is_before_timestamp = msg['timestamp'] < before_timestamp
            
            # Include only non-deleted messages or ones sent by the user (for UI continuity)
            not_deleted_or_own = not msg['deleted'] or msg['sender_id'] == user_id
            
            if is_relevant and is_before_timestamp and not_deleted_or_own:
                relevant_messages.append(msg)
        
        # Sort by timestamp descending
        relevant_messages.sort(key=lambda m: m['timestamp'], reverse=True)
        
        # Apply limit
        if limit > 0:
            relevant_messages = relevant_messages[:limit]
        
        # Store result
        result = {
            'success': True,
            'messages': relevant_messages
        }
        self._store_command_result(command_id, result)
        
        self.logger.info(f"Applied GET_MESSAGES command between {user_id} and {other_user_id}")
    
    def _handle_delete_message(self, command: Dict):
        """Handle deleting a message"""
        user_id = command['user_id']
        message_id = command['message_id']
        command_id = command['command_id']
        
        # Get messages
        messages = self._get_chat_data('messages', {})
        
        result = {}
        message_details = None
        
        if message_id not in messages:
            result = {
                'success': False,
                'message': "Message not found"
            }
        else:
            message = messages[message_id]
            
            # Only the sender can delete their own message
            if message['sender_id'] != user_id:
                result = {
                    'success': False,
                    'message': "You can only delete your own messages"
                }
            else:
                # Mark as deleted
                message['deleted'] = True
                messages[message_id] = message
                
                # Save message details for event broadcast
                message_details = message.copy()
                
                # Save to state machine
                self._set_chat_data('messages', messages)
                
                result = {
                    'success': True,
                    'message': "Message deleted successfully",
                    'message_details': message_details
                }
        
        # Store result
        self._store_command_result(command_id, result)
        
        self.logger.info(f"Applied DELETE_MESSAGE command for message {message_id}")
    
    def _handle_mark_messages_read(self, command: Dict):
        """Handle marking messages as read"""
        user_id = command['user_id']
        other_user_id = command['other_user_id']
        message_ids = command.get('message_ids', [])
        command_id = command['command_id']
        
        # Get messages
        messages = self._get_chat_data('messages', {})
        
        marked_count = 0
        marked_messages = []
        
        # If specific message IDs provided
        if message_ids:
            for msg_id in message_ids:
                if msg_id in messages:
                    msg = messages[msg_id]
                    
                    # Only mark messages from the other user
                    if msg['sender_id'] == other_user_id and msg['recipient_id'] == user_id and not msg['read']:
                        msg['read'] = True
                        messages[msg_id] = msg
                        marked_count += 1
                        marked_messages.append(msg)
        
        # Otherwise mark all unread messages from the other user
        else:
            for msg_id, msg in messages.items():
                if msg['sender_id'] == other_user_id and msg['recipient_id'] == user_id and not msg['read']:
                    msg['read'] = True
                    messages[msg_id] = msg
                    marked_count += 1
                    marked_messages.append(msg)
        
        # Save to state machine if any changes
        if marked_count > 0:
            self._set_chat_data('messages', messages)
        
        # Store result
        result = {
            'success': True,
            'marked_count': marked_count,
            'marked_messages': marked_messages
        }
        self._store_command_result(command_id, result)
        
        self.logger.info(f"Applied MARK_MESSAGES_READ command, marked {marked_count} messages as read")
