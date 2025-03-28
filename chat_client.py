#!/usr/bin/env python3
import os
import sys
import json
import time
import uuid
import logging
import argparse
import threading
from datetime import datetime

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QMessageBox, QListWidget,
    QStackedWidget, QTextEdit, QSplitter, QFrame, QDialog,
    QFormLayout, QDialogButtonBox, QListWidgetItem, QScrollArea
)
from PyQt6.QtCore import Qt, QSize, QTimer, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QColor, QIcon

import grpc
from generated import replication_pb2 as pb2
from generated import replication_pb2_grpc as pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ChatClient')

class ChatClientBackend(QObject):
    """Backend for handling gRPC communication with the server"""
    # Define signals to notify the UI of changes
    login_status_changed = pyqtSignal(bool, str)
    message_received = pyqtSignal(str, str, str, int)
    users_updated = pyqtSignal(list)
    online_users_updated = pyqtSignal(list)
    unread_counts_updated = pyqtSignal(dict)
    message_deleted = pyqtSignal(str)
    
    def __init__(self, config_path):
        super().__init__()
        self.config_path = config_path
        self.config = self.load_config()
        self.nodes = self.config.get('nodes', [])
        self.leader_addr = None
        self.session_token = None
        self.user_id = None
        self.username = None
        self.users_cache = {}  # Map of user_id to user data
        self.is_connected = False
        self.should_poll = False
        self.poll_thread = None
        self.load_config()
        
    def load_config(self):
        """Load the server configuration"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"nodes": []}
    
    def get_channel(self):
        """Get a gRPC channel to a server"""
        # If we have a leader, try that first
        if self.leader_addr:
            try:
                channel = grpc.insecure_channel(self.leader_addr)
                return channel
            except Exception:
                self.leader_addr = None
        
        # Try random nodes until one works
        for node in self.nodes:
            host, port = node.get('host'), node.get('port')
            addr = f"{host}:{port}"
            try:
                channel = grpc.insecure_channel(addr)
                return channel
            except Exception:
                continue
        
        raise ConnectionError("Could not connect to any server node")
    
    def register(self, username, password, display_name=None):
        """Register a new user account"""
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            if not display_name:
                display_name = username
                
            request = pb2.RegisterRequest(
                username=username,
                password=password,
                display_name=display_name
            )
            
            response = stub.Register(request, timeout=5.0)
            
            if response.success:
                logger.info(f"Registration successful for {username}")
                self.login_status_changed.emit(True, response.message)
                return True, response.message
            else:
                logger.warning(f"Registration failed: {response.message}")
                self.login_status_changed.emit(False, response.message)
                return False, response.message
                
        except grpc.RpcError as e:
            error_message = f"RPC error: {e.code()}: {e.details()}"
            logger.error(error_message)
            self.login_status_changed.emit(False, error_message)
            return False, error_message
            
        except Exception as e:
            error_message = f"Error: {str(e)}"
            logger.error(error_message)
            self.login_status_changed.emit(False, error_message)
            return False, error_message
    
    def login(self, username, password):
        """Log in to the server"""
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.LoginRequest(
                username=username,
                password=password
            )
            
            response = stub.Login(request, timeout=5.0)
            
            if response.success:
                self.session_token = response.session_token
                self.user_id = response.user_id
                self.username = username
                self.is_connected = True
                
                # Start polling for updates
                self.should_poll = True
                self.poll_thread = threading.Thread(target=self.poll_updates)
                self.poll_thread.daemon = True
                self.poll_thread.start()
                
                logger.info(f"Login successful for {username}")
                self.login_status_changed.emit(True, "Login successful")
                return True, "Login successful"
            else:
                logger.warning(f"Login failed: {response.message}")
                self.login_status_changed.emit(False, response.message)
                return False, response.message
                
        except grpc.RpcError as e:
            error_message = f"RPC error: {e.code()}: {e.details()}"
            logger.error(error_message)
            self.login_status_changed.emit(False, error_message)
            return False, error_message
            
        except Exception as e:
            error_message = f"Error: {str(e)}"
            logger.error(error_message)
            self.login_status_changed.emit(False, error_message)
            return False, error_message
    
    def logout(self):
        """Log out from the server"""
        if not self.session_token:
            logger.warning("Not logged in")
            return True, "Not logged in"
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.LogoutRequest(
                session_token=self.session_token
            )
            
            response = stub.Logout(request, timeout=5.0)
            
            # Stop polling regardless of response
            self.should_poll = False
            if self.poll_thread:
                self.poll_thread.join(timeout=1.0)
            
            self.session_token = None
            self.user_id = None
            self.username = None
            self.is_connected = False
            
            if response.success:
                logger.info("Logged out successfully")
                self.login_status_changed.emit(False, "Logged out")
                return True, "Logged out successfully"
            else:
                logger.warning(f"Logout failed: {response.message}")
                self.login_status_changed.emit(False, response.message)
                return False, response.message
                
        except Exception as e:
            logger.error(f"Error during logout: {e}")
            self.session_token = None
            self.user_id = None
            self.is_connected = False
            self.login_status_changed.emit(False, str(e))
            return False, str(e)
    
    def delete_account(self, password):
        """Delete user account"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in"
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.DeleteAccountRequest(
                session_token=self.session_token,
                password=password
            )
            
            response = stub.DeleteAccount(request, timeout=5.0)
            
            if response.success:
                logger.info("Account deleted successfully")
                # Log out after account deletion
                self.should_poll = False
                if self.poll_thread:
                    self.poll_thread.join(timeout=1.0)
                
                self.session_token = None
                self.user_id = None
                self.username = None
                self.is_connected = False
                
                self.login_status_changed.emit(False, "Account deleted")
                return True, "Account deleted successfully"
            else:
                logger.warning(f"Account deletion failed: {response.message}")
                return False, response.message
                
        except Exception as e:
            logger.error(f"Error during account deletion: {e}")
            return False, str(e)
    
    def send_message(self, receiver_id, content):
        """Send a message to another user"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in", None
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.SendMessageRequest(
                session_token=self.session_token,
                receiver_id=receiver_id,
                content=content
            )
            
            response = stub.SendMessage(request, timeout=5.0)
            
            if response.success:
                logger.info(f"Message sent successfully, ID: {response.message_id}")
                # Emit a signal with our own message for immediate display
                self.message_received.emit(
                    response.message_id,
                    self.user_id,
                    content,
                    int(time.time())
                )
                return True, "Message sent", response.message_id
            else:
                logger.warning(f"Failed to send message: {response.message}")
                return False, response.message, None
                
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False, str(e), None
    
    def get_messages(self, other_user_id, limit=50, before_timestamp=None):
        """Get messages between the current user and another user"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in", []
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            if before_timestamp is None:
                before_timestamp = int(time.time() * 1000)
            
            request = pb2.GetMessagesRequest(
                session_token=self.session_token,
                other_user_id=other_user_id,
                limit=limit,
                before_timestamp=before_timestamp
            )
            
            response = stub.GetMessages(request, timeout=5.0)
            
            if response.success:
                messages = []
                for msg in response.messages:
                    messages.append({
                        'message_id': msg.message_id,
                        'sender_id': msg.sender_id,
                        'receiver_id': msg.receiver_id,
                        'content': msg.content,
                        'timestamp': msg.timestamp,
                        'read': msg.read
                    })
                logger.info(f"Retrieved {len(messages)} messages")
                return True, "Messages retrieved", messages
            else:
                logger.warning(f"Failed to retrieve messages: {response.message}")
                return False, response.message, []
                
        except Exception as e:
            logger.error(f"Error retrieving messages: {e}")
            return False, str(e), []
    
    def delete_message(self, message_id):
        """Delete a message"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in"
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.DeleteMessageRequest(
                session_token=self.session_token,
                message_id=message_id
            )
            
            response = stub.DeleteMessage(request, timeout=5.0)
            
            if response.success:
                logger.info(f"Message {message_id} deleted successfully")
                self.message_deleted.emit(message_id)
                return True, "Message deleted"
            else:
                logger.warning(f"Failed to delete message: {response.message}")
                return False, response.message
                
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            return False, str(e)
    
    def mark_message_read(self, message_id):
        """Mark a message as read"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in"
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.MarkMessageReadRequest(
                session_token=self.session_token,
                message_id=message_id
            )
            
            response = stub.MarkMessageRead(request, timeout=5.0)
            
            if response.success:
                logger.info(f"Message {message_id} marked as read")
                return True, "Message marked as read"
            else:
                logger.warning(f"Failed to mark message as read: {response.message}")
                return False, response.message
                
        except Exception as e:
            logger.error(f"Error marking message as read: {e}")
            return False, str(e)
    
    def get_all_users(self):
        """Get all users"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in", []
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.GetAllUsersRequest(
                session_token=self.session_token
            )
            
            response = stub.GetAllUsers(request, timeout=5.0)
            
            if response.success:
                users = []
                for user in response.users:
                    self.users_cache[user.user_id] = {
                        'user_id': user.user_id,
                        'username': user.username,
                        'display_name': user.display_name,
                        'online': user.online
                    }
                    
                    users.append({
                        'user_id': user.user_id,
                        'username': user.username,
                        'display_name': user.display_name,
                        'online': user.online
                    })
                
                logger.info(f"Retrieved {len(users)} users")
                self.users_updated.emit(users)
                return True, "Users retrieved", users
            else:
                logger.warning(f"Failed to retrieve users: {response.message}")
                return False, response.message, []
                
        except Exception as e:
            logger.error(f"Error retrieving users: {e}")
            return False, str(e), []
    
    def get_online_users(self):
        """Get online users"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in", []
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.GetOnlineUsersRequest(
                session_token=self.session_token
            )
            
            response = stub.GetOnlineUsers(request, timeout=5.0)
            
            if response.success:
                users = []
                for user in response.users:
                    # Update cache
                    self.users_cache[user.user_id] = {
                        'user_id': user.user_id,
                        'username': user.username,
                        'display_name': user.display_name,
                        'online': True
                    }
                    
                    users.append({
                        'user_id': user.user_id,
                        'username': user.username,
                        'display_name': user.display_name
                    })
                
                logger.info(f"Retrieved {len(users)} online users")
                self.online_users_updated.emit(users)
                return True, "Online users retrieved", users
            else:
                logger.warning(f"Failed to retrieve online users: {response.message}")
                return False, response.message, []
                
        except Exception as e:
            logger.error(f"Error retrieving online users: {e}")
            return False, str(e), []
    
    def get_unread_counts(self):
        """Get unread message counts by user"""
        if not self.session_token:
            logger.warning("Not logged in")
            return False, "Not logged in", {}
        
        try:
            channel = self.get_channel()
            stub = pb2_grpc.MessageServiceStub(channel)
            
            request = pb2.GetUnreadCountsRequest(
                session_token=self.session_token
            )
            
            response = stub.GetUnreadCounts(request, timeout=5.0)
            
            if response.success:
                unread_counts = {}
                for uc in response.unread_counts:
                    unread_counts[uc.user_id] = uc.count
                
                logger.info(f"Retrieved unread counts for {len(unread_counts)} users")
                self.unread_counts_updated.emit(unread_counts)
                return True, "Unread counts retrieved", unread_counts
            else:
                logger.warning(f"Failed to retrieve unread counts: {response.message}")
                return False, response.message, {}
                
        except Exception as e:
            logger.error(f"Error retrieving unread counts: {e}")
            return False, str(e), {}
    
    def poll_updates(self):
        """Periodically poll for updates (users, messages, etc.)"""
        while self.should_poll:
            try:
                # Get online users
                self.get_online_users()
                
                # Get unread counts
                self.get_unread_counts()
                
                # Sleep for a bit
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error during polling: {e}")
                time.sleep(10)  # Wait a bit longer on error
    
    def get_user_display_name(self, user_id):
        """Get a user's display name from cache or fetch it"""
        if user_id in self.users_cache:
            return self.users_cache[user_id].get('display_name', 'Unknown')
        else:
            # Try to refresh users cache
            success, _, _ = self.get_all_users()
            if success and user_id in self.users_cache:
                return self.users_cache[user_id].get('display_name', 'Unknown')
            return 'Unknown'

# Main client application UI will be in another file to keep this more manageable 