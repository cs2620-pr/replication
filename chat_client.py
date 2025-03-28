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
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QLineEdit,
    QMessageBox,
    QListWidget,
    QStackedWidget,
    QTextEdit,
    QSplitter,
    QFrame,
    QDialog,
    QFormLayout,
    QDialogButtonBox,
    QListWidgetItem,
    QScrollArea,
)
from PyQt6.QtCore import Qt, QSize, QTimer, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QColor, QIcon

import grpc
from generated import replication_pb2 as pb2
from generated import replication_pb2_grpc as pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ChatClient")


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
        self.nodes = self.config.get("nodes", [])
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
            with open(self.config_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"nodes": []}

    def get_channel(self):
        """Get a gRPC channel to a server"""
        # This method is kept for backward compatibility but won't be used for RPC calls
        # If we have a leader, try that first
        if self.leader_addr:
            try:
                channel = grpc.insecure_channel(self.leader_addr)
                return channel
            except Exception:
                self.leader_addr = None

        # Try random nodes until one works
        for node in self.nodes:
            host, port = node.get("host"), node.get("port")
            addr = f"{host}:{port}"
            try:
                channel = grpc.insecure_channel(addr)
                return channel
            except Exception:
                continue

        raise ConnectionError("Could not connect to any server node")

    def execute_rpc(
        self, rpc_function, request, timeout=5.0, retry_on_invalid_session=True
    ):
        """Execute an RPC function with failover to other servers if the primary fails"""
        # Keep track of tried addresses to avoid infinite loops
        tried_addresses = set()
        last_exception = None
        invalid_session_detected = False
        server_unavailable = False

        # Construct a list of all node addresses from the config
        all_addresses = []
        for node in self.nodes:
            host, port = node.get("host"), node.get("port")
            addr = f"{host}:{port}"
            all_addresses.append(addr)

        # If we have a leader, try it first, then try all other addresses
        if self.leader_addr and self.leader_addr in all_addresses:
            addresses_to_try = [self.leader_addr] + [
                addr for addr in all_addresses if addr != self.leader_addr
            ]
            logger.info(
                f"Starting with leader {self.leader_addr}, will try {len(addresses_to_try)} servers"
            )
        else:
            addresses_to_try = all_addresses
            logger.info(
                f"No leader known, will try all {len(addresses_to_try)} servers"
            )

        # Try each address one by one
        for addr in addresses_to_try:
            # Skip already tried addresses (though this shouldn't happen with our ordering)
            if addr in tried_addresses:
                continue

            tried_addresses.add(addr)
            logger.info(f"Trying server at {addr}...")

            try:
                # Create a new channel for this server
                channel = grpc.insecure_channel(addr)
                stub = pb2_grpc.MessageServiceStub(channel)

                # Try to execute the RPC
                response = rpc_function(stub, request, timeout=timeout)

                # Check for Invalid session response
                if hasattr(response, "success") and not response.success:
                    if (
                        hasattr(response, "message")
                        and "Invalid session" in response.message
                        and retry_on_invalid_session
                        and self.session_token
                    ):

                        logger.warning(f"Invalid session detected on {addr}")
                        invalid_session_detected = True
                        # Continue trying other servers instead of immediately handling
                        continue

                # If successful, update the leader address and return the response
                logger.info(f"Request succeeded on {addr}")
                self.leader_addr = addr
                return response

            except grpc.RpcError as e:
                # If connection refused or unavailable, mark this server as down
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.warning(f"Server {addr} is unavailable: {e.details()}")
                    server_unavailable = True
                    # If this was our leader, reset it
                    if addr == self.leader_addr:
                        logger.info(f"Resetting leader address {self.leader_addr}")
                        self.leader_addr = None
                else:
                    logger.warning(f"RPC failed on {addr}: {e.code()}: {e.details()}")

                last_exception = e
                # Continue to next server
                continue

            except Exception as e:
                logger.error(f"Unexpected error on {addr}: {str(e)}")
                if addr == self.leader_addr:
                    logger.info(f"Resetting leader address {self.leader_addr}")
                    self.leader_addr = None
                last_exception = e
                # Continue to next server
                continue

        # After trying all servers
        logger.warning(f"Tried all {len(tried_addresses)} servers without success")

        # Only trigger invalid session handling if ALL servers report invalid session
        # AND none of them are just unavailable (which would explain why they can't validate the session)
        if invalid_session_detected and not server_unavailable:
            logger.warning(
                "Invalid session detected on all available servers, triggering recovery"
            )
            self._handle_invalid_session()
            return None

        # If we've tried all servers and none worked, raise the last exception
        if last_exception:
            logger.error("All servers are unavailable")
            raise last_exception
        else:
            raise ConnectionError("Could not connect to any server node")

    def _handle_invalid_session(self):
        """Handle an invalid session by notifying UI"""
        logger.info("Handling invalid session - will need to re-login")
        # Only clear session token if we're truly invalidated
        # (not just if servers are temporarily down)
        self.session_token = None
        self.leader_addr = None
        self.is_connected = False
        # Notify UI that session is invalid
        self.login_status_changed.emit(
            False, "Your session has expired. Please login again."
        )

    def register(self, username, password, display_name=None):
        """Register a new user account"""
        try:
            if not display_name:
                display_name = username

            request = pb2.RegisterRequest(
                username=username, password=password, display_name=display_name
            )

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.Register(req, timeout=timeout), request
            )

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
            request = pb2.LoginRequest(username=username, password=password)

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.Login(req, timeout=timeout), request
            )

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
            request = pb2.LogoutRequest(session_token=self.session_token)

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.Logout(req, timeout=timeout), request
            )

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
            request = pb2.DeleteAccountRequest(
                session_token=self.session_token, password=password
            )

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.DeleteAccount(req, timeout=timeout),
                request,
            )

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
            request = pb2.SendMessageRequest(
                session_token=self.session_token,
                receiver_id=receiver_id,
                content=content,
            )

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.SendMessage(req, timeout=timeout),
                request,
            )

            # Handle invalid session
            if response is None:
                return False, "Invalid session", None

            if response.success:
                logger.info(f"Message sent successfully, ID: {response.message_id}")
                # Emit a signal with our own message for immediate display
                self.message_received.emit(
                    response.message_id, self.user_id, content, int(time.time())
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
            if before_timestamp is None:
                before_timestamp = int(time.time() * 1000)

            request = pb2.GetMessagesRequest(
                session_token=self.session_token,
                other_user_id=other_user_id,
                limit=limit,
                before_timestamp=before_timestamp,
            )

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.GetMessages(req, timeout=timeout),
                request,
            )

            # Handle invalid session
            if response is None:
                return False, "Invalid session", []

            if response.success:
                messages = []
                for msg in response.messages:
                    messages.append(
                        {
                            "message_id": msg.message_id,
                            "sender_id": msg.sender_id,
                            "receiver_id": msg.receiver_id,
                            "content": msg.content,
                            "timestamp": msg.timestamp,
                            "read": msg.read,
                        }
                    )
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
            request = pb2.DeleteMessageRequest(
                session_token=self.session_token, message_id=message_id
            )

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.DeleteMessage(req, timeout=timeout),
                request,
            )

            # Handle invalid session
            if response is None:
                return False, "Invalid session"

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
            request = pb2.MarkMessageReadRequest(
                session_token=self.session_token, message_id=message_id
            )

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.MarkMessageRead(req, timeout=timeout),
                request,
            )

            # Handle invalid session
            if response is None:
                return False, "Invalid session"

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
            request = pb2.GetAllUsersRequest(session_token=self.session_token)

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.GetAllUsers(req, timeout=timeout),
                request,
            )

            # Handle invalid session
            if response is None:
                return False, "Invalid session", []

            if response.success:
                users = []
                for user in response.users:
                    self.users_cache[user.user_id] = {
                        "user_id": user.user_id,
                        "username": user.username,
                        "display_name": user.display_name,
                        "online": user.online,
                    }

                    users.append(
                        {
                            "user_id": user.user_id,
                            "username": user.username,
                            "display_name": user.display_name,
                            "online": user.online,
                        }
                    )

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
            request = pb2.GetOnlineUsersRequest(session_token=self.session_token)

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.GetOnlineUsers(req, timeout=timeout),
                request,
            )

            # Handle invalid session
            if response is None:
                return False, "Invalid session", []

            if response.success:
                users = []
                for user in response.users:
                    # Update cache
                    self.users_cache[user.user_id] = {
                        "user_id": user.user_id,
                        "username": user.username,
                        "display_name": user.display_name,
                        "online": True,
                    }

                    users.append(
                        {
                            "user_id": user.user_id,
                            "username": user.username,
                            "display_name": user.display_name,
                        }
                    )

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
            request = pb2.GetUnreadCountsRequest(session_token=self.session_token)

            response = self.execute_rpc(
                lambda stub, req, timeout: stub.GetUnreadCounts(req, timeout=timeout),
                request,
            )

            # Handle invalid session
            if response is None:
                return False, "Invalid session", {}

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
                # Get all users (so we detect new registrations)
                success, error_msg, _ = self.get_all_users()

                # If session is invalid or servers are down, retry after a delay
                if not success:
                    logger.warning(f"Polling failed: {error_msg}")
                    # Reset the leader if we're getting connection errors
                    if (
                        "Connection refused" in error_msg
                        or "unavailable" in error_msg.lower()
                    ):
                        if self.leader_addr:
                            logger.info(
                                f"Resetting leader address {self.leader_addr} due to connection error"
                            )
                            self.leader_addr = None

                    time.sleep(5)
                    continue

                # Get online users
                online_success, _, _ = self.get_online_users()

                # Get unread counts
                unread_success, _, _ = self.get_unread_counts()

                # Log success summary
                logger.debug(
                    f"Poll cycle completed - Users: {success}, Online: {online_success}, Unread: {unread_success}"
                )

                # Sleep for a bit
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error during polling: {e}")
                # Reset leader on any exception during polling
                if self.leader_addr:
                    logger.info(
                        f"Resetting leader address {self.leader_addr} due to polling error"
                    )
                    self.leader_addr = None
                time.sleep(10)  # Wait a bit longer on error

    def get_user_display_name(self, user_id):
        """Get a user's display name from cache or fetch it"""
        if user_id in self.users_cache:
            return self.users_cache[user_id].get("display_name", "Unknown")
        else:
            # Try to refresh users cache
            success, _, _ = self.get_all_users()
            if success and user_id in self.users_cache:
                return self.users_cache[user_id].get("display_name", "Unknown")
            return "Unknown"


# Main client application UI will be in another file to keep this more manageable
