import json
import time
import uuid
import hashlib
import logging
from typing import Dict, List, Optional, Any, Tuple

import grpc

from generated import replication_pb2 as pb2
from generated import replication_pb2_grpc as pb2_grpc

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("MessageService")


class MessageService(pb2_grpc.MessageServiceServicer):
    """
    Implementation of the chat messaging service on top of Raft consensus.
    This service connects to the Raft node to store and retrieve data.
    """

    def __init__(self, raft_node):
        """
        Initialize the message service with a reference to the Raft node.

        Args:
            raft_node: Reference to the RaftNode object
        """
        self.raft_node = raft_node
        self.session_tokens = {}  # Map of session tokens to user IDs

        # Initialize data structures in the Raft state machine if they don't exist
        self._initialize_data_structures()

    def _initialize_data_structures(self):
        """Initialize required data structures in the Raft state machine"""
        # User data structure
        if not self._get_value("users"):
            self._set_value("users", {})

        # Messages data structure
        if not self._get_value("messages"):
            self._set_value("messages", {})

        # Online users
        if not self._get_value("online_users"):
            self._set_value("online_users", [])

        # Sessions data structure - store as key-value instead of SQL
        if not self._get_value("sessions"):
            self._set_value("sessions", {})

    def _get_value(self, key: str) -> Any:
        """Get a value from the Raft state machine"""
        try:
            result = self.raft_node.get_value(key)

            # For debugging sessions
            if key == "sessions":
                logger.info(f"Retrieved sessions from state machine: {result}")

            if isinstance(result, tuple):
                if len(result) == 3:
                    success, value, _ = result
                elif len(result) == 2:
                    success, value = result
                else:
                    return None

                if success:
                    try:
                        parsed_value = json.loads(value)
                        if key == "sessions":
                            logger.info(f"Parsed sessions data: {parsed_value}")
                        return parsed_value
                    except:
                        return value
            return None
        except Exception as e:
            logger.error(f"Error getting value for key {key}: {e}")
            return None

    def _set_value(self, key: str, value: Any) -> bool:
        """Set a value in the Raft state machine"""
        try:
            value_str = json.dumps(value)

            # Log sessions data for debugging
            if key == "sessions":
                logger.info(f"Saving sessions to state machine: {value}")

            # Direct state machine access if possible
            if hasattr(self.raft_node, "apply_to_state_machine"):
                command = {"type": "set", "key": key, "value": value_str}
                success = self.raft_node.apply_to_state_machine(command)
                if key == "sessions":
                    logger.info(f"Apply sessions to state machine result: {success}")
                return True
            # Try direct append_entry if available
            elif hasattr(self.raft_node, "append_entry"):
                command = {"type": "set", "key": key, "value": value_str}
                success = self.raft_node.append_entry(command)
                if key == "sessions":
                    logger.info(f"Append sessions to log result: {success}")
                return success
            # Fallback: Create a simple client to call the leader
            else:
                for node in self.raft_node.nodes:
                    try:
                        node_id, host, port = (
                            node.get("id"),
                            node.get("host"),
                            node.get("port"),
                        )
                        channel = grpc.insecure_channel(f"{host}:{port}")
                        stub = pb2_grpc.DataServiceStub(channel)

                        request = pb2.SetRequest(key=key, value=value_str)
                        response = stub.Set(request, timeout=5.0)

                        if response.success:
                            if key == "sessions":
                                logger.info(
                                    f"Set sessions via RPC success on node {node_id}"
                                )
                            return True
                    except Exception as e:
                        logger.error(f"Error setting value through node {node_id}: {e}")
                        continue

                return False
        except Exception as e:
            logger.error(f"Error setting value for key {key}: {e}")
            return False

    def _hash_password(self, password: str) -> str:
        """Create a password hash"""
        return hashlib.sha256(password.encode()).hexdigest()

    def _generate_token(self, user_id: str) -> str:
        """Generate a session token for a user"""
        # Get the username for the user
        users = self._get_value("users") or {}
        username = None
        for uid, user_data in users.items():
            if uid == user_id:
                username = user_data.get("username")
                break

        if not username:
            logger.warning(f"Cannot find username for user_id {user_id}")
            return ""

        # Generate a new token
        token = str(uuid.uuid4())

        # Calculate expiration (30 days from now)
        now = int(time.time())
        expires_at = now + (30 * 24 * 60 * 60)  # 30 days in seconds

        # Store session in the state machine
        sessions = self._get_value("sessions") or {}
        sessions[token] = {
            "user_id": user_id,
            "username": username,
            "created_at": now,
            "expires_at": expires_at,
        }

        # Save to state machine
        self._set_value("sessions", sessions)

        # Keep a local copy for faster lookups
        self.session_tokens[token] = user_id

        logger.info(f"Generated new session token for user {username}")
        return token

    def _authenticate(self, session_token: str) -> Optional[str]:
        """Authenticate a user by session token, return user_id if valid"""
        # First check local cache for performance
        if session_token in self.session_tokens:
            return self.session_tokens[session_token]

        # Not in local cache, check state machine
        try:
            # Get sessions from state machine
            sessions = self._get_value("sessions") or {}
            session = sessions.get(session_token)

            if session:
                # Check if session is expired
                now = int(time.time())
                if now > session.get("expires_at", 0):
                    logger.info(f"Session {session_token[:8]}... expired")
                    # Remove expired session
                    del sessions[session_token]
                    self._set_value("sessions", sessions)
                    return None

                # Session is valid, cache it
                user_id = session.get("user_id")
                if user_id:
                    self.session_tokens[session_token] = user_id
                    return user_id

            logger.info(f"Session {session_token[:8]}... not found in state machine")
            return None

        except Exception as e:
            logger.error(f"Error authenticating session: {e}")
            return None

    def _get_user_by_username(
        self, username: str
    ) -> Tuple[Optional[str], Optional[Dict]]:
        """Get a user by username, returns (user_id, user_data)"""
        users = self._get_value("users") or {}
        for user_id, user_data in users.items():
            if user_data.get("username") == username:
                return user_id, user_data
        return None, None

    def _get_user_by_id(self, user_id: str) -> Optional[Dict]:
        """Get a user by ID"""
        users = self._get_value("users") or {}
        return users.get(user_id)

    def Register(self, request, context):
        """Handle user registration"""
        username = request.username
        password = request.password
        display_name = request.display_name or username

        # Validate input
        if not username or not password:
            return pb2.RegisterResponse(
                success=False, message="Username and password are required"
            )

        # Check if username already exists
        user_id, _ = self._get_user_by_username(username)
        if user_id:
            return pb2.RegisterResponse(
                success=False, message="Username already exists"
            )

        # Create user
        user_id = str(uuid.uuid4())
        users = self._get_value("users") or {}
        users[user_id] = {
            "username": username,
            "password_hash": self._hash_password(password),
            "display_name": display_name,
            "created_at": int(time.time()),
        }

        # Save to Raft state machine
        if self._set_value("users", users):
            return pb2.RegisterResponse(
                success=True, message="Registration successful", user_id=user_id
            )
        else:
            return pb2.RegisterResponse(
                success=False, message="Failed to register user"
            )

    def Login(self, request, context):
        """Handle user login"""
        username = request.username
        password = request.password

        # Validate input
        if not username or not password:
            return pb2.LoginResponse(
                success=False, message="Username and password are required"
            )

        # Find user
        user_id, user_data = self._get_user_by_username(username)
        if not user_id or not user_data:
            return pb2.LoginResponse(
                success=False, message="Invalid username or password"
            )

        # Check password
        if user_data.get("password_hash") != self._hash_password(password):
            return pb2.LoginResponse(
                success=False, message="Invalid username or password"
            )

        # Generate session token
        session_token = self._generate_token(user_id)
        logger.info(
            f"Login: Generated session token {session_token[:8]}... for user {username}"
        )

        # Verify session was saved
        sessions = self._get_value("sessions") or {}
        if session_token in sessions:
            logger.info(
                f"Login: Session {session_token[:8]}... successfully saved in state machine"
            )
        else:
            logger.warning(
                f"Login: Session {session_token[:8]}... NOT found in state machine after generation!"
            )

        # Mark user as online
        online_users = self._get_value("online_users") or []
        if user_id not in online_users:
            online_users.append(user_id)
            self._set_value("online_users", online_users)

        return pb2.LoginResponse(
            success=True,
            message="Login successful",
            session_token=session_token,
            user_id=user_id,
        )

    def Logout(self, request, context):
        """Handle user logout"""
        session_token = request.session_token

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.LogoutResponse(success=False, message="Invalid session")

        # Remove session token from state machine
        sessions = self._get_value("sessions") or {}
        if session_token in sessions:
            del sessions[session_token]
            self._set_value("sessions", sessions)

        # Remove from local cache
        if session_token in self.session_tokens:
            del self.session_tokens[session_token]

        # Mark user as offline
        online_users = self._get_value("online_users") or []
        if user_id in online_users:
            online_users.remove(user_id)
            self._set_value("online_users", online_users)

        return pb2.LogoutResponse(success=True, message="Logout successful")

    def DeleteAccount(self, request, context):
        """Handle account deletion"""
        session_token = request.session_token
        password = request.password

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.DeleteAccountResponse(success=False, message="Invalid session")

        # Verify password
        user_data = self._get_user_by_id(user_id)
        if not user_data or user_data.get("password_hash") != self._hash_password(
            password
        ):
            return pb2.DeleteAccountResponse(success=False, message="Invalid password")

        # Remove user
        users = self._get_value("users") or {}
        if user_id in users:
            del users[user_id]

        # Remove user from online users
        online_users = self._get_value("online_users") or []
        if user_id in online_users:
            online_users.remove(user_id)

        # Remove all messages involving this user
        messages = self._get_value("messages") or {}
        messages_to_delete = []

        for msg_id, msg in messages.items():
            if msg.get("sender_id") == user_id or msg.get("receiver_id") == user_id:
                messages_to_delete.append(msg_id)

        for msg_id in messages_to_delete:
            if msg_id in messages:
                del messages[msg_id]

        # Update Raft state machine
        self._set_value("users", users)
        self._set_value("online_users", online_users)
        self._set_value("messages", messages)

        # Remove session token
        if session_token in self.session_tokens:
            del self.session_tokens[session_token]

        return pb2.DeleteAccountResponse(
            success=True, message="Account deleted successfully"
        )

    def SendMessage(self, request, context):
        """Handle sending a message"""
        session_token = request.session_token
        receiver_id = request.receiver_id
        content = request.content

        # Authenticate
        sender_id = self._authenticate(session_token)
        if not sender_id:
            return pb2.SendMessageResponse(success=False, message="Invalid session")

        # Validate input
        if not content:
            return pb2.SendMessageResponse(
                success=False, message="Message content cannot be empty"
            )

        # Verify receiver exists
        receiver = self._get_user_by_id(receiver_id)
        if not receiver:
            return pb2.SendMessageResponse(
                success=False, message="Recipient does not exist"
            )

        # Create message
        message_id = str(uuid.uuid4())
        timestamp = int(time.time())

        messages = self._get_value("messages") or {}
        messages[message_id] = {
            "message_id": message_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "timestamp": timestamp,
            "read": False,
        }

        # Save to Raft state machine
        if self._set_value("messages", messages):
            return pb2.SendMessageResponse(
                success=True, message="Message sent", message_id=message_id
            )
        else:
            return pb2.SendMessageResponse(
                success=False, message="Failed to send message"
            )

    def GetMessages(self, request, context):
        """Handle retrieving messages between two users"""
        session_token = request.session_token
        other_user_id = request.other_user_id
        limit = request.limit or 50  # Default to 50 messages
        before_timestamp = request.before_timestamp or int(
            time.time() * 1000
        )  # Default to current time

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.GetMessagesResponse(success=False, message="Invalid session")

        # Verify other user exists
        other_user = self._get_user_by_id(other_user_id)
        if not other_user:
            return pb2.GetMessagesResponse(success=False, message="User does not exist")

        # Get messages between these users
        all_messages = self._get_value("messages") or {}
        conversation_messages = []

        for msg_id, msg in all_messages.items():
            # Check if message is part of this conversation
            is_sender = (
                msg.get("sender_id") == user_id
                and msg.get("receiver_id") == other_user_id
            )
            is_receiver = (
                msg.get("sender_id") == other_user_id
                and msg.get("receiver_id") == user_id
            )

            if (is_sender or is_receiver) and msg.get(
                "timestamp", 0
            ) < before_timestamp:
                conversation_messages.append(msg)

        # Sort by timestamp (newest first) and limit
        conversation_messages.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        conversation_messages = conversation_messages[:limit]

        # Mark messages as read if user is the receiver
        if conversation_messages:
            messages_updated = False
            all_messages = self._get_value("messages") or {}

            for msg in conversation_messages:
                if msg.get("receiver_id") == user_id and not msg.get("read", False):
                    msg_id = msg.get("message_id")
                    if msg_id in all_messages:
                        all_messages[msg_id]["read"] = True
                        messages_updated = True

            if messages_updated:
                self._set_value("messages", all_messages)

        # Convert to protocol buffer messages
        pb_messages = []
        for msg in conversation_messages:
            pb_messages.append(
                pb2.Message(
                    message_id=msg.get("message_id", ""),
                    sender_id=msg.get("sender_id", ""),
                    receiver_id=msg.get("receiver_id", ""),
                    content=msg.get("content", ""),
                    timestamp=msg.get("timestamp", 0),
                    read=msg.get("read", False),
                )
            )

        return pb2.GetMessagesResponse(
            success=True, message="Messages retrieved", messages=pb_messages
        )

    def DeleteMessage(self, request, context):
        """Handle deleting a message"""
        session_token = request.session_token
        message_id = request.message_id

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.DeleteMessageResponse(success=False, message="Invalid session")

        # Get message
        messages = self._get_value("messages") or {}
        message = messages.get(message_id)

        if not message:
            return pb2.DeleteMessageResponse(success=False, message="Message not found")

        # Verify sender is deleting the message
        if message.get("sender_id") != user_id:
            return pb2.DeleteMessageResponse(
                success=False, message="Not authorized to delete this message"
            )

        # Delete message
        del messages[message_id]

        # Save to Raft state machine
        if self._set_value("messages", messages):
            return pb2.DeleteMessageResponse(success=True, message="Message deleted")
        else:
            return pb2.DeleteMessageResponse(
                success=False, message="Failed to delete message"
            )

    def MarkMessageRead(self, request, context):
        """Handle marking a message as read"""
        session_token = request.session_token
        message_id = request.message_id

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.MarkMessageReadResponse(success=False, message="Invalid session")

        # Get message
        messages = self._get_value("messages") or {}
        message = messages.get(message_id)

        if not message:
            return pb2.MarkMessageReadResponse(
                success=False, message="Message not found"
            )

        # Verify user is the recipient
        if message.get("receiver_id") != user_id:
            return pb2.MarkMessageReadResponse(
                success=False, message="Not authorized to mark this message as read"
            )

        # Mark as read
        messages[message_id]["read"] = True

        # Save to Raft state machine
        if self._set_value("messages", messages):
            return pb2.MarkMessageReadResponse(
                success=True, message="Message marked as read"
            )
        else:
            return pb2.MarkMessageReadResponse(
                success=False, message="Failed to mark message as read"
            )

    def GetAllUsers(self, request, context):
        """Handle retrieving all users"""
        session_token = request.session_token

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.GetAllUsersResponse(success=False, message="Invalid session")

        # Get users
        users = self._get_value("users") or {}
        online_users = self._get_value("online_users") or []

        # Convert to protocol buffer users
        pb_users = []
        for uid, user_data in users.items():
            pb_users.append(
                pb2.User(
                    user_id=uid,
                    username=user_data.get("username", ""),
                    display_name=user_data.get("display_name", ""),
                    online=(uid in online_users),
                )
            )

        return pb2.GetAllUsersResponse(
            success=True, message="Users retrieved", users=pb_users
        )

    def GetOnlineUsers(self, request, context):
        """Handle retrieving online users"""
        session_token = request.session_token

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.GetOnlineUsersResponse(success=False, message="Invalid session")

        # Get online users
        users = self._get_value("users") or {}
        online_users = self._get_value("online_users") or []

        # Convert to protocol buffer users
        pb_users = []
        for uid in online_users:
            user_data = users.get(uid, {})
            pb_users.append(
                pb2.User(
                    user_id=uid,
                    username=user_data.get("username", ""),
                    display_name=user_data.get("display_name", ""),
                    online=True,
                )
            )

        return pb2.GetOnlineUsersResponse(
            success=True, message="Online users retrieved", users=pb_users
        )

    def GetUnreadCounts(self, request, context):
        """Handle retrieving unread message counts by user"""
        session_token = request.session_token

        # Authenticate
        user_id = self._authenticate(session_token)
        if not user_id:
            return pb2.GetUnreadCountsResponse(success=False, message="Invalid session")

        # Get unread counts by sender
        messages = self._get_value("messages") or {}
        unread_counts = {}

        for msg_id, msg in messages.items():
            if msg.get("receiver_id") == user_id and not msg.get("read", False):
                sender_id = msg.get("sender_id", "")
                if sender_id:
                    unread_counts[sender_id] = unread_counts.get(sender_id, 0) + 1

        # Convert to protocol buffer unread counts
        pb_unread_counts = []
        for sender_id, count in unread_counts.items():
            pb_unread_counts.append(pb2.UserUnreadCount(user_id=sender_id, count=count))

        return pb2.GetUnreadCountsResponse(
            success=True,
            message="Unread counts retrieved",
            unread_counts=pb_unread_counts,
        )
