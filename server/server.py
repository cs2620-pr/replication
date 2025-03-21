import grpc  # type: ignore
from concurrent import futures
import time
import uuid
import logging
import argparse
import os
from typing import Dict, List, Tuple, Optional, cast, Any
import sys

from .chat.chat_pb2 import (
    CreateAccountRequest,
    CreateAccountResponse,
    LoginRequest,
    LoginResponse,
    ListAccountsRequest,
    ListAccountsResponse,
    AccountInfo,
    DeleteAccountRequest,
    DeleteAccountResponse,
    SendMessageRequest,
    SendMessageResponse,
    GetMessagesRequest,
    GetMessagesResponse,
    DeleteMessagesRequest,
    DeleteMessagesResponse,
    Message,
    LogoutRequest,
    LogoutResponse,
    MarkConversationAsReadRequest,
    MarkConversationAsReadResponse,
)
from .chat.chat_pb2_grpc import ChatServiceServicer, add_ChatServiceServicer_to_server
from .replication.replication_pb2_grpc import add_ReplicationServiceServicer_to_server
from .database import ChatDatabase  # type: ignore
from .constants import ErrorMessage, SuccessMessage
from .replicated_chat import ReplicatedChatServicer
from .replication import ReplicationServicer

# Create logs directory if it doesn't exist
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG if you need more details
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(log_dir, "server.log")
        ),  # Save logs to the logs directory
        logging.StreamHandler(),  # Also print logs to the console
    ],
)

logger = logging.getLogger("server")  # Use a specific logger for clarity


class ChatServicer(ChatServiceServicer):
    def __init__(self, db_path: str = "chat.db", enable_logging: bool = False) -> None:
        self.db = ChatDatabase(db_path)
        # In-memory cache of online users and their session tokens
        self.online_users: Dict[str, str] = {}  # username -> session_token

        # Set up protocol logging
        self.enable_logging = enable_logging
        self.protocol_logger = logging.getLogger("protocol_metrics")

        if enable_logging:
            # Create logs directory if it doesn't exist
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # Configure logger if it's enabled
            self.protocol_logger.setLevel(logging.INFO)

            # Check if handler already exists to avoid duplicates
            if not self.protocol_logger.handlers:
                protocol_handler = logging.FileHandler(
                    os.path.join(log_dir, "protocol_metrics_server.log")
                )
                protocol_handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                    )
                )
                self.protocol_logger.addHandler(protocol_handler)

            self.protocol_logger.info(
                "Protocol metrics logging enabled in ChatServicer"
            )
        else:
            self.protocol_logger.setLevel(logging.WARNING)
            # Make sure there's at least a NullHandler to avoid "no handlers" warnings
            if not self.protocol_logger.handlers:
                self.protocol_logger.addHandler(logging.NullHandler())

    def log_message(
        self, direction: str, method_name: str, message: Any, details: str = ""
    ) -> None:
        """Log gRPC message size and details if logging is enabled"""
        if not self.enable_logging:
            return

        size = len(message.SerializeToString()) if message else 0
        log_msg = f"GRPC {direction} - {method_name} - Size: {size} bytes"
        if details:
            log_msg += f" | {details}"
        self.protocol_logger.info(log_msg)

    def CreateAccount(
        self,
        request: CreateAccountRequest,
        context: grpc.ServicerContext,
    ) -> CreateAccountResponse:
        """Handle account creation request."""
        logger.info(f"Creating account for user: {request.username}")
        self.log_message(
            "Incoming", "CreateAccount", request, f"Username: {request.username}"
        )

        success, message = self.db.create_user(request.username, request.password)

        response = CreateAccountResponse(
            success=success, error_message=message if not success else ""
        )

        self.log_message("Outgoing", "CreateAccount", response, f"Success: {success}")
        return response

    def Login(
        self,
        request: LoginRequest,
        context: grpc.ServicerContext,
    ) -> LoginResponse:
        """Handle login request."""
        logger.info(f"Login attempt for user: {request.username}")
        self.log_message("Incoming", "Login", request, f"Username: {request.username}")

        success, message = self.db.verify_user(request.username, request.password)

        if not success:
            response = LoginResponse(success=False, error_message=message)
            self.log_message("Outgoing", "Login", response, f"Failed: {message}")
            return response

        # Generate session token
        session_token = str(uuid.uuid4())
        self.db.create_session(request.username, session_token)
        self.online_users[request.username] = session_token

        # Get unread message count
        unread_count = self.db.get_unread_message_count(request.username)

        response = LoginResponse(
            success=True,
            error_message="",
            unread_message_count=unread_count,
            session_token=session_token,
        )

        self.log_message(
            "Outgoing", "Login", response, f"Success, unread: {unread_count}"
        )
        return response

    def ListAccounts(
        self,
        request: ListAccountsRequest,
        context: grpc.ServicerContext,
    ) -> ListAccountsResponse:
        """Handle account listing request."""
        self.log_message("Incoming", "ListAccounts", request)

        # Verify session
        username = self.db.verify_session(request.session_token)
        if not username:
            response = ListAccountsResponse(
                error_message=ErrorMessage.INVALID_SESSION.value
            )
            self.log_message("Outgoing", "ListAccounts", response, "Invalid session")
            return response

        # Get accounts
        accounts = self.db.list_accounts(
            pattern=request.pattern if request.pattern else None,
            limit=request.page_size,
            offset=request.page_size * request.page_number,
        )

        # Convert to proto format
        account_infos = []
        for account in accounts:
            is_online = account["username"] in self.online_users
            account_infos.append(
                AccountInfo(username=account["username"], is_online=is_online)
            )

        response = ListAccountsResponse(
            accounts=account_infos,
            has_more=len(accounts) == request.page_size,
            total_count=len(accounts),
            error_message="",
        )

        self.log_message(
            "Outgoing", "ListAccounts", response, f"Retrieved {len(accounts)} accounts"
        )
        return response

    def DeleteAccount(
        self,
        request: DeleteAccountRequest,
        context: grpc.ServicerContext,
    ) -> DeleteAccountResponse:
        """Handle account deletion request."""
        self.log_message("Incoming", "DeleteAccount", request)

        # Verify session
        username = self.db.verify_session(request.session_token)
        if not username:
            response = DeleteAccountResponse(
                success=False, error_message=ErrorMessage.INVALID_SESSION.value
            )
            self.log_message("Outgoing", "DeleteAccount", response, "Invalid session")
            return response

        # Log the account deletion attempt
        print(
            f"ACCOUNT DELETION: User '{username}' is attempting to delete their account"
        )

        # Delete account
        success, message = self.db.delete_account(username)
        if success:
            # Log successful deletion
            print(
                f"ACCOUNT DELETION SUCCESSFUL: User '{username}' has deleted their account"
            )

            # Remove from online users if present
            self.online_users.pop(username, None)
            # Delete session
            self.db.delete_session(request.session_token)
        else:
            # Log failed deletion
            print(
                f"ACCOUNT DELETION FAILED: User '{username}' failed to delete their account. Reason: {message}"
            )

        response = DeleteAccountResponse(
            success=success, error_message=message if not success else ""
        )

        self.log_message("Outgoing", "DeleteAccount", response, f"Success: {success}")
        return response

    def SendMessage(
        self,
        request: SendMessageRequest,
        context: grpc.ServicerContext,
    ) -> SendMessageResponse:
        """Handle message sending request with logging."""
        self.log_message(
            "Incoming",
            "SendMessage",
            request,
            f"Sender: {request.session_token} -> Recipient: {request.recipient}",
        )

        # Verify session
        username = self.db.verify_session(request.session_token)
        if not username:
            response = SendMessageResponse(
                success=False, error_message=ErrorMessage.INVALID_SESSION.value
            )
            self.log_message("Outgoing", "SendMessage", response, "Invalid session")
            return response

        # Generate message ID
        message_id = str(uuid.uuid4())

        # Send message
        success, message = self.db.send_message(
            sender=username,
            recipient=request.recipient,
            content=request.content,
            message_id=message_id,
        )

        response = SendMessageResponse(
            success=success,
            error_message=message if not success else "",
            message_id=message_id if success else "",
        )

        self.log_message("Outgoing", "SendMessage", response, f"Success: {success}")
        return response

    def GetMessages(
        self,
        request: GetMessagesRequest,
        context: grpc.ServicerContext,
    ) -> GetMessagesResponse:
        """Handle message retrieval request with logging."""
        self.log_message(
            "Incoming",
            "GetMessages",
            request,
            f"Session Token: {request.session_token}",
        )

        # Verify session
        username = self.db.verify_session(request.session_token)
        if not username:
            response = GetMessagesResponse(
                error_message=ErrorMessage.INVALID_SESSION.value
            )
            self.log_message("Outgoing", "GetMessages", response, "Invalid session")
            return response

        # Get messages
        messages = self.db.get_messages(username=username, limit=request.max_messages)

        # Convert to proto format
        message_protos = []

        # Track unique conversation partners to mark messages as read
        conversation_partners = set()

        for msg in messages:
            message_protos.append(
                Message(
                    message_id=msg["message_id"],
                    sender=msg["sender"],
                    recipient=msg.get("recipient", username),
                    content=msg["content"],
                    timestamp=msg["timestamp"],
                    delivered=True,
                    unread=msg.get("unread", not msg["delivered"]),
                    deleted=msg.get("deleted", False),
                )
            )

            # Track conversation partners
            if msg["sender"] != username:
                conversation_partners.add(msg["sender"])
            if "recipient" in msg and msg["recipient"] != username:
                conversation_partners.add(msg["recipient"])

        response = GetMessagesResponse(
            messages=message_protos,
            has_more=len(messages) == request.max_messages,
            error_message="",
        )

        self.log_message(
            "Outgoing", "GetMessages", response, f"Retrieved {len(messages)} messages"
        )
        return response

    def DeleteMessages(
        self,
        request: DeleteMessagesRequest,
        context: grpc.ServicerContext,
    ) -> DeleteMessagesResponse:
        """Handle message deletion request."""
        self.log_message("Incoming", "DeleteMessages", request)

        # Verify session
        username = self.db.verify_session(request.session_token)
        if not username:
            response = DeleteMessagesResponse(
                success=False, error_message=ErrorMessage.INVALID_SESSION.value
            )
            self.log_message("Outgoing", "DeleteMessages", response, "Invalid session")
            return response

        # Delete messages
        success, failed_ids = self.db.delete_messages(
            message_ids=list(request.message_ids), username=username
        )

        # Provide a specific error message if deletion failed
        error_message = ""
        if not success:
            error_message = ErrorMessage.FAILED_DELETE_MESSAGES.value
        elif failed_ids:
            error_message = "You can only delete messages that you sent."

        response = DeleteMessagesResponse(
            success=success and not failed_ids,
            error_message=error_message,
            failed_message_ids=failed_ids,
        )

        self.log_message("Outgoing", "DeleteMessages", response, f"Success: {success}")
        return response

    def Logout(
        self,
        request: LogoutRequest,
        context: grpc.ServicerContext,
    ) -> LogoutResponse:
        """Handle logout request."""
        self.log_message("Incoming", "Logout", request)

        # Verify session
        username = self.db.verify_session(request.session_token)
        if not username:
            response = LogoutResponse(
                success=False, error_message=ErrorMessage.INVALID_SESSION.value
            )
            self.log_message("Outgoing", "Logout", response, "Invalid session")
            return response

        # Log the logout attempt
        logger.info(f"Logout attempt for user: {username}")

        # Delete session from database
        success = self.db.delete_session(request.session_token)

        # Remove from online users cache
        if username in self.online_users:
            del self.online_users[username]

        response = LogoutResponse(
            success=success, error_message="" if success else "Failed to logout"
        )

        self.log_message("Outgoing", "Logout", response, f"Success: {success}")
        return response

    def MarkConversationAsRead(
        self,
        request: MarkConversationAsReadRequest,
        context: grpc.ServicerContext,
    ) -> MarkConversationAsReadResponse:
        """Handle marking a conversation as read."""
        self.log_message("Incoming", "MarkConversationAsRead", request)

        # Verify session
        username = self.db.verify_session(request.session_token)
        if not username:
            response = MarkConversationAsReadResponse(
                success=False, error_message=ErrorMessage.INVALID_SESSION.value
            )
            self.log_message(
                "Outgoing", "MarkConversationAsRead", response, "Invalid session"
            )
            return response

        # Mark conversation as read
        success = self.db.mark_conversation_as_read(username, request.other_user)

        response = MarkConversationAsReadResponse(
            success=success,
            error_message="" if success else "Failed to mark conversation as read",
        )

        self.log_message(
            "Outgoing", "MarkConversationAsRead", response, f"Success: {success}"
        )
        return response


def serve(
    host: str = "localhost",
    port: int = 50051,
    max_workers: int = 10,
    db_path: str = "chat.db",
    enable_logging: bool = False,
    replica_id: Optional[str] = None,
) -> None:
    """Start the gRPC server with replication support."""
    if replica_id is None:
        replica_id = f"replica_{uuid.uuid4().hex[:8]}"

    # Create server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

    # Add services
    chat_servicer = ReplicatedChatServicer(replica_id, f"{host}:{port}", db_path)
    replication_servicer = ReplicationServicer(replica_id, f"{host}:{port}", db_path)

    add_ChatServiceServicer_to_server(chat_servicer, server)
    add_ReplicationServiceServicer_to_server(replication_servicer, server)

    # Start server
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    logger.info(f"Server started on {host}:{port} with replica ID: {replica_id}")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


def main() -> None:
    """Main entry point for the server."""
    parser = argparse.ArgumentParser(description="Start the chat server")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=50051, help="Port to bind to")
    parser.add_argument(
        "--max-workers", type=int, default=10, help="Maximum number of worker threads"
    )
    parser.add_argument(
        "--db-path", default="chat.db", help="Path to the SQLite database"
    )
    parser.add_argument(
        "--enable-logging", action="store_true", help="Enable protocol logging"
    )
    parser.add_argument("--replica-id", help="Unique ID for this replica")

    args = parser.parse_args()
    serve(
        host=args.host,
        port=args.port,
        max_workers=args.max_workers,
        db_path=args.db_path,
        enable_logging=args.enable_logging,
        replica_id=args.replica_id,
    )


if __name__ == "__main__":
    main()
