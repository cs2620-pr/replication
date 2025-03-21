import grpc
from typing import Any, Dict, Optional
import logging
from .chat.chat_pb2 import (
    CreateAccountRequest,
    CreateAccountResponse,
    LoginRequest,
    LoginResponse,
    ListAccountsRequest,
    ListAccountsResponse,
    DeleteAccountRequest,
    DeleteAccountResponse,
    SendMessageRequest,
    SendMessageResponse,
    GetMessagesRequest,
    GetMessagesResponse,
    DeleteMessagesRequest,
    DeleteMessagesResponse,
    LogoutRequest,
    LogoutResponse,
    MarkConversationAsReadRequest,
    MarkConversationAsReadResponse,
)
from .chat.chat_pb2_grpc import ChatServiceServicer
from .replication import ReplicationServicer
from .replication.replication_pb2 import (
    ReplicatedRequest,
    ReplicatedResponse,
    FindLeaderRequest,
)
from .replication.replication_pb2_grpc import ReplicationServiceStub

logger = logging.getLogger("replicated_chat")


class ReplicatedChatServicer(ChatServiceServicer):
    def __init__(self, replica_id: str, address: str, db_path: str):
        self.replication_service = ReplicationServicer(replica_id, address, db_path)
        self._request_handlers: Dict[str, Any] = {
            "CreateAccount": (CreateAccountRequest, CreateAccountResponse),
            "Login": (LoginRequest, LoginResponse),
            "ListAccounts": (ListAccountsRequest, ListAccountsResponse),
            "DeleteAccount": (DeleteAccountRequest, DeleteAccountResponse),
            "SendMessage": (SendMessageRequest, SendMessageResponse),
            "GetMessages": (GetMessagesRequest, GetMessagesResponse),
            "DeleteMessages": (DeleteMessagesRequest, DeleteMessagesResponse),
            "Logout": (LogoutRequest, LogoutResponse),
            "MarkConversationAsRead": (
                MarkConversationAsReadRequest,
                MarkConversationAsReadResponse,
            ),
        }

    def _handle_request(
        self, method_name: str, request: Any, context: grpc.ServicerContext
    ) -> Any:
        """Handle a request by replicating it through the replication service"""
        # First, find the leader
        find_leader_request = FindLeaderRequest(
            client_id=self.replication_service.replica_id
        )
        leader_response = self.replication_service.FindLeader(
            find_leader_request, context
        )

        if not leader_response.success:
            # Create an error response of the appropriate type
            request_type, response_type = self._request_handlers[method_name]
            if hasattr(response_type, "success"):
                error_response = response_type(
                    success=False, error_message="No leader found in the cluster"
                )
            else:
                error_response = response_type(
                    error_message="No leader found in the cluster"
                )
            return error_response

        # Create a replicated request
        replicated_request = ReplicatedRequest(
            operation=method_name, payload=request.SerializeToString()
        )

        # Send to leader
        try:
            channel = grpc.insecure_channel(leader_response.leader_address)
            stub = ReplicationServiceStub(channel)
            response = stub.HandleClientRequest(replicated_request)
            channel.close()
        except Exception as e:
            logger.error(f"Failed to send request to leader: {e}")
            request_type, response_type = self._request_handlers[method_name]
            if hasattr(response_type, "success"):
                error_response = response_type(
                    success=False,
                    error_message=f"Failed to connect to leader: {str(e)}",
                )
            else:
                error_response = response_type(
                    error_message=f"Failed to connect to leader: {str(e)}"
                )
            return error_response

        if not response.success:
            # Create an error response of the appropriate type
            request_type, response_type = self._request_handlers[method_name]
            if hasattr(response_type, "success"):
                error_response = response_type(
                    success=False, error_message=response.error_message
                )
            else:
                error_response = response_type(error_message=response.error_message)
            return error_response

        # Get the appropriate response type
        request_type, response_type = self._request_handlers[method_name]

        # Deserialize the response
        return response_type.FromString(response.result)

    def CreateAccount(
        self, request: CreateAccountRequest, context: grpc.ServicerContext
    ) -> CreateAccountResponse:
        """Handle account creation request."""
        return self._handle_request("CreateAccount", request, context)

    def Login(
        self, request: LoginRequest, context: grpc.ServicerContext
    ) -> LoginResponse:
        """Handle login request."""
        return self._handle_request("Login", request, context)

    def ListAccounts(
        self, request: ListAccountsRequest, context: grpc.ServicerContext
    ) -> ListAccountsResponse:
        """Handle account listing request."""
        return self._handle_request("ListAccounts", request, context)

    def DeleteAccount(
        self, request: DeleteAccountRequest, context: grpc.ServicerContext
    ) -> DeleteAccountResponse:
        """Handle account deletion request."""
        return self._handle_request("DeleteAccount", request, context)

    def SendMessage(
        self, request: SendMessageRequest, context: grpc.ServicerContext
    ) -> SendMessageResponse:
        """Handle message sending request."""
        return self._handle_request("SendMessage", request, context)

    def GetMessages(
        self, request: GetMessagesRequest, context: grpc.ServicerContext
    ) -> GetMessagesResponse:
        """Handle message retrieval request."""
        return self._handle_request("GetMessages", request, context)

    def DeleteMessages(
        self, request: DeleteMessagesRequest, context: grpc.ServicerContext
    ) -> DeleteMessagesResponse:
        """Handle message deletion request."""
        return self._handle_request("DeleteMessages", request, context)

    def Logout(
        self, request: LogoutRequest, context: grpc.ServicerContext
    ) -> LogoutResponse:
        """Handle logout request."""
        return self._handle_request("Logout", request, context)

    def MarkConversationAsRead(
        self, request: MarkConversationAsReadRequest, context: grpc.ServicerContext
    ) -> MarkConversationAsReadResponse:
        """Handle marking conversation as read request."""
        return self._handle_request("MarkConversationAsRead", request, context)
