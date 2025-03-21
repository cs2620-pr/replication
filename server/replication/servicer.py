import grpc
import time
import logging
from typing import Dict, Optional, List
from datetime import datetime
import uuid

from ..chat.chat_pb2 import (
    LoginRequest,
    LoginResponse,
    ListAccountsRequest,
    ListAccountsResponse,
    AccountInfo,
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
from .replication_pb2 import (
    HeartbeatRequest,
    HeartbeatResponse,
    JoinClusterRequest,
    JoinClusterResponse,
    ReplicaInfo,
    ReplicatedRequest,
    ReplicatedResponse,
    Ack,
    ElectLeaderRequest,
    ElectLeaderResponse,
    FindLeaderRequest,
    FindLeaderResponse,
)
from .replication_pb2_grpc import ReplicationServiceServicer, ReplicationServiceStub
from ..database import ChatDatabase

logger = logging.getLogger("replication")


class ReplicationServicer(ReplicationServiceServicer):
    def __init__(self, replica_id: str, address: str, db_path: str):
        self.replica_id = replica_id
        self.address = address
        self.db_path = db_path
        self.role = "follower"  # Start as follower
        self.current_sequence = 0
        self.last_applied_sequence = 0
        self.is_leader = False
        self.replicas: Dict[str, ReplicaInfo] = {}
        self.last_heartbeat = int(time.time())  # Convert to integer
        self.heartbeat_interval = 1  # seconds
        self.election_timeout = 5  # seconds
        self.last_leader_heartbeat = 0
        self.election_timer = None
        self.known_replicas: set[str] = set()  # Set of known replica addresses
        self.online_users: Dict[str, str] = {}  # username -> session_token
        self.db = ChatDatabase(db_path)  # Initialize database

        # Add self to replicas
        self.replicas[self.replica_id] = ReplicaInfo(
            replica_id=self.replica_id,
            address=self.address,
            role="follower",
            current_sequence=0,
            last_applied_sequence=0,
            is_leader=False,
            last_heartbeat=int(time.time()),
        )

        # Start discovery process
        self.discover_replicas()

    def discover_replicas(self) -> None:
        """Discover other replicas in the cluster."""
        # Try common ports (50051-50060)
        for port in range(50051, 50061):
            address = f"localhost:{port}"
            if address != self.address:  # Don't try to connect to self
                try:
                    channel = grpc.insecure_channel(address)
                    stub = ReplicationServiceStub(channel)

                    # Try to join the cluster
                    join_request = JoinClusterRequest(
                        replica_id=self.replica_id,
                        address=self.address,
                        last_applied_sequence=self.last_applied_sequence,
                    )

                    response = stub.JoinCluster(join_request)
                    if response.success:
                        logger.info(f"Successfully joined cluster through {address}")
                        # Add the replica to our list
                        self.replicas[response.replica_id] = ReplicaInfo(
                            replica_id=response.replica_id,
                            address=address,
                            role="follower",
                            current_sequence=response.current_sequence,
                            last_applied_sequence=response.last_applied_sequence,
                            is_leader=False,
                            last_heartbeat=int(time.time()),
                        )
                        self.known_replicas.add(address)

                    channel.close()
                except Exception as e:
                    logger.debug(f"Failed to connect to {address}: {e}")

    def start_election(self) -> None:
        """Start a new leader election."""
        logger.info(f"Starting leader election from replica {self.replica_id}")
        self.role = "candidate"
        self.is_leader = False

        # Vote for self
        votes_received = 1
        total_votes = len(self.replicas) + 1  # Include self

        # Request votes from all other replicas
        for replica_id, replica in self.replicas.items():
            try:
                channel = grpc.insecure_channel(replica.address)
                stub = ReplicationServiceStub(channel)

                request = ElectLeaderRequest(
                    candidate_id=self.replica_id,
                    last_applied_sequence=self.last_applied_sequence,
                    timestamp=int(time.time()),
                )

                response = stub.ElectLeader(request)
                if response.success:
                    votes_received += 1

                channel.close()
            except Exception as e:
                logger.error(f"Failed to get vote from {replica_id}: {e}")

        # Check if we won the election
        if votes_received > total_votes / 2:
            logger.info(
                f"Replica {self.replica_id} won the election with {votes_received}/{total_votes} votes"
            )
            self.role = "leader"
            self.is_leader = True
            self.last_heartbeat = int(time.time())
        else:
            logger.info(
                f"Replica {self.replica_id} lost the election with {votes_received}/{total_votes} votes"
            )
            self.role = "follower"
            self.is_leader = False

    def ElectLeader(
        self, request: ElectLeaderRequest, context: grpc.ServicerContext
    ) -> ElectLeaderResponse:
        """Handle leader election request."""
        # If we're already a leader, reject the request
        if self.is_leader:
            return ElectLeaderResponse(
                success=False,
                error_message="Already have a leader",
                elected_leader_id=self.replica_id,
            )

        # If we're a candidate, only vote for ourselves
        if self.role == "candidate":
            return ElectLeaderResponse(
                success=(request.candidate_id == self.replica_id),
                error_message="",
                elected_leader_id=(
                    self.replica_id if request.candidate_id == self.replica_id else ""
                ),
            )

        # If we're a follower, vote for the candidate
        return ElectLeaderResponse(
            success=True, error_message="", elected_leader_id=request.candidate_id
        )

    def HandleClientRequest(
        self, request: ReplicatedRequest, context: grpc.ServicerContext
    ) -> ReplicatedResponse:
        """Handle a client request by replicating it to all replicas."""
        if not self.is_leader:
            return ReplicatedResponse(
                success=False,
                error_message="This replica is not the leader",
            )

        # Increment sequence number
        self.current_sequence += 1
        request.sequence_number = self.current_sequence
        request.timestamp = int(time.time())

        # Replicate to all followers
        for replica_id, replica in self.replicas.items():
            if replica_id != self.replica_id:  # Don't send to self
                try:
                    # Create channel to follower
                    channel = grpc.insecure_channel(replica.address)
                    stub = ReplicationServiceStub(channel)

                    # Send replicated request
                    stub.ReplicateRequest(request)

                    channel.close()
                except Exception as e:
                    logger.error(f"Failed to replicate to {replica_id}: {e}")

        # Apply the request locally
        try:
            # Deserialize the request based on operation type
            if request.operation == "Login":
                login_request = LoginRequest.FromString(request.payload)
                success, message = self.db.verify_user(
                    login_request.username, login_request.password
                )

                if not success:
                    login_response = LoginResponse(success=False, error_message=message)
                else:
                    # Generate session token
                    session_token = str(uuid.uuid4())
                    self.db.create_session(login_request.username, session_token)
                    self.online_users[login_request.username] = session_token

                    # Get unread message count
                    unread_count = self.db.get_unread_message_count(
                        login_request.username
                    )

                    login_response = LoginResponse(
                        success=True,
                        error_message="",
                        unread_message_count=unread_count,
                        session_token=session_token,
                    )

                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=login_response.SerializeToString(),
                )
            elif request.operation == "Logout":
                logout_request = LogoutRequest.FromString(request.payload)

                # Verify session
                username = self.db.verify_session(logout_request.session_token)
                if not username:
                    logout_response = LogoutResponse(
                        success=False,
                        error_message="Invalid session token",
                    )
                else:
                    # Remove user from online users
                    if username in self.online_users:
                        del self.online_users[username]

                    # Delete session
                    self.db.delete_session(logout_request.session_token)

                    logout_response = LogoutResponse(
                        success=True,
                        error_message="",
                    )

                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=logout_response.SerializeToString(),
                )
            elif request.operation == "ListAccounts":
                list_request = ListAccountsRequest.FromString(request.payload)

                # Verify session
                username = self.db.verify_session(list_request.session_token)
                if not username:
                    list_response = ListAccountsResponse(
                        error_message="Invalid session token"
                    )
                else:
                    # Get accounts
                    accounts = self.db.list_accounts(
                        pattern=list_request.pattern if list_request.pattern else None,
                        limit=list_request.page_size,
                        offset=list_request.page_size * list_request.page_number,
                    )

                    # Convert to proto format
                    account_infos = []
                    for account in accounts:
                        is_online = account["username"] in self.online_users
                        account_infos.append(
                            AccountInfo(
                                username=account["username"], is_online=is_online
                            )
                        )

                    list_response = ListAccountsResponse(
                        accounts=account_infos,
                        has_more=len(accounts) == list_request.page_size,
                        total_count=len(accounts),
                        error_message="",
                    )

                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=list_response.SerializeToString(),
                )
            elif request.operation == "SendMessage":
                send_request = SendMessageRequest.FromString(request.payload)

                # Verify session
                username = self.db.verify_session(send_request.session_token)
                if not username:
                    send_response = SendMessageResponse(
                        success=False,
                        error_message="Invalid session token",
                    )
                else:
                    # Generate message ID
                    message_id = str(uuid.uuid4())

                    # Send message
                    success, message = self.db.send_message(
                        sender=username,
                        recipient=send_request.recipient,
                        content=send_request.content,
                        message_id=message_id,
                    )

                    send_response = SendMessageResponse(
                        success=success,
                        error_message=message if not success else "",
                        message_id=message_id if success else "",
                    )

                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=send_response.SerializeToString(),
                )
            elif request.operation == "GetMessages":
                get_request = GetMessagesRequest.FromString(request.payload)

                # Verify session
                username = self.db.verify_session(get_request.session_token)
                if not username:
                    get_response = GetMessagesResponse(
                        error_message="Invalid session token"
                    )
                else:
                    # Get messages
                    messages = self.db.get_messages(
                        username=username, limit=get_request.max_messages
                    )

                    # Convert to proto format
                    message_protos = []
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

                    get_response = GetMessagesResponse(
                        messages=message_protos,
                        has_more=len(messages) == get_request.max_messages,
                        error_message="",
                    )

                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=get_response.SerializeToString(),
                )
            elif request.operation == "DeleteMessages":
                delete_request = DeleteMessagesRequest.FromString(request.payload)

                # Verify session
                username = self.db.verify_session(delete_request.session_token)
                if not username:
                    delete_response = DeleteMessagesResponse(
                        success=False,
                        error_message="Invalid session token",
                    )
                else:
                    # Delete messages
                    success, failed_ids = self.db.delete_messages(
                        message_ids=list(delete_request.message_ids),
                        username=username,
                    )

                    delete_response = DeleteMessagesResponse(
                        success=success and not failed_ids,
                        error_message=(
                            ""
                            if success and not failed_ids
                            else "Failed to delete some messages"
                        ),
                        failed_message_ids=failed_ids,
                    )

                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=delete_response.SerializeToString(),
                )
            elif request.operation == "MarkConversationAsRead":
                mark_read_request = MarkConversationAsReadRequest.FromString(
                    request.payload
                )

                # Verify session
                username = self.db.verify_session(mark_read_request.session_token)
                if not username:
                    mark_read_response = MarkConversationAsReadResponse(
                        success=False,
                        error_message="Invalid session token",
                    )
                else:
                    # Mark conversation as read
                    success = self.db.mark_conversation_as_read(
                        username=username,
                        other_user=mark_read_request.other_user,
                    )

                    mark_read_response = MarkConversationAsReadResponse(
                        success=success,
                        error_message=(
                            "" if success else "Failed to mark conversation as read"
                        ),
                    )

                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=mark_read_response.SerializeToString(),
                )
            else:
                # For other operations, just return the request payload for now
                return ReplicatedResponse(
                    success=True,
                    error_message="",
                    result=request.payload,
                )
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            return ReplicatedResponse(
                success=False,
                error_message=str(e),
            )

    def ReplicateRequest(
        self, request: ReplicatedRequest, context: grpc.ServicerContext
    ) -> Ack:
        """Handle a replicated request from the leader."""
        try:
            # Update sequence number
            if request.sequence_number > self.current_sequence:
                self.current_sequence = request.sequence_number

            # Return acknowledgment
            return Ack(
                sequence_number=request.sequence_number,
                replica_id=self.replica_id,
                success=True,
            )
        except Exception as e:
            logger.error(f"Failed to handle replicated request: {e}")
            return Ack(
                sequence_number=request.sequence_number,
                replica_id=self.replica_id,
                success=False,
                error_message=str(e),
            )

    def Heartbeat(
        self, request: HeartbeatRequest, context: grpc.ServicerContext
    ) -> HeartbeatResponse:
        """Handle heartbeat request from coordinator."""
        current_time = int(time.time())
        self.last_heartbeat = current_time

        # If we're a follower and haven't received a heartbeat in a while, start election
        if (
            self.role == "follower"
            and current_time - self.last_leader_heartbeat > self.election_timeout
        ):
            logger.info(f"Replica {self.replica_id} starting election due to timeout")
            self.start_election()

        # If we're a leader, send heartbeat to all followers
        if self.is_leader:
            for replica_id, replica in self.replicas.items():
                if replica_id != self.replica_id:  # Don't send to self
                    try:
                        channel = grpc.insecure_channel(replica.address)
                        stub = ReplicationServiceStub(channel)
                        stub.Heartbeat(request)
                        channel.close()
                    except Exception as e:
                        logger.error(f"Failed to send heartbeat to {replica_id}: {e}")

        # If we're a follower and received a heartbeat from a leader, update our leader heartbeat
        if not self.is_leader and request.coordinator_id:
            self.last_leader_heartbeat = current_time

        return HeartbeatResponse(
            replica_id=self.replica_id,
            role=self.role,
            current_sequence=self.current_sequence,
            last_applied_sequence=self.last_applied_sequence,
            is_leader=self.is_leader,
        )

    def JoinCluster(
        self, request: JoinClusterRequest, context: grpc.ServicerContext
    ) -> JoinClusterResponse:
        """Handle request from a new replica to join the cluster."""
        if request.replica_id in self.replicas:
            return JoinClusterResponse(
                success=False,
                error_message=f"Replica {request.replica_id} already exists",
            )

        # Add new replica to the cluster
        self.replicas[request.replica_id] = ReplicaInfo(
            replica_id=request.replica_id,
            address=request.address,
            role="follower",
            current_sequence=0,
            last_applied_sequence=request.last_applied_sequence,
            is_leader=False,
            last_heartbeat=int(time.time()),
        )

        # If we're the leader, send our current state
        if self.is_leader:
            return JoinClusterResponse(
                success=True,
                error_message="",
                current_sequence=self.current_sequence,
            )

        logger.info(f"Replica {request.replica_id} joined the cluster")
        return JoinClusterResponse(success=True, error_message="")

    def FindLeader(
        self, request: FindLeaderRequest, context: grpc.ServicerContext
    ) -> FindLeaderResponse:
        """Help clients find the current leader."""
        # If we're the leader, return our info
        if self.is_leader:
            return FindLeaderResponse(
                success=True,
                error_message="",
                leader_id=self.replica_id,
                leader_address=self.address,
            )

        # If we're not the leader, try to find the leader
        for replica_id, replica in self.replicas.items():
            try:
                channel = grpc.insecure_channel(replica.address)
                stub = ReplicationServiceStub(channel)
                response = stub.FindLeader(request)
                channel.close()

                if response.success:
                    return response
            except Exception as e:
                logger.error(f"Failed to find leader through {replica_id}: {e}")

        # If we couldn't find a leader, return error
        return FindLeaderResponse(
            success=False,
            error_message="No leader found in the cluster",
        )
