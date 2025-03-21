import grpc
from concurrent import futures
import time
import uuid
import logging
import threading
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass
import json
from enum import Enum

from .replication.replication_pb2 import (
    ReplicatedRequest,
    ReplicatedResponse,
    Ack,
    HeartbeatRequest,
    HeartbeatResponse,
    JoinClusterRequest,
    JoinClusterResponse,
    RequestMissingOpsRequest,
    RequestMissingOpsResponse,
    ElectLeaderRequest,
    ElectLeaderResponse,
)
from .replication.replication_pb2_grpc import (
    ReplicationServiceServicer,
    add_ReplicationServiceServicer_to_server,
)
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

logger = logging.getLogger("replication")


class NodeRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class ReplicaInfo:
    id: str
    address: str
    last_heartbeat: float
    is_active: bool
    last_applied_sequence: int


class ReplicationServicer(ReplicationServiceServicer):
    def __init__(self, replica_id: str, address: str, db_path: str):
        self.replica_id = replica_id
        self.address = address
        self.role = NodeRole.FOLLOWER
        self.current_leader: Optional[str] = None
        self.sequence_number = 0
        self.last_applied_sequence = 0
        self.replicas: Dict[str, ReplicaInfo] = {}
        self.pending_requests: Dict[int, ReplicatedRequest] = {}
        self.applied_requests: Dict[int, Any] = {}

        # Heartbeat and election timeouts
        self.heartbeat_interval = 0.1  # 100ms
        self.election_timeout = 1.0  # 1s
        self.last_heartbeat = time.time()

        # Threading
        self.lock = threading.Lock()
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True
        )
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)

        # Start background threads
        self.heartbeat_thread.start()
        self.election_thread.start()

    def _heartbeat_loop(self) -> None:
        """Background thread for sending heartbeats if leader, or checking for leader timeout if follower"""
        while True:
            if self.role == NodeRole.LEADER:
                self._send_heartbeats()
            elif self.role == NodeRole.FOLLOWER:
                self._check_leader_timeout()
            time.sleep(0.1)

    def _election_loop(self) -> None:
        """Background thread for handling leader election"""
        while True:
            if self.role == NodeRole.CANDIDATE:
                self._conduct_election()
            time.sleep(0.1)

    def _send_heartbeats(self) -> None:
        """Send heartbeats to all replicas"""
        with self.lock:
            for replica_id, info in self.replicas.items():
                if replica_id != self.replica_id:
                    try:
                        # TODO: Implement actual gRPC call to send heartbeat
                        pass
                    except Exception as e:
                        logger.error(f"Failed to send heartbeat to {replica_id}: {e}")

    def _check_leader_timeout(self) -> None:
        """Check if leader has timed out"""
        with self.lock:
            if time.time() - self.last_heartbeat > self.election_timeout:
                logger.info("Leader timeout detected, starting election")
                self.role = NodeRole.CANDIDATE
                self.current_leader = None

    def _conduct_election(self) -> None:
        """Conduct leader election"""
        with self.lock:
            # Request votes from all replicas
            votes_received = 0
            for replica_id, info in self.replicas.items():
                if replica_id != self.replica_id:
                    try:
                        # TODO: Implement actual gRPC call to request vote
                        pass
                    except Exception as e:
                        logger.error(f"Failed to request vote from {replica_id}: {e}")

            # If we received majority of votes, become leader
            if votes_received >= len(self.replicas) // 2:
                self.role = NodeRole.LEADER
                self.current_leader = self.replica_id
                logger.info(f"Elected as leader: {self.replica_id}")

    def HandleClientRequest(
        self, request: ReplicatedRequest, context: grpc.ServicerContext
    ) -> ReplicatedResponse:
        """Handle client requests (only for leader)"""
        if self.role != NodeRole.LEADER:
            return ReplicatedResponse(success=False, error_message="Not the leader")

        with self.lock:
            # Assign sequence number
            self.sequence_number += 1
            request.sequence_number = self.sequence_number
            request.coordinator_id = self.replica_id
            request.timestamp = int(time.time())

            # Store request
            self.pending_requests[self.sequence_number] = request

            # Replicate to followers
            acks_received = 0
            for replica_id, info in self.replicas.items():
                if replica_id != self.replica_id:
                    try:
                        # TODO: Implement actual gRPC call to replicate request
                        pass
                    except Exception as e:
                        logger.error(f"Failed to replicate to {replica_id}: {e}")

            # Wait for quorum
            if acks_received >= len(self.replicas) // 2:
                # Apply request locally
                result = self._apply_request(request)
                self.applied_requests[self.sequence_number] = result
                return ReplicatedResponse(success=True, result=result)
            else:
                return ReplicatedResponse(
                    success=False, error_message="Failed to achieve quorum"
                )

    def ReplicateRequest(
        self, request: ReplicatedRequest, context: grpc.ServicerContext
    ) -> Ack:
        """Handle replicated requests from leader"""
        with self.lock:
            # Verify sequence number
            if request.sequence_number <= self.last_applied_sequence:
                return Ack(
                    sequence_number=request.sequence_number,
                    replica_id=self.replica_id,
                    success=True,
                )

            # Apply request
            try:
                result = self._apply_request(request)
                self.applied_requests[request.sequence_number] = result
                self.last_applied_sequence = request.sequence_number
                return Ack(
                    sequence_number=request.sequence_number,
                    replica_id=self.replica_id,
                    success=True,
                )
            except Exception as e:
                logger.error(f"Failed to apply request: {e}")
                return Ack(
                    sequence_number=request.sequence_number,
                    replica_id=self.replica_id,
                    success=False,
                    error_message=str(e),
                )

    def _apply_request(self, request: ReplicatedRequest) -> bytes:
        """Apply a replicated request to the local state"""
        # Deserialize the original request
        original_request = self._deserialize_request(request.operation, request.payload)

        # Apply the request based on operation type
        # TODO: Implement actual request handling logic
        return b""  # Placeholder

    def _deserialize_request(self, operation: str, payload: bytes) -> Any:
        """Deserialize a request payload based on operation type"""
        # TODO: Implement deserialization logic
        return None

    def Heartbeat(
        self, request: HeartbeatRequest, context: grpc.ServicerContext
    ) -> HeartbeatResponse:
        """Handle heartbeat from leader"""
        with self.lock:
            self.last_heartbeat = time.time()
            self.current_leader = request.coordinator_id

            # Include replica state in response
            return HeartbeatResponse(
                success=True,
                error_message="",
                replica_id=self.replica_id,
                role=self.role.value,
                current_sequence=self.sequence_number,
                last_applied_sequence=self.last_applied_sequence,
                is_leader=self.role == NodeRole.LEADER,
            )

    def JoinCluster(
        self, request: JoinClusterRequest, context: grpc.ServicerContext
    ) -> JoinClusterResponse:
        """Handle new replica joining the cluster"""
        with self.lock:
            # Add new replica to the list
            self.replicas[request.replica_id] = ReplicaInfo(
                id=request.replica_id,
                address=request.address,
                last_heartbeat=time.time(),
                is_active=True,
                last_applied_sequence=request.last_applied_sequence,
            )

            # If we're the leader, send missing operations
            if self.role == NodeRole.LEADER:
                missing_ops = self._get_missing_operations(
                    request.last_applied_sequence
                )
                return JoinClusterResponse(
                    success=True,
                    missing_operations=missing_ops,
                    current_sequence=self.sequence_number,
                )

            return JoinClusterResponse(success=True)

    def _get_missing_operations(self, last_applied: int) -> List[ReplicatedRequest]:
        """Get operations that a replica is missing"""
        missing = []
        for seq, req in sorted(self.pending_requests.items()):
            if seq > last_applied:
                missing.append(req)
        return missing

    def RequestMissingOps(
        self, request: RequestMissingOpsRequest, context: grpc.ServicerContext
    ) -> RequestMissingOpsResponse:
        """Handle request for missing operations"""
        with self.lock:
            missing_ops = self._get_missing_operations(request.from_sequence)
            return RequestMissingOpsResponse(success=True, operations=missing_ops)

    def ElectLeader(
        self, request: ElectLeaderRequest, context: grpc.ServicerContext
    ) -> ElectLeaderResponse:
        """Handle leader election request"""
        with self.lock:
            # Simple election: choose replica with highest last applied sequence
            if request.last_applied_sequence > self.last_applied_sequence:
                return ElectLeaderResponse(
                    success=True, elected_leader_id=request.candidate_id
                )
            return ElectLeaderResponse(success=False, elected_leader_id=self.replica_id)
