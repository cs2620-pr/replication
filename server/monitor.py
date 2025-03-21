import sys
import grpc
import time
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QSpinBox,
    QLineEdit,
    QMessageBox,
    QHeaderView,
)
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QColor, QCloseEvent
import logging
from typing import Dict, Optional
import subprocess
import signal
import os

from .replication.replication_pb2 import (
    HeartbeatRequest,
    HeartbeatResponse,
    JoinClusterRequest,
    JoinClusterResponse,
)
from .replication.replication_pb2_grpc import ReplicationServiceStub

logger = logging.getLogger("monitor")


class ReplicaInfo:
    def __init__(
        self, id: str, address: str, process: Optional[subprocess.Popen] = None
    ):
        self.id = id
        self.address = address
        self.process = process
        self.is_active = False
        self.last_heartbeat = 0
        self.role = "Unknown"
        self.sequence_number = 0


class MonitorWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Chat System Monitor")
        self.setGeometry(100, 100, 800, 600)

        # Store replica information
        self.replicas: Dict[str, ReplicaInfo] = {}

        # Create main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)

        # Create replica table
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(
            ["Replica ID", "Address", "Status", "Role", "Last Sequence"]
        )
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)

        # Create control panel
        control_panel = QWidget()
        control_layout = QHBoxLayout(control_panel)

        # Add replica controls
        add_replica_group = QWidget()
        add_replica_layout = QHBoxLayout(add_replica_group)

        self.port_input = QSpinBox()
        self.port_input.setRange(50051, 50060)
        self.port_input.setValue(50051)
        add_replica_layout.addWidget(QLabel("Port:"))
        add_replica_layout.addWidget(self.port_input)

        self.replica_id_input = QLineEdit()
        self.replica_id_input.setPlaceholderText("Replica ID (optional)")
        add_replica_layout.addWidget(self.replica_id_input)

        add_button = QPushButton("Add Replica")
        add_button.clicked.connect(self.add_replica)
        add_replica_layout.addWidget(add_button)

        control_layout.addWidget(add_replica_group)

        # Add refresh button
        refresh_button = QPushButton("Refresh")
        refresh_button.clicked.connect(self.refresh_status)
        control_layout.addWidget(refresh_button)

        layout.addWidget(control_panel)

        # Set up status update timer
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_status)
        self.timer.start(1000)  # Update every second

        # Start with one replica
        self.add_replica()

    def add_replica(self) -> None:
        """Add a new replica to the system."""
        port = self.port_input.value()
        replica_id = self.replica_id_input.text() or f"replica_{len(self.replicas) + 1}"
        address = f"localhost:{port}"

        if replica_id in self.replicas:
            QMessageBox.warning(self, "Error", f"Replica {replica_id} already exists!")
            return

        # Start the replica process
        cmd = [
            sys.executable,
            "-m",
            "server.server",
            "--port",
            str(port),
            "--replica-id",
            replica_id,
        ]

        try:
            process = subprocess.Popen(cmd)
            replica = ReplicaInfo(replica_id, address, process)
            self.replicas[replica_id] = replica

            # Update table
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(replica_id))
            self.table.setItem(row, 1, QTableWidgetItem(address))
            self.table.setItem(row, 2, QTableWidgetItem("Starting..."))
            self.table.setItem(row, 3, QTableWidgetItem("Unknown"))
            self.table.setItem(row, 4, QTableWidgetItem("0"))

            # Increment port for next replica
            self.port_input.setValue(port + 1)

            # Give the replica time to start up
            time.sleep(2)

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to start replica: {str(e)}")
            logger.error(f"Failed to start replica: {e}")

    def refresh_status(self) -> None:
        """Refresh the status of all replicas."""
        for replica_id, replica in self.replicas.items():
            try:
                # Create gRPC channel with timeout
                channel = grpc.insecure_channel(
                    replica.address,
                    options=[
                        ("grpc.connect_timeout_ms", 1000),  # 1 second timeout
                        ("grpc.server_uri", replica.address),
                    ],
                )
                stub = ReplicationServiceStub(channel)

                # Send heartbeat
                request = HeartbeatRequest(
                    coordinator_id="monitor", sequence_number=0, timestamp=0
                )
                response = stub.Heartbeat(request)

                # Update replica status
                replica.is_active = True
                replica.last_heartbeat = 0  # Reset heartbeat timer
                replica.role = response.role
                replica.sequence_number = response.current_sequence

                # Update table
                for row in range(self.table.rowCount()):
                    if self.table.item(row, 0).text() == replica_id:
                        # Update status
                        status_item = QTableWidgetItem("Active")
                        status_item.setBackground(QColor(0, 255, 0))
                        self.table.setItem(row, 2, status_item)

                        # Update role
                        role_item = QTableWidgetItem(response.role)
                        if response.is_leader:
                            role_item.setBackground(QColor(255, 255, 0))
                        self.table.setItem(row, 3, role_item)

                        # Update sequence
                        self.table.setItem(
                            row, 4, QTableWidgetItem(str(response.current_sequence))
                        )
                        break

                channel.close()

            except grpc.RpcError as e:
                # Replica is not responding
                replica.is_active = False
                logger.debug(f"RPC error for replica {replica_id}: {e}")

                # Update table
                for row in range(self.table.rowCount()):
                    if self.table.item(row, 0).text() == replica_id:
                        # Update status
                        status_item = QTableWidgetItem("Inactive")
                        status_item.setBackground(QColor(255, 0, 0))
                        self.table.setItem(row, 2, status_item)

                        # Clear role and sequence
                        self.table.setItem(row, 3, QTableWidgetItem("Unknown"))
                        self.table.setItem(row, 4, QTableWidgetItem("0"))
                        break

            except Exception as e:
                # Handle other errors
                replica.is_active = False
                logger.error(f"Unexpected error for replica {replica_id}: {e}")

                # Update table
                for row in range(self.table.rowCount()):
                    if self.table.item(row, 0).text() == replica_id:
                        # Update status
                        status_item = QTableWidgetItem("Error")
                        status_item.setBackground(QColor(255, 0, 0))
                        self.table.setItem(row, 2, status_item)

                        # Clear role and sequence
                        self.table.setItem(row, 3, QTableWidgetItem("Unknown"))
                        self.table.setItem(row, 4, QTableWidgetItem("0"))
                        break

    def closeEvent(self, event: QCloseEvent) -> None:
        """Handle window close event."""
        # Stop all replica processes
        for replica in self.replicas.values():
            if replica.process:
                replica.process.send_signal(signal.SIGTERM)
                replica.process.wait()
        event.accept()


def main() -> int:
    """Main entry point for the monitor."""
    app = QApplication(sys.argv)
    window = MonitorWindow()
    window.show()
    return app.exec_()


if __name__ == "__main__":
    main()
