#!/usr/bin/env python3
import sys
import os
import time
import json
import logging
import threading
import subprocess
import socket
import grpc
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                           QLabel, QPushButton, QFrame, QGridLayout, QTabWidget,
                           QTableWidget, QTableWidgetItem, QHeaderView, QGroupBox,
                           QAction, QMenu, QDialog, QCheckBox, QMessageBox)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QObject
from PyQt5.QtGui import QColor, QPalette, QFont

import generated.replication_pb2 as replication_pb2
import generated.replication_pb2_grpc as replication_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RaftMonitor")

class MonitorSignals(QObject):
    """Custom signals for the monitor"""
    cluster_status_updated = pyqtSignal(dict)
    node_status_updated = pyqtSignal(int, dict)
    error_occurred = pyqtSignal(str)

class DarkPalette(QPalette):
    """Dark mode color palette for PyQt applications"""
    def __init__(self):
        super().__init__()
        
        # Set dark theme colors
        self.setColor(QPalette.Window, QColor(53, 53, 53))
        self.setColor(QPalette.WindowText, QColor(255, 255, 255))
        self.setColor(QPalette.Base, QColor(25, 25, 25))
        self.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
        self.setColor(QPalette.ToolTipBase, QColor(255, 255, 255))
        self.setColor(QPalette.ToolTipText, QColor(255, 255, 255))
        self.setColor(QPalette.Text, QColor(255, 255, 255))
        self.setColor(QPalette.Button, QColor(53, 53, 53))
        self.setColor(QPalette.ButtonText, QColor(255, 255, 255))
        self.setColor(QPalette.BrightText, QColor(255, 0, 0))
        self.setColor(QPalette.Highlight, QColor(42, 130, 218))
        self.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
        self.setColor(QPalette.Link, QColor(42, 130, 218))
        self.setColor(QPalette.LinkVisited, QColor(152, 64, 208))

class RaftMonitor:
    """Class to monitor Raft cluster health and status"""
    def __init__(self, config_path="config.json"):
        self.config_path = config_path
        self.config = self.load_config()
        self.nodes = self.config['nodes']
        self.signals = MonitorSignals()
        
        # Get local hostname
        self.local_hostname = socket.gethostname()
        # For localhost testing, treat 127.0.0.1 and localhost as local
        self.local_aliases = ['localhost', '127.0.0.1', self.local_hostname]
        
        # Cache data
        self.health_cache = {}
        self.status_cache = {}
        self.state_machine_cache = None
        self.last_cache_update = 0
        self.CACHE_TTL = 2  # Cache for 2 seconds
        
        # Thread for background polling
        self.running = True
        self.update_thread = threading.Thread(target=self.update_data_periodically)
        self.update_thread.daemon = True
        self.update_thread.start()
    
    def load_config(self):
        """Load configuration from file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {"nodes": []}
            
    def is_local_node(self, node_id):
        """Check if a node is running on the local machine"""
        node = next((n for n in self.nodes if n['id'] == node_id), None)
        if not node:
            return False
        
        # Check if node's host matches local hostname or aliases
        return node['host'] in self.local_aliases
    
    def stop_node(self, node_id):
        """Stop a specific node"""
        try:
            # Find the node's process
            result = subprocess.run(
                ["pgrep", "-f", f"server.py --id {node_id}"],
                capture_output=True,
                text=True
            )
            if result.stdout.strip():
                pid = result.stdout.strip()
                # Kill the process
                subprocess.run(["kill", pid])
                logger.info(f"Stopped node {node_id} (PID: {pid})")
                return True
            else:
                logger.warning(f"No process found for node {node_id}")
                return False
        except Exception as e:
            logger.error(f"Error stopping node {node_id}: {e}")
            return False
    
    def start_node(self, node_id):
        """Start a specific node if it's on the local machine"""
        if not self.is_local_node(node_id):
            logger.warning(f"Cannot start node {node_id}: not a local node")
            return False
        
        try:
            # Start the node
            cmd = [
                "python", "server.py",
                "--id", str(node_id),
                "--config", self.config_path
            ]
            
            # Redirect output to log file
            log_dir = "logs"
            os.makedirs(log_dir, exist_ok=True)
            stdout_file = open(f"{log_dir}/node{node_id}.log", "a")
            
            # Start the process
            process = subprocess.Popen(
                cmd,
                stdout=stdout_file,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            logger.info(f"Started node {node_id}, PID: {process.pid}")
            return True
        except Exception as e:
            logger.error(f"Error starting node {node_id}: {e}")
            return False
    
    def get_node_health(self, node_id):
        """Get health of a specific node"""
        current_time = time.time()
        
        # Use cached data if available and fresh
        cache_key = f"health_{node_id}"
        if cache_key in self.health_cache and current_time - self.health_cache[cache_key]['timestamp'] < self.CACHE_TTL:
            return self.health_cache[cache_key]['data']
        
        # Find the node in config
        node = next((n for n in self.nodes if n['id'] == node_id), None)
        if not node:
            return {"status": "UNKNOWN", "error": f"Node {node_id} not found in config"}
        
        # Connect to the node and get health
        try:
            with grpc.insecure_channel(f"{node['host']}:{node['port']}") as channel:
                stub = replication_pb2_grpc.MonitoringServiceStub(channel)
                response = stub.HealthCheck(replication_pb2.HealthCheckRequest(), timeout=1.0)
                
                # Convert response to dict
                result = {
                    "status": self._serving_status_to_str(response.status),
                    "version": response.version,
                    "uptime_seconds": response.uptime_seconds,
                    "timestamp": current_time
                }
                
                # Cache the result
                self.health_cache[cache_key] = {
                    "data": result,
                    "timestamp": current_time
                }
                
                return result
        except Exception as e:
            logger.error(f"Error getting health for node {node_id}: {e}")
            result = {"status": "NOT_SERVING", "error": str(e), "timestamp": current_time}
            self.health_cache[cache_key] = {
                "data": result,
                "timestamp": current_time
            }
            return result
    
    def _serving_status_to_str(self, status):
        """Convert serving status enum to string"""
        status_map = {
            0: "UNKNOWN",
            1: "SERVING",
            2: "NOT_SERVING",
            3: "SERVICE_UNKNOWN"
        }
        return status_map.get(status, "UNKNOWN")
    
    def get_node_status(self, node_id, include_log=False, include_state_machine=False):
        """Get detailed status of a specific node"""
        current_time = time.time()
        
        # Use cached data if available and fresh
        cache_key = f"status_{node_id}_{include_log}_{include_state_machine}"
        if cache_key in self.status_cache and current_time - self.status_cache[cache_key]['timestamp'] < self.CACHE_TTL:
            return self.status_cache[cache_key]['data']
        
        # Find the node in config
        node = next((n for n in self.nodes if n['id'] == node_id), None)
        if not node:
            return {"error": f"Node {node_id} not found in config"}
        
        # Connect to the node and get status
        try:
            with grpc.insecure_channel(f"{node['host']}:{node['port']}") as channel:
                stub = replication_pb2_grpc.MonitoringServiceStub(channel)
                response = stub.NodeStatus(
                    replication_pb2.NodeStatusRequest(
                        include_log=include_log,
                        include_state_machine=include_state_machine
                    ), 
                    timeout=1.0
                )
                
                # Convert NodeState enum to string
                state_map = {
                    0: "FOLLOWER",
                    1: "CANDIDATE",
                    2: "LEADER"
                }
                
                # Build peers info
                peers = []
                for peer in response.peers:
                    peers.append({
                        "node_id": peer.node_id,
                        "is_connected": peer.is_connected,
                        "last_contact_ms": peer.last_contact_ms,
                        "next_index": peer.next_index,
                        "match_index": peer.match_index
                    })
                
                # Convert response to dict
                result = {
                    "node_id": response.node_id,
                    "state": state_map.get(response.state, "UNKNOWN"),
                    "current_term": response.current_term,
                    "committed_index": response.committed_index,
                    "last_applied": response.last_applied,
                    "current_leader": response.current_leader,
                    "peers": peers,
                    "log_size": response.log_size
                }
                
                # Include state machine if requested
                if include_state_machine:
                    result["state_machine"] = {k: v for k, v in response.state_machine.items()}
                
                # Cache the result
                self.status_cache[cache_key] = {
                    "data": result,
                    "timestamp": current_time
                }
                
                # If this is the leader and has state machine data, cache it
                if include_state_machine and result["state"] == "LEADER":
                    self.state_machine_cache = {
                        "data": result["state_machine"],
                        "timestamp": current_time
                    }
                
                return result
        except Exception as e:
            logger.error(f"Error getting status for node {node_id}: {e}")
            result = {"error": str(e)}
            self.status_cache[cache_key] = {
                "data": result,
                "timestamp": current_time
            }
            return result
    
    def get_cluster_status(self, include_all_node_details=False):
        """Get cluster-wide status (from leader)"""
        # Try to find the leader first
        leader_id = None
        leader_status = None
        
        # First check if we already have a known leader
        for node_id in [n['id'] for n in self.nodes]:
            cache_key = f"status_{node_id}_False_False"
            if cache_key in self.status_cache:
                status = self.status_cache[cache_key]['data']
                if "state" in status and status["state"] == "LEADER":
                    leader_id = node_id
                    leader_status = status
                    break
        
        # If no leader in cache, query all nodes
        if not leader_id:
            for node_id in [n['id'] for n in self.nodes]:
                status = self.get_node_status(node_id)
                if "state" in status and status["state"] == "LEADER":
                    leader_id = node_id
                    leader_status = status
                    break
        
        # If still no leader, try direct cluster status query
        if not leader_id:
            # Try each node to see if any responds to cluster status
            for node_id in [n['id'] for n in self.nodes]:
                try:
                    node = next((n for n in self.nodes if n['id'] == node_id), None)
                    if not node:
                        continue
                    
                    with grpc.insecure_channel(f"{node['host']}:{node['port']}") as channel:
                        stub = replication_pb2_grpc.MonitoringServiceStub(channel)
                        response = stub.ClusterStatus(
                            replication_pb2.ClusterStatusRequest(
                                include_all_node_details=include_all_node_details
                            ), 
                            timeout=1.0
                        )
                        
                        # If we get here, the node responded so either it's the leader
                        # or it knows the leader
                        if response.leader_id > 0:
                            leader_id = response.leader_id
                            
                            # Get leader data
                            leader_node = next((n for n in self.nodes if n['id'] == leader_id), None)
                            if leader_node:
                                # Get fresh leader status
                                leader_status = self.get_node_status(leader_id, include_state_machine=True)
                            break
                except Exception:
                    continue
        
        if not leader_id or not leader_status:
            return {"error": "No leader found in the cluster"}
        
        # Collect status from all nodes
        node_statuses = []
        healthy_nodes = 0
        
        for node in self.nodes:
            node_id = node['id']
            status = self.get_node_status(node_id)
            
            if "error" not in status:
                healthy_nodes += 1
                node_statuses.append(status)
        
        # Get state machine data
        state_machine = {}
        state_machine_update_time = 0
        
        if self.state_machine_cache:
            state_machine = self.state_machine_cache["data"]
            state_machine_update_time = int((time.time() - self.state_machine_cache["timestamp"]) * 1000)
        elif "state_machine" in leader_status:
            state_machine = leader_status["state_machine"]
            state_machine_update_time = 0  # Just refreshed
        
        # Construct cluster status
        cluster_status = {
            "leader_id": leader_id,
            "current_term": leader_status.get("current_term", 0),
            "total_nodes": len(self.nodes),
            "healthy_nodes": healthy_nodes,
            "state_machine_size": len(state_machine),
            "state_machine": state_machine,
            "nodes": node_statuses,
            "last_state_machine_update_ms": state_machine_update_time
        }
        
        return cluster_status
    
    def update_data_periodically(self):
        """Update data in background thread"""
        while self.running:
            try:
                # Get fresh cluster status
                cluster_status = self.get_cluster_status(include_all_node_details=True)
                if "error" not in cluster_status:
                    self.signals.cluster_status_updated.emit(cluster_status)
                
                # Get individual node statuses
                for node in self.nodes:
                    node_id = node['id']
                    status = self.get_node_status(node_id, include_state_machine=False)
                    self.signals.node_status_updated.emit(node_id, status)
            except Exception as e:
                logger.error(f"Error updating data: {e}")
                self.signals.error_occurred.emit(str(e))
            
            # Sleep between updates
            time.sleep(2)
    
    def stop(self):
        """Stop the monitor"""
        self.running = False
        if self.update_thread.is_alive():
            self.update_thread.join(timeout=1.0)


class RaftMonitorUI(QMainWindow):
    """Main window for the Raft Monitor application"""
    def __init__(self):
        super().__init__()
        
        # Setup monitor
        self.monitor = RaftMonitor()
        
        # Connect signals
        self.monitor.signals.cluster_status_updated.connect(self.update_cluster_status)
        self.monitor.signals.node_status_updated.connect(self.update_node_status)
        self.monitor.signals.error_occurred.connect(self.display_error)
        
        # Initialize UI
        self.init_ui()
        
        # Cache for node status displays
        self.node_frames = {}
        
        # Create node status displays
        self.create_node_displays()
        
        # Refresh timer
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.update_ui)
        self.refresh_timer.start(2000)  # Update UI every 2 seconds
        
        # Last refresh time
        self.last_refresh = time.time()
        
        # Dark mode support
        self.dark_palette = DarkPalette()
        self.light_palette = QApplication.palette()
        
        # Detect system theme and set initial dark mode state
        self.dark_mode = self.is_dark_mode_enabled()
        if self.dark_mode:
            QApplication.setPalette(self.dark_palette)
        
        # Add dark mode toggle to menu
        self.dark_mode_action = QAction("Dark Mode", self)
        self.dark_mode_action.setCheckable(True)
        self.dark_mode_action.setChecked(self.dark_mode)
        self.dark_mode_action.triggered.connect(self.toggle_dark_mode)
        
        # Add system theme option
        self.system_theme_action = QAction("Use System Theme", self)
        self.system_theme_action.setCheckable(True)
        self.system_theme_action.setChecked(True)
        self.system_theme_action.triggered.connect(self.use_system_theme)
        
        self.menu_bar = self.menuBar()
        self.view_menu = self.menu_bar.addMenu("View")
        self.view_menu.addAction(self.dark_mode_action)
        self.view_menu.addAction(self.system_theme_action)
        
        # Set initial state based on system theme
        self.use_system_theme()

    def is_dark_mode_enabled(self):
        """Detect if system is using dark mode"""
        # For macOS
        if sys.platform == 'darwin':
            try:
                result = subprocess.run(
                    ['defaults', 'read', '-g', 'AppleInterfaceStyle'],
                    capture_output=True,
                    text=True
                )
                return result.stdout.strip() == 'Dark'
            except Exception:
                # If we can't detect, default to light mode
                return False
        # For other platforms, could add detection here
        # Default to light mode if we can't detect
        return False

    def init_ui(self):
        """Initialize the UI components"""
        self.setWindowTitle('Raft Cluster Monitor')
        self.setGeometry(100, 100, 1200, 800)
        
        # Main widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Header with refresh info
        header_layout = QHBoxLayout()
        self.title_label = QLabel('Raft Consensus Cluster Monitor')
        self.title_label.setFont(QFont('Arial', 16, QFont.Bold))
        header_layout.addWidget(self.title_label)
        
        self.refresh_label = QLabel('Last refresh: N/A')
        header_layout.addWidget(self.refresh_label, alignment=Qt.AlignRight)
        
        self.refresh_button = QPushButton('Refresh Now')
        self.refresh_button.clicked.connect(self.update_ui)
        header_layout.addWidget(self.refresh_button)
        
        self.restart_button = QPushButton('Restart Monitor')
        self.restart_button.clicked.connect(self.restart_monitor)
        header_layout.addWidget(self.restart_button)
        
        main_layout.addLayout(header_layout)
        
        # Tab widget for different views
        self.tabs = QTabWidget()
        
        # Overview tab
        self.overview_tab = QWidget()
        overview_layout = QVBoxLayout(self.overview_tab)
        
        # Cluster status box
        cluster_group = QGroupBox("Cluster Overview")
        cluster_layout = QGridLayout(cluster_group)
        
        self.leader_label = QLabel("Leader: Detecting...")
        self.term_label = QLabel("Current Term: N/A")
        self.nodes_label = QLabel("Nodes: N/A")
        self.healthy_nodes_label = QLabel("Healthy Nodes: N/A")
        
        cluster_layout.addWidget(self.leader_label, 0, 0)
        cluster_layout.addWidget(self.term_label, 1, 0)
        cluster_layout.addWidget(self.nodes_label, 0, 1)
        cluster_layout.addWidget(self.healthy_nodes_label, 1, 1)
        
        overview_layout.addWidget(cluster_group)
        
        # Nodes grid
        self.nodes_grid = QGridLayout()
        overview_layout.addLayout(self.nodes_grid)
        
        # State machine tab
        self.state_machine_tab = QWidget()
        state_machine_layout = QVBoxLayout(self.state_machine_tab)
        
        self.state_size_label = QLabel("State Machine Size: N/A entries")
        state_machine_layout.addWidget(self.state_size_label)
        
        self.state_table = QTableWidget(0, 2)
        self.state_table.setHorizontalHeaderLabels(["Key", "Value"])
        self.state_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        state_machine_layout.addWidget(self.state_table)
        
        # Add tabs to tab widget
        self.tabs.addTab(self.overview_tab, "Cluster Overview")
        self.tabs.addTab(self.state_machine_tab, "State Machine")
        
        main_layout.addWidget(self.tabs)
        
        # Status bar
        self.statusBar().showMessage('Ready')
    
    def create_node_displays(self):
        """Create UI components for each node"""
        # First, get the configuration
        nodes = self.monitor.nodes
        
        # Clear existing grid
        for i in reversed(range(self.nodes_grid.count())):
            self.nodes_grid.itemAt(i).widget().setParent(None)
        
        # Create a frame for each node
        row, col = 0, 0
        max_cols = 3  # 3 nodes per row
        
        for node in nodes:
            node_id = node['id']
            
            # Create frame
            node_frame = QFrame()
            node_frame.setFrameStyle(QFrame.Panel | QFrame.Raised)
            node_frame.setLineWidth(2)
            
            # Node layout
            node_layout = QVBoxLayout(node_frame)
            
            # Node header
            header_layout = QHBoxLayout()
            node_title = QLabel(f"Node {node_id} ({node['host']}:{node['port']})")
            node_title.setFont(QFont('Arial', 12, QFont.Bold))
            header_layout.addWidget(node_title)
            
            self.node_state_label = QLabel("State: Unknown")
            self.node_state_label.setStyleSheet("color: gray;")
            header_layout.addWidget(self.node_state_label, alignment=Qt.AlignRight)
            
            node_layout.addLayout(header_layout)
            
            # Node info grid
            info_layout = QGridLayout()
            
            self.node_term_label = QLabel("Term: N/A")
            self.node_commit_label = QLabel("Commit: N/A")
            self.node_applied_label = QLabel("Applied: N/A")
            self.node_log_label = QLabel("Log: N/A entries")
            self.node_health_label = QLabel("Health: N/A")
            self.node_uptime_label = QLabel("Uptime: N/A")
            
            info_layout.addWidget(self.node_term_label, 0, 0)
            info_layout.addWidget(self.node_commit_label, 1, 0)
            info_layout.addWidget(self.node_applied_label, 0, 1)
            info_layout.addWidget(self.node_log_label, 1, 1)
            info_layout.addWidget(self.node_health_label, 2, 0)
            info_layout.addWidget(self.node_uptime_label, 2, 1)
            
            node_layout.addLayout(info_layout)
            
            # Peer connections
            self.peer_label = QLabel("Peer Connections:")
            node_layout.addWidget(self.peer_label)
            
            self.peer_layout = QVBoxLayout()
            node_layout.addLayout(self.peer_layout)
            
            # Add start/stop buttons
            control_layout = QHBoxLayout()
            
            # Stop button - always enabled
            stop_button = QPushButton("Stop Server")
            stop_button.clicked.connect(lambda checked, nid=node_id: self.stop_node(nid))
            control_layout.addWidget(stop_button)
            
            # Start button - only enabled for local nodes
            start_button = QPushButton("Start Server")
            start_button.clicked.connect(lambda checked, nid=node_id: self.start_node(nid))
            if not self.monitor.is_local_node(node_id):
                start_button.setEnabled(False)
                start_button.setToolTip("Can only start nodes on the local machine")
            control_layout.addWidget(start_button)
            
            node_layout.addLayout(control_layout)
            
            # Add to grid
            self.nodes_grid.addWidget(node_frame, row, col)
            
            # Cache the labels for this node
            self.node_frames[node_id] = {
                'frame': node_frame,
                'state_label': self.node_state_label,
                'term_label': self.node_term_label,
                'commit_label': self.node_commit_label,
                'applied_label': self.node_applied_label,
                'log_label': self.node_log_label,
                'health_label': self.node_health_label,
                'uptime_label': self.node_uptime_label,
                'peer_layout': self.peer_layout,
                'stop_button': stop_button,
                'start_button': start_button
            }
            
            # Increment column, and if needed, row
            col += 1
            if col >= max_cols:
                col = 0
                row += 1
                
    def update_ui(self):
        """Refresh the UI data"""
        self.last_refresh = time.time()
        self.refresh_label.setText(f"Last refresh: {time.strftime('%H:%M:%S')}")
        
        # Status bar update
        self.statusBar().showMessage(f'Refreshing data at {time.strftime("%H:%M:%S")}')
    
    def update_cluster_status(self, status):
        """Update the cluster status display"""
        if "error" in status:
            self.leader_label.setText(f"Leader: Error - {status['error']}")
            return
        
        # Update leader info
        self.leader_label.setText(f"Leader: Node {status['leader_id']}")
        self.term_label.setText(f"Current Term: {status['current_term']}")
        
        # Update nodes info
        self.nodes_label.setText(f"Nodes: {status['total_nodes']}")
        
        healthy_text = f"Healthy Nodes: {status['healthy_nodes']}/{status['total_nodes']}"
        self.healthy_nodes_label.setText(healthy_text)
        
        if status['healthy_nodes'] == status['total_nodes']:
            if self.dark_mode:
                self.healthy_nodes_label.setStyleSheet("color: #4caf50; font-weight: bold;")
            else:
                self.healthy_nodes_label.setStyleSheet("color: #2e7d32; font-weight: bold;")
        elif status['healthy_nodes'] >= status['total_nodes'] // 2 + 1:
            if self.dark_mode:
                self.healthy_nodes_label.setStyleSheet("color: #ffb74d; font-weight: bold;")
            else:
                self.healthy_nodes_label.setStyleSheet("color: #ef6c00; font-weight: bold;")
        else:
            if self.dark_mode:
                self.healthy_nodes_label.setStyleSheet("color: #ff3333; font-weight: bold;")
            else:
                self.healthy_nodes_label.setStyleSheet("color: #d32f2f; font-weight: bold;")
        
        # Update state machine info
        self.state_size_label.setText(f"State Machine Size: {status['state_machine_size']} entries")
        
        # Update state machine table
        state_machine = status.get('state_machine', {})
        self.state_table.setRowCount(len(state_machine))
        
        for i, (key, value) in enumerate(state_machine.items()):
            self.state_table.setItem(i, 0, QTableWidgetItem(key))
            self.state_table.setItem(i, 1, QTableWidgetItem(value))
    
    def update_node_status(self, node_id, status):
        """Update the display for a specific node"""
        if node_id not in self.node_frames:
            return
        
        if "error" in status:
            self.node_frames[node_id]['state_label'].setText("State: ERROR")
            
            # Use appropriate colors for dark/light mode
            if self.dark_mode:
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #ff3333;")
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #3a2222; border: 1px solid #ff3333;")
            else:
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #d32f2f;")
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #ffebee; border: 1px solid #d32f2f;")
            return
        
        # Update frame style based on state
        if status['state'] == "LEADER":
            if self.dark_mode:
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #1e3b2f; border: 2px solid #4caf50;")
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #4caf50;")
            else:
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #e8f5e9; border: 2px solid #4caf50;")
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #2e7d32;")
        elif status['state'] == "CANDIDATE":
            if self.dark_mode:
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #3e3629; border: 1px solid #ffb74d;")
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #ffb74d;")
            else:
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #fff8e1; border: 1px solid #ff9800;")
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #ef6c00;")
        else:  # FOLLOWER
            if self.dark_mode:
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #1e2a3b; border: 1px solid #64b5f6;")
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #64b5f6;")
            else:
                self.node_frames[node_id]['frame'].setStyleSheet("background-color: #e3f2fd; border: 1px solid #2196f3;")
                self.node_frames[node_id]['state_label'].setStyleSheet("color: #1565c0;")
        
        # Update node info
        self.node_frames[node_id]['state_label'].setText(f"State: {status['state']}")
        self.node_frames[node_id]['term_label'].setText(f"Term: {status['current_term']}")
        self.node_frames[node_id]['commit_label'].setText(f"Commit: {status['committed_index']}")
        self.node_frames[node_id]['applied_label'].setText(f"Applied: {status['last_applied']}")
        self.node_frames[node_id]['log_label'].setText(f"Log: {status['log_size']} entries")
        
        # Update health info
        health = self.monitor.get_node_health(node_id)
        if "error" in health:
            self.node_frames[node_id]['health_label'].setText("Health: ERROR")
            if self.dark_mode:
                self.node_frames[node_id]['health_label'].setStyleSheet("color: #ff3333;")
            else:
                self.node_frames[node_id]['health_label'].setStyleSheet("color: #d32f2f;")
        else:
            self.node_frames[node_id]['health_label'].setText(f"Health: {health['status']}")
            if health['status'] == "SERVING":
                if self.dark_mode:
                    self.node_frames[node_id]['health_label'].setStyleSheet("color: #4caf50;")
                else:
                    self.node_frames[node_id]['health_label'].setStyleSheet("color: #2e7d32;")
            else:
                if self.dark_mode:
                    self.node_frames[node_id]['health_label'].setStyleSheet("color: #ff3333;")
                else:
                    self.node_frames[node_id]['health_label'].setStyleSheet("color: #d32f2f;")
            
            # Update uptime
            uptime = health['uptime_seconds']
            if uptime < 60:
                uptime_str = f"{uptime} seconds"
            elif uptime < 3600:
                uptime_str = f"{uptime // 60} minutes"
            else:
                uptime_str = f"{uptime // 3600} hours, {(uptime % 3600) // 60} minutes"
            
            self.node_frames[node_id]['uptime_label'].setText(f"Uptime: {uptime_str}")
        
        # Update peer info
        # First clear old peers
        for i in reversed(range(self.node_frames[node_id]['peer_layout'].count())):
            self.node_frames[node_id]['peer_layout'].itemAt(i).widget().setParent(None)
        
        # Add peer status
        for peer in status['peers']:
            peer_id = peer['node_id']
            is_connected = peer['is_connected']
            last_contact = peer['last_contact_ms']
            
            # Format last contact time
            if last_contact == 0:
                contact_str = "Never"
            elif last_contact < 1000:
                contact_str = "Just now"
            else:
                contact_str = f"{last_contact // 1000} seconds ago"
            
            peer_label = QLabel(f"Node {peer_id}: {'Connected' if is_connected else 'Disconnected'} ({contact_str})")
            
            if is_connected:
                if self.dark_mode:
                    peer_label.setStyleSheet("color: #4caf50;")
                else:
                    peer_label.setStyleSheet("color: #2e7d32;")
            else:
                if self.dark_mode:
                    peer_label.setStyleSheet("color: #ff3333;")
                else:
                    peer_label.setStyleSheet("color: #d32f2f;")
            
            self.node_frames[node_id]['peer_layout'].addWidget(peer_label)
    
    def display_error(self, error_msg):
        """Display an error in the status bar"""
        self.statusBar().showMessage(f'Error: {error_msg}')
    
    def closeEvent(self, event):
        """Handle window close event"""
        self.monitor.stop()
        event.accept()
    
    def toggle_dark_mode(self):
        """Toggle dark mode on/off"""
        # If system theme is enabled, disable it
        if self.system_theme_action.isChecked():
            self.system_theme_action.setChecked(False)
        
        self.dark_mode = self.dark_mode_action.isChecked()
        
        if self.dark_mode:
            QApplication.setPalette(self.dark_palette)
        else:
            QApplication.setPalette(self.light_palette)
            
    def use_system_theme(self):
        """Set theme based on system preference"""
        if self.system_theme_action.isChecked():
            # Detect system theme
            system_dark_mode = self.is_dark_mode_enabled()
            
            # Update dark mode action to match system
            self.dark_mode_action.setChecked(system_dark_mode)
            self.dark_mode = system_dark_mode
            
            # Apply appropriate theme
            if self.dark_mode:
                QApplication.setPalette(self.dark_palette)
            else:
                QApplication.setPalette(self.light_palette)
    
    def restart_monitor(self):
        """Restart monitoring with fresh connections"""
        # Stop current monitoring
        self.monitor.stop()
        
        # Clear existing node frames
        for i in reversed(range(self.nodes_grid.count())):
            widget = self.nodes_grid.itemAt(i).widget()
            if widget:
                widget.setParent(None)
                
        # Create a new monitor
        self.monitor = RaftMonitor()
        
        # Reconnect signals
        self.monitor.signals.cluster_status_updated.connect(self.update_cluster_status)
        self.monitor.signals.node_status_updated.connect(self.update_node_status)
        self.monitor.signals.error_occurred.connect(self.display_error)
        
        # Recreate node displays
        self.node_frames = {}
        self.create_node_displays()
        
        # Update status bar
        self.statusBar().showMessage('Monitor restarted')
    
    def stop_node(self, node_id):
        """Handle stop node button click"""
        reply = QMessageBox.question(
            self, 
            'Confirm Stop', 
            f'Are you sure you want to stop Node {node_id}?',
            QMessageBox.Yes | QMessageBox.No, 
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            success = self.monitor.stop_node(node_id)
            if success:
                self.statusBar().showMessage(f'Node {node_id} stopped successfully')
            else:
                self.statusBar().showMessage(f'Failed to stop Node {node_id}')
    
    def start_node(self, node_id):
        """Handle start node button click"""
        if not self.monitor.is_local_node(node_id):
            QMessageBox.warning(
                self, 
                'Cannot Start Node', 
                f'Node {node_id} is not on this machine. You can only start nodes running on your local machine.',
                QMessageBox.Ok
            )
            return
            
        success = self.monitor.start_node(node_id)
        if success:
            self.statusBar().showMessage(f'Node {node_id} started successfully')
        else:
            self.statusBar().showMessage(f'Failed to start Node {node_id}')


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = RaftMonitorUI()
    window.show()
    sys.exit(app.exec_())
