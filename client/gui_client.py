import sys
import os
import grpc
import uuid
import time
import argparse
import logging
from typing import Optional, Tuple, Any, NoReturn, List, Dict, Union, cast
from datetime import datetime, timedelta

from PyQt5.QtWidgets import (  # type: ignore
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QMessageBox,
    QStackedWidget,
    QFormLayout,
    QGroupBox,
    QStatusBar,
    QListWidget,
    QListWidgetItem,
    QTextEdit,
    QSplitter,
    QFrame,
    QScrollArea,
    QMenu,
    QAction,
    QDialog,
    QDialogButtonBox,
)
from PyQt5.QtCore import Qt, pyqtSignal, QSize, QTimer, QThread, QPoint  # type: ignore
from PyQt5.QtGui import QFont, QIcon, QPixmap, QColor, QCloseEvent, QPalette  # type: ignore

# Add the parent directory to sys.path to import server modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.chat.chat_pb2 import (
    CreateAccountRequest,
    LoginRequest,
    ListAccountsRequest,
    SendMessageRequest,
    GetMessagesRequest,
    DeleteMessagesRequest,
    AccountInfo,
    Message,
    LogoutRequest,
    MarkConversationAsReadRequest,
    DeleteAccountRequest,
)
from server.chat.chat_pb2_grpc import ChatServiceStub
from server.constants import ErrorMessage, SuccessMessage

# Create logs directory if it doesn't exist
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG if needed
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(log_dir, "client.log")
        ),  # Save logs to the logs directory
        logging.StreamHandler(),  # Print logs to the console
    ],
)

logger = logging.getLogger("client")  # Use a specific logger for clarity


# Global settings
CLIENT_SETTINGS = {
    "host": "localhost",
    "port": 8000,
    "enable_logging": False,
}


# Logger mixin that can be used by any class that needs to log gRPC messages
class GrpcLoggerMixin:
    def __init__(self, enable_logging: bool = False) -> None:
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
                    os.path.join(log_dir, "protocol_metrics_client.log")
                )
                protocol_handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                    )
                )
                self.protocol_logger.addHandler(protocol_handler)

            self.protocol_logger.info("Protocol metrics logging enabled in client")
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


class MessageWidget(QFrame):
    """Widget to display a single message."""

    delete_requested = pyqtSignal(str)  # Signal to emit when delete is requested

    def __init__(
        self, message: Message, is_from_me: bool = False, is_dark_mode: bool = False
    ) -> None:
        super().__init__()
        self.message = message
        self.is_from_me = is_from_me
        self.is_dark_mode = is_dark_mode
        self.init_ui()

        # Enable context menu
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)

    def init_ui(self) -> None:
        # Set frame style
        self.setFrameShape(QFrame.StyledPanel)
        self.setFrameShadow(QFrame.Raised)

        # Set background color based on sender and theme (iMessage-like)
        if self.message.deleted:
            # Deleted message style
            if self.is_dark_mode:
                self.setStyleSheet(
                    "background-color: rgba(58, 58, 60, 0.7); color: rgba(255, 255, 255, 0.7); border-radius: 10px; padding: 4px;"
                )
            else:
                self.setStyleSheet(
                    "background-color: rgba(229, 229, 234, 0.7); color: rgba(0, 0, 0, 0.7); border-radius: 10px; padding: 4px;"
                )
        elif self.is_from_me:
            if self.is_dark_mode:
                self.setStyleSheet(
                    "background-color: #0A84FF; color: white; border-radius: 10px; padding: 4px;"
                )
            else:
                self.setStyleSheet(
                    "background-color: #007AFF; color: white; border-radius: 10px; padding: 4px;"
                )
        else:
            if self.is_dark_mode:
                self.setStyleSheet(
                    "background-color: #3A3A3C; color: white; border-radius: 10px; padding: 4px;"
                )
            else:
                self.setStyleSheet(
                    "background-color: #E5E5EA; color: black; border-radius: 10px; padding: 4px;"
                )

        # Create layout
        layout = QVBoxLayout()
        layout.setContentsMargins(4, 2, 4, 2)  # Further reduced margins
        layout.setSpacing(1)  # Further reduced spacing

        # Add message content
        if self.message.deleted:
            content_label = QLabel("<i>This message was deleted</i>")
        else:
            content_label = QLabel(self.message.content)

        content_label.setWordWrap(True)
        content_font = QFont()
        content_font.setPointSize(12)
        content_label.setFont(content_font)
        layout.addWidget(content_label)

        # Add timestamp
        timestamp = datetime.fromtimestamp(
            self.message.timestamp / 1000
        )  # Convert from milliseconds
        time_str = timestamp.strftime("%I:%M %p")
        time_label = QLabel(f"<small>{time_str}</small>")
        time_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        if self.is_from_me:
            time_label.setStyleSheet("color: rgba(255, 255, 255, 0.7);")
        else:
            if self.is_dark_mode:
                time_label.setStyleSheet("color: rgba(255, 255, 255, 0.5);")
            else:
                time_label.setStyleSheet("color: rgba(0, 0, 0, 0.5);")
        layout.addWidget(time_label)

        self.setLayout(layout)
        self.setMaximumWidth(350)

    def show_context_menu(self, position: QPoint) -> None:
        """Show context menu on right-click."""
        # Only show delete option for non-deleted messages that were sent by the current user
        if not self.message.deleted and self.is_from_me:
            context_menu = QMenu(self)
            delete_action = QAction("Delete", self)
            delete_action.triggered.connect(self.request_delete)
            context_menu.addAction(delete_action)
            context_menu.exec_(self.mapToGlobal(position))

    def request_delete(self) -> None:
        """Emit signal to request message deletion."""
        self.delete_requested.emit(self.message.message_id)


class ChatWidget(QWidget, GrpcLoggerMixin):
    """Widget for displaying and interacting with chat."""

    def __init__(
        self,
        session_token: str,
        username: str,
        stub: ChatServiceStub,
        enable_logging: bool = False,
    ) -> None:
        QWidget.__init__(self)
        GrpcLoggerMixin.__init__(self, enable_logging)

        self.session_token = session_token
        self.username = username
        self.stub = stub
        self.selected_user: Optional[str] = None
        self.users: Dict[str, bool] = {}  # username -> online status
        self.messages: List[Message] = []
        self.last_update: float = 0.0  # Store as float for time.time() compatibility
        self.update_in_progress = False
        self.unread_counts: Dict[str, int] = {}  # username -> unread message count

        # Start with dark mode detection
        self.is_dark_mode = self._is_dark_mode()

        self.init_ui()

        # Set up polling timer
        self.poll_timer = QTimer(self)
        self.poll_timer.timeout.connect(self.poll_for_updates)
        self.poll_timer.start(1000)  # Poll every second

    def _is_dark_mode(self) -> bool:
        """Detect if system is using dark mode."""
        try:
            app = QApplication.instance()
            if app and isinstance(app, QApplication):
                palette = app.palette()
                bg_color = palette.color(QPalette.Window)
                # If background is dark, assume dark mode
                return bool(bg_color.lightness() < 128)
        except Exception:
            # If there's any error with palette detection, default to light mode
            pass
        return False

    def init_ui(self) -> None:
        # Main layout
        main_layout = QHBoxLayout()
        main_layout.setContentsMargins(
            0, 0, 0, 0
        )  # Remove margins to make layout flush
        main_layout.setSpacing(0)  # Remove spacing between chat view and sidebar

        # Set theme colors
        if self.is_dark_mode:
            bg_color = "#1C1C1E"  # Dark background
            text_color = "#FFFFFF"  # White text
            input_bg = "#2C2C2E"  # Slightly lighter input background
            border_color = "#3A3A3C"  # Border color
        else:
            bg_color = "#F5F5F5"  # Light gray background
            text_color = "#000000"  # Black text
            input_bg = "#FFFFFF"  # White input background
            border_color = "#E5E5EA"  # Border color

        # Set widget style
        self.setStyleSheet(
            f"""
            QWidget {{ 
                background-color: {bg_color}; 
                color: {text_color}; 
            }}
            QGroupBox {{ 
                border: 1px solid {border_color}; 
                border-radius: 5px; 
                margin-top: 1ex; 
            }}
            QGroupBox::title {{ 
                subcontrol-origin: margin; 
                left: 10px; 
                padding: 0 3px 0 3px; 
            }}
            QTextEdit, QLineEdit {{ 
                background-color: {input_bg}; 
                border: 1px solid {border_color}; 
                border-radius: 4px; 
                padding: 2px; 
            }}
            QPushButton {{ 
                background-color: {input_bg}; 
                border: 1px solid {border_color}; 
                border-radius: 4px; 
                padding: 4px 8px; 
            }}
            QPushButton:hover {{ 
                background-color: {'#3A3A3C' if self.is_dark_mode else '#E5E5EA'}; 
            }}
        """
        )

        # Chat area (left side)
        chat_area = QWidget()
        chat_layout = QVBoxLayout()
        chat_layout.setContentsMargins(10, 10, 10, 10)  # Add inner padding to chat area
        chat_layout.setSpacing(10)  # Add spacing between elements in chat area

        # Messages display
        self.messages_area = QScrollArea()
        self.messages_area.setWidgetResizable(True)
        self.messages_area.setStyleSheet(
            f"""
            QScrollArea {{
                background-color: {bg_color}; 
                border: none;
                border-radius: 4px;
            }}
            QScrollBar:vertical {{
                background: {bg_color};
                width: 8px;
                margin: 0px;
            }}
            QScrollBar::handle:vertical {{
                background: {'#3A3A3C' if self.is_dark_mode else '#CCCCCC'};
                min-height: 20px;
                border-radius: 4px;
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0px;
            }}
        """
        )

        self.messages_container = QWidget()
        self.messages_container.setStyleSheet(f"background-color: {bg_color};")
        self.messages_layout = QVBoxLayout(self.messages_container)
        self.messages_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.messages_layout.setSpacing(4)  # Further reduced spacing between messages
        self.messages_layout.setContentsMargins(
            0, 0, 0, 0
        )  # Remove padding to make flush
        self.messages_container.setLayout(self.messages_layout)
        self.messages_area.setWidget(self.messages_container)
        chat_layout.addWidget(self.messages_area, 1)

        # Message input area
        input_layout = QHBoxLayout()
        input_layout.setContentsMargins(0, 5, 0, 0)  # Add some top padding
        input_layout.setSpacing(8)  # Add spacing between input and button

        # Replace QTextEdit with QLineEdit for single-line input
        self.message_input = QLineEdit()
        self.message_input.setPlaceholderText("Type a message...")
        self.message_input.setStyleSheet(
            f"""
            QLineEdit {{
                background-color: {input_bg};
                border: 1px solid {border_color};
                border-radius: 4px;
                padding: 8px;
            }}
        """
        )
        # Connect Enter key to send message
        self.message_input.returnPressed.connect(self.send_message)
        input_layout.addWidget(self.message_input, 1)

        send_button = QPushButton("Send")
        send_button.clicked.connect(self.send_message)
        send_button.setMinimumWidth(70)
        send_button.setStyleSheet(
            f"""
            QPushButton {{
                background-color: {'#0A84FF' if self.is_dark_mode else '#007AFF'};
                color: white;
                border: none;
                border-radius: 4px;
                padding: 8px;
            }}
            QPushButton:hover {{
                background-color: {'#0070E0' if self.is_dark_mode else '#0062CC'};
            }}
        """
        )
        input_layout.addWidget(send_button)

        chat_layout.addLayout(input_layout)
        chat_area.setLayout(chat_layout)

        # Sidebar (right side)
        sidebar = QWidget()
        sidebar_layout = QVBoxLayout()
        sidebar_layout.setContentsMargins(
            10, 10, 10, 10
        )  # Add inner padding to sidebar
        sidebar_layout.setSpacing(10)  # Add spacing between elements in sidebar

        # Current user display
        current_user_group = QGroupBox("Logged in as")
        current_user_group.setStyleSheet(
            f"""
            QGroupBox {{
                border: 1px solid {border_color};
                border-radius: 6px;
                margin-top: 12px;
                padding-top: 8px;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 3px;
                color: {'#8E8E93' if self.is_dark_mode else '#666666'};
            }}
        """
        )

        current_user_layout = QVBoxLayout()
        current_user_layout.setContentsMargins(5, 5, 5, 5)
        self.current_user_label = QLabel(self.username)
        self.current_user_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        user_font = QFont()
        user_font.setBold(True)
        user_font.setPointSize(12)
        self.current_user_label.setFont(user_font)
        self.current_user_label.setStyleSheet(
            f"""
            QLabel {{
                color: {'#0A84FF' if self.is_dark_mode else '#007AFF'};
                padding: 5px;
            }}
        """
        )
        current_user_layout.addWidget(self.current_user_label)
        current_user_group.setLayout(current_user_layout)
        sidebar_layout.addWidget(current_user_group)

        # Users list
        users_group = QGroupBox("Users")
        users_group.setStyleSheet(
            f"""
            QGroupBox {{
                border: 1px solid {border_color};
                border-radius: 6px;
                margin-top: 12px;
                padding-top: 8px;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 3px;
                color: {'#8E8E93' if self.is_dark_mode else '#666666'};
            }}
        """
        )

        users_layout = QVBoxLayout()
        users_layout.setContentsMargins(5, 5, 5, 5)  # Add inner padding to users list

        # Add search input for users
        search_layout = QHBoxLayout()
        search_layout.setContentsMargins(0, 0, 0, 5)  # Add bottom margin

        self.user_search_input = QLineEdit()
        self.user_search_input.setPlaceholderText("Search users...")
        self.user_search_input.setStyleSheet(
            f"""
            QLineEdit {{
                background-color: {input_bg};
                border: 1px solid {border_color};
                border-radius: 4px;
                padding: 6px;
            }}
            """
        )
        self.user_search_input.textChanged.connect(self._filter_users)
        search_layout.addWidget(self.user_search_input)

        # Add clear button
        clear_button = QPushButton("✕")
        clear_button.setFixedSize(24, 24)
        clear_button.setStyleSheet(
            f"""
            QPushButton {{
                background-color: {'#3A3A3C' if self.is_dark_mode else '#E5E5EA'};
                color: {text_color};
                border: 1px solid {border_color};
                border-radius: 12px;
                padding: 0px;
                font-size: 10px;
            }}
            QPushButton:hover {{
                background-color: {'#2C2C2E' if self.is_dark_mode else '#D1D1D6'};
            }}
            """
        )
        clear_button.clicked.connect(lambda: self.user_search_input.clear())
        search_layout.addWidget(clear_button)

        users_layout.addLayout(search_layout)

        self.users_list = QListWidget()
        self.users_list.setStyleSheet(
            f"""
            QListWidget {{
                background-color: {input_bg};
                border: 1px solid {border_color};
                border-radius: 4px;
                padding: 2px;
            }}
            QListWidget::item {{
                padding: 6px;
                border-bottom: 1px solid {border_color};
            }}
            QListWidget::item:selected {{
                background-color: {'#0A84FF' if self.is_dark_mode else '#007AFF'};
                color: white;
                border-radius: 3px;
            }}
            QListWidget::item:hover:!selected {{
                background-color: {'#2C2C2E' if self.is_dark_mode else '#E5E5EA'};
                border-radius: 3px;
            }}
        """
        )
        self.users_list.itemClicked.connect(self.on_user_selected)
        users_layout.addWidget(self.users_list)

        users_group.setLayout(users_layout)
        sidebar_layout.addWidget(users_group, 1)

        # Logout and Delete Account buttons
        buttons_layout = QHBoxLayout()

        # Delete Account button
        delete_account_button = QPushButton("Delete Account")
        delete_account_button.clicked.connect(self.delete_account)
        delete_account_button.setStyleSheet(
            f"""
            QPushButton {{
                background-color: {'#FF453A' if self.is_dark_mode else '#FF3B30'};
                color: white;
                border: none;
                border-radius: 4px;
                padding: 8px;
            }}
            QPushButton:hover {{
                background-color: {'#D93B31' if self.is_dark_mode else '#E63028'};
            }}
        """
        )
        buttons_layout.addWidget(delete_account_button)

        # Logout button
        logout_button = QPushButton("Logout")
        logout_button.clicked.connect(self.logout)
        logout_button.setStyleSheet(
            f"""
            QPushButton {{
                background-color: {'#3A3A3C' if self.is_dark_mode else '#E5E5EA'};
                color: {text_color};
                border: 1px solid {border_color};
                border-radius: 4px;
                padding: 8px;
            }}
            QPushButton:hover {{
                background-color: {'#2C2C2E' if self.is_dark_mode else '#D1D1D6'};
            }}
        """
        )
        buttons_layout.addWidget(logout_button)

        sidebar_layout.addLayout(buttons_layout)

        sidebar.setLayout(sidebar_layout)
        sidebar.setMaximumWidth(200)

        # Add a vertical divider between chat area and sidebar
        divider = QFrame()
        divider.setFrameShape(QFrame.VLine)
        divider.setFrameShadow(QFrame.Sunken)
        divider.setStyleSheet(f"background-color: {border_color};")

        # Add chat area, divider, and sidebar to main layout
        main_layout.addWidget(chat_area, 1)
        main_layout.addWidget(divider)
        main_layout.addWidget(sidebar)

        self.setLayout(main_layout)

    def poll_for_updates(self) -> None:
        """Poll for updates from the server."""
        try:
            # Update users list
            self.update_users_list()

            # Update messages if a user is selected
            if self.selected_user:
                self.update_messages()

            self.last_update = time.time()
        except grpc.RpcError as e:
            print(f"Error polling for updates: {e}")

    def update_users_list(self) -> None:
        """Update the list of users."""
        try:
            request = ListAccountsRequest(
                session_token=self.session_token,
                page_size=100,  # Get up to 100 users
                page_number=0,
            )
            response = self.stub.ListAccounts(request)

            if response.error_message:
                print(f"Error getting users: {response.error_message}")
                return

            # Update our user status dictionary
            new_users = {}
            for user in response.accounts:
                if user.username != self.username:  # Skip current user
                    new_users[user.username] = user.is_online

            # Check if anything changed
            if self._users_changed(new_users):
                self.users = new_users
                self._refresh_users_list()

            # Get messages to update unread counts
            self.update_unread_counts()

        except grpc.RpcError as e:
            print(f"Error updating users list: {e}")

    def _users_changed(self, new_users: Dict[str, bool]) -> bool:
        """Check if the users list has changed."""
        if len(new_users) != len(self.users):
            return True

        for username, is_online in new_users.items():
            if username not in self.users or self.users[username] != is_online:
                return True

        return False

    def _refresh_users_list(self) -> None:
        """Refresh the users list UI."""
        # If there's an active search filter, use that instead
        if hasattr(self, "user_search_input") and self.user_search_input.text().strip():
            self._filter_users(self.user_search_input.text())
            return

        # Remember the currently selected user
        selected_username = self.selected_user

        # Clear the list
        self.users_list.clear()

        # Add users to the list
        selected_index = -1
        current_index = 0

        # First add online users
        for username, is_online in sorted(self.users.items()):
            if is_online:
                item = QListWidgetItem()
                # Add unread count if there are unread messages
                unread_count = self.unread_counts.get(username, 0)
                if unread_count > 0:
                    item.setText(f"● {username} ({unread_count})")
                    # Set bold font for unread messages
                    font_online = item.font()  # type: QFont
                    font_online.setBold(True)
                    item.setFont(font_online)
                else:
                    item.setText(f"● {username}")

                item.setForeground(
                    QColor("#34C759" if self.is_dark_mode else "green")
                )  # Green dot for online
                self.users_list.addItem(item)

                if username == selected_username:
                    selected_index = current_index

                current_index += 1

        # Then add offline users
        for username, is_online in sorted(self.users.items()):
            if not is_online:
                item = QListWidgetItem()
                # Add unread count if there are unread messages
                unread_count = self.unread_counts.get(username, 0)
                if unread_count > 0:
                    item.setText(f"○ {username} ({unread_count})")
                    # Set bold font for unread messages
                    font_offline = item.font()  # type: QFont
                    font_offline.setBold(True)
                    item.setFont(font_offline)
                else:
                    item.setText(f"○ {username}")

                item.setForeground(
                    QColor("#8E8E93" if self.is_dark_mode else "gray")
                )  # Gray circle for offline
                self.users_list.addItem(item)

                if username == selected_username:
                    selected_index = current_index

                current_index += 1

        # Restore selection if we had one
        if selected_index >= 0:
            self.users_list.setCurrentRow(selected_index)

    def update_unread_counts(self) -> None:
        """Update unread message counts for all users."""
        try:
            # Get all messages
            request = GetMessagesRequest(
                session_token=self.session_token,
                max_messages=1000,  # Get a large number of messages
            )
            response = self.stub.GetMessages(request)

            if response.error_message:
                print(f"Error getting messages: {response.error_message}")
                return

            # Count unread messages by sender
            unread_counts: Dict[str, int] = {}
            for msg in response.messages:
                # Only count messages where:
                # 1. Current user is the recipient
                # 2. Message is unread
                if msg.recipient == self.username and msg.unread:
                    sender = msg.sender
                    if sender not in unread_counts:
                        unread_counts[sender] = 0
                    unread_counts[sender] += 1

            # Update unread counts if changed
            if unread_counts != self.unread_counts:
                self.unread_counts = unread_counts
                self._refresh_users_list()

        except grpc.RpcError as e:
            print(f"Error updating unread counts: {e}")

    def update_messages(self) -> None:
        """Update the messages for the selected user with logging."""
        try:
            logger.info(f"Updating messages for selected user: {self.selected_user}")

            request = GetMessagesRequest(
                session_token=self.session_token,
                max_messages=100,  # Get up to 100 messages
            )

            self.log_message("Outgoing", "GetMessages", request)

            response = self.stub.GetMessages(request)

            self.log_message(
                "Incoming",
                "GetMessages Response",
                response,
                f"Messages: {len(response.messages)}",
            )

            if response.error_message:
                logger.warning(f"Error getting messages: {response.error_message}")
                return

            logger.info(f"Received {len(response.messages)} total messages from server")

            # Filter messages for the selected user
            filtered_messages = []
            has_unread = False
            for msg in response.messages:
                # Include messages where:
                # 1. Current user is sender and selected user is recipient
                # 2. Selected user is sender and current user is recipient
                if (
                    msg.sender == self.username and msg.recipient == self.selected_user
                ) or (
                    msg.sender == self.selected_user and msg.recipient == self.username
                ):
                    # Add deleted field if it doesn't exist (for backward compatibility)
                    if not hasattr(msg, "deleted"):
                        msg.deleted = False

                    filtered_messages.append(msg)

                    # Check if there are any unread messages from the selected user
                    if (
                        msg.sender == self.selected_user
                        and msg.recipient == self.username
                        and msg.unread
                    ):
                        has_unread = True

                    logger.debug(
                        f"Including message: {msg.sender} -> {msg.recipient}: {msg.content[:30]}... (deleted: {msg.deleted})"
                    )
                else:
                    logger.debug(
                        f"Filtering out message: {msg.sender} -> {msg.recipient}"
                    )

            logger.info(
                f"Filtered to {len(filtered_messages)} messages for conversation with {self.selected_user}"
            )

            # If there are unread messages and the chat is currently open, mark them as read
            if has_unread and self.selected_user is not None:
                logger.info(f"Marking conversation as read for {self.selected_user}")
                self.mark_conversation_as_read(self.selected_user)

            # Check if messages have changed before updating display
            if self._messages_changed(filtered_messages):
                self.messages = filtered_messages
                self._display_messages()
                logger.info(f"Updated display with {len(filtered_messages)} messages")
            else:
                logger.info("No changes in messages, skipping display update")

        except grpc.RpcError as e:
            logger.error(f"Error updating messages: {e}")

    def _messages_changed(self, new_messages: List[Message]) -> bool:
        """Check if the messages have changed."""
        if len(new_messages) != len(self.messages):
            return True

        for i, msg in enumerate(new_messages):
            if i >= len(self.messages):
                return True

            old_msg = self.messages[i]
            # Check if message ID or deleted status has changed
            if msg.message_id != old_msg.message_id or msg.deleted != old_msg.deleted:
                return True

        return False

    def _display_messages(self) -> None:
        """Display the messages in the UI."""
        # Save current scroll position before updating
        scroll_bar = self.messages_area.verticalScrollBar()
        current_scroll_position = scroll_bar.value() if scroll_bar else 0
        max_scroll_position = scroll_bar.maximum() if scroll_bar else 0
        # Determine if we were at the bottom before updating
        was_at_bottom = (
            current_scroll_position >= max_scroll_position - 20
        )  # Allow some margin

        # Clear current messages
        while self.messages_layout.count():
            item = self.messages_layout.takeAt(0)
            if item:
                widget = item.widget()
                if widget:
                    widget.deleteLater()
                layout = item.layout()
                if layout:
                    # Clear nested layouts
                    while layout.count():
                        nested_item = layout.takeAt(0)
                        if nested_item:
                            widget = nested_item.widget()
                            if widget:
                                widget.deleteLater()
                    # Remove the layout itself
                    if layout:
                        layout.setParent(None)

        # If there are no messages, just return after clearing
        if not self.messages:
            print(f"No messages to display for {self.selected_user}")
            return

        # Add date separator at the top
        first_msg_time = datetime.fromtimestamp(self.messages[0].timestamp / 1000)
        self._add_date_separator(first_msg_time)

        # Sort messages by timestamp
        sorted_messages = sorted(self.messages, key=lambda m: m.timestamp)

        # Group messages by date
        current_date = None

        # Add messages
        for msg in sorted_messages:
            # Check if we need a date separator
            msg_time = datetime.fromtimestamp(msg.timestamp / 1000)
            msg_date = msg_time.date()
            if current_date != msg_date:
                current_date = msg_date
                if (
                    sorted_messages.index(msg) > 0
                ):  # Don't add another separator for the first message
                    self._add_date_separator(msg_time)

            is_from_me = msg.sender == self.username
            message_widget = MessageWidget(msg, is_from_me, self.is_dark_mode)

            # Connect the delete_requested signal
            message_widget.delete_requested.connect(self.delete_message)

            # Align messages based on sender
            h_layout = QHBoxLayout()
            if is_from_me:
                h_layout.addStretch()
                h_layout.addWidget(message_widget)
                h_layout.setContentsMargins(
                    80, 1, 5, 1
                )  # Further reduced vertical margins
            else:
                h_layout.addWidget(message_widget)
                h_layout.addStretch()
                h_layout.setContentsMargins(
                    5, 1, 80, 1
                )  # Further reduced vertical margins

            self.messages_layout.addLayout(h_layout)

        # Scroll to the appropriate position
        QTimer.singleShot(
            100,
            lambda: self._restore_scroll_position(
                was_at_bottom, current_scroll_position
            ),
        )

    def _restore_scroll_position(
        self, was_at_bottom: bool, previous_position: int
    ) -> None:
        """Restore the scroll position after updating messages."""
        scrollbar = self.messages_area.verticalScrollBar()

        if was_at_bottom:
            # If we were at the bottom before, scroll to bottom again
            if scrollbar:
                scrollbar.setValue(scrollbar.maximum())
        else:
            # Otherwise try to maintain the previous scroll position
            # Note: The actual position might be different due to content changes
            if scrollbar:
                scrollbar.setValue(previous_position)

    def _add_date_separator(self, date: datetime) -> None:
        """Add a date separator to the messages layout."""
        # Format the date
        today = datetime.now().date()
        yesterday = (datetime.now() - timedelta(days=1)).date()

        if date.date() == today:
            date_str = "Today"
        elif date.date() == yesterday:
            date_str = "Yesterday"
        else:
            date_str = date.strftime("%b %d, %Y")  # More compact date format

        # Create the separator
        separator_layout = QHBoxLayout()
        separator_layout.setContentsMargins(
            0, 2, 0, 2
        )  # Further reduced vertical margins

        line_left = QFrame()
        line_left.setFrameShape(QFrame.HLine)
        line_left.setFrameShadow(QFrame.Sunken)
        line_left.setStyleSheet(
            f"background-color: {'#3A3A3C' if self.is_dark_mode else '#CCCCCC'};"
        )

        date_label = QLabel(date_str)
        date_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        if self.is_dark_mode:
            date_label.setStyleSheet(
                "color: #8E8E93; background-color: #2C2C2E; padding: 1px 6px; border-radius: 6px; font-size: 9px;"
            )
        else:
            date_label.setStyleSheet(
                "color: #666666; background-color: #F5F5F5; padding: 1px 6px; border-radius: 6px; font-size: 9px;"
            )

        line_right = QFrame()
        line_right.setFrameShape(QFrame.HLine)
        line_right.setFrameShadow(QFrame.Sunken)
        line_right.setStyleSheet(
            f"background-color: {'#3A3A3C' if self.is_dark_mode else '#CCCCCC'};"
        )

        separator_layout.addWidget(line_left)
        separator_layout.addWidget(date_label)
        separator_layout.addWidget(line_right)

        self.messages_layout.addLayout(separator_layout)

    def on_user_selected(self, item: QListWidgetItem) -> None:
        """Handle user selection from the list."""
        # Extract username from the item text (remove online/offline indicator and unread count)
        display_name = item.text()
        username = ""

        if display_name.startswith("● "):
            # Online user
            username = display_name[2:]
        elif display_name.startswith("○ "):
            # Offline user
            username = display_name[2:]
        else:
            username = display_name

        # Remove unread count if present
        if " (" in username:
            username = username.split(" (")[0]

        # Only update if the selection has changed
        if username != self.selected_user:
            print(f"Switching from {self.selected_user} to {username}")
            self.selected_user = username

            # Clear messages immediately
            self.messages = []
            self._display_messages()  # This will clear the message area

            # Mark conversation as read
            self.mark_conversation_as_read(username)

            # Fetch messages for the selected user
            self.update_messages()

            # Always scroll to bottom when selecting a new user
            QTimer.singleShot(
                100,
                lambda: self._scroll_to_bottom(),
            )

    def _scroll_to_bottom(self) -> None:
        """Helper method to safely scroll to bottom."""
        scroll_bar = self.messages_area.verticalScrollBar()
        if scroll_bar is not None:
            scroll_bar.setValue(scroll_bar.maximum())

    def send_message(self) -> None:
        """Send a message to the selected user with logging."""
        if not self.selected_user:
            QMessageBox.warning(
                self, "No Recipient", "Please select a user to message."
            )
            logger.warning("Attempted to send a message without selecting a recipient.")
            return

        message_text = self.message_input.text().strip()
        if not message_text:
            return

        try:
            logger.info(
                f"Attempting to send message to {self.selected_user}: {message_text[:50]}{'...' if len(message_text) > 50 else ''}"
            )

            request = SendMessageRequest(
                session_token=self.session_token,
                recipient=self.selected_user,
                content=message_text,
            )

            self.log_message(
                "Outgoing", "SendMessage", request, f"To: {self.selected_user}"
            )

            response = self.stub.SendMessage(request)

            self.log_message(
                "Incoming",
                "SendMessage Response",
                response,
                f"Success: {response.success}",
            )

            if response.success:
                logger.info(
                    f"Message sent successfully to {self.selected_user}, message_id: {response.message_id}"
                )

                # Clear input field
                self.message_input.clear()

                # Force an immediate update and scroll to bottom
                self.update_messages()
                QTimer.singleShot(100, lambda: self._scroll_to_bottom())
            else:
                logger.warning(
                    f"Failed to send message to {self.selected_user}: {response.error_message}"
                )

                # Check if the error is because the recipient's account was deleted
                if "user's account has been deleted" in response.error_message:
                    logger.warning(
                        f"User '{self.selected_user}' has deleted their account. Cannot send messages."
                    )

                    QMessageBox.warning(
                        self,
                        "User Deleted",
                        f"The user '{self.selected_user}' has deleted their account. You cannot send messages to this user anymore.",
                    )

                    # Force a refresh of the users list to remove the deleted user
                    self.last_update = 0
                    self.poll_for_updates()

                    # Clear the selected user
                    self.selected_user = None

                    # Clear the messages area
                    self.messages = []
                    self._display_messages()
                else:
                    QMessageBox.warning(self, "Send Failed", response.error_message)

        except grpc.RpcError as e:
            logger.error(f"GRPC SendMessage Error: {str(e)}")
            QMessageBox.critical(self, "Send Failed", f"Server error: {str(e)}")

    def logout(self) -> None:
        """Handle logout button click."""
        reply = QMessageBox.question(
            self,
            "Logout",
            "Are you sure you want to logout?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            # Call the Logout RPC method
            try:
                request = LogoutRequest(session_token=self.session_token)
                response = self.stub.Logout(request)

                if not response.success:
                    print(f"Warning: Logout failed: {response.error_message}")
            except Exception as e:
                print(f"Error during logout: {e}")

            # Stop polling
            self.poll_timer.stop()

            # Exit the application
            QApplication.quit()

    def mark_conversation_as_read(self, other_user: str) -> None:
        """Mark all messages in a conversation as read."""
        try:
            request = MarkConversationAsReadRequest(
                session_token=self.session_token,
                other_user=other_user,
            )
            response = self.stub.MarkConversationAsRead(request)

            if not response.success:
                print(f"Error marking conversation as read: {response.error_message}")
                return

            # Update unread counts locally immediately
            if other_user in self.unread_counts:
                self.unread_counts[other_user] = 0
                self._refresh_users_list()
                print(f"Marked all messages from {other_user} as read")

        except grpc.RpcError as e:
            print(f"Error marking conversation as read: {e}")

    def delete_message(self, message_id: str) -> None:
        """Delete a message."""
        try:
            print(f"Deleting message with ID: {message_id}")
            request = DeleteMessagesRequest(
                session_token=self.session_token,
                message_ids=[message_id],
            )
            response = self.stub.DeleteMessages(request)

            if response.success:
                print(f"Message deleted successfully")
                # Force an immediate update to refresh the UI
                print("Forcing immediate update of messages after deletion")

                # Reset the last update time to force an immediate poll
                self.last_update = 0

                # Trigger an immediate poll for updates
                self.poll_for_updates()
            else:
                print(f"Failed to delete message: {response.error_message}")
                if response.failed_message_ids:
                    print(f"Failed message IDs: {response.failed_message_ids}")

                # Show the error message to the user
                error_title = "Delete Failed"
                if (
                    "You can only delete messages that you sent"
                    in response.error_message
                ):
                    error_title = "Permission Denied"

                QMessageBox.warning(self, error_title, response.error_message)

        except grpc.RpcError as e:
            print(f"Error deleting message: {e}")
            QMessageBox.critical(self, "Delete Failed", f"Server error: {str(e)}")

    def _filter_users(self, text: str) -> None:
        """Filter users based on the provided pattern matching algorithm."""
        search_pattern = text.strip().lower()

        # If search is empty, show all users
        if not search_pattern:
            self._refresh_users_list()
            return

        # Remember the currently selected user
        selected_username = self.selected_user

        # Clear the list
        self.users_list.clear()

        # Filter and add users to the list
        selected_index = -1
        current_index = 0

        # First add online users that match the pattern
        for username, is_online in sorted(self.users.items()):
            # Apply the pattern matching algorithm
            if self._matches_pattern(username.lower(), search_pattern) and is_online:
                item = QListWidgetItem()
                # Add unread count if there are unread messages
                unread_count = self.unread_counts.get(username, 0)
                if unread_count > 0:
                    item.setText(f"● {username} ({unread_count})")
                    # Set bold font for unread messages
                    font_online = item.font()  # type: QFont
                    font_online.setBold(True)
                    item.setFont(font_online)
                else:
                    item.setText(f"● {username}")

                item.setForeground(
                    QColor("#34C759" if self.is_dark_mode else "green")
                )  # Green dot for online
                self.users_list.addItem(item)

                if username == selected_username:
                    selected_index = current_index

                current_index += 1

        # Then add offline users that match the pattern
        for username, is_online in sorted(self.users.items()):
            # Apply the pattern matching algorithm
            if (
                self._matches_pattern(username.lower(), search_pattern)
                and not is_online
            ):
                item = QListWidgetItem()
                # Add unread count if there are unread messages
                unread_count = self.unread_counts.get(username, 0)
                if unread_count > 0:
                    item.setText(f"○ {username} ({unread_count})")
                    # Set bold font for unread messages
                    font_offline = item.font()  # type: QFont
                    font_offline.setBold(True)
                    item.setFont(font_offline)
                else:
                    item.setText(f"○ {username}")

                item.setForeground(
                    QColor("#8E8E93" if self.is_dark_mode else "gray")
                )  # Gray circle for offline
                self.users_list.addItem(item)

                if username == selected_username:
                    selected_index = current_index

                current_index += 1

        # Restore selection if we had one
        if selected_index >= 0:
            self.users_list.setCurrentRow(selected_index)

    def _matches_pattern(self, string: str, pattern: str) -> bool:
        """
        Check if string matches the pattern according to the specified algorithm.

        The algorithm checks if the characters in the pattern appear in the same order
        (but not necessarily consecutively) in the string.
        """
        if len(string) < len(pattern):
            return False
        p = 0
        for s in range(len(string)):
            if pattern[p] == string[s]:
                p += 1
            if p == len(pattern):
                return True
        return False

    def delete_account(self) -> None:
        """Handle delete account button click."""
        reply = QMessageBox.warning(
            self,
            "Delete Account",
            "Are you sure you want to delete your account? This action cannot be undone!",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            # Create a custom dialog with QDialog instead of QMessageBox
            confirm_dialog = QDialog(self)
            confirm_dialog.setWindowTitle("Confirm Account Deletion")
            confirm_dialog.setMinimumWidth(400)

            # Create layout
            layout = QVBoxLayout()

            # Add warning icon and text
            warning_label = QLabel(
                "⚠️ WARNING: ALL your data will be permanently deleted!"
            )
            warning_label.setStyleSheet("font-weight: bold; color: red;")
            layout.addWidget(warning_label)

            info_label = QLabel("This action cannot be undone.")
            layout.addWidget(info_label)

            confirm_label = QLabel("Type 'DELETE' to confirm:")
            layout.addWidget(confirm_label)

            # Add text input
            text_input = QLineEdit()
            text_input.setPlaceholderText("Type DELETE here")
            layout.addWidget(text_input)

            # Add buttons
            button_box = QDialogButtonBox(QDialogButtonBox.Cancel)
            delete_button = button_box.addButton(
                "Delete My Account", QDialogButtonBox.AcceptRole
            )

            # Style the delete button
            if self.is_dark_mode:
                if delete_button:
                    delete_button.setStyleSheet("color: #FF453A;")
            else:
                if delete_button:
                    delete_button.setStyleSheet("color: #FF3B30;")

            # Disable delete button initially
            if delete_button:
                delete_button.setEnabled(False)

            # Connect signals
            button_box.rejected.connect(confirm_dialog.reject)
            button_box.accepted.connect(confirm_dialog.accept)

            # Enable delete button only when "DELETE" is typed
            def check_text() -> None:
                if delete_button:
                    delete_button.setEnabled(text_input.text() == "DELETE")

            text_input.textChanged.connect(check_text)

            layout.addWidget(button_box)
            confirm_dialog.setLayout(layout)

            # Show dialog and get result
            result = confirm_dialog.exec_()

            # If user confirmed and typed DELETE correctly
            if result == QDialog.Accepted and text_input.text() == "DELETE":
                # Call the DeleteAccount RPC method
                try:
                    request = DeleteAccountRequest(session_token=self.session_token)
                    response = self.stub.DeleteAccount(request)

                    if response.success:
                        QMessageBox.information(
                            self,
                            "Account Deleted",
                            "Your account has been successfully deleted.",
                        )
                        # Stop polling
                        self.poll_timer.stop()
                        # Exit the application
                        QApplication.quit()
                    else:
                        QMessageBox.warning(
                            self, "Delete Failed", response.error_message
                        )
                except Exception as e:
                    print(f"Error during account deletion: {e}")
                    QMessageBox.critical(
                        self, "Delete Failed", f"Server error: {str(e)}"
                    )


class AuthWidget(QWidget, GrpcLoggerMixin):
    """Widget for handling authentication (login and registration)."""

    login_successful = pyqtSignal(
        str, str, ChatServiceStub
    )  # session_token, username, stub

    def __init__(self, enable_logging: bool = False) -> None:
        QWidget.__init__(self)
        GrpcLoggerMixin.__init__(self, enable_logging)

        self.init_ui()
        self.stub: Optional[ChatServiceStub] = None

        # Connect to server automatically after a short delay
        QTimer.singleShot(100, self.connect_to_server)

    def init_ui(self) -> None:
        # Main layout
        main_layout = QVBoxLayout()
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(30, 30, 30, 30)

        # App title
        title_label = QLabel("Chat Application")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_font = QFont()
        title_font.setPointSize(24)
        title_font.setBold(True)
        title_label.setFont(title_font)
        main_layout.addWidget(title_label)

        # Auth form
        auth_group = QGroupBox("Authentication")
        auth_layout = QFormLayout()

        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("Enter your username")
        auth_layout.addRow(QLabel("Username:"), self.username_input)

        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("Enter your password")
        self.password_input.setEchoMode(QLineEdit.Password)
        auth_layout.addRow(QLabel("Password:"), self.password_input)

        # Buttons layout
        buttons_layout = QHBoxLayout()

        login_button = QPushButton("Login")
        login_button.setDefault(True)  # Make it the default button (Enter key)
        login_button.clicked.connect(self.handle_login)
        buttons_layout.addWidget(login_button)

        register_button = QPushButton("Register")
        register_button.clicked.connect(self.handle_register)
        buttons_layout.addWidget(register_button)

        auth_layout.addRow("", buttons_layout)

        auth_group.setLayout(auth_layout)
        main_layout.addWidget(auth_group)

        # Status label
        self.status_label = QLabel("Connecting to server...")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(self.status_label)

        self.setLayout(main_layout)

    def connect_to_server(self) -> None:
        """Connect to the gRPC server."""
        try:
            # Use the host and port from settings
            server_address = f"{CLIENT_SETTINGS['host']}:{CLIENT_SETTINGS['port']}"
            if CLIENT_SETTINGS["enable_logging"]:
                logger.info(f"Connecting to server at {server_address}")

            self.status_label.setText(f"Connecting to server at {server_address}...")

            # Create channel with timeout
            self.channel = grpc.insecure_channel(server_address)
            self.stub = ChatServiceStub(self.channel)

            # Test connection by making a simple request with a timeout
            test_request = ListAccountsRequest(session_token="test_connection")
            if self.stub:  # Type checking
                self.stub.ListAccounts(test_request, timeout=3)

            self.status_label.setText(f"Connected to server at {server_address}")
            if CLIENT_SETTINGS["enable_logging"]:
                logger.info(f"Successfully connected to server at {server_address}")
        except grpc.RpcError as e:
            # If we get an "invalid session" error, that means we connected to the server
            # but the request was rejected due to authentication, which is expected
            if "Invalid or expired session" in str(e):
                self.status_label.setText(f"Connected to server at {server_address}")
                if CLIENT_SETTINGS["enable_logging"]:
                    logger.info(f"Successfully connected to server at {server_address}")
            else:
                error_msg = f"Failed to connect to server: {str(e)}"
                self.status_label.setText(f"Connection error: {error_msg}")
                if CLIENT_SETTINGS["enable_logging"]:
                    logger.error(error_msg)

                # Provide more specific error messages based on common failure modes
                user_msg = "Failed to connect to server."
                if "deadline exceeded" in str(e).lower():
                    user_msg = f"Connection timeout. Server at {server_address} is not responding."
                elif "failed to connect to all addresses" in str(e).lower():
                    user_msg = f"Could not reach server at {server_address}. Check if the server is running."
                elif "connection refused" in str(e).lower():
                    user_msg = f"Connection refused by {server_address}. Check if the server is running on the specified port."

                QMessageBox.critical(self, "Connection Failed", user_msg)
        except Exception as e:
            error_msg = f"Failed to connect to server: {str(e)}"
            self.status_label.setText(f"Connection error: {error_msg}")
            if CLIENT_SETTINGS["enable_logging"]:
                logger.error(error_msg)
            QMessageBox.critical(self, "Connection Failed", error_msg)

    def handle_login(self) -> None:
        """Handle login button click."""
        if not self.stub:
            QMessageBox.warning(
                self, "Not Connected", "Please wait for server connection."
            )
            return

        username = self.username_input.text().strip()
        password = self.password_input.text()

        if not username or not password:
            QMessageBox.warning(
                self, "Login Failed", "Please enter both username and password."
            )
            return

        try:
            request = LoginRequest(username=username, password=password)
            self.log_message("Outgoing", "Login", request, f"User: {username}")

            response = self.stub.Login(request)
            self.log_message(
                "Incoming", "Login Response", response, f"Success: {response.success}"
            )

            if response.success:
                self.login_successful.emit(response.session_token, username, self.stub)
            else:
                QMessageBox.warning(self, "Login Failed", response.error_message)
        except grpc.RpcError as e:
            QMessageBox.critical(self, "Login Failed", f"Server error: {str(e)}")

    def handle_register(self) -> None:
        """Handle register button click."""
        if not self.stub:
            QMessageBox.warning(
                self, "Not Connected", "Please wait for server connection."
            )
            return

        username = self.username_input.text().strip()
        password = self.password_input.text()

        if not username or not password:
            QMessageBox.warning(
                self, "Registration Failed", "Please enter both username and password."
            )
            return

        try:
            request = CreateAccountRequest(username=username, password=password)
            self.log_message("Outgoing", "CreateAccount", request, f"User: {username}")

            response = self.stub.CreateAccount(request)
            self.log_message(
                "Incoming",
                "CreateAccount Response",
                response,
                f"Success: {response.success}",
            )

            if response.success:
                QMessageBox.information(
                    self,
                    "Registration Successful",
                    "Account created successfully. You can now log in.",
                )
            else:
                QMessageBox.warning(self, "Registration Failed", response.error_message)
        except grpc.RpcError as e:
            QMessageBox.critical(self, "Registration Failed", f"Server error: {str(e)}")


class ChatApp(QMainWindow):
    """Main window for the chat application."""

    def __init__(self, enable_logging: bool = False) -> None:
        super().__init__()
        self.session_token: Optional[str] = None
        self.username: Optional[str] = None
        self.stub: Optional[ChatServiceStub] = None
        self.enable_logging = enable_logging
        self.init_ui()

    def init_ui(self) -> None:
        """Initialize the user interface."""
        self.setWindowTitle("Chat Application")
        self.setGeometry(100, 100, 1200, 800)

        # Status bar to show connection status
        status_bar = self.statusBar()
        if status_bar is not None:
            status_bar.showMessage("Not connected")

        # Create stacked widget to switch between login and chat
        self.stacked_widget = QStackedWidget()
        self.setCentralWidget(self.stacked_widget)

        # Create and add auth widget
        self.auth_widget = AuthWidget(enable_logging=self.enable_logging)
        self.auth_widget.login_successful.connect(self.on_login_successful)
        self.stacked_widget.addWidget(self.auth_widget)

        self.show()

    def on_login_successful(
        self, session_token: str, username: str, stub: ChatServiceStub
    ) -> None:
        """Handle successful login."""
        self.session_token = session_token
        self.username = username
        self.stub = stub

        # Create and show chat widget
        self.chat_widget = ChatWidget(
            session_token, username, stub, enable_logging=self.enable_logging
        )
        self.stacked_widget.addWidget(self.chat_widget)
        self.stacked_widget.setCurrentWidget(self.chat_widget)

        # Update status bar
        status_bar = self.statusBar()
        if status_bar is not None:
            status_bar.showMessage(f"Logged in as {username}")

    def closeEvent(self, event: Optional[QCloseEvent]) -> None:
        """Handle window close event."""
        # Only attempt logout if user is logged in
        if hasattr(self, "chat_widget") and self.session_token:
            try:
                # Call the Logout RPC method
                request = LogoutRequest(session_token=self.session_token)
                if self.stub:  # Type checking
                    response = self.stub.Logout(request)
                    print(
                        f"Logout on window close: {'Success' if response.success else 'Failed'}"
                    )
            except Exception as e:
                print(f"Error during logout on window close: {e}")

        # Accept the close event
        if event is not None:
            event.accept()


def main() -> None:
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Chat Client")
    parser.add_argument(
        "--host", default="localhost", help="Server host address to connect to"
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="Server port number to connect to"
    )
    parser.add_argument(
        "--enable-logging", action="store_true", help="Enable protocol metrics logging"
    )

    args = parser.parse_args()

    # Check if we're running under pytest (which sets PYTEST_CURRENT_TEST env var)
    is_test_environment = "PYTEST_CURRENT_TEST" in os.environ
    # If we're in a test environment, automatically enable logging
    if is_test_environment and not args.enable_logging:
        args.enable_logging = True
        logger.info(
            "Test environment detected, automatically enabling protocol metrics logging"
        )

    # Update global settings
    CLIENT_SETTINGS["host"] = args.host
    CLIENT_SETTINGS["port"] = args.port
    CLIENT_SETTINGS["enable_logging"] = args.enable_logging

    # Configure loggers based on command line arguments
    if args.enable_logging:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)

    # Initialize Qt application
    app = QApplication(sys.argv)
    chat_app = ChatApp(enable_logging=args.enable_logging)
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
