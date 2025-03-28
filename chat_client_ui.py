#!/usr/bin/env python3
import os
import sys
import time
import argparse
from datetime import datetime

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QMessageBox, QListWidget,
    QStackedWidget, QTextEdit, QSplitter, QFrame, QDialog,
    QFormLayout, QDialogButtonBox, QListWidgetItem, QScrollArea,
    QMenu, QSystemTrayIcon
)
from PyQt6.QtCore import Qt, QSize, QTimer, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QColor, QAction, QIcon

from chat_client import ChatClientBackend

class LoginDialog(QDialog):
    """Dialog for user login"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Login")
        self.setMinimumWidth(350)
        
        self.layout = QVBoxLayout()
        
        # Form for login fields
        self.form_layout = QFormLayout()
        
        self.username_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        
        self.form_layout.addRow("Username:", self.username_input)
        self.form_layout.addRow("Password:", self.password_input)
        
        self.layout.addLayout(self.form_layout)
        
        # Buttons
        self.button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | 
            QDialogButtonBox.StandardButton.Cancel
        )
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        
        # Register button
        self.register_button = QPushButton("Register")
        self.button_box.addButton(self.register_button, QDialogButtonBox.ButtonRole.ActionRole)
        
        self.layout.addWidget(self.button_box)
        
        self.setLayout(self.layout)
    
    def get_credentials(self):
        """Get entered username and password"""
        return self.username_input.text(), self.password_input.text()

class RegisterDialog(QDialog):
    """Dialog for user registration"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Register")
        self.setMinimumWidth(350)
        
        self.layout = QVBoxLayout()
        
        # Form for registration fields
        self.form_layout = QFormLayout()
        
        self.username_input = QLineEdit()
        self.display_name_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.confirm_password_input = QLineEdit()
        self.confirm_password_input.setEchoMode(QLineEdit.EchoMode.Password)
        
        self.form_layout.addRow("Username:", self.username_input)
        self.form_layout.addRow("Display Name:", self.display_name_input)
        self.form_layout.addRow("Password:", self.password_input)
        self.form_layout.addRow("Confirm Password:", self.confirm_password_input)
        
        self.layout.addLayout(self.form_layout)
        
        # Buttons
        self.button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | 
            QDialogButtonBox.StandardButton.Cancel
        )
        self.button_box.accepted.connect(self.validate_and_accept)
        self.button_box.rejected.connect(self.reject)
        
        self.layout.addWidget(self.button_box)
        
        self.setLayout(self.layout)
    
    def validate_and_accept(self):
        """Validate inputs before accepting"""
        username = self.username_input.text()
        display_name = self.display_name_input.text()
        password = self.password_input.text()
        confirm_password = self.confirm_password_input.text()
        
        if not username:
            QMessageBox.warning(self, "Validation Error", "Username is required")
            return
        
        if not password:
            QMessageBox.warning(self, "Validation Error", "Password is required")
            return
        
        if password != confirm_password:
            QMessageBox.warning(self, "Validation Error", "Passwords do not match")
            return
        
        self.accept()
    
    def get_registration_data(self):
        """Get entered registration data"""
        return (
            self.username_input.text(),
            self.password_input.text(),
            self.display_name_input.text() or self.username_input.text()
        )

class DeleteAccountDialog(QDialog):
    """Dialog for account deletion confirmation"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Delete Account")
        self.setMinimumWidth(350)
        
        self.layout = QVBoxLayout()
        
        # Warning label
        self.warning_label = QLabel(
            "WARNING: This will permanently delete your account and all your messages. "
            "This action cannot be undone."
        )
        self.warning_label.setWordWrap(True)
        self.warning_label.setStyleSheet("color: red; font-weight: bold;")
        
        self.layout.addWidget(self.warning_label)
        
        # Confirm with password
        self.form_layout = QFormLayout()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.form_layout.addRow("Confirm Password:", self.password_input)
        
        self.layout.addLayout(self.form_layout)
        
        # Buttons
        self.button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | 
            QDialogButtonBox.StandardButton.Cancel
        )
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        
        # Make the OK button red
        ok_button = self.button_box.button(QDialogButtonBox.StandardButton.Ok)
        ok_button.setText("Delete My Account")
        ok_button.setStyleSheet("background-color: #ff5555; color: white;")
        
        self.layout.addWidget(self.button_box)
        
        self.setLayout(self.layout)
    
    def get_password(self):
        """Get entered password"""
        return self.password_input.text()

class MessageBubble(QWidget):
    """Custom widget for message bubble (like iMessage)"""
    delete_requested = pyqtSignal(str)  # Signal for message deletion
    
    def __init__(self, message_id, content, sender_id, timestamp, current_user_id, parent=None):
        super().__init__(parent)
        self.message_id = message_id
        self.sender_id = sender_id
        self.content = content
        self.timestamp = timestamp
        self.is_own_message = sender_id == current_user_id
        
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        
        # Message bubble
        msg_layout = QHBoxLayout()
        
        # Main message text
        self.msg_text = QLabel(self.content)
        self.msg_text.setWordWrap(True)
        self.msg_text.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        
        # Style based on sender
        if self.is_own_message:
            self.msg_text.setStyleSheet(
                "background-color: #0b93f6; color: white; "
                "border-radius: 10px; padding: 10px;"
            )
            msg_layout.addStretch()
            msg_layout.addWidget(self.msg_text)
        else:
            self.msg_text.setStyleSheet(
                "background-color: #e5e5ea; color: black; "
                "border-radius: 10px; padding: 10px;"
            )
            msg_layout.addWidget(self.msg_text)
            msg_layout.addStretch()
        
        layout.addLayout(msg_layout)
        
        # Timestamp
        time_layout = QHBoxLayout()
        dt = datetime.fromtimestamp(self.timestamp)
        time_str = dt.strftime("%I:%M %p")
        self.time_label = QLabel(time_str)
        self.time_label.setStyleSheet("color: #888888; font-size: 8pt;")
        
        if self.is_own_message:
            time_layout.addStretch()
            time_layout.addWidget(self.time_label)
        else:
            time_layout.addWidget(self.time_label)
            time_layout.addStretch()
        
        layout.addLayout(time_layout)
        
        self.setLayout(layout)
        
        # Context menu for message actions
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)
    
    def show_context_menu(self, pos):
        """Show context menu for message actions"""
        # Only allow deletion of own messages
        if self.is_own_message:
            context_menu = QMenu(self)
            delete_action = QAction("Delete Message", self)
            delete_action.triggered.connect(self.request_deletion)
            context_menu.addAction(delete_action)
            context_menu.exec(self.mapToGlobal(pos))
    
    def request_deletion(self):
        """Emit signal requesting message deletion"""
        self.delete_requested.emit(self.message_id)

class ChatWindow(QMainWindow):
    """Main chat application window"""
    def __init__(self, config_path):
        super().__init__()
        
        # Initialize backend client
        self.client = ChatClientBackend(config_path)
        
        # Connect signals
        self.client.login_status_changed.connect(self.on_login_status_changed)
        self.client.users_updated.connect(self.on_users_updated)
        self.client.online_users_updated.connect(self.on_online_users_updated)
        self.client.unread_counts_updated.connect(self.on_unread_counts_updated)
        self.client.message_deleted.connect(self.on_message_deleted)
        
        # Instance variables
        self.current_chat_user_id = None
        self.unread_counts = {}
        
        self.init_ui()
    
    def init_ui(self):
        """Initialize the user interface"""
        self.setWindowTitle("Raft Chat")
        self.setGeometry(100, 100, 800, 600)
        
        # Main layout with contacts on left, chat on right
        self.main_widget = QWidget()
        self.setCentralWidget(self.main_widget)
        
        self.main_layout = QVBoxLayout(self.main_widget)
        
        # Stacked widget for login/main screens
        self.stacked_widget = QStackedWidget()
        
        # Login screen
        self.login_widget = QWidget()
        self.login_layout = QVBoxLayout(self.login_widget)
        
        self.welcome_label = QLabel("Welcome to Raft Chat")
        self.welcome_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.welcome_label.setFont(QFont("Arial", 20))
        
        self.login_button = QPushButton("Login")
        self.login_button.clicked.connect(self.show_login_dialog)
        
        self.register_button = QPushButton("Register")
        self.register_button.clicked.connect(self.show_register_dialog)
        
        self.login_layout.addStretch()
        self.login_layout.addWidget(self.welcome_label)
        self.login_layout.addWidget(self.login_button)
        self.login_layout.addWidget(self.register_button)
        self.login_layout.addStretch()
        
        # Main chat screen with splitter
        self.chat_widget = QWidget()
        self.chat_layout = QHBoxLayout(self.chat_widget)
        
        # Splitter for resizable panes
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left pane: contacts
        self.contacts_widget = QWidget()
        self.contacts_layout = QVBoxLayout(self.contacts_widget)
        
        # User info section
        self.user_info_layout = QHBoxLayout()
        self.username_label = QLabel("Not logged in")
        self.logout_button = QPushButton("Logout")
        self.logout_button.clicked.connect(self.logout)
        self.user_info_layout.addWidget(self.username_label)
        self.user_info_layout.addWidget(self.logout_button)
        
        # Settings button
        self.settings_button = QPushButton("Settings")
        self.settings_button.clicked.connect(self.show_settings_menu)
        self.user_info_layout.addWidget(self.settings_button)
        
        self.contacts_layout.addLayout(self.user_info_layout)
        
        # Search bar
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search Contacts")
        self.search_input.textChanged.connect(self.filter_contacts)
        self.contacts_layout.addWidget(self.search_input)
        
        # Contacts list
        self.contacts_list = QListWidget()
        self.contacts_list.currentItemChanged.connect(self.on_contact_selected)
        self.contacts_layout.addWidget(self.contacts_list)
        
        # Right pane: chat
        self.chat_content_widget = QWidget()
        self.chat_content_layout = QVBoxLayout(self.chat_content_widget)
        
        # Chat header
        self.chat_header_layout = QHBoxLayout()
        self.chat_user_label = QLabel("Select a contact")
        self.chat_user_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.chat_header_layout.addWidget(self.chat_user_label)
        self.chat_header_layout.addStretch()
        
        # Online status
        self.online_status_label = QLabel()
        self.chat_header_layout.addWidget(self.online_status_label)
        
        self.chat_content_layout.addLayout(self.chat_header_layout)
        
        # Message display area
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        
        self.messages_container = QWidget()
        self.messages_layout = QVBoxLayout(self.messages_container)
        self.messages_layout.addStretch()  # Push messages to bottom
        
        self.scroll_area.setWidget(self.messages_container)
        self.chat_content_layout.addWidget(self.scroll_area)
        
        # Message input
        self.message_input_layout = QHBoxLayout()
        
        self.message_input = QTextEdit()
        self.message_input.setPlaceholderText("Type a message...")
        self.message_input.setMaximumHeight(80)
        
        self.send_button = QPushButton("Send")
        self.send_button.clicked.connect(self.send_message)
        
        self.message_input_layout.addWidget(self.message_input)
        self.message_input_layout.addWidget(self.send_button)
        
        self.chat_content_layout.addLayout(self.message_input_layout)
        
        # Add panes to splitter
        self.splitter.addWidget(self.contacts_widget)
        self.splitter.addWidget(self.chat_content_widget)
        
        # Set split proportions
        self.splitter.setSizes([200, 600])
        
        self.chat_layout.addWidget(self.splitter)
        
        # Add both screens to stacked widget
        self.stacked_widget.addWidget(self.login_widget)
        self.stacked_widget.addWidget(self.chat_widget)
        
        self.main_layout.addWidget(self.stacked_widget)
        
        # Show login screen by default
        self.stacked_widget.setCurrentIndex(0)
        
        # Set up a timer to periodically check for new messages
        self.message_check_timer = QTimer(self)
        self.message_check_timer.timeout.connect(self.check_current_chat_messages)
        self.message_check_timer.start(5000)  # Check every 5 seconds
    
    def show_login_dialog(self):
        """Show the login dialog"""
        dialog = LoginDialog(self)
        dialog.register_button.clicked.connect(lambda: (dialog.reject(), self.show_register_dialog()))
        
        if dialog.exec():
            username, password = dialog.get_credentials()
            if username and password:
                self.login(username, password)
    
    def show_register_dialog(self):
        """Show the registration dialog"""
        dialog = RegisterDialog(self)
        
        if dialog.exec():
            username, password, display_name = dialog.get_registration_data()
            self.register(username, password, display_name)
    
    def show_settings_menu(self):
        """Show settings menu"""
        menu = QMenu(self)
        
        # Account settings
        delete_account_action = QAction("Delete Account", self)
        delete_account_action.triggered.connect(self.show_delete_account_dialog)
        menu.addAction(delete_account_action)
        
        # Show menu at settings button
        menu.exec(self.settings_button.mapToGlobal(self.settings_button.rect().bottomLeft()))
    
    def show_delete_account_dialog(self):
        """Show dialog to confirm account deletion"""
        dialog = DeleteAccountDialog(self)
        
        if dialog.exec():
            password = dialog.get_password()
            if password:
                success, message = self.client.delete_account(password)
                if success:
                    QMessageBox.information(self, "Account Deleted", message)
                    self.stacked_widget.setCurrentIndex(0)
                else:
                    QMessageBox.warning(self, "Error", message)
    
    def login(self, username, password):
        """Log in to the server"""
        success, message = self.client.login(username, password)
        if success:
            self.username_label.setText(f"Logged in as: {username}")
            self.stacked_widget.setCurrentIndex(1)
            
            # Initialize contact list
            self.refresh_contacts()
        else:
            QMessageBox.warning(self, "Login Failed", message)
    
    def register(self, username, password, display_name):
        """Register a new user"""
        success, message = self.client.register(username, password, display_name)
        if success:
            QMessageBox.information(self, "Registration Successful", message)
            self.login(username, password)
        else:
            QMessageBox.warning(self, "Registration Failed", message)
    
    def logout(self):
        """Log out of the server"""
        success, message = self.client.logout()
        if success:
            self.stacked_widget.setCurrentIndex(0)
            self.clear_chat()
            self.contacts_list.clear()
            self.current_chat_user_id = None
        else:
            QMessageBox.warning(self, "Logout Error", message)
    
    def refresh_contacts(self):
        """Refresh the contacts list"""
        self.client.get_all_users()
    
    def filter_contacts(self, text):
        """Filter contacts list by search text"""
        for i in range(self.contacts_list.count()):
            item = self.contacts_list.item(i)
            user_id = item.data(Qt.ItemDataRole.UserRole)
            user_data = self.client.users_cache.get(user_id, {})
            display_name = user_data.get('display_name', '')
            username = user_data.get('username', '')
            
            # Show if either display name or username contains search text
            if (text.lower() in display_name.lower() or 
                text.lower() in username.lower()):
                item.setHidden(False)
            else:
                item.setHidden(True)
    
    def on_login_status_changed(self, is_logged_in, message):
        """Handle login status change"""
        if not is_logged_in and self.stacked_widget.currentIndex() == 1:
            # Force logout if server says we're not logged in
            self.stacked_widget.setCurrentIndex(0)
            QMessageBox.information(self, "Session Ended", message)
    
    def on_users_updated(self, users):
        """Handle updated user list"""
        self.contacts_list.clear()
        
        # Current user ID to exclude from list
        current_user_id = self.client.user_id
        
        for user in users:
            user_id = user.get('user_id')
            
            # Skip current user
            if user_id == current_user_id:
                continue
            
            display_name = user.get('display_name')
            is_online = user.get('online', False)
            unread_count = self.unread_counts.get(user_id, 0)
            
            # Create list item
            item = QListWidgetItem()
            
            # Set display text with unread count if any
            if unread_count > 0:
                item.setText(f"{display_name} ({unread_count})")
                # Highlight unread
                item.setBackground(QColor("#e6f7ff"))
            else:
                item.setText(display_name)
            
            # Set online status icon
            if is_online:
                item.setIcon(QIcon.fromTheme("user-available", QIcon()))
            else:
                item.setIcon(QIcon.fromTheme("user-offline", QIcon()))
            
            # Store user ID as data
            item.setData(Qt.ItemDataRole.UserRole, user_id)
            
            self.contacts_list.addItem(item)
    
    def on_online_users_updated(self, users):
        """Handle updated online users list"""
        online_user_ids = [user.get('user_id') for user in users]
        
        # Update the online status in the contacts list
        for i in range(self.contacts_list.count()):
            item = self.contacts_list.item(i)
            user_id = item.data(Qt.ItemDataRole.UserRole)
            
            if user_id in online_user_ids:
                item.setIcon(QIcon.fromTheme("user-available", QIcon()))
            else:
                item.setIcon(QIcon.fromTheme("user-offline", QIcon()))
            
            # Also update online indicator of current chat
            if user_id == self.current_chat_user_id:
                if user_id in online_user_ids:
                    self.online_status_label.setText("🟢 Online")
                    self.online_status_label.setStyleSheet("color: green;")
                else:
                    self.online_status_label.setText("⚫ Offline")
                    self.online_status_label.setStyleSheet("color: gray;")
    
    def on_unread_counts_updated(self, unread_counts):
        """Handle updated unread message counts"""
        self.unread_counts = unread_counts
        
        # Update the contacts list with unread counts
        for i in range(self.contacts_list.count()):
            item = self.contacts_list.item(i)
            user_id = item.data(Qt.ItemDataRole.UserRole)
            display_name = self.client.get_user_display_name(user_id)
            unread_count = unread_counts.get(user_id, 0)
            
            if unread_count > 0:
                item.setText(f"{display_name} ({unread_count})")
                item.setBackground(QColor("#e6f7ff"))
            else:
                item.setText(display_name)
                item.setBackground(QColor("transparent"))
    
    def on_contact_selected(self, current, previous):
        """Handle contact selection"""
        if not current:
            return
        
        user_id = current.data(Qt.ItemDataRole.UserRole)
        display_name = self.client.get_user_display_name(user_id)
        
        self.current_chat_user_id = user_id
        self.chat_user_label.setText(display_name)
        
        # Check online status
        is_online = False
        if user_id in self.client.users_cache:
            is_online = self.client.users_cache[user_id].get('online', False)
        
        if is_online:
            self.online_status_label.setText("🟢 Online")
            self.online_status_label.setStyleSheet("color: green;")
        else:
            self.online_status_label.setText("⚫ Offline")
            self.online_status_label.setStyleSheet("color: gray;")
        
        # Load messages for this conversation
        self.load_chat_messages(user_id)
    
    def load_chat_messages(self, user_id):
        """Load messages for the conversation with a user"""
        self.clear_chat()
        
        # Get messages
        success, message, messages = self.client.get_messages(user_id)
        
        if success:
            for msg in messages:
                self.add_message_bubble(
                    msg.get('message_id'),
                    msg.get('content'),
                    msg.get('sender_id'),
                    msg.get('timestamp')
                )
            
            # Scroll to bottom
            self.scroll_area.verticalScrollBar().setValue(
                self.scroll_area.verticalScrollBar().maximum()
            )
        else:
            # Show error in chat
            error_label = QLabel(f"Error loading messages: {message}")
            error_label.setStyleSheet("color: red;")
            error_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.messages_layout.addWidget(error_label)
    
    def check_current_chat_messages(self):
        """Check for new messages in the current chat"""
        if not self.current_chat_user_id:
            return
            
        # Only reload if we're in an active chat
        self.load_chat_messages(self.current_chat_user_id)
    
    def add_message_bubble(self, message_id, content, sender_id, timestamp):
        """Add a message bubble to the chat"""
        bubble = MessageBubble(
            message_id, 
            content, 
            sender_id, 
            timestamp, 
            self.client.user_id
        )
        bubble.delete_requested.connect(self.delete_message)
        
        # Add to layout
        self.messages_layout.addWidget(bubble)
    
    def clear_chat(self):
        """Clear the chat display"""
        # Delete all widgets from messages layout except the stretch at the beginning
        while self.messages_layout.count() > 1:
            item = self.messages_layout.itemAt(1)
            if item:
                widget = item.widget()
                if widget:
                    widget.deleteLater()
                self.messages_layout.removeItem(item)
    
    def send_message(self):
        """Send a message"""
        if not self.current_chat_user_id:
            QMessageBox.warning(self, "Error", "No contact selected")
            return
        
        content = self.message_input.toPlainText().strip()
        if not content:
            return
        
        success, message, message_id = self.client.send_message(
            self.current_chat_user_id, 
            content
        )
        
        if success:
            # Add to UI (the backend signal will handle this)
            self.message_input.clear()
        else:
            QMessageBox.warning(self, "Error", f"Failed to send message: {message}")
    
    def delete_message(self, message_id):
        """Delete a message"""
        reply = QMessageBox.question(
            self, 
            "Confirm Delete", 
            "Are you sure you want to delete this message?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            success, message = self.client.delete_message(message_id)
            
            if success:
                # Refresh the chat
                self.load_chat_messages(self.current_chat_user_id)
            else:
                QMessageBox.warning(self, "Error", f"Failed to delete message: {message}")
    
    def on_message_deleted(self, message_id):
        """Handle message deletion event"""
        if self.current_chat_user_id:
            self.load_chat_messages(self.current_chat_user_id)
    
    def closeEvent(self, event):
        """Handle window close event"""
        # Try to logout
        if self.client.is_connected:
            self.client.logout()
        event.accept()

def main():
    parser = argparse.ArgumentParser(description="Raft Chat Client")
    parser.add_argument("--config", "-c", type=str, default="config.json", help="Path to config file")
    
    args = parser.parse_args()
    
    app = QApplication(sys.argv)
    window = ChatWindow(args.config)
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 