import sys
import os
import time
import json
from datetime import datetime
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                           QLabel, QPushButton, QLineEdit, QTextEdit, QListWidget, QListWidgetItem,
                           QTabWidget, QStackedWidget, QMessageBox, QSplitter, QDialog, QComboBox,
                           QDialogButtonBox, QFormLayout, QToolButton, QFrame, QMenu, QScrollArea, QScrollBar)
from PyQt5.QtCore import Qt, QSize, QTimer, pyqtSlot
from PyQt5.QtGui import QIcon, QColor, QFont, QPixmap

from chat_client_utils import ChatClientAPI
from generated import replication_pb2

class LoginDialog(QDialog):
    """Dialog for user login and registration"""
    
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client = client
        self.initUI()
    
    def initUI(self):
        self.setWindowTitle("iMessage Chat - Login")
        self.setMinimumWidth(400)
        
        layout = QVBoxLayout()
        
        # Add logo or welcome image
        logo_label = QLabel()
        logo_label.setAlignment(Qt.AlignCenter)
        # Use a blank placeholder for the logo
        logo_label.setText("iMessage Chat")
        logo_label.setStyleSheet("font-size: 24px; font-weight: bold; color: #0084ff; margin: 20px;")
        layout.addWidget(logo_label)
        
        # Tab widget for login/register
        self.tab_widget = QTabWidget()
        
        # Login tab
        login_widget = QWidget()
        login_layout = QFormLayout()
        
        self.login_username = QLineEdit()
        self.login_password = QLineEdit()
        self.login_password.setEchoMode(QLineEdit.Password)
        
        login_layout.addRow("Username:", self.login_username)
        login_layout.addRow("Password:", self.login_password)
        
        login_button = QPushButton("Login")
        login_button.setStyleSheet("background-color: #0084ff; color: white; padding: 8px;")
        login_button.clicked.connect(self.login)
        
        login_layout.addRow("", login_button)
        login_widget.setLayout(login_layout)
        
        # Register tab
        register_widget = QWidget()
        register_layout = QFormLayout()
        
        self.register_username = QLineEdit()
        self.register_password = QLineEdit()
        self.register_password.setEchoMode(QLineEdit.Password)
        self.register_display_name = QLineEdit()
        
        register_layout.addRow("Username:", self.register_username)
        register_layout.addRow("Password:", self.register_password)
        register_layout.addRow("Display Name:", self.register_display_name)
        
        register_button = QPushButton("Register")
        register_button.setStyleSheet("background-color: #0084ff; color: white; padding: 8px;")
        register_button.clicked.connect(self.register)
        
        register_layout.addRow("", register_button)
        register_widget.setLayout(register_layout)
        
        # Add tabs
        self.tab_widget.addTab(login_widget, "Login")
        self.tab_widget.addTab(register_widget, "Register")
        
        layout.addWidget(self.tab_widget)
        
        # Status message
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: red;")
        layout.addWidget(self.status_label)
        
        self.setLayout(layout)
    
    def login(self):
        """Handle login button click"""
        username = self.login_username.text().strip()
        password = self.login_password.text()
        
        if not username or not password:
            self.status_label.setText("Username and password are required")
            return
        
        # Attempt login
        result = self.client.login(username, password)
        
        if result["success"]:
            self.accept()  # Close dialog with accept status
        else:
            self.status_label.setText(result["message"])
    
    def register(self):
        """Handle register button click"""
        username = self.register_username.text().strip()
        password = self.register_password.text()
        display_name = self.register_display_name.text().strip()
        
        if not username or not password:
            self.status_label.setText("Username and password are required")
            return
        
        if not display_name:
            display_name = username
        
        # Attempt registration
        result = self.client.register_user(username, password, display_name)
        
        if result["success"]:
            QMessageBox.information(self, "Registration Successful", 
                "Registration successful! You can now login with your credentials.")
            
            # Switch to login tab and pre-fill username
            self.tab_widget.setCurrentIndex(0)
            self.login_username.setText(username)
            self.login_password.setText("")
        else:
            self.status_label.setText(result["message"])


class ChatBubble(QFrame):
    """Custom widget for chat message bubbles"""
    
    def __init__(self, message, is_sent, parent=None):
        super().__init__(parent)
        self.message = message
        self.is_sent = is_sent
        self.initUI()
    
    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 5, 10, 5)
        
        # Message content
        content = QLabel(self.message["content"])
        content.setWordWrap(True)
        content.setTextInteractionFlags(Qt.TextSelectableByMouse)
        
        # Style based on sent/received and read status
        if self.is_sent:
            # Outgoing message
            self.setStyleSheet("background-color: #0084ff; border-radius: 10px; color: white;")
            layout.setAlignment(Qt.AlignRight)
        else:
            # Incoming message
            self.setStyleSheet("background-color: #e5e5ea; border-radius: 10px; color: black;")
            layout.setAlignment(Qt.AlignLeft)
        
        # Show "deleted" for deleted messages
        if self.message.get("deleted", False):
            content.setText("This message has been deleted")
            content.setStyleSheet("color: #999;")
        
        layout.addWidget(content)
        
        # Add timestamp
        timestamp = datetime.fromtimestamp(self.message["timestamp"])
        time_str = timestamp.strftime("%H:%M")
        
        time_label = QLabel(time_str)
        time_label.setStyleSheet("color: rgba(255, 255, 255, 0.7);" if self.is_sent else "color: rgba(0, 0, 0, 0.5);")
        time_label.setAlignment(Qt.AlignRight if self.is_sent else Qt.AlignLeft)
        
        # Add "read" indicator for sent messages
        if self.is_sent and self.message.get("read", False):
            time_label.setText(f"Read • {time_str}")
        
        layout.addWidget(time_label)
        
        self.setLayout(layout)
        
        # Add right-click menu for message actions
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.showContextMenu)
    
    def showContextMenu(self, position):
        """Show context menu for message actions"""
        menu = QMenu()
        
        # Only show delete for sent messages that aren't already deleted
        if self.is_sent and not self.message.get("deleted", False):
            delete_action = menu.addAction("Delete Message")
            delete_action.triggered.connect(self.deleteMessage)
        
        # Add copy action
        copy_action = menu.addAction("Copy Text")
        copy_action.triggered.connect(self.copyText)
        
        menu.exec_(self.mapToGlobal(position))
    
    def deleteMessage(self):
        """Delete this message"""
        # Emit signal to delete message
        self.parent().parent().parent().parent().parent().deleteMessage(self.message["message_id"])
    
    def copyText(self):
        """Copy message text to clipboard"""
        if not self.message.get("deleted", False):
            QApplication.clipboard().setText(self.message["content"])


class ChatWidget(QWidget):
    """Widget for a single chat conversation"""
    
    def __init__(self, client, user, parent=None):
        super().__init__(parent)
        self.client = client
        self.user = user
        self.messages = []
        self.loading = False
        self.initUI()
        
        # Connect to client signals
        self.client.messageReceived.connect(self.onMessageUpdate)
    
    def initUI(self):
        # Main layout
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Messages area
        self.messages_area = QWidget()
        self.messages_layout = QVBoxLayout()
        self.messages_layout.setAlignment(Qt.AlignBottom)
        self.messages_layout.addStretch()
        self.messages_area.setLayout(self.messages_layout)
        
        # Scroll area for messages
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.messages_area)
        
        # Loading indicator
        self.loading_label = QLabel("Loading messages...")
        self.loading_label.setAlignment(Qt.AlignCenter)
        self.loading_label.setStyleSheet("color: gray;")
        self.loading_label.setVisible(False)
        
        # Input area
        input_widget = QWidget()
        input_layout = QHBoxLayout()
        
        self.message_input = QTextEdit()
        self.message_input.setPlaceholderText("Type a message...")
        self.message_input.setMaximumHeight(80)
        
        send_button = QPushButton("Send")
        send_button.setStyleSheet("background-color: #0084ff; color: white;")
        send_button.clicked.connect(self.sendMessage)
        
        input_layout.addWidget(self.message_input)
        input_layout.addWidget(send_button)
        
        input_widget.setLayout(input_layout)
        
        # Add to main layout
        layout.addWidget(self.loading_label, 0)
        layout.addWidget(self.scroll_area, 1)
        layout.addWidget(input_widget, 0)
        
        self.setLayout(layout)
        
        # Connect enter key in message input
        self.message_input.installEventFilter(self)
    
    def eventFilter(self, obj, event):
        """Handle events for child widgets"""
        if obj == self.message_input and event.type() == event.KeyPress:
            if event.key() == Qt.Key_Return and not event.modifiers() & Qt.ShiftModifier:
                self.sendMessage()
                return True
        return super().eventFilter(obj, event)
    
    def sendMessage(self):
        """Send a message"""
        content = self.message_input.toPlainText().strip()
        if not content:
            return
        
        result = self.client.send_message(self.user["user_id"], content)
        
        if result["success"]:
            self.message_input.clear()
            self.loadMessages()
        else:
            QMessageBox.warning(self, "Error", f"Failed to send message: {result['message']}")
    
    def loadMessages(self):
        """Load messages for this conversation"""
        if self.loading:
            return
            
        self.loading = True
        self.loading_label.setVisible(True)
        
        # Request is now async - actual loading happens in onMessageUpdate
        self.client.get_messages(self.user["user_id"])
        
        # Mark messages as read - no need to wait for the result
        self.client.mark_messages_read(self.user["user_id"])
    
    def onMessageUpdate(self, result):
        """Handle asynchronous message update"""
        # Check if this update is for our conversation
        if not result.get("success", False) or not "other_user_id" in result:
            return
            
        if result["other_user_id"] != self.user["user_id"]:
            return
            
        # Update our messages
        self.messages = result["messages"]
        self.displayMessages()
        
        # Hide loading indicator
        self.loading = False
        self.loading_label.setVisible(False)
    
    def displayMessages(self):
        """Display messages in the UI"""
        # Clear existing messages
        for i in reversed(range(self.messages_layout.count())):
            item = self.messages_layout.itemAt(i)
            if item.widget():
                item.widget().deleteLater()
        
        # Add stretch at the top
        self.messages_layout.addStretch()
        
        # Add messages
        for message in sorted(self.messages, key=lambda m: m["timestamp"]):
            is_sent = message["sender_id"] == self.client.user_id
            bubble = ChatBubble(message, is_sent, self)
            self.messages_layout.addWidget(bubble)
        
        # Scroll to bottom
        QTimer.singleShot(100, self.scrollToBottom)
    
    def scrollToBottom(self):
        """Scroll to the bottom of the messages area"""
        self.scroll_area.verticalScrollBar().setValue(
            self.scroll_area.verticalScrollBar().maximum()
        )
    
    def updateMessage(self, message_id, read=None, deleted=None):
        """Update a message's status"""
        for i, message in enumerate(self.messages):
            if message["message_id"] == message_id:
                if read is not None:
                    self.messages[i]["read"] = read
                if deleted is not None:
                    self.messages[i]["deleted"] = deleted
                break
        
        # Refresh display
        self.displayMessages()
    
    def addMessage(self, message):
        """Add a new message to the conversation"""
        # Check if message already exists
        for existing in self.messages:
            if existing["message_id"] == message["message_id"]:
                return
        
        self.messages.append(message)
        self.displayMessages()
        
        # Mark message as read if it's an incoming message
        if message["recipient_id"] == self.client.user_id:
            self.client.mark_messages_read(message["sender_id"], [message["message_id"]])


class UserListItem(QWidget):
    """Custom widget for user list items"""
    
    def __init__(self, user, parent=None):
        super().__init__(parent)
        self.user = user
        self.initUI()
    
    def initUI(self):
        layout = QHBoxLayout()
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Avatar placeholder
        avatar = QLabel()
        avatar.setFixedSize(40, 40)
        avatar.setStyleSheet("background-color: #ddd; border-radius: 20px;")
        initials = "".join([name[0].upper() for name in self.user["display_name"].split() if name])
        if not initials and self.user["username"]:
            initials = self.user["username"][0].upper()
        avatar.setText(initials)
        avatar.setAlignment(Qt.AlignCenter)
        
        layout.addWidget(avatar)
        
        # User info
        info_layout = QVBoxLayout()
        
        name_label = QLabel(self.user["display_name"])
        name_label.setStyleSheet("font-weight: bold;")
        
        # Status indicator
        status_text = "Online" if self.user["status"] == 1 else "Offline"
        status_label = QLabel(status_text)
        status_label.setStyleSheet(f"color: {'green' if self.user['status'] == 1 else 'gray'};")
        
        info_layout.addWidget(name_label)
        info_layout.addWidget(status_label)
        
        layout.addLayout(info_layout)
        
        # Unread count badge
        if self.user["unread_message_count"] > 0:
            badge = QLabel(str(self.user["unread_message_count"]))
            badge.setStyleSheet(
                "background-color: #ff3b30; color: white; border-radius: 10px; padding: 2px 6px;"
            )
            badge.setAlignment(Qt.AlignCenter)
            layout.addWidget(badge)
        else:
            # Add spacer to maintain layout
            layout.addStretch()
        
        self.setLayout(layout)
        
        # Set cursor for clickable appearance
        self.setCursor(Qt.PointingHandCursor)


class MainWindow(QMainWindow):
    """Main chat application window"""
    
    def __init__(self, client):
        super().__init__()
        self.client = client
        self.chat_tabs = {}  # Maps user_id to chat widget
        self.initUI()
        
        # Connect client signals to slots
        self.client.userListUpdated.connect(self.onUserListUpdated)
        self.client.messageReceived.connect(self.onMessageReceived)
        self.client.messageRead.connect(self.onMessageRead)
        self.client.messageDeleted.connect(self.onMessageDeleted)
        self.client.userStatusChanged.connect(self.onUserStatusChanged)
        self.client.connectionError.connect(self.onConnectionError)
    
    def initUI(self):
        self.setWindowTitle("iMessage Chat")
        self.setMinimumSize(800, 600)
        
        # Create main widget and layout
        main_widget = QWidget()
        main_layout = QHBoxLayout()
        
        # Create splitter for resizable panels
        splitter = QSplitter(Qt.Horizontal)
        
        # Left panel with contacts
        self.left_panel = QWidget()
        left_layout = QVBoxLayout()
        
        # User info
        user_info = QWidget()
        user_layout = QHBoxLayout()
        
        self.user_label = QLabel()
        self.user_label.setStyleSheet("font-weight: bold;")
        
        # Status selector
        self.status_selector = QComboBox()
        self.status_selector.addItem("Online", 1)
        self.status_selector.addItem("Away", 2)
        self.status_selector.addItem("Offline", 0)
        self.status_selector.currentIndexChanged.connect(self.updateStatus)
        
        user_layout.addWidget(self.user_label)
        user_layout.addWidget(self.status_selector)
        
        user_info.setLayout(user_layout)
        left_layout.addWidget(user_info)
        
        # Search bar
        search_bar = QLineEdit()
        search_bar.setPlaceholderText("Search contacts...")
        search_bar.textChanged.connect(self.filterContacts)
        left_layout.addWidget(search_bar)
        
        # Contacts list
        self.contacts_list = QListWidget()
        self.contacts_list.setSelectionMode(QListWidget.SingleSelection)
        self.contacts_list.itemClicked.connect(self.onContactSelected)
        left_layout.addWidget(self.contacts_list)
        
        # Refresh button
        refresh_button = QPushButton("Refresh")
        refresh_button.clicked.connect(self.refreshUserList)
        left_layout.addWidget(refresh_button)
        
        # Logout button
        logout_button = QPushButton("Logout")
        logout_button.clicked.connect(self.logout)
        left_layout.addWidget(logout_button)
        
        self.left_panel.setLayout(left_layout)
        self.left_panel.setMinimumWidth(250)
        
        # Right panel with chat tabs
        self.right_panel = QTabWidget()
        self.right_panel.setTabsClosable(True)
        self.right_panel.tabCloseRequested.connect(self.closeTab)
        
        # Add welcome tab
        welcome_widget = QWidget()
        welcome_layout = QVBoxLayout()
        welcome_label = QLabel("Select a contact to start chatting")
        welcome_label.setAlignment(Qt.AlignCenter)
        welcome_layout.addWidget(welcome_label)
        welcome_widget.setLayout(welcome_layout)
        
        self.right_panel.addTab(welcome_widget, "Welcome")
        self.right_panel.setTabsClosable(False)
        
        # Add panels to splitter
        splitter.addWidget(self.left_panel)
        splitter.addWidget(self.right_panel)
        
        # Set splitter proportions
        splitter.setSizes([250, 550])
        
        main_layout.addWidget(splitter)
        main_widget.setLayout(main_layout)
        
        self.setCentralWidget(main_widget)
        
        # Set user label
        if self.client.username:
            self.user_label.setText(f"Logged in as: {self.client.username}")
        
        # Initial user list refresh
        self.refreshUserList()
        
        # Periodic refresh for user list - moderate rate
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.refreshUserList)
        self.refresh_timer.start(5000)  # Refresh every 5 seconds
        
        # Periodic refresh for messages in open chats
        self.message_refresh_timer = QTimer()
        self.message_refresh_timer.timeout.connect(self.refreshOpenChats)
        self.message_refresh_timer.start(3000)  # Refresh every 3 seconds
    
    def refreshUserList(self):
        """Refresh the users list"""
        self.client.get_users_list()
    
    def refreshOpenChats(self):
        """Refresh messages in all open chat tabs"""
        # Only refresh the active chat tab to reduce load
        current_index = self.right_panel.currentIndex()
        if current_index >= 0 and self.right_panel.tabText(current_index) != "Welcome":
            widget = self.right_panel.widget(current_index)
            for user_id, chat_widget in self.chat_tabs.items():
                if chat_widget == widget:
                    chat_widget.loadMessages()
                    break
    
    def filterContacts(self, text):
        """Filter contacts list by search text"""
        for i in range(self.contacts_list.count()):
            item = self.contacts_list.item(i)
            widget = self.contacts_list.itemWidget(item)
            
            # Check if the display name or username contains the search text
            if (text.lower() in widget.user["display_name"].lower() or 
                text.lower() in widget.user["username"].lower()):
                item.setHidden(False)
            else:
                item.setHidden(True)
    
    def updateStatus(self):
        """Update user status"""
        status = self.status_selector.currentData()
        self.client.update_status(status)
    
    def onUserListUpdated(self, users):
        """Handle updated user list"""
        self.contacts_list.clear()
        
        for user in users:
            item = QListWidgetItem()
            item.setSizeHint(QSize(0, 60))  # Height for item
            
            widget = UserListItem(user)
            
            self.contacts_list.addItem(item)
            self.contacts_list.setItemWidget(item, widget)
    
    def onContactSelected(self, item):
        """Handle contact selection"""
        user = self.contacts_list.itemWidget(item).user
        
        # Check if chat tab already exists
        if user["user_id"] in self.chat_tabs:
            # Switch to existing tab
            index = self.right_panel.indexOf(self.chat_tabs[user["user_id"]])
            self.right_panel.setCurrentIndex(index)
        else:
            # Create new chat tab
            chat_widget = ChatWidget(self.client, user)
            
            # Enable tab closing
            if self.right_panel.count() == 1 and self.right_panel.tabText(0) == "Welcome":
                # Remove welcome tab
                self.right_panel.removeTab(0)
                self.right_panel.setTabsClosable(True)
            
            # Add new tab
            index = self.right_panel.addTab(chat_widget, user["display_name"])
            self.right_panel.setCurrentIndex(index)
            
            # Store reference
            self.chat_tabs[user["user_id"]] = chat_widget
            
            # Load messages
            chat_widget.loadMessages()
    
    def closeTab(self, index):
        """Handle tab close request"""
        # Find the user_id for this tab
        widget = self.right_panel.widget(index)
        
        for user_id, chat_widget in list(self.chat_tabs.items()):
            if chat_widget == widget:
                del self.chat_tabs[user_id]
                break
        
        # Remove the tab
        self.right_panel.removeTab(index)
        
        # If no tabs left, add welcome tab back
        if self.right_panel.count() == 0:
            welcome_widget = QWidget()
            welcome_layout = QVBoxLayout()
            welcome_label = QLabel("Select a contact to start chatting")
            welcome_label.setAlignment(Qt.AlignCenter)
            welcome_layout.addWidget(welcome_label)
            welcome_widget.setLayout(welcome_layout)
            
            self.right_panel.addTab(welcome_widget, "Welcome")
            self.right_panel.setTabsClosable(False)
    
    def onMessageReceived(self, message_result):
        """Handle incoming message updates"""
        # Check if this is a full messages result from an async operation
        if isinstance(message_result, dict) and "messages" in message_result:
            # This is the result of an async get_messages call
            # The individual ChatWidget instances will handle this through their onMessageUpdate method
            return
            
        # Otherwise, this is a single new message from the real-time updates
        message = message_result
        
        try:
            sender_id = message["sender_id"]
            recipient_id = message["recipient_id"]
            
            # Determine the other user ID
            other_user_id = sender_id if sender_id != self.client.user_id else recipient_id
            
            # If chat tab is open, add message to it
            if other_user_id in self.chat_tabs:
                self.chat_tabs[other_user_id].addMessage(message)
                
                # If this is the current tab, mark as read
                current_index = self.right_panel.currentIndex()
                if self.right_panel.widget(current_index) == self.chat_tabs[other_user_id]:
                    self.client.mark_messages_read(other_user_id)
            
            # Update user list to show unread count
            self.refreshUserList()
        except KeyError as e:
            # Log the error but don't crash
            print(f"Error processing message: {e}. Message format: {message}")
    
    def onMessageRead(self, message):
        """Handle message read status update"""
        message_id = message["message_id"]
        sender_id = message["sender_id"]
        recipient_id = message["recipient_id"]
        
        # Determine the other user ID
        other_user_id = recipient_id if sender_id == self.client.user_id else sender_id
        
        # If chat tab is open, update message status
        if other_user_id in self.chat_tabs:
            self.chat_tabs[other_user_id].updateMessage(message_id, read=True)
    
    def onMessageDeleted(self, message):
        """Handle message deletion"""
        message_id = message["message_id"]
        sender_id = message["sender_id"]
        recipient_id = message["recipient_id"]
        
        # Determine the other user ID
        other_user_id = recipient_id if sender_id == self.client.user_id else sender_id
        
        # If chat tab is open, update message status
        if other_user_id in self.chat_tabs:
            self.chat_tabs[other_user_id].updateMessage(message_id, deleted=True)
    
    def onUserStatusChanged(self, user):
        """Handle user status change"""
        # Refresh user list to show updated status
        self.refreshUserList()
    
    def onConnectionError(self, error):
        """Handle connection error"""
        QMessageBox.warning(self, "Connection Error", 
            f"Error connecting to server: {error}")
    
    def deleteMessage(self, message_id):
        """Delete a message"""
        result = self.client.delete_message(message_id)
        
        if not result["success"]:
            QMessageBox.warning(self, "Error", f"Failed to delete message: {result['message']}")
    
    def logout(self):
        """Handle logout"""
        reply = QMessageBox.question(self, "Logout", 
            "Are you sure you want to logout?", 
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            self.client.logout()
            self.close()  # Close the window


def main():
    app = QApplication(sys.argv)
    
    # Set app style
    app.setStyle("Fusion")
    
    # Create client
    client = ChatClientAPI("config.json")
    
    # Show login dialog
    login_dialog = LoginDialog(client)
    if login_dialog.exec_() != QDialog.Accepted:
        sys.exit()
    
    # Show main window
    window = MainWindow(client)
    window.show()
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
