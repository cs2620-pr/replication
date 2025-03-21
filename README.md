# Wire Protocols Chat Application with gRPC

This repository contains a chat application built using gRPC for client-server communication. The application allows users to create accounts, send messages to other users, view message history, and delete messages or accounts.

## Features

- User authentication (registration, login, logout)
- Real-time messaging between users
- Message history with timestamps
- Unread message indicators
- Online/offline user status
- Message deletion
- Account deletion
- Dark mode support (automatically detected from system settings)
- Search functionality for users

## Requirements

- Python 3.10+
- PyQt5 (for the GUI client)
- gRPC and Protocol Buffers
- SQLite3 (included in Python standard library)
- bcrypt (for password hashing)
- pytest (for running tests)

## Installation

1. Clone the repository:

   ```
   git clone https://github.com/cs2620-pr/wire-protocols-grpc.git
   cd wire-protocols-grpc
   ```

2. Create a virtual environment (optional but recommended):

   ```
   python -m venv venv

   # On Windows
   venv\Scripts\activate

   # On macOS/Linux
   source venv/bin/activate
   ```

3. Install the required dependencies using the provided requirements.txt file:

   ```
   pip install -r requirements.txt
   ```

   This will install all necessary packages including:

   - grpcio and grpcio-tools for gRPC functionality
   - protobuf for Protocol Buffers
   - bcrypt for password hashing
   - pytest for running tests

4. Compile the proto file to generate proto stubs:

   ```
   # On macOS/Linux
   ./compile_proto.sh

   # On Windows
   python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/chat.proto
   ```

## Project Structure

```
wire-protocols-grpc/
├── client/                  # Client-side code
│   └── gui_client.py        # PyQt5 GUI client implementation
├── server/                  # Server-side code
│   ├── server.py            # gRPC server implementation
│   ├── database.py          # Database operations
│   ├── constants.py         # Constant definitions
│   └── chat/                # Generated gRPC code
├── protos/                  # Protocol Buffer definitions
│   └── chat.proto           # gRPC service and message definitions
├── tests/                   # Test files
├── compile_proto.sh         # Script to compile .proto files
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

## Protocol Efficiency

This application has been implemented using three different protocols:

| Protocol    | Avg Size (bytes) | Pros                                                | Cons                                        |
| ----------- | ---------------- | --------------------------------------------------- | ------------------------------------------- |
| gRPC        | 534.53           | Auto-generated code, strong typing, HTTP/2 benefits | Larger message size (in our implementation) |
| JSON        | 238.00           | Human-readable, widely supported                    | Manual parsing/validation                   |
| Custom Wire | 53.82            | Most bandwidth efficient                            | Hard to debug, manual implementation        |

### Operation-Specific Sizes

| Operation    | gRPC Avg Size (bytes) | JSON Avg Size (bytes) | CustomWire Avg Size (bytes) |
| ------------ | --------------------- | --------------------- | --------------------------- |
| GetMessages  | 880.71                | 231.48                | 49.02                       |
| ListAccounts | 51.14                 | -                     | -                           |
| SendMessage  | 48.96                 | 242.11                | 63.40                       |
| Login        | 23.48                 | 244.19                | 67.88                       |
| Logout       | 20.00                 | 233.28                | 49.24                       |

Note: While gRPC/Protobuf is typically more efficient than JSON, our current implementation retrieves all messages at once in GetMessages operations, leading to larger payload sizes. For operations like SendMessage and Login, gRPC is more efficient than JSON.

## Running the Application

### Starting the Server

To start the server with default settings:

```
python -m server.server
```

The server will start on localhost:8000 by default and create a SQLite database file named `chat.db` in the current directory.

#### Server Command-line Options

- `--host`: Host address to bind the server to (default: localhost)
- `--port`: Port number to listen on (default: 8000)
- `--db-path`: Path to the SQLite database file (default: chat.db)
- `--enable-logging`: Enable protocol metrics logging
- `--log-level`: Set logging level (default: INFO)

Example with custom settings:

```
python -m server.server --host 0.0.0.0 --port 9000 --db-path custom_database.db --enable-logging --log-level DEBUG
```

### Starting the Client

To start the client with default settings:

```
python -m client.gui_client
```

The client will attempt to connect to a server running on localhost:8000 by default.

#### Client Command-line Options

- `--host`: Server host address to connect to (default: localhost)
- `--port`: Server port number to connect to (default: 8000)
- `--enable-logging`: Enable protocol metrics logging
- `--log-level`: Set logging level (default: INFO)
- `--poll-interval`: Set polling interval in seconds (default: 2.0)

Example with custom settings:

```
python -m client.gui_client --host 192.168.1.100 --port 9000 --enable-logging --poll-interval 1.0
```

## Using the Application

### Authentication

1. When you start the client, you'll see a login screen.
2. To create a new account, enter a username and password, then click "Register".
3. To log in, enter your credentials and click "Login".

### Messaging

1. After logging in, you'll see a list of users on the right side of the window.
2. Online users are indicated with a green dot, offline users with a gray circle.
3. Click on a user to start or continue a conversation with them.
4. Type your message in the input field at the bottom and press Enter or click "Send".
5. Messages you send appear on the right side in blue, and messages you receive appear on the left side.

### Additional Features

- **Deleting Messages**: Right-click on a message you sent to delete it.
- **Searching Users**: Use the search box above the user list to filter users.
- **Unread Messages**: Users with unread messages will have a count displayed next to their name.
- **Logging Out**: Click the "Logout" button at the bottom right to log out.
- **Deleting Your Account**: Click the "Delete Account" button to permanently delete your account and all your messages.

## Architecture

- **Server**: Implemented in Python using gRPC, with a SQLite database for persistence.
- **Client**: Built with PyQt5 for the GUI and gRPC for communication with the server.
- **Protocol**: Uses Protocol Buffers for message serialization and gRPC for RPC calls.
- **Communication Pattern**: Uses polling rather than streaming for simplicity and reliability.

### Key Design Decisions

1. **Polling vs. Broadcasting**: The application uses polling where clients periodically request updates from the server, rather than server-push broadcasting as in the original socket implementation. This trades some real-time responsiveness for better reliability and simplified implementation.

2. **Session Management**: Sessions are managed via tokens rather than being tied to connections, allowing for better resilience to network issues.

3. **Protocol Format**: gRPC with Protocol Buffers provides strong typing and abstracted communication but results in larger message sizes for some operations compared to a custom wire protocol.

## Running Tests

To run the tests:

```
python -m pytest
```

For more detailed test output with coverage information:

```
python -m pytest --cov=server --cov=client
```

## Performance Considerations

- **Bandwidth Usage**: Our implementation of gRPC uses more bandwidth than both JSON and custom wire protocols, primarily due to how we retrieve all messages at once. For 1 million messages:

  - CustomWireProtocol: 51.3 MB
  - JSONProtocol: 227.0 MB
  - gRPC: 534.5 MB

- **Polling Interval**: The default polling interval is 2 seconds. Decreasing this will make the application more responsive but increase bandwidth usage and server load. You can adjust this with the `--poll-interval` option.

- **Message Batching**: Consider implementing message pagination or incremental updates to reduce bandwidth usage in production deployments.

## Troubleshooting

- **Connection Issues**: Ensure the server is running and that the host and port settings match between client and server.
- **Database Errors**: If you encounter database errors, try deleting the chat.db file and restarting the server.
- **Client Freezes**: If the client becomes unresponsive, it may be due to connection issues with the server. Try restarting the client.
- **Protocol Buffer Compilation**: If you modify the .proto files, you'll need to recompile them using the provided script:
  ```
  ./compile_proto.sh
  ```
- **Permission Issues**: If you encounter permission issues with the compile_proto.sh script, make it executable:
  ```
  chmod +x compile_proto.sh
  ```

## Debugging

- **Binary Protocol Issues**: Since gRPC uses binary encoding, debugging message contents can be challenging. Use tools like `grpcurl` or enable logging with the `--enable-logging` flag.
- **Client-Server Communication**: Set the log level to DEBUG to see detailed information about RPC calls and responses.
