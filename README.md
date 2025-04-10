## Redis Communication Module Documentation

This document provides an overview and instructions for the utilization of the **Redis Communication Module** implemented in the `redis_communication.py`. The module leverages the `redis` Python library to provide an interface for interacting with a Redis server. Below is a detailed explanation of its features, classes, and methods.

---

### **Used messages**

Firstly, following is a list of the messages and their usage.

#### Streams
- `'detect'`: int(sleep delay)

#### Messages
- `test`: list(1, 0)

---

### **Overview**

The module simplifies interaction with Redis by implementing the `RedisClient` class, which abstracts various Redis functionalities such as:
- Publishing and subscribing to channels (Pub/Sub).
- Managing Redis streams (adding messages and fetching stream messages).
- Handling JSON serialization for data compatibility.

The class is designed with error handling for Redis connection failures and ensures methods are intuitive for common Redis use cases.

---

### **Core Features**

#### **Initialization**
Upon instantiating the `RedisClient`:
- A connection is established to the Redis server running on `localhost:6379` by default.
- A `pubsub` object is created for Pub/Sub functionality.
- The script will exit if the Redis server is not reachable, providing a clear error message.

#### **Error Handling**
The module handles key errors such as:
- Failed Redis server connection (using `ping()`).
- JSON serialization errors when publishing or writing data.

---

### **Notes**
1. **Dependencies**: Ensure the `redis` Python package is installed (`pip install redis`).
2. **Redis Server**: The module requires an active Redis server on `localhost:6379` by default.

---

### **Classes and Methods**

#### **Class: `RedisClient`**

The main class providing Redis communication functionality.

---

#### **Methods**

##### **1. `add_subscriber`**
Subscribes to a Redis channel for message notifications.
- **Parameters**:
  - `channel_name` (str): Name of the channel to subscribe to.
  - `callback` (callable): Function to handle incoming messages for the channel (to be provided outside the module).
  - `signature` (set, optional): Metadata or message signature (not fully implemented here).
- **Return**: None.

##### **2. `remove_subscriber`**
Unsubscribes from a Redis channel.
- **Parameters**:
  - `channel_name` (str): Name of the channel.
- **Return**: None.

##### **3. `send_message`**
Publishes a message to a Redis channel.
- **Parameters**:
  - `channel_name` (str): Target channel for the message.
  - `content` (any serializable type: `str`, `int`, `float`, `dict`, `list`): The message data to send.
- **Behavior**: Adds a timestamp to the message before publishing it.

##### **4. `add_stream_message`**
Adds a new message to a Redis stream.
- **Parameters**:
  - `stream_name` (str): Name of the Redis stream.
  - `content` (any serializable type): The message to add.
- **Behavior**: Converts the message into a JSON string and appends it to the stream with a timestamp.

##### **5. `get_latest_stream_message`**
Fetches the most recent message from a Redis stream.
- **Parameters**:
  - `stream_name` (str): The name of the stream to retrieve messages from.
- **Returns**:
  - A tuple containing:
    - `timestamp` (int, int): The server time when the message was sent.
    - `content` (deserialized JSON object): The actual message from the stream.
  - Returns `(None, None)` if there are no messages in the stream.

##### **6. `get_unread_stream_messages`**
Fetches unread messages from a Redis stream since the last read message.
- **Parameters**:
  - `stream_name` (str): The name of the stream.
  - `max_messages` (int, optional): Maximum number of messages to fetch.
- **Returns**: 
  - A list of tuples, where each tuple contains:
    - `timestamp` (int, int).
    - `content` (deserialized JSON object).
  - Returns `[(None, None)]` if no unread messages are available.

---

### **Usage Examples**

#### **1. Creating a Redis Client**
```python
redis_client = RedisClient()
```

#### **2. Publishing a Message**

```python
channel = "news"
content = {"headline": "Redis is great!", "content": "Learn how to use Redis in Python easily."}
redis_client.send_message(channel, content)
```

#### **3. Subscribing to a Channel and awaiting messages**
```python
def my_callback(timestamp, content):
    print(f"Received message: {content}")

redis_client.add_subscriber("news", my_callback)
redis_client.listen()
```

#### **4. Adding Messages to a Stream**
```python
stream_name = "logs"
content = {"level": "info", "message": "Application started."}
redis_client.add_stream_message(stream_name, content)
```

#### **5. Fetching Latest Stream Message**
```python
stream_name = "logs"
timestamp, content = redis_client.get_latest_stream_message(stream_name)
print(f"Timestamp: {timestamp}, Message: {content}")
```

---
