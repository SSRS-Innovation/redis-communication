import redis
import time
import json


class RedisClient:
    """
    Initializes the RedisClient.

    :param redis_client: The Redis connection object used for communication with the Redis server.
    """

    def __init__(
            self,
            host: str = "localhost",
            port: int = 6379,
    ):
        self._r = redis.Redis(host=host, port=port, db=0)
        self._callbacks = {}
        self._last_stream_ids = {}

        try:
            self._r.ping()
        except redis.exceptions.ConnectionError as e:
            print("Redis server is not responding.")
            print(e, flush=True)

            exit(1)

        self._pubsub = self._r.pubsub()

    def add_subscriber(
            self,
            channel_name: str,
            callback: callable
    ):
        # return if channel has already been added
        if channel_name in self._callbacks:
            print(f'Error: channel "{channel_name}" already exists.', flush=True)
            return
        # save subscription
        self._callbacks[channel_name] = callback
        self._pubsub.subscribe(channel_name)

    def remove_subscriber(
            self,
            channel_name: str
    ):
        try:
            self._callbacks.pop(channel_name)
        except KeyError:
            return
        self._pubsub.unsubscribe(channel_name)

    def send_message(
            self,
            channel_name: str,
            content: str | int | float | dict | list
    ):
        # add timestamp
        content = {"timestamp": time.time(), "content": content}
        content = self._to_json(content)
        if content is not None:
            # send message
            self._r.publish(channel_name, content)

    def listen(self):
        for event in self._pubsub.listen():
            if event["type"] == "message":
                channel_name = event["channel"].decode("utf-8")
                message = self._from_json(event["data"])
                if not {'timestamp', 'content'} == message.keys():
                    print(f"Error: message should contain 'timestamp' and 'content' but got: {message}", flush=True)
                    continue
                try:
                    self._callbacks[channel_name](message['timestamp'], message['content'])
                except TypeError:
                    print(f"Error: callback for channel '{channel_name}' has invalid signature. Should be (timestamp:float, content: Any)", flush=True)
                    continue

    def add_stream_message(
            self,
            stream_name: str,
            content: str | int | float | dict | list
    ):
        content = self._to_json(content)
        self._r.xadd(stream_name, {'timestamp': time.time(), 'content': content})

    def get_latest_stream_message(
            self,
            stream_name: str
    ) -> tuple[float, str | int | float | dict | list] | tuple[None, None]:
        # read the latest message
        raw_message = self._r.xrevrange(stream_name, count=1)
        # return None if no messages
        if len(raw_message) == 0:
            return None, None
        # latest message
        raw_message = raw_message[0]
        # save last read stream id
        self._last_stream_ids[stream_name] = raw_message[0].decode("utf-8")

        return self._decode_stream_message(raw_message)

    def get_unread_stream_messages(
            self,
            stream_name: str,
            max_messages: int = None
    ) -> list[tuple[float, str | int | float | dict | list]] | list[tuple[None, None]]:
        messages = []
        # if it is the first time, get all messages (indicated by 0)
        stream_id = self._last_stream_ids.get(stream_name, "0")
        raw_messages = self._r.xrange(stream_name, stream_id, "+", count=max_messages)
        if len(raw_messages) == 0:
            return [(None, None)]
        # if it not the first time, omit the first message in list because xrange is inclusive
        if stream_id != "0":
            raw_messages = raw_messages[1:]
        if len(raw_messages) == 0:
            return [(None, None)]

        # save the newest stream id
        self._last_stream_ids[stream_name] = raw_messages[-1][0].decode("utf-8")
        for r in raw_messages:
            messages.append(self._decode_stream_message(r))
        return messages

    @staticmethod
    def _to_json(
            message: str | int | float | dict | list
    ) -> str | None:
        # test if json serializable
        try:
            return json.dumps(message)
        except TypeError:
            print(f"Error: message is not JSON serializable: {message}", flush=True)
            return None

    @staticmethod
    def _from_json(
            message: str
    ) -> str | int | float | dict | list:
        try:
            return json.loads(message)
        except json.decoder.JSONDecodeError as e:
            print(f"Error: could not read message: {message}. {e}", flush=True)
        except TypeError as e:
            print(e, flush=True)

    @staticmethod
    def _decode_stream_message(
            message: list[dict[bytes]]
    ) -> tuple[float, str | int | float | dict | list]:
        # decode timestamp and message
        timestamp = float(message[1][b"timestamp"].decode("utf-8"))
        content = RedisClient._from_json(message[1][b"content"])
        return timestamp, content
