import sys
from datetime import datetime
from redis_communication import RedisClient


class Logger:
    def __init__(self, name: str='No Name', redis: RedisClient=None, verbose=False):
        """
        Logger class to handle logging messages to both console and Redis stream.
        :param name: Name of the logger instance. Will be displayed in the log messages.
        :param redis: RedisClient instance to send log messages to a Redis stream.
        :param verbose: If True, log messages will be printed to the console.
        """
        self.buffer = []
        self._standard_out = sys.stdout  # Save the original stdout
        self._verbose = verbose
        self._redis = redis
        self.name = name

    def set_active(self, active):
        if active:
            sys.stdout = self
            self.handle('Logging enabled')
        else:
            sys.stdout = sys.stdout

    def write(self, message):
        if message.strip():  # avoid capturing empty newlines if you want
            #self.buffer.append(message)
            self.handle(message)

    def flush(self):
        # messages, self.buffer = self.buffer, []
        # for message in messages:
        #     self.handle(message)
        pass

    def handle(self, message):
        formated_message = f"[{datetime.now()} | {self.name}] {message}"
        if self._verbose:
            print(formated_message, end='\n', file=self._standard_out, flush=True)
        if self._redis:
            self._redis.add_stream_message('log', formated_message)
