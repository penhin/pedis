import traceback

from enum import Enum, auto

from app.protocol import RESPParser, RESPEncoder, RESPError

from .types import Blocked
from .context import Context
from .block_handler import ListStrategy, StreamStrategy, WaitStrategy

CLIENT_NORMAL = "NORMAL"
CLIENT_MASTER = "MASTER"
CLIENT_REPLICA = "REPLICA"

class BlockedType(Enum):
    NONE = None
    LIST = ListStrategy
    WAIT = WaitStrategy
    ZSET = None
    STREAM = StreamStrategy

class Client:

    def __init__(self, connection, address, server, flags=None):
        self.connection = connection
        self.address = address
        self.server = server
        self.parser = RESPParser(connection)
        self.encoder = RESPEncoder()

        self.blocked = False
        self.blocked_keys = []
        self.blocked_ids = []
        self.blocked_timeout = None
        self.blocked_type = BlockedType.NONE
        self.block_strategy = None

        self.in_multi = False
        self.multi_queue = []

        self.flags = set(flags or [])

        if CLIENT_MASTER in self.flags:
            self.handler = _MasterHandler(self)
        else:
            self.handler = _NormalHandler(self)
    
    def send(self, data: bytes):
        print(">>> SEND TO:", self, data)
        self.connection.sendall(self.encoder.encode(data))
    
    def send_raw(self, data: any):
        print(">>> SENDING RAW TO:", self, data)
        self.connection.sendall(data)
    
    def close(self):
        pass


class _NormalHandler:
    def __init__(self, client: Client):
        self.client = client

    def handle(self, selector):
        try:
            cmd_list, _ = self.client.parser.parse()
            raw_command = self.client.encoder.encode(cmd_list)
            print(f"Master received command: {self.client}, {cmd_list}")
        except BlockingIOError:
            return
        except ConnectionError:
            selector.unregister(self.client.connection)
            self.client.connection.close()
            return

        context = Context(self.client.server, self.client)

        try:
            message = self.client.server.dispatcher.dispatch(cmd_list, raw_command, context)
            print(f"Master command result: {self.client}, {message}")
        except RESPError as e:
            print(str(e))
            self.client.send(e)
            return
        except Exception as e:
            print(str(e))
            self.client.send(RESPError(str(e)))
            traceback.print_exc()
            return

        if isinstance(message, tuple) and len(message) == 2:
            header, payload = message
            if header is not None:
                self.client.send(header)
            if payload is not None:
                self.client.send_raw(self.client.encoder.bulk_raw(payload))
            return

        if message is not None and not isinstance(message, Blocked):
            self.client.send(message)

class _MasterHandler:
    def __init__(self, client: Client):
        self.client = client

    def handle(self, selector):
        try:
            while True:
                parsed, captured_bytes = self.client.parser.parse()

                print(f"Replica received command: {parsed}")
                
                if isinstance(parsed, bytes):
                    nxt = self.client.server.replication.handle_master_response(self.client, parsed)
                    if nxt:
                        self.client.send(nxt)
                else:
                    context = Context(self.client.server, self.client)
                    result = self.client.server.dispatcher.dispatch(parsed, b"", context)
                    print(f"Replica command result: {result}")

                    self.client.server.replication.repl_offset += captured_bytes

        except BlockingIOError:
            return
        except ConnectionError:
            selector.unregister(self.client.connection)
            self.client.connection.close()
            return
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return
    