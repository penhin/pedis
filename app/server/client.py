from dataclasses import dataclass, field
import traceback

from enum import Enum

from app.commands.core.base import CommandResult
from app.protocol import RESPParser, RESPEncoder, RESPError

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


@dataclass
class BlockingState:
    active: bool = False
    keys: list[bytes] = field(default_factory=list)
    ids: list[bytes] = field(default_factory=list)
    timeout: float | None = None
    kind: BlockedType = BlockedType.NONE
    strategy: object | None = None

    def clear(self):
        self.active = False
        self.keys.clear()
        self.ids.clear()
        self.timeout = None
        self.kind = BlockedType.NONE
        self.strategy = None

@dataclass
class PubSubState:
    active: bool = False
    channels: set[bytes] = field(default_factory=set)
    
    def clear(self):
        self.active = False
        self.channels.clear()
    
    def add(self, channel: bytes) -> int:
        self.channels.add(channel)
        self.active = True
        return len(self.channels)

    def remove(self, channel: bytes) -> int:
        self.channels.discard(channel)
        self.active = len(self.channels) > 0
        return len(self.channels)

@dataclass
class TransactionState:
    active: bool = False
    queue: list[tuple[list[bytes], bytes]] = field(default_factory=list)
    watched_keys: dict[bytes, int] = field(default_factory=dict)

    def reset(self, clear_watches: bool = True):
        self.active = False
        self.queue.clear()
        if clear_watches:
            self.watched_keys.clear()

    def watch(self, key: bytes, version: int):
        self.watched_keys[key] = version

    def unwatch(self):
        self.watched_keys.clear()


@dataclass
class RoleState:
    flags: set[str] = field(default_factory=set)

    def has(self, flag: str) -> bool:
        return flag in self.flags

    def add(self, flag: str):
        self.flags.add(flag)


@dataclass
class AuthState:
    user: bytes = b"default"
    authenticated: bool = True

    def set_user(self, user: bytes):
        self.user = user
        self.authenticated = True

    def require_auth(self):
        self.user = b"default"
        self.authenticated = False

class Client:

    def __init__(self, connection, address, server, flags=None):
        self.connection = connection
        self.address = address
        self.server = server
        self.parser = RESPParser(connection)
        self.encoder = RESPEncoder()

        self.pubsub = PubSubState()
        self.blocking = BlockingState()
        self.transaction = TransactionState()
        self.role = RoleState(set(flags or []))
        self.auth = AuthState()
        if self.role.has(CLIENT_NORMAL) and self.server.acl.requires_authentication():
            self.auth.require_auth()

        if self.role.has(CLIENT_MASTER):
            self.handler = _MasterHandler(self)
        else:
            self.handler = _NormalHandler(self)
    
    def send(self, data: bytes):
        print(">>> SEND TO:", self, data)
        self.connection.sendall(self.encoder.encode(data))
    
    def send_raw(self, data: any):
        print(">>> SENDING RAW TO:", self, data)
        self.connection.sendall(data)

    def send_result(self, result: CommandResult):
        for frame in result.frames:
            if frame.kind == "resp":
                self.send(frame.value)
            elif frame.kind == "raw":
                self.send_raw(self.encoder.bulk_raw(frame.value))
            else:
                raise ValueError(f"Unknown response frame kind: {frame.kind}")
    
    def close(self):
        connection = self.connection
        if connection is None:
            return

        self.server.blocked_manager.remove_client(self)
        self.server.pubsub.remove_client(self)
        self.server.replication.remove_client(self)
        self.server.clients.discard(self)

        try:
            self.server.sel.unregister(connection)
        except Exception:
            pass

        try:
            connection.close()
        except Exception:
            pass

        self.connection = None


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
            self.client.close()
            return

        context = Context(self.client.server, self.client)

        try:
            result = self.client.server.dispatcher.dispatch(cmd_list, raw_command, context)
            print(f"Master command result: {self.client}, {result}")
        except RESPError as e:
            print(str(e))
            self.client.send(e)
            return
        except Exception as e:
            print(str(e))
            self.client.send(RESPError(str(e)))
            traceback.print_exc()
            return

        if not result.blocked:
            self.client.send_result(result)

class _MasterHandler:
    def __init__(self, client: Client):
        self.client = client

    def handle(self, selector):
        try:
            while True:
                if self.client.server.replication.repl_state == "RDB_TRANSFER":
                    self.client.server.replication.finish_rdb_transfer(self.client)
                    continue

                parsed, captured_bytes = self.client.parser.parse()

                print(f"Replica received command: {parsed}")
                
                if isinstance(parsed, bytes):
                    nxt = self.client.server.replication.handle_master_response(self.client, parsed)
                    if nxt:
                        self.client.send_result(nxt)
                else:
                    context = Context(self.client.server, self.client)
                    result = self.client.server.dispatcher.dispatch(parsed, b"", context)
                    print(f"Replica command result: {result}")

                    if self._should_reply_to_master(parsed, result):
                        self.client.send_result(result)

                    self.client.server.replication.repl_offset += captured_bytes

        except BlockingIOError:
            return
        except ConnectionError:
            self.client.close()
            return
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return

    def _should_reply_to_master(self, parsed, result: CommandResult) -> bool:
        if result.blocked or not result.frames:
            return False
        if not isinstance(parsed, list) or not parsed:
            return False

        command_name = parsed[0].upper()
        return command_name == b"REPLCONF"
    
