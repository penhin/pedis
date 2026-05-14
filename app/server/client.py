from dataclasses import dataclass, field
import logging

from enum import Enum

from app.commands.core.base import CommandResult
from app.protocol import RESPParser, RESPEncoder, RESPError, ProtocolError

from .context import Context
from .block_handler import ListStrategy, StreamStrategy, WaitStrategy


logger = logging.getLogger(__name__)

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
    dirty: bool = False

    def reset(self, clear_watches: bool = True):
        self.active = False
        self.queue.clear()
        self.dirty = False
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
        logger.debug("Sending RESP to %s: %r", self, data)
        self.connection.sendall(self.encoder.encode(data))
    
    def send_raw(self, data: any):
        logger.debug("Sending raw data to %s: %r", self, data)
        self.connection.sendall(data)

    def send_result(self, result: CommandResult):
        parts = []
        for frame in result.frames:
            if frame.kind == "resp":
                parts.append(self.encoder.encode(frame.value))
            elif frame.kind == "raw":
                parts.append(self.encoder.bulk_raw(frame.value))
            else:
                raise ValueError(f"Unknown response frame kind: {frame.kind}")
        if parts:
            data = b"".join(parts)
            logger.debug("Sending command result to %s: %r", self, data)
            self.connection.sendall(data)
    
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
    MAX_COMMANDS_PER_TICK = 256

    def __init__(self, client: Client):
        self.client = client

    def handle(self, selector):
        processed = 0
        try:
            while processed < self.MAX_COMMANDS_PER_TICK:
                cmd_list, _ = self.client.parser.parse()
                raw_command = self.client.encoder.encode(cmd_list)
                logger.debug("Master received command from %s: %r", self.client, cmd_list)
                processed += 1

                context = Context(self.client.server, self.client)

                try:
                    result = self.client.server.dispatcher.dispatch(cmd_list, raw_command, context)
                    logger.debug("Master command result for %s: %r", self.client, result)
                except ProtocolError as e:
                    logger.debug("Protocol error: %s", e)
                    self.client.send(e)
                    self.client.close()
                    return
                except RESPError as e:
                    logger.debug("RESP error: %s", e)
                    self.client.send(e)
                    continue
                except Exception as e:
                    logger.exception("Unhandled client command error: %s", e)
                    self.client.send(RESPError(str(e)))
                    continue

                if not result.blocked:
                    self.client.send_result(result)
                else:
                    return

            if self.client.parser.reader.buffer:
                self.client.server.schedule_client(self.client)
        except ProtocolError as e:
            logger.debug("Protocol parse error: %s", e)
            self.client.send(e)
            self.client.close()
            return
        except BlockingIOError:
            return
        except ConnectionError:
            self.client.close()
            return

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

                logger.debug("Replica received command: %r", parsed)
                
                if isinstance(parsed, bytes):
                    nxt = self.client.server.replication.handle_master_response(self.client, parsed)
                    if nxt:
                        self.client.send_result(nxt)
                else:
                    context = Context(self.client.server, self.client)
                    result = self.client.server.dispatcher.dispatch(parsed, b"", context)
                    logger.debug("Replica command result: %r", result)

                    if self._should_reply_to_master(parsed, result):
                        self.client.send_result(result)

                    self.client.server.replication.repl_offset += captured_bytes

        except BlockingIOError:
            return
        except ConnectionError:
            self.client.close()
            return
        except Exception as e:
            logger.exception("Unhandled master connection error: %s", e)
            return

    def _should_reply_to_master(self, parsed, result: CommandResult) -> bool:
        if result.blocked or not result.frames:
            return False
        if not isinstance(parsed, list) or not parsed:
            return False

        command_name = parsed[0].upper()
        return command_name == b"REPLCONF"
    
