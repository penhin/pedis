from __future__ import annotations

import time

from typing import List

from app.commands.core.base import CommandResult

from .types import Blocked
from .client import CLIENT_REPLICA

class ReplicationManager:
    """Manage replication state and responses inside RedisServer.

    Commands should delegate to these methods; the manager keeps all
    replication-related state so command modules remain thin.
    """

    EMPTY_RDB = b"REDIS0009\xff\x00\x00\x00\x00\x00\x00\x00\x00"

    def __init__(self, server: "RedisServer"):
        self.server = server
        self.role = server.config.role
        self.capabilities: List[bytes] = []

        self.master_replid = ""
        self.replid = server.config.replid
        self.master_repl_offset = server.config.master_repl_offset       
        self.repl_offset = 0

        self.replica_client = set()
        self.master_connection = None
        self.repl_state: str | None = None
        self.buffer = ""
        
        # {client: client_offset}
        self.replica_acks = {} 
        # [(client, required_offset, required_replicas, deadline)]
        self.wait_clients = []  
        
    def _generate_empty_rdb(self) -> bytes:
        """Generate an empty rdb file"""
        return self.EMPTY_RDB

    def _check_wait_clients(self):
        """Check waiting clients and unblock if enough replicas acknowledged."""
        to_unblock = []

        for entry in list(self.wait_clients):
            client, required_offset, required_replicas, deadline = entry

            acknowledged = 0
            for replica in self.replica_client:
                offset = self.replica_acks.get(replica, 0)
                if offset >= required_offset:
                    acknowledged += 1

            if acknowledged >= required_replicas:
                self._send_wait_result(client, acknowledged)
                print(
                    f"WAIT unblocked: required_offset={required_offset}, "
                    f"required_replicas={required_replicas}, acknowledged={acknowledged}"
                )
                to_unblock.append(entry)

        for entry in to_unblock:
            self.wait_clients.remove(entry)
    
    def check_timeouts(self, now):
        """Send acknowledged replicas whatever if enough"""
        to_unblock = []

        for entry in list(self.wait_clients):
            client, required_offset, required_replicas, deadline = entry
            
            if deadline is not None and now >= deadline:
                acknowledged = 0
                for replica in self.replica_client:
                    offset = self.replica_acks.get(replica, 0)
                    if offset >= required_offset:
                        acknowledged += 1

                self._send_wait_result(client, acknowledged)
                print(
                    f"WAIT timeout: required_offset={required_offset}, "
                    f"required_replicas={required_replicas}, acknowledged={acknowledged}"
                )
                to_unblock.append(entry)
        
        for entry in to_unblock:
            self.wait_clients.remove(entry)

    def _send_wait_result(self, client, acknowledged: int):
        client.blocking.active = False
        client.send_result(CommandResult.resp(acknowledged, propagate=False))

    def start_replication(self, client) -> CommandResult:
        """Called when a new master connection is established.

        Returns the first command to send (list of bytes) so that the
        caller can encode it.  The manager maintains the handshake state
        thereafter and will emit further commands via
        ``handle_master_response``.
        """
        self.master_connection = client
        self.repl_state = "PING_SENT"
        print("Starting replication handshake: sending PING")
        return CommandResult.resp([b"PING"], propagate=False)

    def handle_master_response(self, client, reply) -> CommandResult | None:
        """Process a single master reply and optionally return the next
        command (as list of bytes) that should be sent.
        """
        print(f"Master response: {reply}, state: {self.repl_state}")
        if self.repl_state == "PING_SENT":
            if reply == b"PONG":
                self.repl_state = "REPLCONF_PORT_SENT"
                return CommandResult.resp(
                    [b"REPLCONF", b"listening-port", str(self.server.config.port).encode()],
                    propagate=False,
                )
        elif self.repl_state == "REPLCONF_PORT_SENT":
            if reply == b"OK":
                self.repl_state = "REPLCONF_CAPA_SENT"
                return CommandResult.resp([b"REPLCONF", b"capa", b"psync2"], propagate=False)
        elif self.repl_state == "REPLCONF_CAPA_SENT":
            if reply == b"OK":
                self.repl_state = "PSYNC_SENT"
                return CommandResult.resp([b"PSYNC", b"?", b"-1"], propagate=False)
        elif self.repl_state == "PSYNC_SENT":
            if isinstance(reply, bytes) and reply.startswith(b"FULLRESYNC"):
                print("Replication handshake completed")
                
                parts = reply.split()
                self.master_replid = parts[1].decode()
                self.master_repl_offset = int(parts[2])
                self.repl_state = "RDB_TRANSFER"
  
        return None

    def finish_rdb_transfer(self, client):
        if self.repl_state != "RDB_TRANSFER":
            return

        rdb = client.parser.parse_rdb_file()
        print(f"rdb data: {rdb}")
        self.repl_state = "HS_SUCCE"

    def replconf(self, client, args: List[bytes]) -> CommandResult | str | None:
        """Process a REPLCONF request from a replica/master connection.
        """
        print(f"REPLCONF received: {args}")
        i = 0
        result = None
        while i < len(args):
            token = args[i].upper()
            if token == b"LISTENING-PORT":
                if i + 1 >= len(args):
                    raise ValueError("ERR syntax error")

                port = int(args[i + 1])
                self.server.config.replica_port = port
                i += 2
            elif token == b"CAPA":
                if i + 1 >= len(args):
                    raise ValueError("ERR syntax error")

                self.capabilities.append(args[i + 1].upper())
                i += 2
            elif token == b"GETACK":
                if i + 1 >= len(args):
                    raise ValueError("ERR syntax error")

                return CommandResult.resp(
                    [b"REPLCONF", b"ACK", f"{self.repl_offset}".encode()],
                    propagate=False,
                )
            elif token == b"ACK":
                if i + 1 >= len(args):
                    raise ValueError("ERR syntax error")
                
                offset = int(args[i + 1])
                self.replica_acks[client] = offset
                print(f"REPLCONF ACK received from {client}: offset={offset}")
                self._check_wait_clients()
                i += 2

                return None
            else:
                raise ValueError("ERR syntax error")
        return result if result is not None else "OK"

    def psync(self, client) -> tuple[str, bytes]:
        """Handle PSYNC from replica and return (header, rdb_bytes).

        The header is the FULLRESYNC line (str) and rdb_bytes is the
        binary RDB payload to send as a RESP bulk string.
        """
        self.replid = self.server.config.replid
        self.master_repl_offset = self.server.config.master_repl_offset

        header = f"FULLRESYNC {self.replid} {self.master_repl_offset}"

        rdb_bytes = self._generate_empty_rdb()

        self.register_replica(client)

        return (header, rdb_bytes)

    def wait_for_replicas(self, client, numreplicas: int, timeout: float):
        """Wait for at least numreplicas to acknowledge the current offset."""
        current_offset = self.master_repl_offset
        acknowledged = sum(1 for offset in self.replica_acks.values() if offset >= current_offset)
        
        if acknowledged >= numreplicas:
            return acknowledged
        
        deadline = time.time() + timeout if timeout > 0 else None
        self.wait_clients.append((client, current_offset, numreplicas, deadline))
        client.blocking.active = True

        for replica in self.replica_client:
            replica.send_result(CommandResult.resp([b"REPLCONF", b"GETACK", b"*"], propagate=False))
        
        return Blocked()

    def register_replica(self, client):
        client.role.add(CLIENT_REPLICA)
        self.replica_client.add(client)

    def remove_client(self, client):
        """Remove a disconnected client from replication bookkeeping."""
        self.replica_client.discard(client)
        self.replica_acks.pop(client, None)
        self.wait_clients = [entry for entry in self.wait_clients if entry[0] is not client]

        if self.master_connection is client:
            self.master_connection = None
            self.repl_state = None
    
    def propagate(self, raw_command: bytes):
        if not raw_command:
            return
        
        replicas_to_remove = []
        for replica in list(self.replica_client):
            try:
                if replica and hasattr(replica, 'connection') and replica.connection:
                    replica.send_raw(raw_command)
            except Exception as e:
                print(f"Error propagating to replica: {e}")
                replicas_to_remove.append(replica)
        
        for replica in replicas_to_remove:
            try:
                replica.close()
            except:  
                pass
            self.replica_client.discard(replica)

        sent_bytes = len(raw_command)
        before = self.master_repl_offset
        self.master_repl_offset += sent_bytes
        print(
            f"propagate: bytes={sent_bytes}, "
            f"master_repl_offset {before}->{self.master_repl_offset}"
        )
