import os
import sys
import time
import socket
import secrets
import selectors
import traceback
from fnmatch import fnmatch

from app.storage.memory import InMemoryStorage
from app.commands.core.dispatcher import CommandDispatcher
from app.persistence.aof import AOFManager
from app.server.acl import ACLManager
from app.server.block_manager import BlockedClientsManager
from app.server.pubsub_manager import PubSubManager
from app.server.replication_manager import ReplicationManager

from .client import Client, CLIENT_MASTER, CLIENT_NORMAL

class ServerConfig:

    def __init__(self):
        self.role = "master"
        # REPLICATION STREAM ID
        self.replid = self.generate_replid()
        # REPLICATION STREAM POSITION
        self.master_repl_offset = 0

        self.port = 6379
        self.bind = "localhost"

        self.is_salve = False
        self.master_host = None
        self.master_port = None

        self.dir = os.getcwd()
        self.dbfilename = "dump.rdb"
        self.requirepass = None
        self.appendonly = False
        self.appenddirname = "appendonlydir"
        self.appendfilename = "appendonly.aof"
        self.appendfsync = "everysec"
    
    def info(self):
        info = (
            f"role:{self.role}\n"
            f"master_replid:{self.replid}\n"
            f"master_repl_offset:{self.master_repl_offset}"
            )

        return info
    
    def get(self, pattern):
        result = []
        pattern_text = pattern.decode()
        
        for key in ("dir", "dbfilename", "appendonly", "appenddirname", "appendfilename", "appendfsync"):
            if fnmatch(key, pattern_text):
                result.append(key.encode())
                value = getattr(self, key)
                if isinstance(value, bool):
                    value = "yes" if value else "no"
                result.append(str(value).encode())
        
        return result
    
    def generate_replid(self):
        return secrets.token_hex(20)
    
    def parse_config(config):
        args = sys.argv[1:]
        
        i = 0

        while i < len(args):
            if args[i] == "--port":
                config.port = int(args[i + 1])
                i += 2
            elif args[i] == "--replicaof":
                master_host, master_port = args[i + 1].split(' ')    
                config.master_host = master_host
                config.master_port = int(master_port)
                config.role = "slave"
                config.is_salve = True
                i += 3
            elif args[i] == "--dir":
                config.dir = args[i + 1]
                i += 2
            elif args[i] == "--dbfilename":
                config.dbfilename = args[i + 1]
                i += 2
            elif args[i] == "--requirepass":
                config.requirepass = args[i + 1].encode()
                i += 2
            elif args[i] == "--appendonly":
                config.appendonly = ServerConfig.parse_yes_no(args[i + 1], "--appendonly")
                i += 2
            elif args[i] == "--appenddirname":
                config.appenddirname = args[i + 1]
                i += 2
            elif args[i] == "--appendfilename":
                config.appendfilename = args[i + 1]
                i += 2
            elif args[i] == "--appendfsync":
                config.appendfsync = args[i + 1].lower()
                if config.appendfsync not in ("always", "everysec", "no"):
                    raise ValueError("--appendfsync must be always, everysec, or no")
                i += 2

            else:
                raise ValueError(f"Unknown option {args[i]}")

        return config

    def aof_path(self):
        return os.path.join(self.dir, self.appenddirname, f"{self.appendfilename}.1.incr.aof")

    def aof_manifest_path(self):
        return os.path.join(self.dir, self.appenddirname, f"{self.appendfilename}.manifest")

    def aof_manifest_content(self):
        return f"file {self.appendfilename}.1.incr.aof seq 1 type i\n"

    def parse_yes_no(value: str, option: str) -> bool:
        normalized = value.lower()
        if normalized == "yes":
            return True
        if normalized == "no":
            return False
        raise ValueError(f"{option} must be yes or no")

class RedisServer:

    def __init__(self, config: ServerConfig):
        self.config = config
        
        self.server_socket: socket = None
        self.master_socket: socket = None
        self.sel = selectors.DefaultSelector()

        self.storage = InMemoryStorage()
        self.acl = ACLManager()
        if self.config.requirepass is not None:
            self.acl.set_user(b"default", [b"on", b"resetpass", b">" + self.config.requirepass, b"~*", b"+@all"])
        self.dispatcher = CommandDispatcher()
        self.blocked_manager = BlockedClientsManager(self)
        self.pubsub = PubSubManager(self)
        
        self.clients = set()
        self.replication = ReplicationManager(self)
        self.aof = AOFManager(self)

    def info(self) -> bytes:
        """Return server information as RESP-safe bytes."""
        return self.config.info().encode()
    
    def get(self, pattern: bytes) -> list:
        """Return server config as RESP array"""
        return self.config.get(pattern)
    
    def start(self):
        self.start_server_socket()
        
        if self.config.is_salve:
            self.connect_to_master()
        
        self.run_event_loop()
    
    def start_server_socket(self):
        server_socket = socket.create_server(
            (self.config.bind, self.config.port),
            reuse_port=False
        )
        server_socket.setblocking(False)
        
        self.sel.register(server_socket, selectors.EVENT_READ, data="accept")
        self.server_socket = server_socket    
        
    def connect_to_master(self):
        sock = socket.create_connection(
            (self.config.master_host, self.config.master_port)
        )
        sock.setblocking(False)
        
        self.master_socket = sock

        client = Client(sock, None, self, flags=[CLIENT_MASTER])
        self.clients.add(client)
        
        self.sel.register(sock, selectors.EVENT_READ, data=client)

        initial = self.replication.start_replication(client)
        if initial:
            client.send_result(initial)
    
    def run_event_loop(self):
        print("Server start running event loop...")
        sel = self.sel
        server_socket = self.server_socket

        while True:
            events = sel.select(timeout=0.05)
            
            for key, mask in events:
                if key.data == "accept":
                    conn, addr = server_socket.accept()
                    conn.setblocking(False)
                    client = Client(conn, addr, self, flags=[CLIENT_NORMAL])
                    
                    print("Detect a new client connection...")
                    sel.register(conn, selectors.EVENT_READ, data=client)
                    self.clients.add(client)
                else:
                    client = key.data
                    try:
                        client.handler.handle(sel)
                    except Exception as e:
                        print(f"Err {e}")
                        client.close()
                        traceback.print_exc()

            self.blocked_manager.check_timeouts(time.time())
            self.replication.check_timeouts(time.time())
            


