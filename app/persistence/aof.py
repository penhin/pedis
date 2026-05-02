import os
import time
from types import SimpleNamespace

from app.protocol import RESPParser
from app.server.client import PubSubState, TransactionState


class AOFFileReader:
    def __init__(self, file):
        self.file = file

    def recv(self, n: int) -> bytes:
        return self.file.read(n)


class AOFManager:
    def __init__(self, server):
        self.server = server
        self.enabled = server.config.appendonly
        self.file = None
        self.last_fsync = 0.0
        self.loading = False

    @property
    def path(self) -> str:
        return self.server.config.aof_path()

    def open(self):
        if not self.enabled:
            return

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.file = open(self.path, "ab")

    def close(self):
        if self.file is None:
            return
        self.file.close()
        self.file = None

    def load(self):
        if not self.enabled or not os.path.exists(self.path):
            return False

        self.loading = True
        try:
            with open(self.path, "rb") as f:
                parser = RESPParser(AOFFileReader(f))
                while True:
                    try:
                        cmd_list, _ = parser.parse()
                    except ConnectionError:
                        break
                    if not cmd_list:
                        continue

                    context = self._load_context()
                    self.server.dispatcher.dispatch(cmd_list, b"", context)
        finally:
            self.loading = False

        return True

    def append(self, raw_command: bytes):
        if not self.enabled or self.loading or not raw_command:
            return

        if self.file is None:
            self.open()

        self.file.write(raw_command)
        self.file.flush()

        if self.server.config.appendfsync == "always":
            os.fsync(self.file.fileno())
            self.last_fsync = time.time()
        elif self.server.config.appendfsync == "everysec":
            now = time.time()
            if now - self.last_fsync >= 1:
                os.fsync(self.file.fileno())
                self.last_fsync = now

    def _load_context(self):
        client = SimpleNamespace(
            pubsub=PubSubState(),
            transaction=TransactionState(),
            auth=SimpleNamespace(user=b"default", authenticated=True),
        )
        return SimpleNamespace(
            server=self.server,
            client=client,
            storage=self.server.storage,
            blocked_manager=self.server.blocked_manager,
            pubsub=self.server.pubsub,
            encoder=None,
        )
