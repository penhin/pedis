import os
import time
from dataclasses import dataclass, field
from types import SimpleNamespace

from app.protocol import RESPParser


class AOFFileReader:
    def __init__(self, file):
        self.file = file

    def recv(self, n: int) -> bytes:
        return self.file.read(n)


@dataclass
class AOFTransactionState:
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

    @property
    def manifest_path(self) -> str:
        return self.server.config.aof_manifest_path()

    def open(self):
        if not self.enabled:
            return

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._write_manifest()
        self.file = open(self.path, "ab")

    def close(self):
        if self.file is None:
            return
        self.file.close()
        self.file = None

    def load(self):
        if not self.enabled:
            return False

        path = self._path_from_manifest()
        if not os.path.exists(path):
            return False

        self.loading = True
        try:
            with open(path, "rb") as f:
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

    def _write_manifest(self):
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            f.write(self.server.config.aof_manifest_content())

    def _path_from_manifest(self):
        if not os.path.exists(self.manifest_path):
            return self.path

        with open(self.manifest_path, "r", encoding="utf-8") as f:
            line = f.readline().strip()

        parts = line.split()
        if len(parts) >= 2 and parts[0] == "file":
            return os.path.join(os.path.dirname(self.manifest_path), parts[1])

        return self.path

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
            pubsub=SimpleNamespace(active=False),
            transaction=AOFTransactionState(),
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
