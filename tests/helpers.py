import os
import socket
import subprocess
import sys
import time
from contextlib import closing


CRLF = b"\r\n"


class RedisError(Exception):
    pass


class RedisClient:
    def __init__(self, port, host="127.0.0.1", timeout=2.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.file = self.sock.makefile("rb")

    def close(self):
        try:
            self.file.close()
        finally:
            self.sock.close()

    def command(self, *parts):
        self.sock.sendall(encode_command(parts))
        return self.read_response()

    def read_response(self):
        prefix = self.file.read(1)
        if not prefix:
            raise ConnectionError("server closed connection")

        if prefix == b"+":
            return self._read_line().decode()
        if prefix == b"-":
            return RedisError(self._read_line().decode())
        if prefix == b":":
            return int(self._read_line())
        if prefix == b"$":
            size = int(self._read_line())
            if size == -1:
                return None
            data = self.file.read(size)
            terminator = self.file.read(2)
            if terminator != CRLF:
                raise ValueError(f"invalid bulk terminator: {terminator!r}")
            return data
        if prefix == b"*":
            size = int(self._read_line())
            if size == -1:
                return None
            return [self.read_response() for _ in range(size)]

        raise ValueError(f"unknown RESP prefix: {prefix!r}")

    def _read_line(self):
        line = self.file.readline()
        if not line.endswith(CRLF):
            raise ValueError(f"invalid RESP line: {line!r}")
        return line[:-2]


class PedisServer:
    def __init__(self, *args):
        self.port = free_port()
        cmd = [
            sys.executable,
            "-m",
            "app.main",
            "--port",
            str(self.port),
            *args,
        ]
        self.proc = subprocess.Popen(
            cmd,
            cwd=repo_root(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._wait_until_ready()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()

    def client(self):
        return RedisClient(self.port)

    def stop(self):
        if self.proc.poll() is not None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=2)

    def _wait_until_ready(self):
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"pedis exited with code {self.proc.returncode}")
            try:
                with closing(socket.create_connection(("127.0.0.1", self.port), timeout=0.1)):
                    return
            except OSError:
                time.sleep(0.02)
        raise TimeoutError("pedis did not start listening in time")


def encode_command(parts):
    encoded = f"*{len(parts)}\r\n".encode()
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        elif isinstance(part, int):
            part = str(part).encode()
        encoded += b"$" + str(len(part)).encode() + CRLF + part + CRLF
    return encoded


def free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def repo_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

