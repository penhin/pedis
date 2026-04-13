import socket

CRLF = b"\r\n"

class RESPError(Exception):
    pass

class NullBulk():
    pass

class NullArray():
    pass

class RESPReader:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buffer = b""
        self.capture = None 
    
    def start_capture(self):
        if self.capture is None:
            self.capture = bytearray()
    
    def stop_capture(self):
        raw = bytes(self.capture) if self.capture else None
        self.capture = None
        return raw

    def _recv(self):
        data = self.sock.recv(4096)
        if not data:
            raise ConnectionError("connection closed")
        self.buffer += data
    
    def read_line(self) -> bytes:
        """Read one line, ending with \\r\\n(not included)"""
        while CRLF not in self.buffer:
            self._recv()
        
        line, self.buffer = self.buffer.split(CRLF, 1)
        
        if self.capture is not None:
            self.capture.extend(line + CRLF)

        return line
    
    def read_exact(self, n: int) -> bytes:
        """Read n bytes exactly"""
        while len(self.buffer) < n:
            self._recv()
        
        data = self.buffer[:n]
        self.buffer = self.buffer[n:]

        if self.capture is not None:
            self.capture.extend(data)
            
        return data

class RESPParser:
    def __init__(self, sock: socket.socket):
        self.reader = RESPReader(sock)
        self._depth = 0
        
    def parse(self):
        if self._depth == 0 :
            self.reader.start_capture()

        self._depth += 1

        try:
            prefix = self.reader.read_exact(1)
            
            if prefix == b'*':
                result = self._parse_array()
            elif prefix == b'$':
                result = self._parse_bulk_string()
            elif prefix == b'+':
                result = self._parse_simple_string()
            elif prefix == b':': 
                result = int(self.reader.read_line())
            else:
                raise Exception(f"unknown RESP type: {prefix}")
        finally:
            self._depth -= 1
        
        if self._depth == 0:
            raw_bytes = self.reader.stop_capture()
            return result, len(raw_bytes)

        return result, 0
    
    def parse_rdb_file(self):
        prefix = self.reader.read_exact(1)
        
        if prefix != b'$':
            raise RESPError("expected bulk string for RDB")

        length = int(self.reader.read_line())

        print(f"Expected {length} bytes,")

        if length == -1:
            return None
        
        data = self.reader.read_exact(length)

        if len(self.reader.buffer) >= 2 and self.reader.buffer[:2] == CRLF:
            self.reader.read_exact(2)
        print(f"Received data: {data}.")

        return data
    
    def _parse_simple_string(self):
        return self.reader.read_line()

    def _parse_bulk_string(self):
        len = int(self.reader.read_line())

        if len == -1:
            return None
        
        data = self.reader.read_exact(len)
        crlf = self.reader.read_exact(2)
        if crlf != CRLF:
            print(f"Expected {len} bytes, received data: {data}. Terminator {crlf} is not CRLF.")
            raise RESPError("invalid bulk String termination")
        
        return data

    def _parse_array(self):
        line = self.reader.read_line()
        n = int(line)
        if n == -1: return None
        
        data = []
        for _ in range(n):
            val, _ = self.parse()
            data.append(val)
        return data

class RESPEncoder:
    def simple(self, s: str) -> bytes:
        data = s.encode("utf-8")
        if b"\r" in data or b"\n" in data:
            raise ValueError("Simple String cannot contatin CR or LF")
        return b"+" + data + CRLF 

    def error(self, s: str) -> bytes:
        error = s.encode("utf-8")           
        return b"-" + error + CRLF 

    def integer(self, n: int) -> bytes:
        return b":" + str(n).encode() + CRLF

    def bulk(self, v: bytes | NullBulk) -> bytes:
        if isinstance(v, NullBulk):
            return b"$-1" + CRLF
        
        return b"$" + str(len(v)).encode() + CRLF + v + CRLF
    
    def bulk_raw(self, v: bytes | NullBulk) -> bytes:
        if isinstance(v, NullBulk):
            return b"$-1" + CRLF
        
        return b"$" + str(len(v)).encode() + CRLF + v

    def array(self, items: list | NullArray) -> bytes:
        if isinstance(items, NullArray):
            return b"*-1" + CRLF

        result = b"*" + str(len(items)).encode() + CRLF

        for item in items:
            result += self.encode(item)

        return result

    def encode(self, value) -> bytes:
        if isinstance(value, Exception):
            return self.error(str(value))
        elif isinstance(value, str):
            return self.simple(value)
        elif isinstance(value, int):
            return self.integer(value)
        elif isinstance(value, NullBulk) or isinstance(value, bytes):
            return self.bulk(value)
        elif isinstance(value, NullArray) or isinstance(value, list):
            return self.array(value)
        else:
            return value