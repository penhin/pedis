import time

from collections import deque

from app.storage.errors import (
    InvalidStreamIdError,
    StreamIdOrderError,
)


class StreamEntry:
    """Represents a single entry in a Redis stream."""

    def __init__(self, ms: int, seq: int, fields: dict[bytes, bytes]):
        self.ms = ms
        self.seq = seq
        self.fields = fields

    @property
    def id(self):
        return (f"{self.ms}-{self.seq}").encode()


class Stream:
    """Redis stream implementation."""

    def __init__(self):
        self.entries = deque()
        self.last_ms = 0
        self.last_seq = 0

    @property
    def last_id(self) -> bytes:
        return f"{self.last_ms}-{self.last_seq}".encode()

    def next_id(self) -> tuple[int, int]:
        """Generate the next stream ID."""
        ms = int(time.time() * 1000)

        if ms == self.last_ms:
            self.last_seq += 1
        else:
            self.last_ms = ms
            self.last_seq = 0

        return self.last_ms, self.last_seq

    def parse_id(self, id: bytes) -> tuple[int | float, int | float | None]:
        """Parse a stream ID string into (ms, seq) tuple."""
        if id == b"-":
            return 0, 0
        if id == b"+":
            return float("inf"), float("inf")
        parts = id.split(b"-")
        if len(parts) == 1:
            ms = int(parts[0])
            seq = None
        elif len(parts) == 2:
            ms = int(parts[0])
            if parts[1] == b"*":
                seq = float("inf")
            else:
                seq = int(parts[1])
        else:
            raise ValueError("Invalid stream ID format")
        return ms, seq

    def add(self, fields: dict[bytes, bytes], id: bytes = b"*") -> bytes:
        """Add an entry to the stream."""
        if id == b"*":
            ms, seq = self.next_id()
        else:
            ms, seq = self.parse_id(id)
            if seq == float("inf"):
                seq = self.last_seq + 1 if ms == self.last_ms else 0
            if (ms, seq) == (0, 0):
                raise InvalidStreamIdError(
                    "The ID specified in XADD must be greater than 0-0"
                )
            if (ms, seq) <= (self.last_ms, self.last_seq):
                raise StreamIdOrderError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                )
            self.last_ms = ms
            self.last_seq = seq

        e = StreamEntry(ms, seq, fields)
        self.entries.append(e)
        return e.id

    def range(self, start: bytes, end: bytes) -> list[list[bytes]]:
        """Get stream entries in ID range."""
        start_ms, start_seq = self.parse_id(start)
        end_ms, end_seq = self.parse_id(end)
        start_seq = 0 if start_seq is None else start_seq
        end_seq = float("inf") if end_seq is None else end_seq
        result = []
        for entry in self.entries:
            if (start_ms, start_seq) <= (entry.ms, entry.seq) <= (end_ms, end_seq):
                id = f"{entry.ms}-{entry.seq}".encode()
                result.append([id, [x for kv in entry.fields.items() for x in kv]])
        return result

    def read(self, id: bytes) -> list[list[bytes]]:
        """Read stream entries after a given ID."""
        ms, seq = self.parse_id(id)
        result = []
        for entry in self.entries:
            if (entry.ms, entry.seq) > (ms, seq):
                id = f"{entry.ms}-{entry.seq}".encode()
                result.append([id, [x for kv in entry.fields.items() for x in kv]])
        return result
