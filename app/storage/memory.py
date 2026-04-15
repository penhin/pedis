import time

from fnmatch import fnmatch
from collections import deque
from typing import Optional, List, Iterable, Any

from app.storage.storages import Storage
from app.storage.errors import (
    InvalidStreamIdError,
    InvalidValueError,
    StreamIdOrderError,
    WrongTypeError,
)


class StreamEntry:
    def __init__(self, ms: int, seq: int, fields: dict[bytes, bytes]):
        self.ms = ms
        self.seq = seq
        self.fields = fields

    @property
    def id(self):
        return (f"{self.ms}-{self.seq}").encode()

class Stream:
    def __init__(self):
        self.entries = deque()
        self.last_ms = 0
        self.last_seq = 0
    
    @property
    def last_id(self):
        return f"{self.last_ms}-{self.last_seq}".encode()

    def next_id(self) -> tuple[int, int]:
        ms = int(time.time() * 1000)
        
        if ms == self.last_ms:
            self.last_seq += 1
        else:
            self.last_ms = ms
            self.last_seq = 0
        
        return self.last_ms, self.last_seq
    
    def parse_id(self, id: bytes) -> tuple[int | float, int | float | None]:

        if id == b'-':
            return 0, 0
        if id == b'+':
            return float('inf'), float('inf')
        parts = id.split(b'-')
        if len(parts) == 1:
            ms = int(parts[0])
            seq = None
        elif len(parts) == 2:
            ms = int(parts[0])
            if parts[1] == b'*':
                seq = float('inf')
            else:
                seq = int(parts[1])
        else:
            raise ValueError("Invalid stream ID format")
        return ms, seq
    
    def add(self, fields: dict[bytes, bytes], id: bytes = b'*') -> bytes:
        
        if id == b'*':
            ms, seq = self.next_id()
        else:
            ms, seq = self.parse_id(id)
            if seq == float('inf'):
                seq = self.last_seq + 1 if ms == self.last_ms else 0
            if (ms, seq) == (0, 0):
                raise InvalidStreamIdError("The ID specified in XADD must be greater than 0-0")
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
        start_ms, start_seq = self.parse_id(start)
        end_ms, end_seq = self.parse_id(end)
        start_seq = 0 if start_seq is None else start_seq
        end_seq = float('inf') if end_seq is None else end_seq
        result = []
        for entry in self.entries:
            if (start_ms, start_seq) <= (entry.ms, entry.seq) <= (end_ms, end_seq):
                id = f'{entry.ms}-{entry.seq}'.encode()
                result.append([id, [x for kv in entry.fields.items() for x in kv]])
        return result
    
    def read(self, id: bytes) -> list[list[bytes]]:
        ms, seq = self.parse_id(id)
        result = []
        for entry in self.entries:
            if (entry.ms, entry.seq) > (ms, seq):
                id = f'{entry.ms}-{entry.seq}'.encode()
                result.append([id, [x for kv in entry.fields.items() for x in kv]])
        return result

class RedisValue:
    """Wrapper for typed Redis values."""

    def __init__(self, dtype: str, value: Any):
        self.type = dtype
        self.value = value

    def is_string(self) -> bool:
        return self.type == "string"

    def is_list(self) -> bool:
        return self.type == "list"

    def is_stream(self) -> bool:
        return self.type == "stream"

class InMemoryStorage():
    """In-memory implementation of Storage interface.
    
    Provides O(1) list operations using deque and lazy expiration cleanup.
    """

    def __init__(self):
        self.store: dict[bytes, RedisValue] = {}
        self.expire: dict[bytes, float] = {} 

    def _is_expired(self, key: bytes) -> bool:
        """Check if key has expired."""
        exp = self.expire.get(key)
        if exp is None:
            return False
        if exp <= time.time():
            self.store.pop(key, None)
            self.expire.pop(key, None)
            return True
        return False
    
    def keys(self, pattern: bytes) -> List[bytes]:
        """Returns all keys matching pattern."""
        result = []

        for key in self.store.keys():
            if fnmatch(key, pattern):
                result.append(key)
                
        return result

    def get_type(self, key: bytes) -> Optional[str]:
        """Get the type of value stored at key."""
        if self._is_expired(key):
            return None
        entry = self.store.get(key)
        return entry.type if entry else None

    def set(self, key: bytes, value: bytes, ttl_seconds: Optional[float] = None) -> None:
        """Set a string value with optional TTL."""
        self.store[key] = RedisValue("string", value)
        if ttl_seconds is None:
            self.expire.pop(key, None)
        elif ttl_seconds > 0:
            self.expire[key] = time.time() + ttl_seconds
        else:
            self.store.pop(key, None)
            self.expire.pop(key, None)

    def get(self, key: bytes) -> Optional[bytes]:
        """Get string value. Returns None if key doesn't exist or is expired."""
        if self._is_expired(key):
            return None

        entry = self.store.get(key)
        if entry is None:
            return None
        if not entry.is_string():
            raise WrongTypeError

        return entry.value
    
    def incr(self, key: bytes) -> bytes:
        """"Increments the number stored at key by one."""
        entry = self.store.get(key)
        
        if entry is None:
            self.set(key, b"1")
            return b"1"
        elif not entry.is_string():
            raise WrongTypeError
        
        try:
            entry.value = str(int(entry.value) + 1).encode()
        except ValueError:
            raise InvalidValueError        

        return entry.value

    def delete(self, key: bytes) -> bool:
        """Delete a key."""
        had_key = key in self.store
        self.store.pop(key, None)
        self.expire.pop(key, None)
        return had_key

    def has_key(self, key: bytes) -> bool:
        """Check if key exists and is not expired."""
        return key in self.store and not self._is_expired(key)

    def ttl(self, key: bytes) -> Optional[float]:
        """Get remaining TTL in seconds. Returns None if no expiration."""
        if key not in self.store or self._is_expired(key):
            return None
        exp = self.expire.get(key)
        if exp is None:
            return None
        remaining = exp - time.time()
        return remaining if remaining > 0 else None

    def push(self, rpush: bool, key: bytes, values: Iterable[bytes]) -> int:
        """Push items to list (rpush=True for right, False for left)."""
        entry = self.store.get(key)

        if entry is None:
            entry = RedisValue("list", deque())
            self.store[key] = entry
        elif not entry.is_list():
            raise WrongTypeError

        lst: deque = entry.value

        if rpush:
            lst.extend(values)
        else:
            for v in values:
                lst.appendleft(v)

        return len(lst)

    def lrange(self, key: bytes, start: int, stop: int) -> List[bytes]:
        """Get list range [start, stop] (inclusive)."""
        if self._is_expired(key):
            return []

        entry = self.store.get(key)
        if entry is None:
            return []
        if not entry.is_list():
            raise WrongTypeError

        lst: deque = entry.value
        n = len(lst)

        if start < 0:
            start += n
        if stop < 0:
            stop += n

        start = max(start, 0)
        stop = min(stop, n - 1)

        if start > stop:
            return []

        return list(lst)[start : stop + 1]

    def llen(self, key: bytes) -> int:
        """Get list length."""
        if self._is_expired(key):
            return 0

        entry = self.store.get(key)
        if entry is None:
            return 0
        if not entry.is_list():
            raise WrongTypeError

        return len(entry.value)

    def lpop(self, key: bytes, count: int = 1) -> Optional[List[bytes]]:
        """Pop count items from left of list."""
        if self._is_expired(key):
            return None

        entry = self.store.get(key)
        if entry is None:
            return None
        if not entry.is_list():
            raise WrongTypeError

        lst: deque = entry.value
        if len(lst) == 0:
            return None

        count = min(count, len(lst))
        result = []

        for _ in range(count):
            result.append(lst.popleft())

        return result[0] if count == 1 else result
    
    def try_lpop(self, key: bytes) -> Optional[bytes]:
        """Pop one item from left of list without raising errors."""
        if self._is_expired(key):
            return None

        entry = self.store.get(key)
        if entry is None or not entry.is_list():
            return None

        lst: deque = entry.value
        if len(lst) == 0:
            return None

        return lst.popleft()
    
    def xadd(self, key: bytes, fields: dict[bytes, bytes], id: bytes = b'*') -> bytes:
        """Appends the specified stream entry to the stream at the specified key"""
        entry = self.store.get(key)
        if entry is None:
            entry = RedisValue("stream", Stream())
            self.store[key] = entry
        elif not entry.is_stream():
            raise WrongTypeError

        stream: Stream = entry.value
        return stream.add(fields, id)

    def xrange(self, key: bytes, start: bytes, end: bytes) -> list[list[bytes]]:
        """Returns the stream entries matching a given range of IDs."""
        entry = self.store.get(key)
        if entry is None:
            return []
        elif not entry.is_stream():
            raise WrongTypeError

        stream: Stream = entry.value
        return stream.range(start, end)

    def xread(self, keys: list[bytes], ids: list[bytes]) -> list[list[Any]]:
        """Read data from one or multiple streams"""
        result = []
        for key, id in zip(keys, ids):
            entry = self.store.get(key)
            if entry is None:
                result.append([key, []])
            elif not entry.is_stream():
                raise WrongTypeError
            else:
                stream: Stream = entry.value
                result.append([key, stream.read(id)])
        return result
    
    def get_last_id(self, key: bytes) -> Optional[bytes]:
        """"Return the stream last ID"""
        entry = self.store.get(key)
        if entry is None:
            return None

        if not entry.is_stream():
            raise WrongTypeError

        stream: Stream = entry.value
        return stream.last_id
