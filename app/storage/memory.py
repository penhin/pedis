import time

from fnmatch import fnmatch
from collections import deque
from typing import Optional, List, Iterable, Any

from app.storage.errors import *
from app.storage.storages import Storage
from app.storage.value import RedisValue, Stream, SortedSet

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

    def _get_entry(self, key: bytes) -> Optional[RedisValue]:
        """Return the current entry for a key, treating expired keys as missing."""
        if self._is_expired(key):
            return None
        return self.store.get(key)
    
    def keys(self, pattern: bytes) -> List[bytes]:
        """Returns all keys matching pattern."""
        result = []

        for key in list(self.store.keys()):
            if self._get_entry(key) is not None and fnmatch(key, pattern):
                result.append(key)
                
        return result

    def get_type(self, key: bytes) -> Optional[str]:
        """Get the type of value stored at key."""
        entry = self._get_entry(key)
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
        entry = self._get_entry(key)
        if entry is None:
            return None
        if not entry.is_string():
            raise WrongTypeError

        return entry.value
    
    def incr(self, key: bytes) -> bytes:
        """"Increments the number stored at key by one."""
        entry = self._get_entry(key)
        
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
        had_key = self._get_entry(key) is not None
        self.store.pop(key, None)
        self.expire.pop(key, None)
        return had_key

    def has_key(self, key: bytes) -> bool:
        """Check if key exists and is not expired."""
        return self._get_entry(key) is not None

    def ttl(self, key: bytes) -> Optional[float]:
        """Get remaining TTL in seconds. Returns None if no expiration."""
        if self._get_entry(key) is None:
            return None
        exp = self.expire.get(key)
        if exp is None:
            return None
        remaining = exp - time.time()
        return remaining if remaining > 0 else None

    def push(self, rpush: bool, key: bytes, values: Iterable[bytes]) -> int:
        """Push items to list (rpush=True for right, False for left)."""
        entry = self._get_entry(key)

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
        entry = self._get_entry(key)
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
        entry = self._get_entry(key)
        if entry is None:
            return 0
        if not entry.is_list():
            raise WrongTypeError

        return len(entry.value)

    def lpop(self, key: bytes, count: int = 1) -> Optional[List[bytes]]:
        """Pop count items from left of list."""
        entry = self._get_entry(key)
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
        entry = self._get_entry(key)
        if entry is None or not entry.is_list():
            return None

        lst: deque = entry.value
        if len(lst) == 0:
            return None

        return lst.popleft()
    
    def xadd(self, key: bytes, fields: dict[bytes, bytes], id: bytes = b'*') -> bytes:
        """Appends the specified stream entry to the stream at the specified key"""
        entry = self._get_entry(key)
        if entry is None:
            entry = RedisValue("stream", Stream())
            self.store[key] = entry
        elif not entry.is_stream():
            raise WrongTypeError

        stream: Stream = entry.value
        return stream.add(fields, id)

    def xrange(self, key: bytes, start: bytes, end: bytes) -> list[list[bytes]]:
        """Returns the stream entries matching a given range of IDs."""
        entry = self._get_entry(key)
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
            entry = self._get_entry(key)
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
        entry = self._get_entry(key)
        if entry is None:
            return None

        if not entry.is_stream():
            raise WrongTypeError

        stream: Stream = entry.value
        return stream.last_id

    def zadd(self, key: bytes, pairs: list[tuple[float, bytes]]) -> int:
        """Add or update scored members in the sorted set and return the count of new inserts."""
        entry = self._get_entry(key)
        if entry is None:
            entry = RedisValue("zset", SortedSet())
            self.store[key] = entry
        elif not entry.is_zset():
            raise WrongTypeError

        zset: SortedSet = entry.value
        return zset.add(pairs)

    def zrank(self, key: bytes, member: bytes) -> Optional[int]:
        """Return the zero-based rank of a member in ascending score order."""
        entry = self._get_entry(key)
        if entry is None:
            return None

        elif not entry.is_zset():
            raise WrongTypeError

        zset: SortedSet = entry.value
        return zset.rank(member)

    def zrange(self, key: bytes, start: int, stop: int) -> list[bytes]:
        """Return members whose ranks fall within the inclusive [start, stop] range."""
        entry = self._get_entry(key)
        if entry is None:
            return []

        elif not entry.is_zset():
            raise WrongTypeError

        zset: SortedSet = entry.value
        return zset.range(start, stop)

    def zcard(self, key: bytes) -> int:
        """Return the number of members currently stored in the sorted set."""
        entry = self._get_entry(key)
        if entry is None:
            return 0
        if not entry.is_zset():
            raise WrongTypeError

        zset: SortedSet = entry.value
        return zset.card()

    def zscore(self, key: bytes, member: bytes) -> Optional[bytes]:
        """Return the score of a member as bytes, or None when the member does not exist."""
        entry = self._get_entry(key)
        if entry is None:
            return None
        if not entry.is_zset():
            raise WrongTypeError

        zset: SortedSet = entry.value
        return zset.score(member)

    def zrem(self, key: bytes, members: list[bytes]) -> int:
        """Remove one or more members from the sorted set and return the number removed."""
        entry = self._get_entry(key)
        if entry is None:
            return 0
        if not entry.is_zset():
            raise WrongTypeError

        zset: SortedSet = entry.value
        return zset.remove(members)
