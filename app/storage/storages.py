from typing import Protocol, Optional, List, Iterable, Any


class Storage(Protocol):
    """Abstract storage interface for Redis data structures.
    
    All methods return Python-native types:
    - get: Optional[bytes] (None means key not found or expired)
    - set: None (side effect only)
    - push/lpop: int or List[bytes] (depends on method)
    - lrange: List[bytes]
    - get_type: Optional[str] ("string", "list", "stream", or None)
    """
    def _is_expired(self, key: bytes) -> bool:
        """Check if key has expired."""
        ...

    def keys(self, pattern: bytes) -> List[bytes]:
        """Returns all keys matching pattern."""
        ...

    def get_type(self, key: bytes) -> Optional[str]:
        """Get the type of value stored at key. Returns None if key doesn't exist."""
        ...

    def set(self, key: bytes, value: bytes, ttl_seconds: Optional[float] = None) -> None:
        """Set a string value with an optional TTL in seconds."""
        ...

    def get(self, key: bytes) -> Optional[bytes]:
        """Get string value. Returns None if key doesn't exist or is expired."""
        ...

    def incr(self, key: bytes) -> bytes:
        """"Increments the number stored at key by one."""
        ...

    def delete(self, key: bytes) -> bool:
        """Delete a key. Returns True if key existed, False otherwise."""
        ...

    def push(self, rpush: bool, key: bytes, values: Iterable[bytes]) -> int:
        """Push items to list (rpush=True for right, False for left).
        
        Returns the new length of the list.
        Returns 0 if key exists but is not a list (caller must check before using result).
        """
        ...

    def lrange(self, key: bytes, start: int, stop: int) -> List[bytes]:
        """Get list range [start, stop] (inclusive). Returns empty list if key doesn't exist or isn't a list."""
        ...

    def llen(self, key: bytes) -> int:
        """Get list length. Returns 0 if key doesn't exist or isn't a list."""
        ...

    def lpop(self, key: bytes, count: int = 1) -> Optional[List[bytes]]:
        """Pop count items from left of list.
        
        Returns None if key doesn't exist or isn't a list.
        Returns list (possibly empty) of popped items. If count > actual items, pops all.
        """
        ...

    def try_lpop(self, key: bytes) -> Optional[bytes]:
        """Pop one item from left of list without checking type errors.
        
        Returns None if key doesn't exist, isn't a list, or list is empty.
        Used for BLPOP wakeup to avoid raising errors.
        """
        ...

    def has_key(self, key: bytes) -> bool:
        """Check if key exists and is not expired."""
        ...

    def ttl(self, key: bytes) -> Optional[float]:
        """Get remaining TTL in seconds. Returns None if key doesn't exist or has no expiration."""
        ...
    
    def xadd(self, key: bytes, fields: dict[bytes, bytes], id: bytes = b'*') -> bytes:
        """Appends the specified stream entry to the stream at the specified key"""
        ...

    def xrange(self, key: bytes, start: bytes, end: bytes) -> list[list[bytes]]:
        """Returns the stream entries matching a given range of IDs."""
        ...

    def xread(self, keys: list[bytes], ids: list[bytes]) -> list[list[Any]]:
        """Read data from one or multiple streams"""
        ...
    
    def get_last_id(self, key: bytes) -> Optional[bytes]:
        """Return the last stream ID for a key, or None if the key does not exist."""
        ...

    def zadd(self, key: bytes, pairs: list[tuple[float, bytes]]) -> int:
        """Add or update scored members in the sorted set and return the count of new inserts."""
        ...

    def zrank(self, key: bytes, member: bytes) -> Optional[int]:
        """Return the zero-based rank of a member in ascending score order."""
        ...

    def zrange(self, key: bytes, start: int, stop: int) -> list[bytes]:
        """Return members whose ranks fall within the inclusive [start, stop] range."""
        ...

    def zcard(self, key: bytes) -> int:
        """Return the number of members currently stored in the sorted set."""
        ...

    def zscore(self, key: bytes, member: bytes) -> Optional[bytes]:
        """Return the score of a member as bytes, or None when the member does not exist."""
        ...

    def zrem(self, key: bytes, members: list[bytes]) -> int:
        """Remove one or more members from the sorted set and return the number removed."""
        ...
