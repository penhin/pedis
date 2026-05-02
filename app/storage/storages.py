from typing import Protocol, Optional, List


class Storage(Protocol):
    """Stable core storage interface.

    This protocol intentionally only describes the small set of storage
    operations that are shared across data types. Command-specific operations
    such as list, stream, sorted-set, and geo helpers can live directly on the
    concrete storage implementation without being duplicated here.
    """
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

    def has_key(self, key: bytes) -> bool:
        """Check if key exists and is not expired."""
        ...

    def ttl(self, key: bytes) -> Optional[float]:
        """Get remaining TTL in seconds. Returns None if key doesn't exist or has no expiration."""
        ...
