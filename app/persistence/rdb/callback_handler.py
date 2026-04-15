from abc import ABC, abstractmethod
import time

class RdbCallback(ABC):
    """Abstract base class for RDB parsing callbacks.

    The parser calls these methods when different events occur while
    reading an RDB file. Subclasses should implement these methods
    to define how parsed data is handled.
    """

    @abstractmethod
    def on_start(self, version):
        """Called when RDB parsing starts.

        Args:
            version: RDB file version
        """
        pass

    @abstractmethod
    def on_database_select(self, db_id):
        """Called when the parser switches to a database.

        Args:
            db_id: Database index being selected
        """
        pass

    @abstractmethod
    def on_set(self, key, value, expiry=None):
        """Called when a string key-value pair is parsed.

        Args:
            key: Key name
            value: Value associated with the key
            expiry: Optional expiration time (None if not set)
        """
        pass

    @abstractmethod
    def on_list_push(self, key, value):
        """Called when a list element is parsed.

        This method may be invoked multiple times for the same key,
        once for each element in the list.

        Args:
            key: List key
            value: Element to push into the list
        """
        pass

    @abstractmethod
    def on_end(self):
        """Called when RDB parsing is complete."""
        pass

class StorageCallback(RdbCallback):
    """Callback that loads parsed RDB data into storage."""

    def __init__(self, storage):
        """Initialize the callback with a storage instance.

        Args:
            storage: Storage instance (InMemoryStorage or compatible)
        """
        self.storage = storage

    def on_start(self, version):
        """Called when RDB parsing starts."""
        pass

    def on_database_select(self, db_id):
        """Called when the parser switches to a database."""
        pass

    def on_set(self, key, value, expiry=None):
        """Called when a string key-value pair is parsed.

        Args:
            key: Key name
            value: Value associated with the key
            expiry: Optional expiration time in milliseconds (or None)
        """
        ttl_seconds = None
        if expiry is not None:
            remaining_time = (expiry - int(time.time() * 1000)) / 1000
            if remaining_time > 0:
                ttl_seconds = remaining_time
            else:
                return

        self.storage.set(key, value, ttl_seconds=ttl_seconds)

    def on_list_push(self, key, value):
        """Called when a list element is parsed.

        Args:
            key: List key
            value: Element to push into the list
        """
        self.storage.push(True, key, [value])

    def on_end(self):
        """Called when RDB parsing is complete."""
        pass
