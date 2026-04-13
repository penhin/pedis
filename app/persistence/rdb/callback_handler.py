from abc import ABC, abstractmethod

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
    pass