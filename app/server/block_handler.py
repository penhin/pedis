from abc import ABC, abstractmethod

class BlockStrategy(ABC):
    """Abstract base class for blocking strategies."""

    should_continue = False
    
    def block(self, manager, client, **kwargs):
        """Block the client according to the strategy.
        
        Args:
            manager: BlockedClientsManager instance
            client: Client instance
            **kwargs: Strategy-specific parameters
        """
        for key in client.blocking.keys:
            manager.blocked_clients[key].append(client)
    
    @abstractmethod
    def can_unblock(self, manager, client, key=None, **kwargs):
        """Check if the client can be unblocked.
        
        Args:
            manager: BlockedClientsManager instance
            client: Client instance
            key: The key being notified (for key-based strategies)
            **kwargs: Additional context
            
        Returns:
            Response data if unblockable, None otherwise
        """
        pass
    
    def get_response(self, manager, client, data):
        """Generate the response for unblocking.
        
        Args:
            manager: BlockedClientsManager instance
            client: Client instance
            data: Data from can_unblock
            
        Returns:
            Response to send to client
        """
        if data is None:
            return None
        
        return data

class ListStrategy(BlockStrategy):
    def block(self, manager, client, **kwargs):
        super().block(manager, client, **kwargs)
    
    def can_unblock(self, manager, client, key=None, **kwargs):
        if key is None:
            return None
        
        value = manager.server.storage.try_lpop(key)
        
        if value is not None:
            return [key, value]
        
        return None
    
    def get_response(self, manager, client, data):
        return super().get_response(manager, client, data)

class StreamStrategy(BlockStrategy):

    should_continue = True

    def block(self, manager, client, **kwargs):
        super().block(manager, client, **kwargs)
    
    def can_unblock(self, manager, client, key=None, **kwargs):
        if key is None:
            return None
        
        idx = client.blocking.keys.index(key)
        last_id = client.blocking.ids[idx]
        result = manager.server.storage.xread([key], [last_id])
        if result and result[0][1]:
            return result
        else:
            return None

    def get_response(self, manager, client, data):
        return super().get_response(manager, client, data)
        
class WaitStrategy(BlockStrategy):

    def block(self, manager, client, **kwargs):
        pass

    def can_unblock(self, manager, client, key=None, **kwargs):
        pass
    
    def get_response(self, manager, client, data):
        pass
