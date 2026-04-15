import heapq

from collections import defaultdict, deque

from app.commands.core.base import CommandResult, normalize_command_result

from .client import BlockedType

class BlockedClientsManager:
    def __init__(self, server):
        self.server = server
        # {(timeout, client_id, client)}
        self.timeout_heap = [] 
        # {key: deque(client..)}
        self.blocked_clients = defaultdict(deque)

    def block_client(self, client, keys, timeout=None, ids=None, blocked_type=BlockedType.LIST, **extra):
        """Block a client on the given keys with optional timeout and ids."""
        client.blocking.keys = list(keys or [])
        client.blocking.ids = list(ids) if ids else [b'0-0'] * len(client.blocking.keys)
        client.blocking.timeout = timeout
        client.blocking.kind = blocked_type
        client.blocking.strategy = blocked_type.value()
        client.blocking.active = True
        
        strategy = client.blocking.strategy
        if strategy:
            strategy.block(self, client, **extra)
        
        if timeout is not None:
            heapq.heappush(self.timeout_heap, (timeout, id(client), client))

    def unblock_client(self, client, response=None):
        """Unblock a client and send response."""
        if not client.blocking.active:
                return

        for key in client.blocking.keys:
            queue = self.blocked_clients.get(key)
            if queue and client in queue:
                queue.remove(client)

        client.blocking.clear()

        result = CommandResult.null_array() if response is None else normalize_command_result(response)
        result.propagate = False
        client.send_result(result)

    def remove_client(self, client):
        """Remove a client from all blocked queues and clear its state."""
        for key in list(client.blocking.keys):
            queue = self.blocked_clients.get(key)
            if queue and client in queue:
                queue.remove(client)

        client.blocking.clear()

    def notify_key(self, key):
        """Notify that a key has been updated, potentially unblocking clients."""
        queue = self.blocked_clients.get(key)
        if not queue:
            return

        while queue:
            client = queue.popleft()
            if not client.blocking.active or key not in client.blocking.keys:
                continue  

            response = None
            strategy = client.blocking.strategy
            if strategy:
                response = strategy.get_response(self, client, strategy.can_unblock(self, client, key))

            if response is not None:
                self.unblock_client(client, response)
                for other_key in client.blocking.keys:
                    if other_key != key:
                        other_queue = self.blocked_clients.get(other_key)
                        if other_queue and client in other_queue:
                            other_queue.remove(client)
                            
                if not strategy.should_continue:
                    break

            else:
                queue.appendleft(client)
                break

    def check_timeouts(self, current_time):
        """Check for timed-out clients and unblock them."""
        while self.timeout_heap:
            timeout, client_id, client = self.timeout_heap[0]
            if timeout > current_time:
                break 

            heapq.heappop(self.timeout_heap)

            if client.blocking.active and client.blocking.timeout == timeout:
                self.unblock_client(client)
