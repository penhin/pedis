class Context:

    def __init__(self, server, client):
        self.server = server       # RedisServer instance
        self.client = client       # Client connection object

    @property
    def storage(self):
        return self.server.storage

    @property
    def blocked_manager(self):
        return self.server.blocked_manager

    @property
    def encoder(self):
        return self.client.encoder

    def wake_client(self, client, result):
        self.blocked_manager.unblock_client(client, result)
