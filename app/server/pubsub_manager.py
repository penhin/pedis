from collections import defaultdict

from app.commands.core.base import CommandResult
from app.protocol import NullBulk


class PubSubManager:
    def __init__(self, server):
        self.server = server
        self.channel_subscribers: dict[bytes, set] = defaultdict(set)

    def subscribe(self, client, channels: list[bytes]) -> CommandResult:
        result = CommandResult(propagate=False)

        for channel in channels:
            self.channel_subscribers[channel].add(client)
            count = client.pubsub.add(channel)
            result.extend(CommandResult.resp([b"subscribe", channel, count], propagate=False))

        return result

    def unsubscribe(self, client, channels: list[bytes] | None = None) -> CommandResult:
        result = CommandResult(propagate=False)
        targets = list(channels) if channels else list(client.pubsub.channels)

        if not targets:
            result.extend(CommandResult.resp([b"unsubscribe", NullBulk(), 0], propagate=False))
            client.pubsub.clear()
            return result

        for channel in targets:
            self.channel_subscribers[channel].discard(client)
            if not self.channel_subscribers[channel]:
                self.channel_subscribers.pop(channel, None)

            count = client.pubsub.remove(channel)
            result.extend(CommandResult.resp([b"unsubscribe", channel, count], propagate=False))

        return result

    def publish(self, channel: bytes, message: bytes) -> int:
        receivers = 0

        for client in list(self.channel_subscribers.get(channel, ())):
            if client.connection is None:
                continue

            client.send_result(
                CommandResult.resp([b"message", channel, message], propagate=False)
            )
            receivers += 1

        return receivers

    def remove_client(self, client):
        channels = list(client.pubsub.channels)
        for channel in channels:
            subscribers = self.channel_subscribers.get(channel)
            if not subscribers:
                continue

            subscribers.discard(client)
            if not subscribers:
                self.channel_subscribers.pop(channel, None)

        client.pubsub.clear()
