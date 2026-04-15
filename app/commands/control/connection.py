from ..core.base import Command, command, CommandFlag

@command("PING", -1)
def ping_command(args, context):
    if args:
        return args[0]
    return "PONG"

@command("ECHO", 1)
def echo_command(args, context):
    return args[0]

@command("SUBSCRIBE", -2)
def subscribe_command(args, context):
    channel = args[0]
    
    context.client.pubsub.subscribed = True
    channel_nums = context.client.pubsub.add(channel)
    
    return [b"subscribe", channel, channel_nums]