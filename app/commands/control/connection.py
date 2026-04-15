from ..core.base import CommandResult, command, CommandFlag

@command("PING", -1, flags=[CommandFlag.ALLOWED_IN_PUBSUB])
def ping_command(args, context):
    if args:
        return args[0]
    return "PONG"

@command("ECHO", 1)
def echo_command(args, context):
    return args[0]

@command("SUBSCRIBE", -2, flags=[CommandFlag.ALLOWED_IN_PUBSUB])
def subscribe_command(args, context):
    return context.pubsub.subscribe(context.client, args)

@command("UNSUBSCRIBE", -1, flags=[CommandFlag.ALLOWED_IN_PUBSUB])
def unsubscribe_command(args, context):
    channels = args if args else None
    return context.pubsub.unsubscribe(context.client, channels)

@command("PUBLISH", 2, flags=[CommandFlag.WRITE])
def publish_command(args, context):
    channel, message = args
    return context.pubsub.publish(channel, message)
