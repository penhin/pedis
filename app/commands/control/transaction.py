from ..core.base import Command, command, CommandFlag

@command("MULTI", -1)
def multi_command(args, context):
    context.client.transaction.active = True

@command("EXEC", -1)
def exec_command(args, context):
    pass

@command("DISCARD", -1)
def discard_command(args, context):
    pass

@command("WATCH", -2)
def watch_command(args, context):
    pass

@command("UNWATCH", -1)
def unwatch_command(args, context):
    pass
