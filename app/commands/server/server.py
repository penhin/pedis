from ..core.base import CommandError, command

@command("INFO", -1)
def info_command(args, context):
    if len(args) > 1:
        raise CommandError("ERR wrong number of arguments for 'info' command")

    return context.server.info()

@command("CONFIG", -3)
def info_command(args, context):
    return context.server.get(args[1])
