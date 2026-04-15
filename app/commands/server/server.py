from ..core.base import command

@command("INFO", 1)
def info_command(args, context):
    return context.server.info()

@command("CONFIG", -3)
def info_command(args, context):
    return context.server.get(args[1])
