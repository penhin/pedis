from ..core.base import Command, command, CommandFlag

@command("TYPE", 1)
def type_command(args, context):
    result = context.storage.get_type(args[0])
    return "none" if result is None else result