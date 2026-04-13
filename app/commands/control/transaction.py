from ..core.base import Command, command, CommandFlag

@command("MULTI", 1)
def multi_command(args, context):
    context.client.in_multi = True