from app.server.types import Blocked

from ..core.base import CommandResult, command, CommandError, CommandFlag

@command("REPLCONF", -2, flags=[CommandFlag.REPL])
def replconf_command(args, context):
    try:
        return context.server.replication.replconf(context.client, args)
    except ValueError as e:
        raise CommandError(f"ERR {e}")

@command("PSYNC", 2, flags=[CommandFlag.REPL])
def psync_command(args, context):
    try:
        header, payload = context.server.replication.psync(context.client)
        return CommandResult.psync(header, payload)
    except Exception as e:
        raise CommandError(f"ERR {e}")
    
@command("WAIT", 2, flags=[CommandFlag.REPL])
def wait_command(args, context):
    try:
        numreplicas = int(args[0])
        timeout_ms = int(args[1])
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

    timeout = timeout_ms / 1000
    try:
        result = context.server.replication.wait_for_replicas(context.client, numreplicas, timeout)
    except Exception as e:
        raise CommandError(f"ERR {e}")
    return CommandResult.blocked_result() if isinstance(result, Blocked) else result
