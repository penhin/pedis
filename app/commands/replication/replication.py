from app.server.types import Blocked

from ..core.base import command, CommandError, CommandFlag

@command("REPLCONF", -2, flags=[CommandFlag.REPL])
def replconf_command(args, context):
    try:
        return context.server.replication.replconf(context.client, args)
    except ValueError as e:
        raise CommandError(f"ERR {e}")

@command("PSYNC", -3, flags=[CommandFlag.REPL])
def psync_command(args, context):
    try:
        response = context.server.replication.psync(context.client) 
        return response
    except Exception as e:
        raise CommandError(f"ERR {e}")
    
@command("WAIT", -3, flags=[CommandFlag.REPL])
def wait_command(args, context):
    try:
        numreplicas = int(args[0])
        timeout_ms = int(args[1])
        timeout = timeout_ms / 1000
        return context.server.replication.wait_for_replicas(context.client, numreplicas, timeout)
    except Exception as e:
        raise CommandError(f"ERR {e}")