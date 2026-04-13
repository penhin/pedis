import time

from app.server.types import Blocked
from app.server.client import BlockedType

from ..core.base import command, CommandError, CommandFlag

@command("XADD", -4, flags=[CommandFlag.WRITE])
def xadd_command(args, context):
    if len(args) % 2 != 0:
        raise CommandError("ERR wrong number of arguments for 'xadd' command")

    key = args[0]
    id = args[1]
    
    fields = {}
    for i in range(2, len(args), 2):
        fields[args[i]] = args[i + 1]
    
    ret_id = context.storage.xadd(key, fields, id)

    context.blocked_manager.notify_key(key)

    return ret_id

@command("XRANGE", -3)
def xrange_command(args, context):
    key = args[0]
    start_id = args[1]
    end_id = args[2]
    return context.storage.xrange(key, start_id, end_id)

@command("XREAD", -3)
def xread_command(args, context):
    if not args:
        raise CommandError("ERR wrong number of arguments for 'xread' command")

    i = 0
    block_timeout = None

    while i < len(args):
        token = args[i].upper()

        if token == b'BLOCK':
            if i + 1 >= len(args):
                raise CommandError("ERR syntax error")
            try:
                block_timeout = int(args[i + 1])
            except ValueError:
                raise CommandError("ERR invalid BLOCK timeout")
            i += 2

        elif token == b'STREAMS':
            i += 1
            break

        else:
            raise CommandError("ERR syntax error")

    remaining = args[i:]

    if len(remaining) == 0 or len(remaining) % 2 != 0:
        raise CommandError("ERR wrong number of arguments for 'xread' command")

    half = len(remaining) // 2
    keys = remaining[:half]
    ids = remaining[half:]

    for i in range(len(ids)):
        if ids[i] == b'$':
            last_id = context.storage.get_last_id(keys[i])
            if last_id is None:
                ids[i] = b'0-0'
            else:
                ids[i] = last_id

    result = context.storage.xread(keys, ids)

    empty = (
        not result or
        all(not entries for _, entries in result)
    )

    if empty and block_timeout is not None:

        context.blocked_manager.block_client(
            context.client, keys, 
            time.time() + (block_timeout / 1000) if block_timeout > 0 else None,
            ids, BlockedType.STREAM
        )

        return Blocked()

    return result