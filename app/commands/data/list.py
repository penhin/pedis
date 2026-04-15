import time

from app.protocol import NullBulk
from app.storage.errors import WrongTypeError

from ..core.base import CommandError, CommandResult, command, CommandFlag

@command("RPUSH", -2, flags=[CommandFlag.WRITE])
def rpush_command(args, context):
    return push_command(args, context, True)

@command("LPUSH", -2, flags=[CommandFlag.WRITE])
def lpush_command(args, context):
    return push_command(args, context, False)

def push_command(args, context, rpush):
    key = args[0]
    values = args[1:]

    try:
        length = context.storage.push(rpush, key, values)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    
    for _ in values:
        context.blocked_manager.notify_key(key)

    return length

@command("LRANGE", 3)
def lrange_command(args, context):
    try:
        return context.storage.lrange(args[0], int(args[1]), int(args[2]))
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")

@command("LLEN", 1)
def llen_command(args, context):
    try:
        return context.storage.llen(args[0])
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")

@command("LPOP", -1, flags=[CommandFlag.WRITE])
def lpop_command(args, context):
    count = int(args[1]) if len(args) > 1 else 1
    try:
        result = context.storage.lpop(args[0], count)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    if result is None:
        return CommandResult.resp(NullBulk(), propagate=False)
    return result

@command("BLPOP", -2, flags=[CommandFlag.WRITE])
def blpop_command(args, context):
    keys = args[0:-1]
    timeout = float(args[-1])
    
    for key in keys:
        value = context.storage.try_lpop(key)
        if value is not None:
            return [key, value]
        
    deadline = time.time() + timeout if timeout > 0 else None
    context.blocked_manager.block_client(context.client, keys, deadline)
    
    return CommandResult.blocked_result()
