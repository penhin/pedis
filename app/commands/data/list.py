import time

from app.protocol import NullBulk
from app.server.types import Blocked

from ..core.base import command, CommandFlag

@command("RPUSH", -2, flags=[CommandFlag.WRITE])
def rpush_command(args, context):
    return push_command(args, context, True)

@command("LPUSH", -2, flags=[CommandFlag.WRITE])
def lpush_command(args, context):
    return push_command(args, context, False)

def push_command(args, context, rpush):
    key = args[0]
    values = args[1:]
    
    length = context.storage.push(rpush, key, values)   
    
    for _ in values:
        context.blocked_manager.notify_key(key)

    return length

@command("LRANGE", 3)
def lrange_command(args, context):
    return context.storage.lrange(args[0], int(args[1]), int(args[2]))

@command("LLEN", 1)
def llen_command(args, context):
    return context.storage.llen(args[0])

@command("LPOP", -1, flags=[CommandFlag.WRITE])
def lpop_command(args, context):
    count = int(args[1]) if len(args) > 1 else 1
    result = context.storage.lpop(args[0], count)
    return NullBulk() if result is None else result

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
    
    return Blocked()