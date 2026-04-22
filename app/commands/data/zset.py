from app.protocol import NullBulk
from app.storage.errors import WrongTypeError

from ..core.base import CommandError, command, CommandFlag, CommandResult

@command("ZADD", -3, flags=[CommandFlag.WRITE])
def zadd_command(args, context):
    if len(args[1:]) % 2 != 0:
        raise CommandError("ERR syntax error")

    key = args[0]
    
    try:
        pairs = []
        for i in range(1, len(args), 2):
            score = float(args[i])
            member = args[i + 1]
            pairs.append((score, member))
    except ValueError:
        raise CommandError("ERR value is not a valid float")

    try:
        return context.storage.zadd(key, pairs)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")

@command("ZRANK", 2, flags=[])
def zrank_command(args, context):
    key = args[0]
    member = args[1]
    
    try:
        result = context.storage.zrank(key, member)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    if result is None:
        return CommandResult.resp(NullBulk(), propagate=False)
    return result

@command("ZRANGE", 3, flags=[])
def zrange_command(args, context):
    key = args[0]
    start = int(args[1])
    stop = int(args[2])
    
    try:
        result = context.storage.zrange(key, start, stop)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    return result

@command("ZCARD", 1, flags=[])
def zcard_command(args, context):
    key = args[0]
    
    try:
        result = context.storage.zcard(key)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    return result

@command("ZSCORE", 2, flags=[])
def zscore_command(args, context):
    key = args[0]
    member = args[1]
    
    try:
        result = context.storage.zscore(key, member)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    if result is None:
        return CommandResult.resp(NullBulk(), propagate=False)    
    return result

@command("ZREM", 2, flags=[CommandFlag.WRITE])
def zrem_command(args, context):
    key = args[0]
    members = args[1:]
    
    try:
        result = context.storage.zrem(key, members)
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    return result