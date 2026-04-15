from app.protocol import NullBulk
from app.storage.errors import InvalidValueError, WrongTypeError

from ..core.base import CommandResult, command, CommandError, CommandFlag

@command("SET", -2, flags=[CommandFlag.WRITE])
def set_command(args, context):
    key, value = args[0], args[1]
    
    options = parse_set(args)
    key_exists = context.storage.has_key(key)

    if options["nx"] and key_exists:
        return CommandResult.resp(NullBulk(), propagate=False)
    if options["xx"] and not key_exists:
        return CommandResult.resp(NullBulk(), propagate=False)

    try:
        previous = context.storage.get(key) if options["get"] else None
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    ttl_seconds = options["ttl_seconds"]
    if options["keepttl"] and ttl_seconds is None:
        ttl_seconds = context.storage.ttl(key)

    context.storage.set(key, value, ttl_seconds=ttl_seconds)

    if options["get"]:
        return NullBulk() if previous is None else previous
    return "OK"

@command("GET", 1)
def get_command(args, context):
    try:
        val = context.storage.get(args[0])
    except WrongTypeError:
        raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    return NullBulk() if val is None else val

@command("INCR", 1, flags=[CommandFlag.WRITE])
def incr_command(args, context):
    try:
        result = int(context.storage.incr(args[0]).decode())
        return result
    except (WrongTypeError, InvalidValueError):
        raise CommandError("ERR value is not an integer or out of range")

def parse_set(args):
    opts = {
        "nx": False,
        "xx": False,
        "get": False,
        "keepttl": False,
        "ttl_seconds": None,
    }

    i = 2
    while i < len(args):
        token = args[i].upper()

        if token == b'NX':
            opts["nx"] = True
            i += 1
        elif token == b'XX':
            opts["xx"] = True
            i += 1
        elif token == b'GET':
            opts["get"] = True
            i += 1
        elif token == b'KEEPTTL':
            opts["keepttl"] = True
            i += 1
        elif token == b'EX':
            opts["ttl_seconds"] = int(args[i+1])
            i += 2
        elif token == b'PX':
            opts["ttl_seconds"] = int(args[i+1]) / 1000
            i += 2
        else:
            raise CommandError("ERR unknown option")

    if opts["nx"] and opts["xx"]:
        raise CommandError("ERR XX and NX options at the same time are not compatible")
    if opts["keepttl"] and opts["ttl_seconds"] is not None:
        raise CommandError("ERR syntax error")

    return opts
