from app.protocol import NullBulk
from ..core.base import Command, command, CommandError, CommandFlag

@command("SET", -2, flags=[CommandFlag.WRITE])
def set_command(args, context):
    key, value = args[0], args[1]
    
    opts = parse_set(args)

    context.storage.set(key, value, opts)
    return "OK"

@command("GET", 1)
def get_command(args, context):
    val = context.storage.get(args[0])
    return NullBulk() if val is None else val

@command("INCR", 1, flags=[CommandFlag.WRITE])
def incr_command(args, context):
    try:
        result = int(context.storage.incr(args[0]).decode())
        return result
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

def parse_set(args):
    opts = {
        "nx": False,
        "xx": False,
        "get": False,
        "keepttl": False,
        "ex": None,
        "px": None
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
            opts["ex"] = int(args[i+1])
            i += 2
        elif token == b'PX':
            opts["px"] = int(args[i+1])
            i += 2
        else:
            raise CommandError("ERR unknown option")

    return opts
