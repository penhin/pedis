from app.storage.errors import WrongTypeError

from ..core.base import CommandError, command, CommandFlag

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
