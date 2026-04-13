from enum import Enum

from app.protocol import RESPError

class CommandError(RESPError):
    pass

class CommandFlag(str, Enum):
    WRITE = "write"
    REPL = "replication"

class Command:

    def __init__(self, name, arity, handler, flags=None):
        self.name = name.upper()
        self.arity = arity
        self.handler = handler
        self.flags = set(flags or [])

    def check_arity(self, argc: int):
        if self.arity > 0:
            if argc != self.arity:
                raise CommandError(
                    f"ERR wrong number of arguments for '{self.name.lower()}' command"
                )
        else:
            min_args = abs(self.arity) - 1
            if argc < min_args:
                raise CommandError(
                    f"ERR wrong number of arguments for '{self.name.lower()}' command"
                )

    def execute(self, args, context):
        self.check_arity(len(args))
        return self.handler(args, context)

COMMANDS = {}

def command(name, arity, flags=None):
    def decorator(func):
        
        cmd = Command(name, arity, func, flags)
        COMMANDS[cmd.name] = cmd
        return func
    
    return decorator