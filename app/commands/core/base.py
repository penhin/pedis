from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from app.protocol import RESPError

class CommandError(RESPError):
    pass

class CommandFlag(str, Enum):
    WRITE = "write"
    REPL = "replication"


@dataclass(frozen=True)
class ResponseFrame:
    kind: str
    value: Any


@dataclass
class CommandResult:
    frames: list[ResponseFrame] = field(default_factory=list)
    blocked: bool = False
    propagate: bool = True

    @classmethod
    def empty(cls):
        return cls(propagate=False)

    @classmethod
    def blocked_result(cls):
        return cls(blocked=True, propagate=False)

    @classmethod
    def resp(cls, value: Any, propagate: bool = True):
        return cls(frames=[ResponseFrame("resp", value)], propagate=propagate)

    @classmethod
    def raw(cls, value: bytes):
        return cls(frames=[ResponseFrame("raw", value)], propagate=False)

    @classmethod
    def psync(cls, header: str, payload: bytes):
        return cls(
            frames=[
                ResponseFrame("resp", header),
                ResponseFrame("raw", payload),
            ],
            propagate=False,
        )

    def extend(self, other: "CommandResult"):
        self.frames.extend(other.frames)
        self.blocked = self.blocked or other.blocked
        self.propagate = self.propagate or other.propagate
        return self

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
        return normalize_command_result(self.handler(args, context))

COMMANDS = {}


def normalize_command_result(value):
    if isinstance(value, CommandResult):
        return value
    if value is None:
        return CommandResult.empty()
    return CommandResult.resp(value)

def command(name, arity, flags=None):
    def decorator(func):
        
        cmd = Command(name, arity, func, flags)
        COMMANDS[cmd.name] = cmd
        return func
    
    return decorator
