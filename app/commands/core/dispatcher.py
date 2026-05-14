import logging

from app.protocol import ProtocolError

from .base import COMMANDS, CommandError, CommandFlag
from .transaction_manager import TransactionManager


logger = logging.getLogger(__name__)

class CommandDispatcher:
    def __init__(self):
        self.transactions = TransactionManager(self)

    def dispatch(self, cmd_list, raw_command, context):
        if not isinstance(cmd_list, list) or not cmd_list or not isinstance(cmd_list[0], bytes):
            raise ProtocolError("ERR Protocol error: expected non-empty array of bulk strings")

        client = context.client
        name = cmd_list[0].decode().upper()
        args = cmd_list[1:]

        if name in COMMANDS:
            command = COMMANDS[name]
            if (
                context.server.acl.requires_authentication()
                and not client.auth.authenticated
                and CommandFlag.NO_AUTH not in command.flags
            ):
                raise CommandError("NOAUTH Authentication required.")
            if (
                client.auth.authenticated
                and CommandFlag.NO_AUTH not in command.flags
                and not context.server.acl.can_execute(client.auth.user, name.encode())
            ):
                raise CommandError("NOPERM this user has no permissions to run the command")
        
        result = self.transactions.handle_command(name, args, context)
        if result is not None:
            return result

        result = self.transactions.enqueue_if_active(name, args, cmd_list, raw_command, context)
        if result is not None:
            return result
        
        if client.pubsub.active:
            if name not in COMMANDS:
                raise CommandError("ERR unknown command")
            pubsub_command = COMMANDS[name]
            if CommandFlag.ALLOWED_IN_PUBSUB not in pubsub_command.flags:
                raise CommandError(
                    f"ERR Can't execute '{name}', only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                )

        if name not in COMMANDS:
            raise CommandError("ERR unknown command")
        
        command = COMMANDS[name]
        response = command.execute(args, context)
        
        aof = getattr(context.server, "aof", None)
        loading_aof = aof is not None and aof.loading
        if CommandFlag.WRITE in command.flags and response.propagate and not loading_aof:
            logger.debug("%r command should be propagated", raw_command)
            if aof is not None:
                aof.append(raw_command)
            context.server.replication.propagate(raw_command)
        
        return response
        
