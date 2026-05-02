from app.protocol import NullArray

from .base import COMMANDS, CommandError, CommandFlag, CommandResult

class CommandDispatcher:
    def dispatch(self, cmd_list, raw_command, context):
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
        
        result = self.handle_transaction_command(name, args, context)
        if result is not None:
            return result

        if client.transaction.active:
            client.transaction.queue.append((cmd_list, raw_command))
            return CommandResult.resp("QUEUED")
        
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
        
        if CommandFlag.WRITE in command.flags and response.propagate:
            print(f"{raw_command} command should be propagated")
            context.server.replication.propagate(raw_command)
        
        return response
    
    def handle_transaction_command(self, name, args, context):
        client = context.client

        if name == "MULTI":
            if args:
                raise CommandError("ERR wrong number of arguments for 'multi' command")
            if client.transaction.active:
                raise CommandError("ERR MUITL calls can not be nested")
            client.transaction.active = True
            client.transaction.queue = []
            return CommandResult.resp("OK")
        elif name == "EXEC":
            if args:
                raise CommandError("ERR wrong number of arguments for 'exec' command")
            if not client.transaction.active:
                raise CommandError("ERR EXEC without MULTI")
            return self.exec_transaction(context)
        elif name == "DISCARD":
            if args:
                raise CommandError("ERR wrong number of arguments for 'discard' command")
            if not client.transaction.active:
                raise CommandError("ERR DISCARD without MULTI")
            client.transaction.reset()
            return CommandResult.resp("OK")
        elif name == "WATCH":
            if client.transaction.active:
                raise CommandError("ERR WATCH inside MULTI is not allowed")
            if not args:
                raise CommandError("ERR wrong number of arguments for 'watch' command")
            for key in args:
                client.transaction.watch(key, context.storage.get_version(key))
            return CommandResult.resp("OK")
        elif name == "UNWATCH" and not client.transaction.active:
            if args:
                raise CommandError("ERR wrong number of arguments for 'unwatch' command")
            client.transaction.unwatch()
            return CommandResult.resp("OK")
        
    def exec_transaction(self, context):
        client = context.client
        queue = list(client.transaction.queue)

        for key, version in client.transaction.watched_keys.items():
            if context.storage.get_version(key) != version:
                client.transaction.reset()
                return CommandResult.resp(NullArray(), propagate=False)

        client.transaction.reset()
        
        results = []
        
        for item in queue:
            try:
                cmd_list, raw_command = item if isinstance(item, tuple) else (item, b"")
                result = self.dispatch(cmd_list, raw_command, context)
                if result.blocked:
                    raise CommandError("ERR blocking commands are not allowed inside MULTI")
                if len(result.frames) != 1 or result.frames[0].kind != "resp":
                    raise CommandError("ERR unsupported response inside EXEC")
                results.append(result.frames[0].value)
            except CommandError as e:
                results.append(e)
        
        return CommandResult.resp(results)
        
