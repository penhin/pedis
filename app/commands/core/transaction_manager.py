from app.protocol import NullArray

from .base import COMMANDS, CommandError, CommandResult


class TransactionManager:
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

    def handle_command(self, name, args, context):
        client = context.client

        if name == "MULTI":
            if args:
                raise CommandError("ERR wrong number of arguments for 'multi' command")
            if client.transaction.active:
                raise CommandError("ERR MUITL calls can not be nested")
            client.transaction.active = True
            client.transaction.queue = []
            return CommandResult.resp("OK")

        if name == "EXEC":
            if args:
                raise CommandError("ERR wrong number of arguments for 'exec' command")
            if not client.transaction.active:
                raise CommandError("ERR EXEC without MULTI")
            if client.transaction.dirty:
                client.transaction.reset()
                raise CommandError("EXECABORT Transaction discarded because of previous errors.")
            return self.exec_transaction(context)

        if name == "DISCARD":
            if args:
                raise CommandError("ERR wrong number of arguments for 'discard' command")
            if not client.transaction.active:
                raise CommandError("ERR DISCARD without MULTI")
            client.transaction.reset()
            return CommandResult.resp("OK")

        if name == "WATCH":
            if client.transaction.active:
                raise CommandError("ERR WATCH inside MULTI is not allowed")
            if not args:
                raise CommandError("ERR wrong number of arguments for 'watch' command")
            for key in args:
                client.transaction.watch(key, context.storage.get_version(key))
            return CommandResult.resp("OK")

        if name == "UNWATCH" and not client.transaction.active:
            if args:
                raise CommandError("ERR wrong number of arguments for 'unwatch' command")
            client.transaction.unwatch()
            return CommandResult.resp("OK")

        return None

    def enqueue_if_active(self, name, args, cmd_list, raw_command, context):
        client = context.client
        if not client.transaction.active:
            return None

        if name not in COMMANDS:
            client.transaction.dirty = True
            raise CommandError("ERR unknown command")

        try:
            COMMANDS[name].check_arity(len(args))
        except CommandError:
            client.transaction.dirty = True
            raise

        client.transaction.queue.append((cmd_list, raw_command))
        return CommandResult.resp("QUEUED")

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
                result = self.dispatcher.dispatch(cmd_list, raw_command, context)
                if result.blocked:
                    raise CommandError("ERR blocking commands are not allowed inside MULTI")
                if len(result.frames) != 1 or result.frames[0].kind != "resp":
                    raise CommandError("ERR unsupported response inside EXEC")
                results.append(result.frames[0].value)
            except CommandError as e:
                results.append(e)

        return CommandResult.resp(results)
