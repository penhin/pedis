from .base import COMMANDS, CommandError, CommandFlag

class CommandDispatcher:
    def dispatch(self, cmd_list, raw_command, context):
        client = context.client
        name = cmd_list[0].decode().upper()
        args = cmd_list[1:]
        
        result = self.handle_transaction_command(name, args, context)
        if result is not None:
            return result

        if client.in_multi:
            client.multi_queue.append((cmd_list, raw_command))
            return 'QUEUED'

        if name not in COMMANDS:
            raise CommandError("ERR unknown command")
        
        command = COMMANDS[name]
        response = command.execute(args, context)
        
        if CommandFlag.WRITE in command.flags:
            print(f"{raw_command} command should be propagated")
            context.server.replication.propagate(raw_command)
        
        return response
    
    def handle_transaction_command(self, name, args, context):
        client = context.client

        if name == "MULTI":
            if client.in_multi:
                raise CommandError("ERR MUITL calls can not be nested")
            client.in_multi = True
            client.multi_queue = []
            return 'OK'
        elif name == "EXEC":
            if not client.in_multi:
                raise CommandError("ERR EXEC without MULTI")
            return self.exec_transaction(context)
        elif name == "DISCARD":
            if not client.in_multi:
                raise CommandError("ERR DISCARD without MULTI")
            client.in_multi = False
            client.multi_queue = []
            return 'OK'
        
    def exec_transaction(self, context):
        client = context.client
        queue = client.multi_queue
        
        client.in_multi = False
        client.multi_queue = []
        
        results = []
        
        for item in queue:
            try:
                cmd_list, raw_command = item if isinstance(item, tuple) else (item, b"")
                result = self.dispatch(cmd_list, raw_command, context)
                results.append(result)
            except CommandError as e:
                results.append(e)
        
        return results
        
