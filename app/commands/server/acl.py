from app.protocol import NullBulk

from ..core.base import CommandError, CommandFlag, CommandResult, command


@command("AUTH", -2, flags=[CommandFlag.NO_AUTH])
def auth_command(args, context):
    if len(args) == 1:
        username = b"default"
        password = args[0]
        default = context.server.acl.default_user()
        if default.nopass and not default.passwords:
            raise CommandError("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
    elif len(args) == 2:
        username = args[0]
        password = args[1]
    else:
        raise CommandError("ERR wrong number of arguments for 'auth' command")

    user = context.server.acl.authenticate(username, password)
    if user is None:
        raise CommandError("WRONGPASS invalid username-password pair or user is disabled.")

    context.client.auth.set_user(user.name)
    return "OK"


@command("ACL", -2)
def acl_command(args, context):
    subcommand = args[0].upper()

    if subcommand == b"WHOAMI":
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'acl|whoami' command")
        return context.client.auth.user

    if subcommand == b"GETUSER":
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'acl|getuser' command")
        user = context.server.acl.get_user(args[1])
        if user is None:
            return CommandResult.resp(NullBulk(), propagate=False)
        return CommandResult.resp(context.server.acl.describe_user(user), propagate=False)

    if subcommand == b"SETUSER":
        if len(args) < 2:
            raise CommandError("ERR wrong number of arguments for 'acl|setuser' command")
        try:
            context.server.acl.set_user(args[1], args[2:])
        except ValueError as e:
            raise CommandError(str(e))
        return "OK"

    raise CommandError("ERR unknown subcommand")
