from dataclasses import dataclass, field
import hashlib
import secrets


@dataclass
class ACLUser:
    name: bytes
    enabled: bool = True
    nopass: bool = True
    passwords: set[bytes] = field(default_factory=set)
    commands: set[bytes] = field(default_factory=lambda: {b"+@all"})
    keys: set[bytes] = field(default_factory=lambda: {b"~*"})
    channels: set[bytes] = field(default_factory=lambda: {b"&*"})

    def can_auth(self, password: bytes) -> bool:
        if not self.enabled:
            return False
        if self.nopass:
            return True
        return password in self.passwords

    def password_hashes(self) -> list[bytes]:
        return [hashlib.sha256(password).hexdigest().encode() for password in sorted(self.passwords)]


class ACLManager:
    def __init__(self):
        self.users: dict[bytes, ACLUser] = {b"default": ACLUser(b"default")}

    def default_user(self) -> ACLUser:
        return self.users[b"default"]

    def get_user(self, name: bytes) -> ACLUser | None:
        return self.users.get(name)

    def get_or_create_user(self, name: bytes) -> ACLUser:
        user = self.users.get(name)
        if user is None:
            user = ACLUser(name=name, enabled=False, nopass=False)
            user.commands.clear()
            user.keys.clear()
            user.channels.clear()
            self.users[name] = user
        return user

    def requires_authentication(self) -> bool:
        default = self.default_user()
        return not (default.enabled and default.nopass)

    def authenticate(self, username: bytes, password: bytes) -> ACLUser | None:
        user = self.get_user(username)
        if user is None or not user.can_auth(password):
            return None
        return user

    def set_user(self, name: bytes, rules: list[bytes]) -> ACLUser:
        user = self.get_or_create_user(name)

        for rule in rules:
            upper = rule.upper()

            if upper == b"ON":
                user.enabled = True
            elif upper == b"OFF":
                user.enabled = False
            elif upper == b"NOPASS":
                user.nopass = True
                user.passwords.clear()
            elif upper == b"RESETPASS":
                user.nopass = False
                user.passwords.clear()
            elif upper == b"RESET":
                user.enabled = False
                user.nopass = False
                user.passwords.clear()
                user.commands.clear()
                user.keys.clear()
                user.channels.clear()
            elif upper == b"ALLCOMMANDS":
                self._replace_rule(user.commands, b"+@all")
            elif upper == b"NOCOMMANDS":
                user.commands.clear()
                user.commands.add(b"-@all")
            elif upper == b"ALLKEYS":
                self._replace_rule(user.keys, b"~*")
            elif upper == b"RESETKEYS":
                user.keys.clear()
            elif upper == b"ALLCHANNELS":
                self._replace_rule(user.channels, b"&*")
            elif upper == b"RESETCHANNELS":
                user.channels.clear()
            elif rule.startswith(b">"):
                user.nopass = False
                user.passwords.add(rule[1:])
            elif rule.startswith(b"<"):
                user.passwords.discard(rule[1:])
            elif rule.startswith((b"+", b"-")):
                self._replace_rule(user.commands, rule)
            elif rule.startswith(b"~"):
                self._replace_rule(user.keys, rule)
            elif rule.startswith(b"&"):
                self._replace_rule(user.channels, rule)
            else:
                raise ValueError(f"ERR Error in ACL SETUSER modifier '{rule.decode(errors='replace')}': Syntax error")

        return user

    def describe_user(self, user: ACLUser) -> list:
        flags = [b"on" if user.enabled else b"off"]
        if user.nopass:
            flags.append(b"nopass")

        commands = sorted(user.commands) if user.commands else [b"-@all"]
        keys = sorted(user.keys) if user.keys else []
        channels = sorted(user.channels) if user.channels else []

        return [
            b"flags",
            flags,
            b"passwords",
            user.password_hashes(),
            b"commands",
            b" ".join(commands),
            b"keys",
            keys,
            b"channels",
            channels,
            b"selectors",
            [],
        ]

    def can_execute(self, username: bytes, command_name: bytes) -> bool:
        user = self.get_user(username)
        if user is None or not user.enabled:
            return False

        command = command_name.lower()
        allow_all = b"+@all" in {rule.lower() for rule in user.commands}
        deny_all = b"-@all" in {rule.lower() for rule in user.commands}

        if b"-" + command in {rule.lower() for rule in user.commands}:
            return False
        if b"+" + command in {rule.lower() for rule in user.commands}:
            return True
        if allow_all:
            return True
        if deny_all:
            return False

        return False

    def random_password(self) -> bytes:
        return secrets.token_urlsafe(32).encode()

    def _replace_rule(self, rules: set[bytes], rule: bytes):
        prefix = rule[:1]
        target = rule[1:].lower()
        for existing in list(rules):
            if existing[:1] in (b"+", b"-") and existing[1:].lower() == target:
                rules.remove(existing)
            elif existing[:1] == prefix and existing[1:].lower() == target:
                rules.remove(existing)
        rules.add(rule)
