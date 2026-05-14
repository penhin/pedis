import unittest

from tests.helpers import PedisServer, RedisError


class ParameterErrorTests(unittest.TestCase):
    def assert_error_contains(self, client, command, expected):
        response = client.command(*command)
        self.assertIsInstance(response, RedisError)
        self.assertIn(expected, str(response))

    def test_integer_and_float_parse_errors_return_command_errors(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assert_error_contains(client, ("SET", "k", "v", "EX", "nope"), "invalid expire time")
            self.assert_error_contains(client, ("SET", "k", "v", "PX"), "syntax error")
            self.assert_error_contains(client, ("LRANGE", "list", "a", "1"), "not an integer")
            self.assert_error_contains(client, ("LPOP", "list", "nope"), "not an integer")
            self.assert_error_contains(client, ("BLPOP", "list", "nope"), "timeout is not a float")
            self.assert_error_contains(client, ("BLPOP", "list", "-1"), "timeout is negative")
            self.assert_error_contains(client, ("ZRANGE", "zset", "a", "1"), "not an integer")
            self.assert_error_contains(client, ("WAIT", "one", "1000"), "not an integer")

            self.assertEqual(client.command("PING"), "PONG")


if __name__ == "__main__":
    unittest.main()
