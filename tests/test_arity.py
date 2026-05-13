import unittest

from tests.helpers import PedisServer, RedisError


class ArityTests(unittest.TestCase):
    def assert_wrong_arity(self, client, *command):
        result = client.command(*command)
        self.assertIsInstance(result, RedisError)
        self.assertIn("wrong number of arguments", str(result))

    def test_common_wrong_arity_cases(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assert_wrong_arity(client, "SET", "key")
            self.assert_wrong_arity(client, "RPUSH", "list")
            self.assert_wrong_arity(client, "LPUSH", "list")
            self.assert_wrong_arity(client, "ZADD", "zset", "1")
            self.assert_wrong_arity(client, "ZREM", "zset")
            self.assert_wrong_arity(client, "GEOADD", "geo", "1", "2")
            self.assert_wrong_arity(client, "GEOPOS", "geo")
            self.assert_wrong_arity(client, "XADD", "stream", "*", "field")
            self.assert_wrong_arity(client, "XRANGE", "stream", "-")
            self.assert_wrong_arity(client, "WAIT", "1")
            self.assert_wrong_arity(client, "PSYNC", "?")
            self.assert_wrong_arity(client, "PING", "a", "b")


if __name__ == "__main__":
    unittest.main()

