import unittest

from tests.helpers import PedisServer


class BasicCommandTests(unittest.TestCase):
    def test_ping_echo_set_get_and_incr(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assertEqual(client.command("PING"), "PONG")
            self.assertEqual(client.command("ECHO", "hello"), b"hello")
            self.assertEqual(client.command("SET", "name", "yosh1"), "OK")
            self.assertEqual(client.command("GET", "name"), b"yosh1")
            self.assertEqual(client.command("INCR", "counter"), 1)
            self.assertEqual(client.command("INCR", "counter"), 2)

    def test_list_commands(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assertEqual(client.command("RPUSH", "tasks", "a", "b"), 2)
            self.assertEqual(client.command("LPUSH", "tasks", "c"), 3)
            self.assertEqual(client.command("LRANGE", "tasks", "0", "-1"), [b"c", b"a", b"b"])
            self.assertEqual(client.command("LLEN", "tasks"), 3)
            self.assertEqual(client.command("LPOP", "tasks"), b"c")

    def test_sorted_set_commands(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assertEqual(client.command("ZADD", "scores", "10", "alice", "5", "bob"), 2)
            self.assertEqual(client.command("ZRANGE", "scores", "0", "-1"), [b"bob", b"alice"])
            self.assertEqual(client.command("ZRANK", "scores", "alice"), 1)
            self.assertEqual(client.command("ZCARD", "scores"), 2)
            self.assertEqual(client.command("ZREM", "scores", "bob"), 1)


if __name__ == "__main__":
    unittest.main()

