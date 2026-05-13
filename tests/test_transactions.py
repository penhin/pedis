import unittest

from tests.helpers import PedisServer, RedisError


class TransactionTests(unittest.TestCase):
    def test_valid_transaction_executes_queued_commands(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assertEqual(client.command("MULTI"), "OK")
            self.assertEqual(client.command("SET", "a", "1"), "QUEUED")
            self.assertEqual(client.command("GET", "a"), "QUEUED")
            self.assertEqual(client.command("EXEC"), ["OK", b"1"])

    def test_unknown_command_marks_transaction_dirty(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assertEqual(client.command("MULTI"), "OK")
            err = client.command("NO_SUCH_COMMAND")
            self.assertIsInstance(err, RedisError)
            self.assertIn("ERR unknown command", str(err))

            err = client.command("EXEC")
            self.assertIsInstance(err, RedisError)
            self.assertIn("EXECABORT", str(err))

    def test_wrong_arity_marks_transaction_dirty(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            self.assertEqual(client.command("MULTI"), "OK")
            err = client.command("SET", "only-key")
            self.assertIsInstance(err, RedisError)
            self.assertIn("wrong number of arguments", str(err))

            err = client.command("EXEC")
            self.assertIsInstance(err, RedisError)
            self.assertIn("EXECABORT", str(err))


if __name__ == "__main__":
    unittest.main()

