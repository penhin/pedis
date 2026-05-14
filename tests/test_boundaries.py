import unittest

from tests.helpers import PedisServer, RedisError, encode_command


class BoundaryTests(unittest.TestCase):
    def test_empty_resp_array_returns_protocol_error(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            client.sock.sendall(b"*0\r\n")
            response = client.read_response()

            self.assertIsInstance(response, RedisError)
            self.assertIn("Protocol error", str(response))

    def test_pipeline_continues_after_blocking_command_times_out(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            client.write_commands([
                ("BLPOP", "missing-list", "0.1"),
                ("PING",),
            ])

            self.assertIsNone(client.read_response())
            self.assertEqual(client.read_response(), "PONG")

    def test_large_pipeline_drains_across_command_limit(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            commands = [("PING",) for _ in range(300)]
            client.write_commands(commands)
            responses = [client.read_response() for _ in commands]

            self.assertEqual(responses, ["PONG"] * len(commands))

    def test_protocol_error_closes_connection(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            client.sock.sendall(b"*0\r\n" + encode_command(("PING",)))

            response = client.read_response()
            self.assertIsInstance(response, RedisError)
            self.assertIn("Protocol error", str(response))
            with self.assertRaises(ConnectionError):
                client.read_response()

    def test_invalid_resp_prefix_returns_protocol_error_and_closes_connection(self):
        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            client.sock.sendall(b"?\r\n")
            response = client.read_response()

            self.assertIsInstance(response, RedisError)
            self.assertIn("Protocol error", str(response))
            with self.assertRaises(ConnectionError):
                client.read_response()


if __name__ == "__main__":
    unittest.main()
