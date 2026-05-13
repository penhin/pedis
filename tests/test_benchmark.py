import logging
import os
import time
import unittest

from tests.helpers import PedisServer


logger = logging.getLogger(__name__)


@unittest.skipUnless(os.getenv("PEDIS_RUN_BENCHMARK") == "1", "set PEDIS_RUN_BENCHMARK=1 to run benchmark tests")
class BenchmarkTests(unittest.TestCase):
    def test_sequential_set_get_throughput(self):
        iterations = int(os.getenv("PEDIS_BENCHMARK_ITERATIONS", "1000"))

        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            start = time.perf_counter()
            for i in range(iterations):
                key = f"bench:{i}"
                value = f"value-{i}"
                self.assertEqual(client.command("SET", key, value), "OK")
                self.assertEqual(client.command("GET", key), value.encode())
            elapsed = time.perf_counter() - start

        ops = iterations * 2
        ops_per_second = ops / elapsed
        logger.warning(
            "Sequential SET/GET benchmark: %s ops in %.3fs, %.0f ops/s",
            ops,
            elapsed,
            ops_per_second,
        )
        self.assertGreater(ops_per_second, 0)

    def test_pipeline_set_get_throughput(self):
        iterations = int(os.getenv("PEDIS_BENCHMARK_ITERATIONS", "1000"))

        commands = []
        expected = []
        for i in range(iterations):
            key = f"pipe:{i}"
            value = f"value-{i}"
            commands.append(("SET", key, value))
            commands.append(("GET", key))
            expected.append("OK")
            expected.append(value.encode())

        with PedisServer() as server:
            client = server.client()
            self.addCleanup(client.close)

            start = time.perf_counter()
            client.write_commands(commands)
            responses = [client.read_response() for _ in expected]
            elapsed = time.perf_counter() - start

        self.assertEqual(responses, expected)

        ops = len(commands)
        ops_per_second = ops / elapsed
        logger.warning(
            "Pipeline SET/GET benchmark: %s ops in %.3fs, %.0f ops/s",
            ops,
            elapsed,
            ops_per_second,
        )
        self.assertGreater(ops_per_second, 0)


if __name__ == "__main__":
    unittest.main()
