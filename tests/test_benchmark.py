import os
import time
import unittest

from tests.helpers import PedisServer


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
        print(f"\nSequential SET/GET benchmark: {ops} ops in {elapsed:.3f}s, {ops_per_second:.0f} ops/s")
        self.assertGreater(ops_per_second, 0)


if __name__ == "__main__":
    unittest.main()

