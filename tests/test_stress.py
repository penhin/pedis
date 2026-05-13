import os
import threading
import unittest

from tests.helpers import PedisServer


@unittest.skipUnless(os.getenv("PEDIS_RUN_STRESS") == "1", "set PEDIS_RUN_STRESS=1 to run stress tests")
class StressTests(unittest.TestCase):
    def test_parallel_set_get_clients(self):
        client_count = int(os.getenv("PEDIS_STRESS_CLIENTS", "8"))
        commands_per_client = int(os.getenv("PEDIS_STRESS_COMMANDS", "200"))
        errors = []

        with PedisServer() as server:
            def worker(worker_id):
                client = server.client()
                try:
                    for i in range(commands_per_client):
                        key = f"stress:{worker_id}:{i}"
                        value = f"value-{i}"
                        if client.command("SET", key, value) != "OK":
                            errors.append(f"SET failed for {key}")
                            return
                        if client.command("GET", key) != value.encode():
                            errors.append(f"GET mismatch for {key}")
                            return
                except Exception as exc:
                    errors.append(repr(exc))
                finally:
                    client.close()

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(client_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=15)

        self.assertEqual(errors, [])
        self.assertTrue(all(not thread.is_alive() for thread in threads))


if __name__ == "__main__":
    unittest.main()

