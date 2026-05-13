# pedis tests

Run the normal test suite:

```sh
uv run python -m unittest discover -s tests
```

Run stress tests explicitly:

```sh
PEDIS_RUN_STRESS=1 uv run python -m unittest tests.test_stress
```

Run the sequential benchmark explicitly:

```sh
PEDIS_RUN_BENCHMARK=1 uv run python -m unittest tests.test_benchmark
```

Optional knobs:

```sh
PEDIS_STRESS_CLIENTS=16 PEDIS_STRESS_COMMANDS=500 PEDIS_RUN_STRESS=1 uv run python -m unittest tests.test_stress
PEDIS_BENCHMARK_ITERATIONS=5000 PEDIS_RUN_BENCHMARK=1 uv run python -m unittest tests.test_benchmark
```
