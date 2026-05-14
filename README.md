# pedis

`pedis` is a small Redis-compatible server written in Python for the CodeCrafters
["Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis).

It implements a RESP server, an in-memory data store, blocking operations,
transactions, pub/sub, basic replication, ACL/authentication, and optional AOF/RDB
persistence support.

## Requirements

- Python managed by `uv`
- `redis-cli` for manual testing

Install `redis-cli` on Ubuntu/WSL:

```sh
sudo apt update
sudo apt install -y redis-tools
```

## Running

Start the server:

```sh
./your_program.sh
```

The default port is `6379`. If Redis, Memurai, or another Redis-compatible server
is already using that port, run pedis on another port:

```sh
./your_program.sh --port 6380
```

Connect with `redis-cli`:

```sh
redis-cli -p 6380
```

Try a few commands:

```redis
PING
SET name yosh1
GET name
RPUSH tasks a b c
LRANGE tasks 0 -1
MULTI
SET counter 1
GET counter
EXEC
```

## Supported Options

```sh
./your_program.sh --port 6380
./your_program.sh --dir /tmp --dbfilename dump.rdb
./your_program.sh --appendonly yes
./your_program.sh --appendfsync everysec
./your_program.sh --requirepass secret
./your_program.sh --replicaof "localhost 6379" --port 6381
```

Supported AOF fsync modes:

```text
always
everysec
no
```

## Supported Commands

Connection and server:

```text
PING ECHO INFO CONFIG
AUTH ACL
```

Strings:

```text
SET GET INCR
```

Lists:

```text
RPUSH LPUSH LRANGE LLEN LPOP BLPOP
```

Generic keys:

```text
KEYS TYPE
```

Sorted sets and geo:

```text
ZADD ZRANGE ZRANK ZCARD ZSCORE ZREM
GEOADD GEOPOS GEODIST GEOSEARCH
```

Streams:

```text
XADD XRANGE XREAD
```

Transactions:

```text
MULTI EXEC DISCARD WATCH UNWATCH
```

Pub/sub and replication:

```text
PUBLISH SUBSCRIBE UNSUBSCRIBE
REPLCONF PSYNC WAIT
```

## Logging

pedis uses Python `logging`. The default log level is `WARNING`.

Enable debug logs:

```sh
PEDIS_LOG_LEVEL=DEBUG ./your_program.sh --port 6380
```

## Tests

Run the normal test suite:

```sh
uv run python -m unittest discover -s tests
```

Run stress tests:

```sh
PEDIS_RUN_STRESS=1 uv run python -m unittest tests.test_stress
```

Run benchmarks:

```sh
PEDIS_RUN_BENCHMARK=1 uv run python -m unittest tests.test_benchmark
```

Tune stress and benchmark sizes:

```sh
PEDIS_STRESS_CLIENTS=16 PEDIS_STRESS_COMMANDS=500 PEDIS_RUN_STRESS=1 uv run python -m unittest tests.test_stress
PEDIS_BENCHMARK_ITERATIONS=5000 PEDIS_RUN_BENCHMARK=1 uv run python -m unittest tests.test_benchmark
```

The benchmark suite includes both sequential request/response tests and pipelined
tests. Sequential throughput is mostly limited by socket round trips; pipelined
throughput is closer to the server's command processing capacity.

## Project Layout

```text
app/main.py                    entry point
app/protocol.py                RESP parser and encoder
app/server/                    event loop, clients, blocking, pub/sub, replication
app/commands/                  command registration and command handlers
app/storage/                   in-memory data structures
app/persistence/               RDB loading and AOF support
tests/                         regression, boundary, stress, and benchmark tests
```

## Notes

This is a learning Redis implementation, not a production database. The goal is
to explore protocol parsing, event loops, Redis command semantics, replication,
and persistence while keeping the code small enough to study.
