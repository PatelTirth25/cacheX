# CacheX Commands

These commands use command-line options, so they do not require exporting a
large set of environment variables. Run each foreground server in its own
PowerShell window.

## Build and test

```powershell
cargo build -p cachex-server -p cachex-cli
cargo test
```

## Single-node smoke test

Terminal 1:

```powershell
cargo run -p cachex-server -- --node-id node-1 --addr 127.0.0.1:7000 --aof-path single-node.aof
```

Terminal 2:

```powershell
cargo run -p cachex-cli -- --addr 127.0.0.1:7000
```

Then enter these commands in the CacheX prompt:

```text
PING
SET name Alice
GET name
INFO
DELETE name
GET name
exit
```

Expected values include `PONG`, `VALUE Alice`, `OK`, and `(nil)` after the
delete.

## Two-node Phase 4 cluster

Use the same topology and partitioner for both servers. The replication
factor is `2` for primary plus one replica.

Terminal 1, node A:

```powershell
cargo run -p cachex-server -- `
  --node-id node-a `
  --addr 127.0.0.1:7001 `
  --aof-path node-a.aof `
  --nodes "node-a=127.0.0.1:7001,node-b=127.0.0.1:7002" `
  --partitioner consistent `
  --replication-factor 2
```

Terminal 2, node B:

```powershell
cargo run -p cachex-server -- `
  --node-id node-b `
  --addr 127.0.0.1:7002 `
  --aof-path node-b.aof `
  --nodes "node-a=127.0.0.1:7001,node-b=127.0.0.1:7002" `
  --partitioner consistent `
  --replication-factor 2
```

Terminal 3, client:

```powershell
cargo run -p cachex-cli -- `
  --nodes "node-a=127.0.0.1:7001,node-b=127.0.0.1:7002" `
  --partitioner consistent `
  --replication-factor 2
```

Run:

```text
PING
SET replicated hello
GET replicated
SET counter one
GET counter
```

## Failure detection and rerouting

While both servers are running, set a value:

```text
SET failover before-failure
GET failover
```

Stop either server with `Ctrl+C`. Keep the client configured with both nodes,
then run:

```text
GET failover
SET after-failure works
GET after-failure
```

The existing value should remain available and the new write should succeed
through the surviving replica. The surviving server should log `SUSPECT` and
then `DEAD` for the stopped node. Restart the stopped server with its original
command and wait for a `RECOVERED` heartbeat message.

## Replication factor 1

Run both servers with:

```text
--replication-factor 1
```

This stores data only on the primary. Heartbeats and failure detection still
run, but a key that existed only on a failed primary is not expected to be
available on another node.

## Optional heartbeat tuning

Heartbeat probes run every second by default. To reduce console activity:

```powershell
cargo run -p cachex-server -- ... --heartbeat-interval-ms 5000
```

## Help

```powershell
cargo run -p cachex-server -- --help
cargo run -p cachex-cli -- --help
```
