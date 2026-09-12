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
cargo run -p cachex-server -- --node-id node-1 --addr 127.0.0.1:7000 --aof-path single-node.aof --dashboard-addr 127.0.0.1:7600
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
  --dashboard-addr 127.0.0.1:7601 `
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
  --dashboard-addr 127.0.0.1:7602 `
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

## React dashboard

From the `dashboard` directory, install the frontend dependencies once and
start the Vite development server:

```powershell
cd dashboard
npm install
npm run dev
```

Open `http://localhost:5173`. For the two-node commands above, enter
`http://127.0.0.1:7601` in the dashboard API field and click **Connect**.
The dashboard reads live overview data from the selected node and can issue
GET, SET, and DELETE operations through `/api/command`.

### Start node B from the dashboard

The dashboard can supervise additional local `cachex-server` processes. Start
one bootstrap node manually first, then open the dashboard's **Server manager**
tab. The default form is ready for a second node; verify the addresses and
click **Start node-b**.

The manager starts the child with the equivalent configuration:

```text
node ID:              node-b
cache address:        127.0.0.1:7002
dashboard address:    127.0.0.1:7602
AOF path:             node-b.aof
cluster nodes:        node-a=127.0.0.1:7001,node-b=127.0.0.1:7002
partitioner:          consistent
replication factor:   2
```

The managed-process list shows its PID and addresses. Use **Stop** there to
terminate a node started by that dashboard. This supervisor is intended for a
local dashboard; do not expose the dashboard API to an untrusted network.

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
