# LAN Distributed Artifact Cache

Peer-to-peer artifact cache for machines on a LAN. The project uses UDP discovery, TCP transfers, SHA-256 content addressing, consistent hashing, persistent storage, and replication planning.

## Features

- UDP peer discovery
- TCP artifact transfer
- SHA-256 content addressing
- Consistent hashing
- Replica planning
- Persistent disk cache
- Concurrent node services

## Architecture

```text
CLI
 |
Node
 +-- Discovery (UDP)
 |    +-- Peer announcements
 |    +-- Peer tracking
 |
 +-- Transport (TCP)
 |    +-- Artifact requests
 |    +-- Artifact transfers
 |
 +-- Cache
 |    +-- Persistent storage
 |
 +-- Hashing
 |    +-- SHA-256 content addressing
 |    +-- Consistent hash ring
 |
 +-- Replication
 |    +-- Replica planning
 |
 +-- Statistics
```

### Data Flow

1. `put` hashes an artifact and stores it in the local cache.
2. Nodes discover peers through UDP broadcast.
3. `get` checks the local cache and, on a cache miss, discovers available peers.
4. The artifact is requested and transferred over TCP using its SHA-256 hash.
5. The received content is verified and cached locally.

## Project Structure

```text
cmd/p2cache/           CLI entry point
internal/cache/        Persistent artifact storage
internal/discovery/    UDP peer discovery
internal/transport/    TCP protocol and transfer
internal/hashing/      SHA-256 and hash ring
internal/replication/  Replica selection
internal/stats/        Statistics
tests/                 Component tests
benchmarks/            Runtime benchmark
```

## Requirements

- Linux
- Go 1.22+

## Build

```bash
go build -o p2cache ./cmd/p2cache
```

## Usage

```bash
./p2cache put file.tar
./p2cache get <sha256> output.tar
./p2cache peers
./p2cache stats
./p2cache serve
```

The CLI reads `P2CACHE_ROOT`, `P2CACHE_NODE_ID`, `P2CACHE_TCP_ADDR`, and `P2CACHE_UDP_ADDR` when set; the defaults are `.p2cache`, `node-1`, `:9001`, and `:9002`. Run `serve` continuously on each node. `get` starts a short-lived node client, while `put` writes directly to the selected local cache.

For example, on a LAN where the two machines can reach each other's TCP addresses, start node A:

```bash
P2CACHE_ROOT=/var/tmp/cache-a P2CACHE_NODE_ID=node-a \
P2CACHE_TCP_ADDR=:9001 P2CACHE_UDP_ADDR=:9002 ./p2cache serve
```

Store an artifact in node A's cache from another terminal on that machine:

```bash
P2CACHE_ROOT=/var/tmp/cache-a P2CACHE_NODE_ID=node-a \
P2CACHE_TCP_ADDR=:9001 P2CACHE_UDP_ADDR=:9002 ./p2cache put file.tar
```

Record the SHA-256 hash printed by the command.

Start node B with the same UDP discovery port and a different TCP port:

```bash
P2CACHE_ROOT=/var/tmp/cache-b P2CACHE_NODE_ID=node-b \
P2CACHE_TCP_ADDR=:9003 P2CACHE_UDP_ADDR=:9002 ./p2cache serve
```

After discovery, retrieve the artifact from node B. Use a separate TCP port for the short-lived client because node B's `serve` process already occupies port `9003`:

```bash
P2CACHE_ROOT=/var/tmp/cache-b P2CACHE_NODE_ID=node-b-client \
P2CACHE_TCP_ADDR=:9004 P2CACHE_UDP_ADDR=:9002 \
./p2cache get <sha256> output.tar
```

The first `get` requests the artifact over TCP, verifies its SHA-256 digest, and stores it in node B's cache. A later `get` for the same hash is served locally. UDP discovery uses broadcast on the configured port; firewalls must allow that UDP port and the configured TCP ports. Placement is static and does not rebalance existing artifacts.

## Testing

Run the automated checks from the project root:

```bash
gofmt -w internal/discovery/discovery.go internal/node/node.go
go test ./...
go test -race ./...
go vet ./...
go build -o p2cache ./cmd/p2cache
```

These checks validate formatting, package tests, race detection, static analysis, and compilation. They do not by themselves prove that peer discovery and artifact transfers work across physical machines.

### Manual LAN Test

Use two machines on the same LAN. Ensure that both machines can reach each other's advertised TCP addresses and that the configured UDP and TCP ports are allowed by their firewalls.

1. Start node A using the `serve` command shown above.
2. Run `put` on node A and record the resulting SHA-256 hash.
3. Start node B using the `serve` command shown above.
4. Run `get` on node B using the recorded hash and a separate client TCP port, as shown above.
5. Verify that the retrieved file matches the original:

   ```bash
   sha256sum file.tar output.tar
   ```

   Both files must produce the same SHA-256 hash.

6. Run `get` again for the same hash to exercise the local-cache path.
7. Check the peer list using `./p2cache peers` with the appropriate node configuration. Do not use a TCP port already occupied by a running `serve` process.

Use a separate, disposable cache directory for testing so that existing artifacts are not overwritten or removed. If retrieval reports that no peers are available, check the UDP broadcast configuration, firewall rules, and whether both nodes are on a network that permits broadcast traffic.

## Benchmark

Run the benchmark directly:

```bash
go run ./benchmarks
```

It reports the measured cache write throughput for the current machine. Example output (illustrative):

```text
puts=100 bytes=104857600 elapsed=19.88ms throughput=5029.31 MB/s
```

Actual throughput depends on the machine, storage, workload, and operating system. Benchmark results are measurements, not guaranteed performance.

## Limitations

- LAN discovery uses plain UDP broadcast on a single port and does not handle network partitions.
- Broadcast discovery may not work across VLANs, routed networks, or networks that restrict broadcast traffic.
- The CLI runs a single local node configuration per invocation. A short-lived `get` client needs a separate TCP port from a running `serve` process on the same machine.
- Replica placement is static; rebalancing is not performed automatically.
- Replica planning does not automatically push artifact copies to selected peers when an artifact is stored.
- Artifact hashes must be shared manually; automatic artifact discovery by filename is not supported.
- Successful automated tests do not guarantee that UDP discovery and TCP transfers will work on every LAN configuration.

## Future Work

- Implement distributed artifact discovery by filename, eliminating manual hash sharing.
- Automate replica transfer and rebalancing.
- Improve discovery and resilience across network partitions.

## License

MIT License