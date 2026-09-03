# GridWeaver

Virtual Thread IoT Microgrid State Engine — Java 21 (Project Loom), Spring Boot 4.1, Kafka, React + Leaflet.

Simulates 10,000 distributed energy nodes across five city zones (solar panels, home batteries, and household demand). Telemetry arrives over raw TCP with one virtual thread per connection. A zone-level state machine decides battery policy, a rebalancer moves surplus power to zones running a deficit, and a live map shows the whole thing updating four times a second.

---

## Headline result

The project exists to demonstrate one thing: virtual threads make blocking I/O cheap at a scale where platform threads collapse.

|                             | Virtual threads | Platform threads |
|-----------------------------|-----------------|------------------|
| Concurrent connections held | **44,166**      | 29,067           |
| OS threads (JMX)            | **26**          | 29,644           |
| Heap used / max             | 1,650 / 2,048 MB| **2,048 / 2,048 MB** |
| Outcome                     | stable          | `OutOfMemoryError` cascade |

Same code, same `-Xmx2g`, same load generator. The only difference is `Thread.ofVirtual()` versus a cached platform thread pool. Both ingestion servers implement byte-identical protocol and handler logic, which is what makes the comparison fair.

The failure was heap exhaustion, not thread creation. Windows created all 29,644 OS threads; what ran out was Java heap holding a `BufferedReader` per connection.

Other measured figures:

- **36× less wire traffic** from snapshot-then-delta WebSocket broadcasting — ~1.6 KB per tick versus ~60 KB for full state
- **32 ms** producer-to-consumer latency through Kafka
- **1.3 ms** to evaluate 10,000 nodes and five zone state machines per tick

---

## What it does

```
LoadGenerator (10k virtual threads)
        │  TCP :9099, pipe-delimited frames
        ▼
TelemetryIngestServer      4 acceptors, 1 virtual thread per connection
        │
        ▼
NodeRegistry               ConcurrentHashMap of immutable NodeState records
        │
        ├──▶ StateEvaluator ─── 250 ms tick
        │       ZoneAggregator     per-zone rollups
        │       ZoneStateMachine   5 machines with hysteresis
        │       BalanceCalculator  surplus / deficit accounting
        │       ZoneRebalancer     greedy nearest-first transfers
        │       EventLog           4096-slot audit ring
        │
        ├──▶ GridBroadcaster ── WebSocket, delta-only
        │
        └──▶ TelemetryPublisher ─ Kafka grid.telemetry, 5 partitions
                    │
                    ▼
              TelemetryConsumer ── 5 threads, one zone each
                    │
                    ▼
                ZoneHistory        materialised time-series
```

Nodes are seeded with a west-to-east bias: zone A is 75% solar, zone E is 67% demand. That gives real surplus and deficit zones for the rebalancer to work with — a flat distribution would leave it correct but idle.

---

## Running it

Needs Java 21, Maven, Docker, Node 20+.

```bash
# 1. Kafka
cd backend
docker compose up -d
docker exec gw-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list        # expect: grid.telemetry

# 2. Backend
mvn spring-boot:run "-Dspring-boot.run.jvmArguments=-Xmx2g -XX:+UseZGC"

# 3. Load generator (separate terminal)
mvn compile exec:java \
  "-Dexec.mainClass=com.GridWeaver.tools.LoadGenerator" \
  "-Dexec.args=10000" \
  "-Dexec.jvmArgs=-Xmx2g -XX:+UseZGC"

# 4. Dashboard
cd ../frontend && npm install && npm run dev
```

Open http://localhost:5173.

### Reproducing the concurrency audit

Set `gridweaver.ingest.mode` in `application.yml` and restart:

- `virtual` — default. `/debug/threads` shows ~26 platform threads at 10k connections.
- `platform` — one OS thread per connection. Expect OOM above ~29k.

---

## Wire protocol

Line-oriented and pipe-delimited rather than JSON. At 10,000 frames per second the per-frame allocation cost of JSON parsing dominates the ingest path.

```
client → HELLO|zone-A/node-0042      server → OK|SOLAR   or   ERR|<reason>
client → T|4.52|0.831                server → (silent)
server → BYE                         (graceful shutdown)
```

The handshake sends the node ID once and the server replies with the node's type, so the client emits telemetry consistent with what the registry believes. Sending the ID on every frame would cost ~160 KB/sec of redundancy at 10k nodes.

---

## Endpoints

| Endpoint | What it shows |
|---|---|
| `/debug/stats` | Connections, frames, Kafka producer health |
| `/debug/threads` | Platform threads vs live connections — the audit number |
| `/debug/zones` | Per-zone generation, consumption, load factor, battery SoC |
| `/debug/states` | Node status counts, tick timing, zone machine states |
| `/debug/balance` | Surplus, deficit, storage headroom, self-balance check |
| `/debug/transfers` | Current transfer plan and unmet deficit |
| `/debug/events` | Audit ring — filter by `zone`, `status`, `kind` |
| `/debug/history` | Kafka-materialised zone time-series |
| `/debug/lag` | Consumer group lag per partition |
| `/actuator/health` | UP / DEGRADED with lag detail |

---

## Design decisions

**Raw TCP for ingestion, not WebSocket.** Spring's WebSocket support is event-driven on an NIO loop — a few threads serving all connections through callbacks. That would mean no thread per connection and nothing to count, which defeats the purpose. Blocking `readLine()` on a virtual thread is the honest demonstration. WebSocket is used for the outbound dashboard feed, where an event loop is the right fit.

**Immutable `NodeState` records.** The registry swaps whole records on each telemetry frame, so the 250 ms broadcaster never sees a half-updated node. Costs one allocation per frame and removes the need for locking anywhere.

**Hand-rolled zone state machine.** Spring Statemachine 4.0.x targets Boot 3.5.x, and the Boot 4 support request was closed as not planned upstream. The implementation keeps what matters — a defined state set, events, guards, transition counting — plus hysteresis, so a zone sitting near a threshold doesn't flap every tick.

**Custom Kafka partitioner.** The default murmur2 partitioner collides badly on five keys: two zones landed on one partition, two on another, and two partitions stayed empty. `ZonePartitioner` maps zone ordinal straight to partition index, so each of the five consumer threads owns exactly one zone and per-zone ordering holds end to end.

**Transfers derive from balance, never from zone status.** A zone can be in `SURPLUS` state (low load factor) while running a net power deficit. Balance is physical truth; status is the policy signal that drives battery behaviour. Conflating them would produce transfers that make no sense.

**Rate-limited transfer auditing.** The transfer plan recomputes four times a second and amounts drift constantly. Logging every one would fill the 4096-slot ring within minutes and flush out every zone transition worth keeping. `TransferTracker` emits only route started, materially changed, or ended.

**Bounded scheduler pool.** Spring's default scheduler spawns a new thread whenever a fixed-rate task overruns. A slow Kafka metadata fetch pushed the tick past 250 ms and the pool grew to 339 threads in ninety seconds. A fixed four-thread pool makes overruns queue, so the cost appears as tick latency instead of silent thread growth.

---

## Things that went wrong, and what fixed them

**Spring Boot 4.1 ships Jackson 3.** Package moved from `com.fasterxml.jackson` to `tools.jackson`, and `JsonMapper` replaced `ObjectMapper` as the auto-configured bean.

**Connections capped around 6,000** with `Connection refused` at the TCP layer — the OS accept queue overflowing. Windows silently clamps the listen backlog, so raising it in config did nothing. Four concurrent acceptor threads plus pacing the generator to 100 connections per 50 ms took it to 10,000 with zero failures.

**`jcmd Thread.print` does not list virtual threads.** By design — dumping millions would be unusable. `Thread.dump_to_file -format=json` does include them. This mattered because the thread count is the project's central evidence.

**A null `nodeId` in the handshake** made `registry.get(null)` throw before any reply was written, so connections closed silently with no error visible. The handler was catching at DEBUG level, which made a crashed connection look identical to a clean disconnect. Raising that to WARN is the real lesson.

**Stale JVMs repeatedly produced contradictory readings.** Counters showing zero while the generator reported thousands of connections, because two backends were alive on different ports. Resolving the PID from the listening socket (`Get-NetTCPConnection -LocalPort 9099`) is deterministic where process listings are not.

---

## Known limitations

- Greedy nearest-first routing, not min-cost flow. Fine for five zones in a line; a real interconnect topology would need rework.
- Transfers are planned and audited but not applied back to node power — the rebalancer produces a plan, not a simulation of its effect.
- Single Kafka broker, replication factor 1. Not a production topology.
- The load generator's random walk is not a realistic demand curve, so zone load factors are stable rather than following a daily cycle.