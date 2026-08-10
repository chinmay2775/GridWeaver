# GridWeaver

Virtual Thread IoT Microgrid State Engine — Java 21 (Project Loom), Spring Boot 3.4, Kafka (KRaft), Leaflet.

## Day 1 status

- [x] Docker Compose: single-broker Kafka in KRaft mode, `grid.telemetry` topic with 5 partitions
- [x] Spring Boot 3.4 / Java 21 skeleton, virtual threads enabled
- [x] `NodeState` immutable record + `NodeRegistry` (ConcurrentHashMap, sized for 50k)
- [x] `nodes.json` — 10,000 seeded nodes across zones A–E
- [x] `/debug/stats` and `/debug/nodes` sanity endpoints

## Conventions locked

| Decision | Value |
|---|---|
| Node ID | `zone-{A..E}/node-{0000..1999}` |
| Zone count | 5 — also the Kafka partition count |
| Power sign | `+` generating/discharging, `-` consuming/charging |
| SoC | 0.0–1.0, meaningful for `BATTERY` only |
| Map rendering | Leaflet `preferCanvas: true` |
| Outbound push | 250 ms tick, delta-only |

## Run

```bash
docker compose up -d          # Kafka + topic creation
docker compose logs kafka-init  # confirm grid.telemetry exists

./mvnw spring-boot:run
curl -s localhost:8080/debug/stats | jq
curl -s 'localhost:8080/debug/nodes?limit=3' | jq
```

Expected `/debug/stats`:

```json
{
  "nodesLoaded": 10000,
  "byZone": { "A": 2000, "B": 2000, "C": 2000, "D": 2000, "E": 2000 },
  "activeConnections": 0,
  "javaVersion": "21.0.x+y"
}
```

## Regenerating topology

```bash
python3 tools/gen_nodes.py    # deterministic, seeded
```

## Next — Day 2

WebSocket ingestion endpoint, one virtual thread per connection, wire `activeConnections` into `/debug/stats`.
