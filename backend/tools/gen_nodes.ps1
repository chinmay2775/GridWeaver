#!/usr/bin/env python3
"""Generates nodes.json - 10,000 static grid nodes across 5 zones.

Coordinates form a 5-column city grid over Pune. Deterministic (seeded), so
regenerating gives identical output and the map does not shuffle between runs.
"""
import json
import random
from collections import Counter

random.seed(20260810)

ZONES = ["A", "B", "C", "D", "E"]
PER_ZONE = 2000
LAT_MIN, LAT_MAX = 18.44, 18.64
LNG_MIN, LNG_MAX = 73.75, 73.99

# Per-zone type bias: west is generation-heavy, east is demand-heavy.
#
# A flat mix across all zones produces identical load factors everywhere, which
# means the week 4 rebalancer has nothing to move -- it would be provably
# correct and visibly idle. This gradient gives real surplus and deficit zones.
#
# Battery share stays flat at 15%: storage is the mechanism that resolves
# imbalance, so it has to exist on both sides of it.
ZONE_MIX = {
    "A": [("SOLAR", 0.75), ("LOAD", 0.10), ("BATTERY", 0.15)],
    "B": [("SOLAR", 0.62), ("LOAD", 0.23), ("BATTERY", 0.15)],
    "C": [("SOLAR", 0.48), ("LOAD", 0.37), ("BATTERY", 0.15)],
    "D": [("SOLAR", 0.32), ("LOAD", 0.53), ("BATTERY", 0.15)],
    "E": [("SOLAR", 0.18), ("LOAD", 0.67), ("BATTERY", 0.15)],
}


def pick_type(zone):
    r = random.random()
    acc = 0.0
    for t, w in ZONE_MIX[zone]:
        acc += w
        if r < acc:
            return t
    return "SOLAR"


def capacity(t):
    if t == "SOLAR":
        return round(random.uniform(3.0, 12.0), 1)
    if t == "BATTERY":
        return round(random.uniform(5.0, 20.0), 1)
    return round(random.uniform(1.5, 8.0), 1)


zone_width = (LNG_MAX - LNG_MIN) / len(ZONES)
nodes = []

for zi, z in enumerate(ZONES):
    lng_lo = LNG_MIN + zi * zone_width
    lng_hi = lng_lo + zone_width
    for i in range(PER_ZONE):
        t = pick_type(z)
        nodes.append({
            "nodeId": f"zone-{z}/node-{i:04d}",
            "zone": z,
            "type": t,
            "lat": round(random.uniform(LAT_MIN, LAT_MAX), 6),
            # keep a small margin inside the zone column so markers do not
            # sit exactly on the boundary line
            "lng": round(random.uniform(lng_lo + 0.002, lng_hi - 0.002), 6),
            "capacityKw": capacity(t),
        })

with open("src/main/resources/nodes.json", "w") as f:
    json.dump(nodes, f, separators=(",", ":"))

print(f"wrote {len(nodes)} nodes")
print()
print(f"{'zone':<6}{'SOLAR':>8}{'LOAD':>8}{'BATTERY':>9}")
for z in ZONES:
    c = Counter(n["type"] for n in nodes if n["zone"] == z)
    print(f"{z:<6}{c['SOLAR']:>8}{c['LOAD']:>8}{c['BATTERY']:>9}")