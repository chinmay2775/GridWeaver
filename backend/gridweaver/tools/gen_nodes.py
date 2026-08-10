#!/usr/bin/env python3
"""Generates nodes.json - 10,000 static grid nodes across 5 zones.
Coordinates form a 5-column city grid over Pune. Deterministic (seeded)."""
import json, random, math

random.seed(20260810)

N = 10_000
ZONES = ["A", "B", "C", "D", "E"]
LAT_MIN, LAT_MAX = 18.44, 18.64
LNG_MIN, LNG_MAX = 73.75, 73.99

# type mix: mostly solar, some load, fewer batteries
TYPE_MIX = [("SOLAR", 0.55), ("LOAD", 0.30), ("BATTERY", 0.15)]

def pick_type():
    r = random.random()
    acc = 0.0
    for t, w in TYPE_MIX:
        acc += w
        if r < acc:
            return t
    return "SOLAR"

def capacity(t):
    if t == "SOLAR":   return round(random.uniform(3.0, 12.0), 1)
    if t == "BATTERY": return round(random.uniform(5.0, 20.0), 1)
    return round(random.uniform(1.5, 8.0), 1)

zone_width = (LNG_MAX - LNG_MIN) / len(ZONES)
per_zone = N // len(ZONES)

nodes = []
for zi, z in enumerate(ZONES):
    lng_lo = LNG_MIN + zi * zone_width
    lng_hi = lng_lo + zone_width
    for i in range(per_zone):
        t = pick_type()
        nodes.append({
            "nodeId": f"zone-{z}/node-{i:04d}",
            "zone": z,
            "type": t,
            # jitter inside the zone column, slight clustering toward the centre
            "lat": round(random.uniform(LAT_MIN, LAT_MAX), 6),
            "lng": round(random.uniform(lng_lo + 0.002, lng_hi - 0.002), 6),
            "capacityKw": capacity(t),
        })

with open("src/main/resources/nodes.json", "w") as f:
    json.dump(nodes, f, separators=(",", ":"))

from collections import Counter
print(f"wrote {len(nodes)} nodes")
print("by zone:", dict(Counter(n['zone'] for n in nodes)))
print("by type:", dict(Counter(n['type'] for n in nodes)))
