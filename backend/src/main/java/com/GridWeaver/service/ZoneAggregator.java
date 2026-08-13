package com.GridWeaver.service;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.NodeType;
import com.GridWeaver.model.Zone;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;

/**
 * Per-zone rollups computed on demand from the registry.
 *
 * Deliberately not incrementally maintained: a full scan of 10k records takes
 * well under a millisecond, and incremental counters would mean contended
 * writes on every one of the ~10k frames/sec hitting the ingest path.
 */
@Service
public class ZoneAggregator {

    private final NodeRegistry registry;

    public ZoneAggregator(NodeRegistry registry) {
        this.registry = registry;
    }

    public record ZoneSummary(
            Zone zone,
            int nodeCount,
            int reporting,
            double generationKw,
            double consumptionKw,
            double netKw,
            double avgBatterySoc,
            double loadFactor
    ) {}

    public Map<Zone, ZoneSummary> summarise(long staleAfterMs) {
        long cutoff = System.currentTimeMillis() - staleAfterMs;

        Map<Zone, double[]> acc = new EnumMap<>(Zone.class);
        for (Zone z : Zone.values()) {
            // [count, reporting, generation, consumption, socSum, batteryCount, capacity]
            acc.put(z, new double[7]);
        }

        for (NodeState n : registry.all()) {
            double[] a = acc.get(n.zone());
            a[0]++;
            a[6] += n.capacityKw();

            if (n.lastSeen() < cutoff) continue;   // stale or never reported
            a[1]++;

            if (n.powerKw() >= 0) a[2] += n.powerKw();
            else                  a[3] += -n.powerKw();

            if (n.type() == NodeType.BATTERY) {
                a[4] += n.soc();
                a[5]++;
            }
        }

        Map<Zone, ZoneSummary> out = new EnumMap<>(Zone.class);
        acc.forEach((z, a) -> {
            double gen = round(a[2]);
            double con = round(a[3]);
            out.put(z, new ZoneSummary(
                    z,
                    (int) a[0],
                    (int) a[1],
                    gen,
                    con,
                    round(gen - con),
                    a[5] > 0 ? round(a[4] / a[5]) : 0.0,
                    a[6] > 0 ? round(con / a[6]) : 0.0
            ));
        });
        return out;
    }

    private static double round(double d) {
        return Math.round(d * 100.0) / 100.0;
    }
}