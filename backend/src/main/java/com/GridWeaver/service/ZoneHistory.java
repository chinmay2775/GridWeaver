package com.GridWeaver.service;

import com.GridWeaver.model.Zone;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Rolling time-series of zone rollups, built from the Kafka stream.
 *
 * This is the reason the consumer exists as a separate concern: the registry
 * holds only current state, and reconstructing "what did zone C look like two
 * minutes ago" from live data is impossible. The topic is the source of truth
 * for history; this is a materialised view of it.
 *
 * Per-zone ring buffers rather than one shared list -- each consumer thread
 * owns exactly one zone's partition, so writes never contend.
 */
@Service
public class ZoneHistory {

    public record Sample(
            long ts,
            String zoneStatus,
            int reporting,
            double generationKw,
            double consumptionKw,
            double netKw,
            double avgBatterySoc,
            double loadFactor
    ) {}

    private final int capacity;
    private final Map<Zone, Sample[]> rings = new EnumMap<>(Zone.class);
    private final Map<Zone, int[]> cursors = new EnumMap<>(Zone.class);

    public ZoneHistory(@Value("${gridweaver.history.samples-per-zone:600}") int capacity) {
        this.capacity = capacity;
        for (Zone z : Zone.values()) {
            rings.put(z, new Sample[capacity]);
            cursors.put(z, new int[]{0});
        }
    }

    /** Called from the consumer thread owning this zone's partition. */
    public void append(Zone zone, Sample s) {
        Sample[] ring = rings.get(zone);
        int[] cursor = cursors.get(zone);
        synchronized (cursor) {
            ring[cursor[0] % capacity] = s;
            cursor[0]++;
        }
    }

    /** Oldest to newest, so it plots left-to-right without reversing. */
    public List<Sample> series(Zone zone, int limit) {
        Sample[] ring = rings.get(zone);
        int[] cursor = cursors.get(zone);
        int n;
        synchronized (cursor) {
            n = cursor[0];
        }
        int available = Math.min(n, capacity);
        int take = Math.min(limit, available);

        List<Sample> out = new ArrayList<>(take);
        for (int i = take; i > 0; i--) {
            Sample s = ring[(n - i) % capacity];
            if (s != null) out.add(s);
        }
        return out;
    }

    public Map<Zone, Integer> depths() {
        Map<Zone, Integer> out = new EnumMap<>(Zone.class);
        cursors.forEach((z, c) -> {
            synchronized (c) {
                out.put(z, Math.min(c[0], capacity));
            }
        });
        return out;
    }

    public int capacity() { return capacity; }
}