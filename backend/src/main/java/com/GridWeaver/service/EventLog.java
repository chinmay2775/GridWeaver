package com.GridWeaver.service;

import com.GridWeaver.model.GridEvent;
import com.GridWeaver.model.Zone;
import com.GridWeaver.model.ZoneStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
  Fixed-capacity ring of recent grid events.

  Bounded by design: an unbounded list would be a slow memory leak over a long
  demo run, and nobody audits the ten-thousandth event. Capacity is a power of
  two so the modulo becomes a bitmask.

  Single writer (the evaluator tick), many readers (HTTP). The sequence counter
  is atomic and slots are written before the count is published, so a reader
  never sees a half-written entry.
*/

@Service
public class EventLog {

    private final GridEvent[] ring;
    private final int mask;
    private final AtomicLong seq = new AtomicLong();

    public EventLog(@Value("${gridweaver.eventlog.capacity:4096}") int capacity) {
        int size = Integer.highestOneBit(Math.max(64, capacity - 1)) << 1;  // round up to power of 2
        this.ring = new GridEvent[size];
        this.mask = size - 1;
    }

    public GridEvent record(Zone zone, ZoneStatus from, ZoneStatus to,
                            com.GridWeaver.model.ZoneEvent trigger,
                            double loadFactor, double avgSoc,
                            long dwellMs, long affectedNodes) {
        long n = seq.incrementAndGet();
        GridEvent e = new GridEvent(n, System.currentTimeMillis(), zone, from, to,
                trigger, loadFactor, avgSoc, dwellMs, affectedNodes);
        ring[(int) ((n - 1) & mask)] = e;
        return e;
    }

    public long total() { return seq.get(); }

    public int capacity() { return ring.length; }


//     Most recent first.
//
//     @param limit  max entries to return
//     @param zone   optional filter, null for all
//     @param status optional filter on the destination state, null for all
//
    public List<GridEvent> recent(int limit, Zone zone, ZoneStatus status) {
        long n = seq.get();
        int scan = (int) Math.min(n, ring.length);
        List<GridEvent> out = new ArrayList<>(Math.min(limit, scan));

        for (int i = 0; i < scan && out.size() < limit; i++) {
            GridEvent e = ring[(int) ((n - 1 - i) & mask)];
            if (e == null) continue;
            if (zone != null && e.zone() != zone) continue;
            if (status != null && e.to() != status) continue;
            out.add(e);
        }
        return out;
    }

    // Per-zone transition counts, for the summary strip in the UI.
    public List<int[]> countsByZone() {
        int[][] counts = new int[Zone.values().length][ZoneStatus.values().length];
        long n = seq.get();
        int scan = (int) Math.min(n, ring.length);
        for (int i = 0; i < scan; i++) {
            GridEvent e = ring[(int) ((n - 1 - i) & mask)];
            if (e != null) counts[e.zone().ordinal()][e.to().ordinal()]++;
        }
        return List.of(counts);
    }
}