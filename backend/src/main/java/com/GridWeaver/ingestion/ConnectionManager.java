package com.GridWeaver.ingestion;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Connection and throughput counters. Deliberately not a Map of sessions --
 * at 50k connections we only need aggregates, and a Map would add contention
 * on every connect/disconnect for no benefit.
 */
@Component
public class ConnectionManager {

    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final LongAdder totalAccepted = new LongAdder();
    private final LongAdder framesReceived = new LongAdder();
    private final LongAdder framesRejected = new LongAdder();

    public void onConnect() {
        int now = active.incrementAndGet();
        totalAccepted.increment();
        peak.updateAndGet(p -> Math.max(p, now));
    }

    public void onDisconnect() {
        active.decrementAndGet();
    }

    public void onFrame() {
        framesReceived.increment();
    }

    public void onBadFrame() {
        framesRejected.increment();
    }

    public int active()          { return active.get(); }
    public int peak()            { return peak.get(); }
    public long totalAccepted()  { return totalAccepted.sum(); }
    public long framesReceived() { return framesReceived.sum(); }
    public long framesRejected() { return framesRejected.sum(); }
}