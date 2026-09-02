package com.GridWeaver.service;

import com.GridWeaver.model.EventKind;
import com.GridWeaver.model.PowerTransfer;
import com.GridWeaver.model.Zone;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a per-tick transfer plan into a stream of route lifecycle events.
 *
 * The plan is recomputed 4x/sec and amounts drift continuously, so logging
 * every transfer would fill a 4096-slot ring in under five minutes and flush
 * out every zone transition worth auditing. What is auditable is a route
 * starting, changing materially, or stopping.
 *
 * Single-threaded: only the evaluator tick calls update().
 */
@Service
public class TransferTracker {

    /** A route currently carrying power, and when it started. */
    private record Route(double amountKw, long startedAt, long lastLoggedAt) {}

    @Value("${gridweaver.balance.transfer-change-threshold:0.25}")
    private double changeThreshold;

    @Value("${gridweaver.balance.transfer-change-min-interval-ms:10000}")
    private long changeMinIntervalMs;

    private final Map<String, Route> active = new HashMap<>();

    /** What changed since last tick, for the caller to record. */
    public record Change(EventKind kind, Zone from, Zone to, double amountKw, long dwellMs) {}

    public List<Change> update(List<PowerTransfer> plan, long now) {
        List<Change> changes = new ArrayList<>();
        Map<String, PowerTransfer> current = new HashMap<>();
        for (PowerTransfer t : plan) {
            current.put(key(t.from(), t.to()), t);
        }

        // Started, or materially changed
        for (var e : current.entrySet()) {
            PowerTransfer t = e.getValue();
            Route prior = active.get(e.getKey());

            if (prior == null) {
                changes.add(new Change(EventKind.TRANSFER_STARTED,
                        t.from(), t.to(), t.amountKw(), 0));
                active.put(e.getKey(), new Route(t.amountKw(), now, now));
                continue;
            }

            // Rate-limited AND magnitude-gated: a route whose amount wanders
            // by a few percent every tick is the same route, not a new event.
            double delta = Math.abs(t.amountKw() - prior.amountKw());
            boolean material = prior.amountKw() > 0
                    && delta / prior.amountKw() >= changeThreshold;
            boolean cooledDown = now - prior.lastLoggedAt() >= changeMinIntervalMs;

            if (material && cooledDown) {
                changes.add(new Change(EventKind.TRANSFER_CHANGED,
                        t.from(), t.to(), t.amountKw(), now - prior.startedAt()));
                active.put(e.getKey(), new Route(t.amountKw(), prior.startedAt(), now));
            } else {
                // Track the amount without logging, so drift accumulates
                // against the last logged value rather than the last tick.
                active.put(e.getKey(), new Route(prior.amountKw(), prior.startedAt(), prior.lastLoggedAt()));
            }
        }

        // Ended
        active.entrySet().removeIf(e -> {
            if (current.containsKey(e.getKey())) return false;
            String[] parts = e.getKey().split("->");
            changes.add(new Change(EventKind.TRANSFER_ENDED,
                    Zone.valueOf(parts[0]), Zone.valueOf(parts[1]),
                    0, now - e.getValue().startedAt()));
            return true;
        });

        return changes;
    }

    private static String key(Zone from, Zone to) {
        return from.name() + "->" + to.name();
    }

    public int activeRoutes() { return active.size(); }
}