package com.gridweaver.config;

import com.gridweaver.model.NodeState;
import com.gridweaver.model.Zone;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Single source of truth for live node state.
 *
 * Sized for 50k entries with concurrencyLevel-free CHM (Java 8+ CHM striping
 * handles this fine). Writers are virtual threads, one per connection.
 * Readers are the 250ms delta broadcaster added in Week 2.
 */
@Component
public class NodeRegistry {

    private final ConcurrentHashMap<String, NodeState> nodes = new ConcurrentHashMap<>(65_536);
    private final Map<Zone, LongAdder> zoneCounts = new EnumMap<>(Zone.class);

    public NodeRegistry() {
        for (Zone z : Zone.values()) zoneCounts.put(z, new LongAdder());
    }

    public void put(NodeState state) {
        NodeState prev = nodes.put(state.nodeId(), state);
        if (prev == null) zoneCounts.get(state.zone()).increment();
    }

    public NodeState get(String nodeId) {
        return nodes.get(nodeId);
    }

    /** Atomic read-modify-write. Returns the new state, or null if unknown node. */
    public NodeState update(String nodeId, java.util.function.UnaryOperator<NodeState> fn) {
        return nodes.computeIfPresent(nodeId, (k, v) -> fn.apply(v));
    }

    public Collection<NodeState> all() {
        return nodes.values();
    }

    public int size() {
        return nodes.size();
    }

    public Map<Zone, Long> countsByZone() {
        Map<Zone, Long> out = new EnumMap<>(Zone.class);
        zoneCounts.forEach((z, adder) -> out.put(z, adder.sum()));
        return out;
    }
}
