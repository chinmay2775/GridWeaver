package com.GridWeaver.config;

import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.Zone;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.UnaryOperator;

/**
 * Single source of truth for live node state.
 *
 * Pre-sized for 50k entries so we never rehash under load. Writers are the
 * virtual threads added on Day 2, one per connection. Readers are the 250ms
 * delta broadcaster added in Week 2.
 */
@Component
public class NodeRegistry {

    private final ConcurrentHashMap<String, NodeState> nodes = new ConcurrentHashMap<>(65536);
    private final Map<Zone, LongAdder> zoneCounts = new EnumMap<>(Zone.class);

    public NodeRegistry() {
        for (Zone z : Zone.values()) {
            zoneCounts.put(z, new LongAdder());
        }
    }

    public void put(NodeState state) {
        NodeState prev = nodes.put(state.nodeId(), state);
        if (prev == null) {
            zoneCounts.get(state.zone()).increment();
        }
    }

    public NodeState get(String nodeId) {
        return nodes.get(nodeId);
    }

    /** Atomic read-modify-write. Returns the new state, or null if unknown node. */
    public NodeState update(String nodeId, UnaryOperator<NodeState> fn) {
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