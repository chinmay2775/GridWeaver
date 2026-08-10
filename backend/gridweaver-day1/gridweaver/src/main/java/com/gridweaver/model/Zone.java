package com.gridweaver.model;

/**
 * Fixed set of grid zones. Zone is the unit of aggregation everywhere:
 * Kafka partition key, state-machine instance, map cluster, rebalance unit.
 */
public enum Zone {
    A, B, C, D, E;

    public static Zone fromNodeId(String nodeId) {
        // nodeId format: "zone-A/node-0042"
        int dash = nodeId.indexOf('-');
        return Zone.valueOf(nodeId.substring(dash + 1, dash + 2));
    }
}
