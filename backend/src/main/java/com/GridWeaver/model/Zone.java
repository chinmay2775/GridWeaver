package com.GridWeaver.model;

public enum Zone {
    A, B, C, D, E;

    public static Zone fromNodeId(String nodeId) {
        // nodeId format: "zone-A/node-0042"
        int dash = nodeId.indexOf('-');
        return Zone.valueOf(nodeId.substring(dash + 1, dash + 2));
    }
}