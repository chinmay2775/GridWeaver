package com.gridweaver.model;

/**
 * Static topology loaded from nodes.json at startup. Never changes at runtime.
 */
public record NodeSeed(
        String nodeId,
        Zone zone,
        NodeType type,
        double lat,
        double lng,
        double capacityKw
) {}
