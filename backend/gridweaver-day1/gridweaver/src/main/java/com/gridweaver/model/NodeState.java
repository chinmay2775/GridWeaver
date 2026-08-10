package com.gridweaver.model;

/**
 * Live state of a single grid node. Immutable: the registry swaps the whole
 * record on each telemetry tick, so readers never see a half-updated node.
 *
 * powerKw  : signed. positive = generating/discharging, negative = consuming/charging.
 * soc      : state of charge 0.0-1.0. Meaningful for BATTERY only; 0 elsewhere.
 * lastSeen : epoch millis of the last telemetry frame.
 */
public record NodeState(
        String nodeId,
        Zone zone,
        NodeType type,
        double lat,
        double lng,
        double capacityKw,
        double powerKw,
        double soc,
        NodeStatus status,
        long lastSeen
) {

    public static NodeState initial(NodeSeed seed) {
        return new NodeState(
                seed.nodeId(), seed.zone(), seed.type(),
                seed.lat(), seed.lng(), seed.capacityKw(),
                0.0,
                seed.type() == NodeType.BATTERY ? 0.5 : 0.0,
                NodeStatus.IDLE,
                0L
        );
    }

    /** Applies an incoming telemetry frame. Status is left untouched here --
     *  Week 2's transition table owns status changes. */
    public NodeState withTelemetry(double powerKw, double soc, long ts) {
        return new NodeState(nodeId, zone, type, lat, lng, capacityKw,
                powerKw, soc, status, ts);
    }

    public NodeState withStatus(NodeStatus newStatus) {
        return new NodeState(nodeId, zone, type, lat, lng, capacityKw,
                powerKw, soc, newStatus, lastSeen);
    }
}
