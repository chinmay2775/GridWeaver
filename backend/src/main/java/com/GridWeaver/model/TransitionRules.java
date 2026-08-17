package com.GridWeaver.model;


public final class TransitionRules {

    private TransitionRules() {}

    public record Thresholds(
            double dischargeAboveLoad,
            double chargeBelowLoad,
            double socFloor,
            double socCeiling,
            double idleBandKw,
            long staleAfterMs
    ) {
        public static Thresholds defaults() {
            return new Thresholds(0.80, 0.40, 0.20, 0.95, 0.10, 5_000);
        }
    }


    public static NodeStatus next(NodeState node, double zoneLoad, long now, Thresholds t) {

        // A node that has stopped reporting is faulted regardless of type or
        // last-known values. Checked first so stale data can never drive a decision.
        if (node.lastSeen() == 0 || now - node.lastSeen() > t.staleAfterMs()) {
            return NodeStatus.FAULT;
        }

        return switch (node.type()) {
            case BATTERY -> battery(node, zoneLoad, t);
            // Generation and consumption nodes have no commanded state -- their
            // status just reflects observed power flow.
            case SOLAR   -> node.powerKw() >  t.idleBandKw() ? NodeStatus.DISCHARGING : NodeStatus.IDLE;
            case LOAD    -> node.powerKw() < -t.idleBandKw() ? NodeStatus.CHARGING    : NodeStatus.IDLE;
        };
    }

    private static NodeStatus battery(NodeState node, double zoneLoad, Thresholds t) {
        double soc = node.soc();

        if (zoneLoad > t.dischargeAboveLoad()) {
            return soc > t.socFloor() ? NodeStatus.DISCHARGING : NodeStatus.IDLE;
        }
        if (zoneLoad < t.chargeBelowLoad()) {
            return soc < t.socCeiling() ? NodeStatus.CHARGING : NodeStatus.IDLE;
        }
        return NodeStatus.IDLE;
    }
}