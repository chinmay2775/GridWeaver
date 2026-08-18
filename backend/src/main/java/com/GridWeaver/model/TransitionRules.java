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


    public static NodeStatus next(NodeState node, ZoneStatus zoneStatus, long now, Thresholds t) {

        if (node.lastSeen() == 0 || now - node.lastSeen() > t.staleAfterMs()) {
            return NodeStatus.FAULT;
        }

        return switch (node.type()) {
            case BATTERY -> battery(node, zoneStatus, t);
            case SOLAR   -> node.powerKw() >  t.idleBandKw() ? NodeStatus.DISCHARGING : NodeStatus.IDLE;
            case LOAD    -> node.powerKw() < -t.idleBandKw() ? NodeStatus.CHARGING    : NodeStatus.IDLE;
        };
    }

    /**
      Batteries execute their zone's policy, bounded by their own SoC.
      SoC limits are checked after the zone command so the zone decides intent
      and the node decides feasibility -- a depleted battery in a STRESSED zone
      simply cannot help, and idles rather than over-discharging.
     */

    private static NodeStatus battery(NodeState node, ZoneStatus zone, Thresholds t) {
        double soc = node.soc();
        return switch (zone) {
            case STRESSED, CRITICAL ->
                    soc > t.socFloor()   ? NodeStatus.DISCHARGING : NodeStatus.IDLE;
            case SURPLUS ->
                    soc < t.socCeiling() ? NodeStatus.CHARGING    : NodeStatus.IDLE;
            case NOMINAL -> NodeStatus.IDLE;
        };
    }
}