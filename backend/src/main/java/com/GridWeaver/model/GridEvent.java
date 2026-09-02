package com.GridWeaver.model;

/** One auditable grid event -- either a zone state transition or a change in
  power routing between zones.

  A single record covers both because an operator auditing "what happened at
  14:32" wants one ordered timeline, not two they have to interleave by hand.
  Fields not relevant to a given kind are zero or null. */

public record GridEvent(
        long seq,
        long ts,
        EventKind kind,
        Zone zone,              // subject zone (sink, for transfers)
        ZoneStatus from,        // transitions only
        ZoneStatus to,          // transitions only
        ZoneEvent trigger,      // transitions only
        Zone counterparty,      // transfers only: the source zone
        double amountKw,        // transfers only
        double loadFactor,
        double avgSoc,
        long dwellMs,
        long affectedNodes
) {

    public static GridEvent transition(long seq, Zone zone, ZoneStatus from, ZoneStatus to,
                                       ZoneEvent trigger, double loadFactor, double avgSoc,
                                       long dwellMs, long affectedNodes) {
        return new GridEvent(seq, System.currentTimeMillis(), EventKind.ZONE_TRANSITION,
                zone, from, to, trigger, null, 0, loadFactor, avgSoc, dwellMs, affectedNodes);
    }

    public static GridEvent transfer(long seq, EventKind kind, Zone source, Zone sink,
                                     double amountKw, double sinkDeficitKw, long dwellMs) {
        return new GridEvent(seq, System.currentTimeMillis(), kind,
                sink, null, null, null, source, amountKw, sinkDeficitKw, 0, dwellMs, 0);
    }
}