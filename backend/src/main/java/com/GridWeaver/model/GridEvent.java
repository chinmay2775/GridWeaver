package com.GridWeaver.model;

/**
 * One auditable grid event. Captures not just what changed but why --
 * the load factor and reserve level that triggered it, plus how long the
 * zone had been in its previous state.
 *
 * Recording the cause at capture time matters: reconstructing "why did zone C
 * go critical at 14:32" from telemetry history afterwards is guesswork, because
 * the thresholds that drove the decision are config and may have changed.
 */
public record GridEvent(
        long seq,
        long ts,
        Zone zone,
        ZoneStatus from,
        ZoneStatus to,
        ZoneEvent trigger,
        double loadFactor,
        double avgSoc,
        long dwellMs,
        long affectedNodes
) {}