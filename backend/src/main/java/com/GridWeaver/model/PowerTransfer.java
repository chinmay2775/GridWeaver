package com.GridWeaver.model;

/** One scheduled power movement between adjacent zones.

  Immutable and recomputed every tick rather than persisted: a transfer is a
  standing instruction valid only for current conditions, not a transaction.
  If zone E's deficit closes, the transfer simply is not recomputed next tick. */

public record PowerTransfer(
        Zone from,
        Zone to,
        double amountKw,
        double fromExportableKw,   // what the source had available
        double toDeficitKw,        // what the sink needed
        long ts
) {
    /** Fraction of the sink's need this transfer covers. */
    public double coverage() {
        return toDeficitKw <= 0 ? 1.0 : Math.min(1.0, amountKw / toDeficitKw);
    }
}