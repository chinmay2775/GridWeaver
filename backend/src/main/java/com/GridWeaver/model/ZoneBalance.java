package com.GridWeaver.model;

/** What a zone can give or needs, in kW.

  Distinguishes raw net power from *transferable* capacity. A zone with
  8,000 kW of net surplus cannot necessarily export 8,000 kW: some of that
  generation is already committed to charging local batteries, and the
  interconnect has a finite rating. Conflating the two is how you end up
  with a rebalancer that promises power it cannot move.

  Sign convention matches NodeState: positive is outbound. */
public record ZoneBalance(
        Zone zone,
        ZoneStatus status,
        double generationKw,
        double consumptionKw,
        double netKw,              // generation - consumption
        double storageHeadroomKw,  // how much batteries here could still absorb
        double storageReserveKw,   // how much batteries here could still supply
        double exportableKw,       // net surplus this zone can actually give away
        double deficitKw           // shortfall this zone cannot cover locally
) {

    public boolean isSurplus() { return exportableKw > 0; }
    public boolean isDeficit() { return deficitKw > 0; }

    /** Zones are a west-to-east line, so neighbours are ordinal ±1. A real
     grid would have an interconnect topology; this is the simplification
     that keeps week 4 tractable. */
    public boolean isAdjacentTo(Zone other) {
        return Math.abs(zone.ordinal() - other.ordinal()) == 1;
    }
}