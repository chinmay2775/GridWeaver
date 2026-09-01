package com.GridWeaver.service;

import com.GridWeaver.model.PowerTransfer;
import com.GridWeaver.model.Zone;
import com.GridWeaver.model.ZoneBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** Matches exportable zones to deficit zones across adjacent interconnects.

  Greedy nearest-first, not optimal. Zones form a west-to-east line, so a
  deficit zone draws from its immediate neighbours before anything further
  away; power routed A->E has to physically traverse B, C and D, consuming
  interconnect capacity at each hop. Solving that properly is a min-cost flow
  problem, which is overkill for five zones and would obscure the mechanism
  this project is actually demonstrating.

  Recomputed from scratch each tick. No transfer state carries forward: if
  conditions change, the plan simply differs next tick. */

@Service
public class ZoneRebalancer {

    private static final Logger log = LoggerFactory.getLogger(ZoneRebalancer.class);

    @Value("${gridweaver.balance.min-transfer-kw:50}")
    private double minTransferKw;

    @Value("${gridweaver.balance.max-hops:2}")
    private int maxHops;

    private final LongAdder plansComputed = new LongAdder();
    private final LongAdder transfersScheduled = new LongAdder();
    private volatile List<PowerTransfer> lastPlan = List.of();
    private volatile double lastUnmetKw;

    /** @param balances current per-zone balance
      @return transfers to execute this tick, source-ordered */

    public List<PowerTransfer> plan(Map<Zone, ZoneBalance> balances) {
        long now = System.currentTimeMillis();
        plansComputed.increment();

        // Working copies -- we decrement as capacity is committed.
        Map<Zone, Double> available = new EnumMap<>(Zone.class);
        Map<Zone, Double> needed = new EnumMap<>(Zone.class);
        balances.forEach((z, b) -> {
            if (b.exportableKw() > 0) available.put(z, b.exportableKw());
            if (b.deficitKw() > 0) needed.put(z, b.deficitKw());
        });

        List<PowerTransfer> plan = new ArrayList<>();

        // Largest deficit first: if capacity runs short, the zone in most
        // trouble should be served before a marginal one.
        List<Zone> sinks = new ArrayList<>(needed.keySet());
        sinks.sort(Comparator.comparingDouble((Zone z) -> needed.get(z)).reversed());

        for (Zone sink : sinks) {
            double remaining = needed.get(sink);

            for (int hop = 1; hop <= maxHops && remaining >= minTransferKw; hop++) {
                for (Zone source : sourcesAtDistance(sink, hop, available)) {
                    double canGive = available.getOrDefault(source, 0.0);
                    if (canGive < minTransferKw) continue;

                    double amount = Math.min(canGive, remaining);
                    if (amount < minTransferKw) continue;

                    plan.add(new PowerTransfer(
                            source, sink, round(amount),
                            round(balances.get(source).exportableKw()),
                            round(balances.get(sink).deficitKw()),
                            now));

                    available.put(source, canGive - amount);
                    remaining -= amount;
                    transfersScheduled.increment();

                    if (remaining < minTransferKw) break;
                }
            }
            needed.put(sink, remaining);
        }

        lastUnmetKw = round(needed.values().stream().mapToDouble(Double::doubleValue).sum());
        lastPlan = List.copyOf(plan);

        if (!plan.isEmpty() && log.isDebugEnabled()) {
            log.debug("rebalance: {} transfers, {} kW unmet", plan.size(), lastUnmetKw);
        }
        return lastPlan;
    }

    /** Zones exactly `hop` positions away that still have capacity, nearer
        side first. Ordinal distance stands in for interconnect topology. */

    private List<Zone> sourcesAtDistance(Zone sink, int hop, Map<Zone, Double> available) {
        List<Zone> out = new ArrayList<>(2);
        int i = sink.ordinal();
        for (int delta : new int[]{-hop, hop}) {
            int j = i + delta;
            if (j < 0 || j >= Zone.values().length) continue;
            Zone candidate = Zone.values()[j];
            if (available.getOrDefault(candidate, 0.0) >= minTransferKw) {
                out.add(candidate);
            }
        }
        return out;
    }

    private static double round(double d) {
        return Math.round(d * 100.0) / 100.0;
    }

    public List<PowerTransfer> lastPlan() { return lastPlan; }

    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("plansComputed", plansComputed.sum());
        m.put("transfersScheduled", transfersScheduled.sum());
        m.put("activeTransfers", lastPlan.size());
        m.put("unmetDeficitKw", lastUnmetKw);
        m.put("minTransferKw", minTransferKw);
        m.put("maxHops", maxHops);
        return m;
    }
}