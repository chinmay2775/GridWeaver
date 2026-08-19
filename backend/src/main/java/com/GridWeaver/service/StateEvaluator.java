package com.GridWeaver.service;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.ingestion.GridBroadcaster;
import com.GridWeaver.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Applies the transition table to every node on a fixed tick.
 *
 * Two full registry scans per tick: one for zone aggregates, one to apply
 * transitions. At 10k nodes and 4 ticks/sec that is ~80k record reads per
 * second -- negligible next to the ~10k frames/sec the ingest path handles.
 */
@Service
public class StateEvaluator {

    private static final Logger log = LoggerFactory.getLogger(StateEvaluator.class);

    private final NodeRegistry registry;
    private final ZoneAggregator aggregator;

    private final Map<NodeStatus, LongAdder> transitionsInto = new EnumMap<>(NodeStatus.class);
    private final AtomicLong ticks = new AtomicLong();
    private final AtomicLong lastTickMicros = new AtomicLong();
    private final AtomicLong lastTransitions = new AtomicLong();
    private final Map<Zone, ZoneStateMachine> machines = new EnumMap<>(Zone.class);
    private final GridBroadcaster broadcaster;
    private final NodeIndex index;

    @Value("${gridweaver.rules.discharge-above-load:0.80}") private double dischargeAbove;
    @Value("${gridweaver.rules.charge-below-load:0.40}")    private double chargeBelow;
    @Value("${gridweaver.rules.soc-floor:0.20}")            private double socFloor;
    @Value("${gridweaver.rules.soc-ceiling:0.95}")          private double socCeiling;
    @Value("${gridweaver.rules.idle-band-kw:0.10}")         private double idleBand;
    @Value("${gridweaver.rules.stale-after-ms:5000}")       private long staleAfterMs;
    @Value("${gridweaver.zone.stressed-enter:0.80}")   private double stressedEnter;
    @Value("${gridweaver.zone.stressed-exit:0.70}")    private double stressedExit;
    @Value("${gridweaver.zone.surplus-enter:0.40}")    private double surplusEnter;
    @Value("${gridweaver.zone.surplus-exit:0.50}")     private double surplusExit;
    @Value("${gridweaver.zone.reserve-floor:0.15}")    private double reserveFloor;
    @Value("${gridweaver.zone.reserve-restore:0.25}")  private double reserveRestore;


    public StateEvaluator(NodeRegistry registry, ZoneAggregator aggregator,NodeIndex index, GridBroadcaster broadcaster) {
        this.registry = registry;
        this.aggregator = aggregator;
        this.index = index;
        this.broadcaster = broadcaster;
        for (NodeStatus s : NodeStatus.values()) {
            transitionsInto.put(s, new LongAdder());
        }
        for (Zone z : Zone.values()) {
            machines.put(z, new ZoneStateMachine());
        }
    }

    private TransitionRules.Thresholds thresholds() {
        return new TransitionRules.Thresholds(
                dischargeAbove, chargeBelow, socFloor, socCeiling, idleBand, staleAfterMs);
    }

    @Scheduled(fixedRateString = "${gridweaver.rules.tick-ms:250}")
    public void evaluate() {
        long t0 = System.nanoTime();
        long now = System.currentTimeMillis();
        var t = thresholds();
        var bands = new ZoneStateMachine.Bands(
                stressedEnter, stressedExit, surplusEnter, surplusExit,
                reserveFloor, reserveRestore);

        // Pass 1: aggregate, then step each zone machine.
        var summaries = aggregator.summarise(staleAfterMs);
        Map<Zone, ZoneStatus> policy = new EnumMap<>(Zone.class);

        summaries.forEach((z, s) -> {
            ZoneStateMachine m = machines.get(z);
            ZoneStatus moved = m.fire(s.loadFactor(), s.avgBatterySoc(), now, bands);
            if (moved != null) {
                log.info("zone {} -> {} (load {}, soc {})",
                        z, moved, s.loadFactor(), s.avgBatterySoc());
            }
            policy.put(z, m.state());
        });

        // Pass 2: apply zone policy, collecting deltas as we go.
        // Sized generously from the observed ~160 transitions/tick.
        int[] deltas = new int[2048];
        int d = 0;
        long changed = 0;

        for (NodeState node : registry.all()) {
            NodeStatus want = TransitionRules.next(
                    node, policy.getOrDefault(node.zone(), ZoneStatus.NOMINAL), now, t);

            if (want != node.status()) {
                registry.update(node.nodeId(), prev ->
                        prev.status() == want ? prev : prev.withStatus(want));
                transitionsInto.get(want).increment();
                changed++;

                if (d + 2 <= deltas.length) {
                    int pos = index.positionOf(node.nodeId());
                    if (pos >= 0) {
                        deltas[d++] = pos;
                        deltas[d++] = want.ordinal();
                    }
                }
            }
        }

        long tick = ticks.incrementAndGet();
        lastTransitions.set(changed);
        lastTickMicros.set((System.nanoTime() - t0) / 1_000);

        // Broadcast on a virtual thread so a slow client cannot delay the next tick.
        int[] payload = java.util.Arrays.copyOf(deltas, d);
        Thread.ofVirtual().start(() -> broadcaster.publish(tick, payload));
    }

    // --- exposed for /debug ---

    public Map<NodeStatus, Long> statusCounts() {
        Map<NodeStatus, Long> out = new EnumMap<>(NodeStatus.class);
        for (NodeStatus s : NodeStatus.values()) out.put(s, 0L);
        for (NodeState n : registry.all()) out.merge(n.status(), 1L, Long::sum);
        return out;
    }

    public Map<String, Object> tickStats() {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("ticks", ticks.get());
        m.put("lastTickMicros", lastTickMicros.get());
        m.put("lastTickTransitions", lastTransitions.get());
        Map<NodeStatus, Long> into = new EnumMap<>(NodeStatus.class);
        transitionsInto.forEach((s, a) -> into.put(s, a.sum()));
        m.put("cumulativeTransitionsInto", into);
        m.put("thresholds", thresholds());
        return m;
    }

    public Map<Zone, Map<String, Object>> zoneMachines() {
        long now = System.currentTimeMillis();
        Map<Zone, Map<String, Object>> out = new EnumMap<>(Zone.class);
        machines.forEach((z, m) -> {
            Map<String, Object> v = new java.util.LinkedHashMap<>();
            v.put("state", m.state());
            v.put("dwellMs", m.dwellMs(now));
            v.put("transitions", m.transitionCount());
            out.put(z, v);
        });
        return out;
    }
}