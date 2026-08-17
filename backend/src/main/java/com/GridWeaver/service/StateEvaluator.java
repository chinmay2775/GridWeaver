package com.GridWeaver.service;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.NodeStatus;
import com.GridWeaver.model.TransitionRules;
import com.GridWeaver.model.Zone;
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

    @Value("${gridweaver.rules.discharge-above-load:0.80}") private double dischargeAbove;
    @Value("${gridweaver.rules.charge-below-load:0.40}")    private double chargeBelow;
    @Value("${gridweaver.rules.soc-floor:0.20}")            private double socFloor;
    @Value("${gridweaver.rules.soc-ceiling:0.95}")          private double socCeiling;
    @Value("${gridweaver.rules.idle-band-kw:0.10}")         private double idleBand;
    @Value("${gridweaver.rules.stale-after-ms:5000}")       private long staleAfterMs;

    public StateEvaluator(NodeRegistry registry, ZoneAggregator aggregator) {
        this.registry = registry;
        this.aggregator = aggregator;
        for (NodeStatus s : NodeStatus.values()) {
            transitionsInto.put(s, new LongAdder());
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

        // One aggregate pass, reused for every node in the zone.
        Map<Zone, Double> load = new EnumMap<>(Zone.class);
        aggregator.summarise(staleAfterMs)
                .forEach((z, s) -> load.put(z, s.loadFactor()));

        long changed = 0;
        for (NodeState node : registry.all()) {
            NodeStatus want = TransitionRules.next(
                    node, load.getOrDefault(node.zone(), 0.0), now, t);

            // Only write when the status actually differs. Without this guard we
            // would allocate 10k replacement records every tick for no reason.
            if (want != node.status()) {
                registry.update(node.nodeId(), prev ->
                        prev.status() == want ? prev : prev.withStatus(want));
                transitionsInto.get(want).increment();
                changed++;
            }
        }

        ticks.incrementAndGet();
        lastTransitions.set(changed);
        lastTickMicros.set((System.nanoTime() - t0) / 1_000);

        if (changed > 0 && log.isDebugEnabled()) {
            log.debug("tick {}: {} transitions in {} us", ticks.get(), changed, lastTickMicros.get());
        }
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
}