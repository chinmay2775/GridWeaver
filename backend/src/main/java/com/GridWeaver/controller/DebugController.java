package com.GridWeaver.controller;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.ingestion.ConnectionManager;
import com.GridWeaver.ingestion.GridBroadcaster;
import com.GridWeaver.model.*;
import com.GridWeaver.service.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Operational and domain debug surface.
 *
 * Started as day-1 sanity endpoints; now also carries the concurrency-audit
 * numbers, Kafka health, zone balance and transfer plan.
 */
@RestController
@RequestMapping("/debug")
public class DebugController {

    private final NodeRegistry registry;
    private final ConnectionManager connections;
    private final ZoneAggregator aggregator;
    private final StateEvaluator evaluator;
    private final GridBroadcaster broadcaster;
    private final NodeIndex index;
    private final EventLog eventLog;
    private final TelemetryPublisher publisher;
    private final TelemetryConsumer consumer;
    private final ZoneHistory history;
    private final ConsumerLagMonitor lagMonitor;
    private final BalanceCalculator calculator;
    private final ZoneRebalancer rebalancer;
    private final TransferTracker tracker;

    @Value("${gridweaver.ingest.mode:virtual}")
    private String ingestMode;

    // ZoneStateMachine is deliberately NOT injected: it is not a Spring bean.
    // The five instances are constructed by hand inside StateEvaluator, and
    // their state is exposed through evaluator.zoneMachines().
    public DebugController(NodeRegistry registry,
                           ConnectionManager connections,
                           ZoneAggregator aggregator,
                           StateEvaluator evaluator,
                           GridBroadcaster broadcaster,
                           NodeIndex index,
                           EventLog eventLog,
                           TelemetryPublisher publisher,
                           TelemetryConsumer consumer,
                           ZoneHistory history,
                           ConsumerLagMonitor lagMonitor,
                           BalanceCalculator calculator,
                           ZoneRebalancer rebalancer,
                           TransferTracker tracker) {
        this.registry = registry;
        this.connections = connections;
        this.aggregator = aggregator;
        this.evaluator = evaluator;
        this.broadcaster = broadcaster;
        this.index = index;
        this.eventLog = eventLog;
        this.publisher = publisher;
        this.consumer = consumer;
        this.history = history;
        this.lagMonitor = lagMonitor;
        this.calculator = calculator;
        this.rebalancer = rebalancer;
        this.tracker = tracker;
    }

    // ---------- ingestion ----------

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodesLoaded", registry.size());
        m.put("byZone", registry.countsByZone());
        m.put("activeConnections", connections.active());
        m.put("peakConnections", connections.peak());
        m.put("totalAccepted", connections.totalAccepted());
        m.put("framesReceived", connections.framesReceived());
        m.put("framesRejected", connections.framesRejected());
        m.put("javaVersion", Runtime.version().toString());
        m.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        m.put("kafka", publisher.stats());
        return m;
    }

    @GetMapping("/threads")
    public Map<String, Object> threads() {
        java.lang.management.ThreadMXBean tmx =
                java.lang.management.ManagementFactory.getThreadMXBean();
        Runtime rt = Runtime.getRuntime();

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", ingestMode);
        // ThreadMXBean counts platform threads only -- virtual threads are
        // invisible to it, which is exactly the contrast we want to show.
        m.put("platformThreadsLive", tmx.getThreadCount());
        m.put("platformThreadsPeak", tmx.getPeakThreadCount());
        m.put("platformThreadsStarted", tmx.getTotalStartedThreadCount());
        m.put("activeConnections", connections.active());
        m.put("heapUsedMb", (rt.totalMemory() - rt.freeMemory()) / 1_048_576);
        m.put("heapMaxMb", rt.maxMemory() / 1_048_576);
        return m;
    }

    // ---------- domain state ----------

    @GetMapping("/nodes")
    public List<NodeState> nodes(@RequestParam(defaultValue = "20") int limit) {
        return registry.all().stream().limit(limit).toList();
    }

    @GetMapping("/zones")
    public Map<Zone, ZoneAggregator.ZoneSummary> zones(
            @RequestParam(defaultValue = "5000") long staleAfterMs) {
        return aggregator.summarise(staleAfterMs);
    }

    @GetMapping("/states")
    public Map<String, Object> states() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("byStatus", evaluator.statusCounts());
        m.put("tick", evaluator.tickStats());
        m.put("zoneMachines", evaluator.zoneMachines());
        m.put("broadcast", broadcaster.stats());
        return m;
    }

    @GetMapping("/index")
    public Map<String, Object> indexInfo() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("size", index.size());
        m.put("sample", index.size() > 0 ? index.idAt(0) : null);
        m.put("lookupTest", index.positionOf("zone-A/node-0000"));
        return m;
    }

    @GetMapping("/events")
    public Map<String, Object> events(
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(required = false) Zone zone,
            @RequestParam(required = false) ZoneStatus status,
            @RequestParam(required = false) EventKind kind) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total", eventLog.total());
        m.put("capacity", eventLog.capacity());
        m.put("activeRoutes", tracker.activeRoutes());
        m.put("events", eventLog.recent(limit, zone, status, kind));
        return m;
    }

    // ---------- kafka ----------

    @GetMapping("/history")
    public Map<String, Object> historyView(
            @RequestParam(required = false) Zone zone,
            @RequestParam(defaultValue = "60") int limit) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("capacity", history.capacity());
        m.put("consumer", consumer.stats());
        if (zone != null) {
            m.put("zone", zone);
            m.put("series", history.series(zone, limit));
        } else {
            Map<Zone, Object> all = new java.util.EnumMap<>(Zone.class);
            for (Zone z : Zone.values()) all.put(z, history.series(z, limit));
            m.put("series", all);
        }
        return m;
    }

    @GetMapping("/lag")
    public Map<String, Object> lag() {
        return lagMonitor.stats();
    }

    // ---------- balance and transfers ----------

    @GetMapping("/balance")
    public Map<String, Object> balance() {
        Map<String, Object> m = new LinkedHashMap<>();
        var balances = evaluator.balances();
        m.put("interconnectKw", calculator.interconnectKw());
        m.put("zones", balances);

        double totalExportable = balances.values().stream()
                .mapToDouble(ZoneBalance::exportableKw).sum();
        double totalDeficit = balances.values().stream()
                .mapToDouble(ZoneBalance::deficitKw).sum();

        m.put("totalExportableKw", Math.round(totalExportable * 100.0) / 100.0);
        m.put("totalDeficitKw", Math.round(totalDeficit * 100.0) / 100.0);
        // If deficit exceeds exportable, no amount of rebalancing fixes the
        // grid -- it needs more generation, not better distribution.
        m.put("gridCanSelfBalance", totalExportable >= totalDeficit);
        return m;
    }

    @GetMapping("/transfers")
    public Map<String, Object> transfers() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("plan", evaluator.transfers());
        m.put("stats", rebalancer.stats());
        return m;
    }
}