package com.GridWeaver.controller;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.ingestion.ConnectionManager;
import com.GridWeaver.ingestion.GridBroadcaster;
import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.Zone;
import com.GridWeaver.model.ZoneStatus;
import com.GridWeaver.service.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CheckedInputStream;

/**
 * Day 1 sanity endpoints. /debug/stats grows into the concurrency-audit
 * surface used at the mid-project review.
 */
@RestController
@RequestMapping("/debug")
public class DebugController {

    private final NodeRegistry registry;
    private final ConnectionManager connections;
    private final ZoneAggregator aggregator;
    private final StateEvaluator evaluator;
    private final ZoneStateMachine zoneMachines;
    private final GridBroadcaster broadcaster;
    private final NodeIndex index;
    private final EventLog eventLog;

    public DebugController(NodeRegistry registry, ConnectionManager connections, ZoneAggregator aggregator, StateEvaluator evaluator, ZoneStateMachine zoneMachines, GridBroadcaster broadcaster, NodeIndex index, EventLog eventLog) {
        this.registry = registry;
        this.connections = connections;
        this.aggregator = aggregator;
        this.evaluator = evaluator;
        this.zoneMachines = zoneMachines;
        this.broadcaster = broadcaster;
        this.index = index;
        this.eventLog = eventLog;

    }

    @Value("${gridweaver.ingest.mode:virtual}")
    private String ingestMode;

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
        return m;
    }

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
            @RequestParam(required = false) ZoneStatus status) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total", eventLog.total());
        m.put("capacity", eventLog.capacity());
        m.put("events", eventLog.recent(limit, zone, status));
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
}