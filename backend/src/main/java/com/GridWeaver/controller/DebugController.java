package com.GridWeaver.controller;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.ingestion.ConnectionManager;
import com.GridWeaver.model.NodeState;
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
    public DebugController(NodeRegistry registry, ConnectionManager connections) {
        this.registry = registry;
        this.connections = connections;
    }

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
}