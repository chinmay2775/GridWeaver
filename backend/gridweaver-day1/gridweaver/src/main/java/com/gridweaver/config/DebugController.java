package com.gridweaver.config;

import com.gridweaver.model.NodeState;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Day 1 sanity endpoints. /debug/stats grows into the concurrency-audit
 * surface used at the mid-project review.
 */
@RestController
@RequestMapping("/debug")
public class DebugController {

    private final NodeRegistry registry;

    public DebugController(NodeRegistry registry) {
        this.registry = registry;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodesLoaded", registry.size());
        m.put("byZone", registry.countsByZone());
        m.put("activeConnections", 0);   // wired up on Day 2
        m.put("javaVersion", Runtime.version().toString());
        m.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        return m;
    }

    @GetMapping("/nodes")
    public List<NodeState> nodes(@RequestParam(defaultValue = "20") int limit) {
        return registry.all().stream().limit(limit).toList();
    }
}
