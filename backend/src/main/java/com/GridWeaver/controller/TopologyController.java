package com.GridWeaver.controller;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/topology")
@CrossOrigin(origins = "http://localhost:5173")
public class TopologyController {

    private final NodeRegistry registry;

    public TopologyController(NodeRegistry registry) {
        this.registry = registry;
    }

    @GetMapping
    public List<Object[]> topology() {
        return registry.all().stream()
                .map(n -> new Object[]{
                        n.nodeId(),
                        n.lat(),
                        n.lng(),
                        n.type().ordinal()
                })
                .toList();
    }
}