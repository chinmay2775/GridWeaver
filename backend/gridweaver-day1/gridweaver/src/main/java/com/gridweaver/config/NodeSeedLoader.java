package com.gridweaver.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gridweaver.model.NodeSeed;
import com.gridweaver.model.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.List;

/**
 * Loads static topology from nodes.json at startup and pre-populates the registry.
 * Runs once, before any connections are accepted.
 */
@Component
public class NodeSeedLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(NodeSeedLoader.class);

    private final NodeRegistry registry;
    private final ObjectMapper mapper;

    public NodeSeedLoader(NodeRegistry registry, ObjectMapper mapper) {
        this.registry = registry;
        this.mapper = mapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        long t0 = System.nanoTime();
        try (InputStream in = new ClassPathResource("nodes.json").getInputStream()) {
            List<NodeSeed> seeds = mapper.readValue(in, new TypeReference<>() {});
            for (NodeSeed s : seeds) {
                registry.put(NodeState.initial(s));
            }
        }
        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("Seeded {} nodes in {} ms | by zone: {}",
                registry.size(), ms, registry.countsByZone());
    }
}
