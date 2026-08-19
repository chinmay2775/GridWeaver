package com.GridWeaver.service;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class NodeIndex {

    private static final Logger log = LoggerFactory.getLogger(NodeIndex.class);

    private final NodeRegistry registry;
    private final Object lock = new Object();

    private volatile String[] ids;
    private volatile Map<String, Integer> positions;

    public NodeIndex(NodeRegistry registry) {
        this.registry = registry;
    }

    private void ensureBuilt() {
        if (ids != null) return;
        synchronized (lock) {
            if (ids != null) return;

            List<NodeState> snapshot = List.copyOf(registry.all());
            if (snapshot.isEmpty()) {
                throw new IllegalStateException("registry empty -- cannot build node index");
            }

            String[] built = new String[snapshot.size()];
            Map<String, Integer> pos = new HashMap<>(snapshot.size() * 2);
            for (int i = 0; i < snapshot.size(); i++) {
                built[i] = snapshot.get(i).nodeId();
                pos.put(built[i], i);
            }

            this.positions = Map.copyOf(pos);
            this.ids = built;                 // assigned last: ids != null means fully built
            log.info("Node index built: {} entries", built.length);
        }
    }

    public int size() {
        ensureBuilt();
        return ids.length;
    }

    /** @return array position, or -1 if unknown */
    public int positionOf(String nodeId) {
        ensureBuilt();
        return positions.getOrDefault(nodeId, -1);
    }

    public String idAt(int position) {
        ensureBuilt();
        return ids[position];
    }

    public List<String> orderedIds() {
        ensureBuilt();
        return List.of(ids);
    }
}