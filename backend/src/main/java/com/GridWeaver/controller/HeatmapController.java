package com.GridWeaver.controller;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.NodeType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 Per-node power for the heatmap overlay.

 Separate from the WebSocket delta stream on purpose: power changes on every
 frame (~10k/sec) while status changes rarely, so streaming power would be
 ~60x the traffic. The overlay polls this only while it is switched on.
 Returns flat triples [lat, lng, weight, ...] rather than objects -- 10k
 notepad tools\gen_nodes.pyobjects with named fields is ~1.5MB of JSON, the flat array is ~250KB.
 */
@RestController
@RequestMapping("/api/heatmap")
public class HeatmapController {

    private final NodeRegistry registry;

    public HeatmapController(NodeRegistry registry) {
        this.registry = registry;
    }

    /**
     * @param mode "generation" (solar + discharging batteries) or
     *             "consumption" (loads + charging batteries)
     * @param staleAfterMs nodes silent longer than this are excluded --
     *                     a dead node is not zero generation, it is unknown
     */
    @GetMapping
    public List<Double> heatmap(
            @RequestParam(defaultValue = "generation") String mode,
            @RequestParam(defaultValue = "5000") long staleAfterMs) {

        boolean generation = !"consumption".equalsIgnoreCase(mode);
        long cutoff = System.currentTimeMillis() - staleAfterMs;

        List<Double> flat = new ArrayList<>(3 * 6000);
        for (NodeState n : registry.all()) {
            if (n.lastSeen() < cutoff) continue;

            double power = n.powerKw();
            double weight = generation
                    ? (power > 0 ? power : 0)
                    : (power < 0 ? -power : 0);
            if (weight <= 0.01) continue;

            flat.add(n.lat());
            flat.add(n.lng());
            flat.add(weight);
        }
        return flat;
    }

    /** Max observed weight, so the frontend can scale colours consistently
     *  instead of renormalising every poll and making the map flicker. */
    @GetMapping("/scale")
    public double scale(@RequestParam(defaultValue = "generation") String mode) {
        boolean generation = !"consumption".equalsIgnoreCase(mode);
        double max = 0;
        for (NodeState n : registry.all()) {
            double p = n.powerKw();
            double w = generation ? (p > 0 ? p : 0) : (p < 0 ? -p : 0);
            max = Math.max(max, w);
        }
        return max;
    }
}