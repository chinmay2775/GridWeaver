package com.GridWeaver.service;

import com.GridWeaver.model.Zone;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Consumes the telemetry topic and materialises zone history.
 *
 * Runs with concurrency 5, so each thread owns one partition and therefore one
 * zone. That is the payoff of the custom partitioner: a consumer thread sees a
 * single zone's rollups in strict order, with no cross-zone interleaving.
 *
 * Deliberately does NOT write back to the registry. The evaluator owns current
 * state; this owns history. Two writers to one store is how you get subtle
 * ordering bugs that only appear under load.
 */
@Service
@ConditionalOnProperty(name = "gridweaver.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class TelemetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    private final ZoneHistory history;
    private final JsonMapper json;

    private final LongAdder rollupsConsumed = new LongAdder();
    private final LongAdder eventsConsumed = new LongAdder();
    private final LongAdder malformed = new LongAdder();
    private final Map<Integer, String> partitionOwners = new ConcurrentHashMap<>();
    private volatile long lastLagMs;

    public TelemetryConsumer(ZoneHistory history, JsonMapper json) {
        this.history = history;
        this.json = json;
    }

    @KafkaListener(
            topics = "${gridweaver.kafka.topic:grid.telemetry}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> rec) {
        try {
            JsonNode n = json.readTree(rec.value());
            String kind = n.path("kind").asString();

            // Producer-to-consumer latency: how long the message sat on the topic.
            long ts = n.path("ts").asLong();
            if (ts > 0) lastLagMs = System.currentTimeMillis() - ts;

            partitionOwners.put(rec.partition(), Thread.currentThread().getName());

            if ("rollup".equals(kind)) {
                Zone zone = Zone.valueOf(n.path("zone").asString());
                history.append(zone, new ZoneHistory.Sample(
                        ts,
                        n.path("zoneStatus").asString(),
                        n.path("reporting").asInt(),
                        n.path("generationKw").asDouble(),
                        n.path("consumptionKw").asDouble(),
                        n.path("netKw").asDouble(),
                        n.path("avgBatterySoc").asDouble(),
                        n.path("loadFactor").asDouble()
                ));
                rollupsConsumed.increment();

            } else if ("transition".equals(kind)) {
                eventsConsumed.increment();
                log.debug("consumed transition: zone {} {} -> {}",
                        n.path("zone").asString(),
                        n.path("from").asString(),
                        n.path("to").asString());
            } else {
                malformed.increment();
            }
        } catch (Exception e) {
            // Swallow and count: one bad message must not stall the partition.
            malformed.increment();
            log.debug("malformed record at {}-{}: {}",
                    rec.topic(), rec.partition(), e.toString());
        }
    }

    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("rollupsConsumed", rollupsConsumed.sum());
        m.put("eventsConsumed", eventsConsumed.sum());
        m.put("malformed", malformed.sum());
        m.put("lastLagMs", lastLagMs);
        m.put("partitionThreads", new java.util.TreeMap<>(partitionOwners));
        m.put("historyDepth", history.depths());
        return m;
    }
}