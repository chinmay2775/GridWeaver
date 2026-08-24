package com.GridWeaver.service;

import com.GridWeaver.model.GridEvent;
import com.GridWeaver.model.Zone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Publishes zone rollups and transition events to Kafka.
 *
  Keyed by zone name so all messages for a zone land on the same partition,
  preserving per-zone ordering. A consumer reading partition 2 sees zone C's
  history in order, which is what makes replay meaningful.

  Raw telemetry frames are deliberately NOT published: at ~10k frames/sec that
  is a firehose of data the registry already holds. What goes on the topic is
  what another service would want to replay -- aggregates and state changes.
 */
@Service
@ConditionalOnProperty(name = "gridweaver.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class TelemetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(TelemetryPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;

    @Value("${gridweaver.kafka.topic:grid.telemetry}")
    private String topic;

    @Value("${gridweaver.kafka.publish-every-n-ticks:4}")
    private int everyNTicks;

    private final LongAdder rollupsPublished = new LongAdder();
    private final LongAdder eventsPublished = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private volatile String lastError;

    public TelemetryPublisher(KafkaTemplate<String, String> kafka, JsonMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    /** True when this tick should emit rollups. At 4 ticks/sec and n=4 that is
     *  one rollup per zone per second -- enough resolution to replay, without
     *  producing 20 messages/sec of near-identical aggregates. */
    public boolean shouldPublish(long tick) {
        return everyNTicks > 0 && tick % everyNTicks == 0;
    }

    public void publishRollup(Zone zone, ZoneAggregator.ZoneSummary s,
                              com.GridWeaver.model.ZoneStatus zoneStatus, long tick) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("kind", "rollup");
        payload.put("tick", tick);
        payload.put("ts", System.currentTimeMillis());
        payload.put("zone", zone.name());
        payload.put("zoneStatus", zoneStatus.name());
        payload.put("reporting", s.reporting());
        payload.put("generationKw", s.generationKw());
        payload.put("consumptionKw", s.consumptionKw());
        payload.put("netKw", s.netKw());
        payload.put("avgBatterySoc", s.avgBatterySoc());
        payload.put("loadFactor", s.loadFactor());
        send(zone.name(), payload, rollupsPublished);
    }

    public void publishEvent(GridEvent e) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("kind", "transition");
        payload.put("seq", e.seq());
        payload.put("ts", e.ts());
        payload.put("zone", e.zone().name());
        payload.put("from", e.from().name());
        payload.put("to", e.to().name());
        payload.put("trigger", e.trigger().name());
        payload.put("loadFactor", e.loadFactor());
        payload.put("avgSoc", e.avgSoc());
        payload.put("dwellMs", e.dwellMs());
        payload.put("affectedNodes", e.affectedNodes());
        send(e.zone().name(), payload, eventsPublished);
    }

    private void send(String key, Map<String, Object> payload, LongAdder counter) {
        try {
            String body = json.writeValueAsString(payload);
            // Async: the callback runs on the producer's IO thread, so nothing
            // here blocks the evaluator tick.
            kafka.send(topic, key, body).whenComplete((result, ex) -> {
                if (ex != null) {
                    failures.increment();
                    lastError = ex.getClass().getSimpleName() + ": " + ex.getMessage();
                } else {
                    counter.increment();
                }
            });
        } catch (Exception e) {
            failures.increment();
            lastError = e.toString();
            log.debug("publish failed: {}", e.toString());
        }
    }

    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("topic", topic);
        m.put("rollupsPublished", rollupsPublished.sum());
        m.put("eventsPublished", eventsPublished.sum());
        m.put("failures", failures.sum());
        m.put("lastError", lastError);
        m.put("publishEveryNTicks", everyNTicks);
        return m;
    }
}