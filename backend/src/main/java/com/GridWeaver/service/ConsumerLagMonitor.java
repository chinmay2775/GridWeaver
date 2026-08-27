package com.GridWeaver.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
  Polls the broker for committed offsets vs end offsets, per partition.

  Lag is the single number that tells you whether a consumer is keeping up.
  It cannot be measured from inside the consumer -- the consumer knows what it
  has read, not what exists. Only the broker knows both, so this uses AdminClient.

  Polled on a slow tick (5s) because each call is a broker round-trip. Lag is a
  trend metric; sampling it four times a second would cost more than it tells you.
 */
@Service
@ConditionalOnProperty(name = "gridweaver.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class ConsumerLagMonitor {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagMonitor.class);
    private final LongAdder bufferFull = new LongAdder();
    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final long warnThreshold;

    private volatile AdminClient admin;
    private volatile Map<Integer, Long> lagByPartition = Map.of();
    private volatile long totalLag;
    private volatile long maxLag;
    private volatile long lastPollMs;
    private volatile String lastError;

    public ConsumerLagMonitor(
            @Value("${gridweaver.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${gridweaver.kafka.group-id:gridweaver-history}") String groupId,
            @Value("${gridweaver.kafka.topic:grid.telemetry}") String topic,
            @Value("${gridweaver.kafka.lag-warn-threshold:500}") long warnThreshold) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.warnThreshold = warnThreshold;
    }

    /** Created lazily so a broker that is down at startup does not block boot. */
    private AdminClient admin() {
        AdminClient a = admin;
        if (a == null) {
            synchronized (this) {
                if (admin == null) {
                    Map<String, Object> props = new HashMap<>();
                    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
                    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
                    admin = AdminClient.create(props);
                }
                a = admin;
            }
        }
        return a;
    }

    @Scheduled(fixedRateString = "${gridweaver.kafka.lag-poll-ms:5000}")
    public void poll() {
        long t0 = System.currentTimeMillis();
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin()
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);

            if (committed.isEmpty()) {
                lagByPartition = Map.of();
                totalLag = 0;
                lastError = "no committed offsets yet";
                return;
            }

            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            committed.keySet().stream()
                    .filter(tp -> tp.topic().equals(topic))
                    .forEach(tp -> request.put(tp, OffsetSpec.latest()));

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin().listOffsets(request).all().get(5, TimeUnit.SECONDS);

            Map<Integer, Long> lags = new TreeMap<>();
            long total = 0, max = 0;
            for (var e : ends.entrySet()) {
                TopicPartition tp = e.getKey();
                long end = e.getValue().offset();
                OffsetAndMetadata c = committed.get(tp);
                long lag = c == null ? end : Math.max(0, end - c.offset());
                lags.put(tp.partition(), lag);
                total += lag;
                max = Math.max(max, lag);
            }

            lagByPartition = lags;
            totalLag = total;
            maxLag = max;
            lastError = null;

            if (max > warnThreshold) {
                log.warn("consumer lag {} on a partition exceeds threshold {}", max, warnThreshold);
            }
        } catch (Exception e) {
            lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
            log.debug("lag poll failed: {}", lastError);
        } finally {
            lastPollMs = System.currentTimeMillis() - t0;
        }
    }

    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("group", groupId);
        m.put("totalLag", totalLag);
        m.put("maxPartitionLag", maxLag);
        m.put("lagByPartition", lagByPartition);
        m.put("warnThreshold", warnThreshold);
        m.put("pollDurationMs", lastPollMs);
        m.put("lastError", lastError);
        m.put("bufferFullDrops", bufferFull.sum());

        return m;
    }

    public long totalLag() { return totalLag; }
    public long maxLag()   { return maxLag; }
}