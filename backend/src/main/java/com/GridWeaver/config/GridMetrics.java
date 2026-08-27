package com.GridWeaver.config;

import com.GridWeaver.ingestion.ConnectionManager;
import com.GridWeaver.ingestion.GridBroadcaster;
import com.GridWeaver.service.ConsumerLagMonitor;
import com.GridWeaver.service.StateEvaluator;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
  Registers the numbers that matter as Micrometer gauges, so they appear on
  /actuator/metrics alongside JVM and HTTP metrics rather than only on the
  hand-rolled /debug endpoints.

  Gauges rather than counters: every value here is a current reading that can
  go down. Counters are for monotonic totals.
 */
@Component
public class GridMetrics {

    private final MeterRegistry registry;
    private final ConnectionManager connections;
    private final StateEvaluator evaluator;
    private final GridBroadcaster broadcaster;
    private final ConsumerLagMonitor lag;

    public GridMetrics(MeterRegistry registry,
                       ConnectionManager connections,
                       StateEvaluator evaluator,
                       GridBroadcaster broadcaster,
                       ConsumerLagMonitor lag) {
        this.registry = registry;
        this.connections = connections;
        this.evaluator = evaluator;
        this.broadcaster = broadcaster;
        this.lag = lag;
    }

    @PostConstruct
    public void register() {
        Gauge.builder("gridweaver.connections.active", connections, ConnectionManager::active)
                .description("Live IoT connections")
                .register(registry);

        Gauge.builder("gridweaver.frames.received", connections, ConnectionManager::framesReceived)
                .description("Telemetry frames parsed")
                .register(registry);

        Gauge.builder("gridweaver.frames.rejected", connections, ConnectionManager::framesRejected)
                .description("Frames failing validation")
                .register(registry);

        Gauge.builder("gridweaver.tick.duration.micros", evaluator,
                        e -> (double) (long) e.tickStats().get("lastTickMicros"))
                .description("Evaluator tick duration")
                .register(registry);

        Gauge.builder("gridweaver.broadcast.clients", broadcaster,
                        b -> (double) (int) b.stats().get("clients"))
                .description("Connected dashboards")
                .register(registry);

        Gauge.builder("gridweaver.kafka.consumer.lag", lag, ConsumerLagMonitor::totalLag)
                .description("Total consumer group lag across partitions")
                .register(registry);

        Gauge.builder("gridweaver.kafka.consumer.lag.max", lag, ConsumerLagMonitor::maxLag)
                .description("Worst single-partition lag")
                .register(registry);
    }
}