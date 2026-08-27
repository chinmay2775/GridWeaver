package com.GridWeaver.config;

import com.GridWeaver.ingestion.ConnectionManager;
import com.GridWeaver.service.ConsumerLagMonitor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
  Turns the metrics into a single up/down verdict for /actuator/health.

  Degraded rather than down when lag is high: the system is still serving,
  just falling behind. Reserving DOWN for genuine unavailability keeps the
  signal useful.
 */
@Component
public class GridHealthIndicator implements HealthIndicator {

    private final ConsumerLagMonitor lag;
    private final ConnectionManager connections;
    private final long lagThreshold;

    public GridHealthIndicator(ConsumerLagMonitor lag,
                               ConnectionManager connections,
                               @Value("${gridweaver.kafka.lag-warn-threshold:500}") long lagThreshold) {
        this.lag = lag;
        this.connections = connections;
        this.lagThreshold = lagThreshold;
    }

    @Override
    public Health health() {
        long total = lag.totalLag();
        var builder = total > lagThreshold ? Health.status("DEGRADED") : Health.up();
        return builder
                .withDetail("activeConnections", connections.active())
                .withDetail("framesReceived", connections.framesReceived())
                .withDetail("consumerLag", total)
                .withDetail("lagThreshold", lagThreshold)
                .build();
    }
}