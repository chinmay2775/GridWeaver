package com.GridWeaver.ingestion;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.Zone;
import com.GridWeaver.model.ZoneStatus;
import com.GridWeaver.service.NodeIndex;
import com.GridWeaver.service.ZoneAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
  Outbound WebSocket to the dashboard. Two message kinds:

    snapshot -- full status array, sent once when a client connects
    delta    -- only nodes whose status changed on the last tick, plus the
                five zone summaries (which are small enough to send whole)

  Node power values are deliberately NOT broadcast. They change on every frame
   (~10k/sec) but only the status drives what the map renders, so streaming
  power would be ~60x the traffic for no visible difference.
 */
@Component
public class GridBroadcaster extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(GridBroadcaster.class);

    /** Per-session send buffer. WebSocketSession.sendMessage is not thread-safe,
       and a slow client must not stall the broadcast thread -- the decorator
       buffers and closes the session if it exceeds the limit. */

    private static final int SEND_BUFFER_BYTES = 512 * 1024;
    private static final int SEND_TIME_LIMIT_MS = 5_000;

    private final NodeRegistry registry;
    private final NodeIndex index;
    private final ZoneAggregator aggregator;
    private final JsonMapper json;

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder deltasSent = new LongAdder();
    private volatile int lastPayloadBytes;

    private final java.util.concurrent.ConcurrentLinkedQueue<Object> pendingEvents =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

    public GridBroadcaster(NodeRegistry registry, NodeIndex index,
                           ZoneAggregator aggregator, JsonMapper json) {
        this.registry = registry;
        this.index = index;
        this.aggregator = aggregator;
        this.json = json;
    }

    // ---------- lifecycle ----------

    @Override
    public void afterConnectionEstablished(WebSocketSession raw) {
        WebSocketSession session = new ConcurrentWebSocketSessionDecorator(
                raw, SEND_TIME_LIMIT_MS, SEND_BUFFER_BYTES);
        sessions.put(session.getId(), session);
        log.info("dashboard connected: {} ({} total)", session.getId(), sessions.size());
        sendSnapshot(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
        log.info("dashboard disconnected: {} ({})", session.getId(), status);
    }

    // ---------- outbound ----------

    /** Full state for a newly connected client, in node-index order. */

    private void sendSnapshot(WebSocketSession session) {
        try {
            int[] statuses = new int[index.size()];
            for (int i = 0; i < statuses.length; i++) {
                NodeState n = registry.get(index.idAt(i));
                statuses[i] = n == null ? 0 : n.status().ordinal();
            }

            var payload = Map.of(
                    "type", "snapshot",
                    "ts", System.currentTimeMillis(),
                    "s", statuses,
                    "zones", zonePayload()
            );
            session.sendMessage(new TextMessage(json.writeValueAsString(payload)));
            messagesSent.increment();
        } catch (Exception e) {
            log.warn("snapshot failed for {}: {}", session.getId(), e.toString());
        }
    }

    /**
      Called at the end of each evaluator tick.

      @param deltas flat pairs: [index0, ordinal0, index1, ordinal1, ...].
                    Flat int array rather than a list of objects -- at 160
                    entries per tick, 4 ticks/sec, object allocation adds up.
     */

    public void publish(long tick, int[] deltas) {
        if (sessions.isEmpty()) return;
        List<Object> events = new ArrayList<>();
        for (Object o = pendingEvents.poll(); o != null; o = pendingEvents.poll()) {
            events.add(o);
        }
        String message;
        try {
            var payload = Map.of(
                    "type", "delta",
                    "t", tick,
                    "ts", System.currentTimeMillis(),
                    "d", deltas,
                    "zones", zonePayload()
            );
            message = json.writeValueAsString(payload);
        } catch (Exception e) {
            log.warn("delta serialisation failed: {}", e.toString());
            return;
        }

        lastPayloadBytes = message.length();
        TextMessage frame = new TextMessage(message);

        for (WebSocketSession s : sessions.values()) {
            if (!s.isOpen()) { sessions.remove(s.getId()); continue; }
            try {
                s.sendMessage(frame);
                messagesSent.increment();
            } catch (Exception e) {
                log.debug("send failed for {}, dropping: {}", s.getId(), e.toString());
                sessions.remove(s.getId());
                try { s.close(); } catch (Exception ignored) { }
            }
        }
        deltasSent.add(deltas.length / 2);
    }

    private List<Map<String, Object>> zonePayload() {
        var summaries = aggregator.summarise(5_000);
        return summaries.entrySet().stream()
                .map(e -> {
                    var s = e.getValue();
                    return Map.<String, Object>of(
                            "z", e.getKey().name(),
                            "gen", s.generationKw(),
                            "con", s.consumptionKw(),
                            "net", s.netKw(),
                            "soc", s.avgBatterySoc(),
                            "lf", s.loadFactor(),
                            "rep", s.reporting()
                    );
                })
                .toList();
    }

    // ---------- for /debug ----------

    public Map<String, Object> stats() {
        return Map.of(
                "clients", sessions.size(),
                "messagesSent", messagesSent.sum(),
                "deltasSent", deltasSent.sum(),
                "lastPayloadBytes", lastPayloadBytes
        );
    }

    /** Queued rather than sent immediately, so events ride along with the next
     *  delta instead of causing an extra frame per transition. */
    public void queueEvent(com.GridWeaver.model.GridEvent e) {
        pendingEvents.add(Map.of(
                "seq", e.seq(),
                "ts", e.ts(),
                "zone", e.zone().name(),
                "from", e.from().name(),
                "to", e.to().name(),
                "trigger", e.trigger().name(),
                "lf", e.loadFactor(),
                "soc", e.avgSoc(),
                "dwellMs", e.dwellMs(),
                "affected", e.affectedNodes()
        ));
        // Guard against unbounded growth if no client is connected.
        while (pendingEvents.size() > 200) pendingEvents.poll();
    }
}