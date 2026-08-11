package com.GridWeaver.ingestion;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.TelemetryFrame;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Blocking TCP ingestion server, one virtual thread per connection.
 *
 * The blocking read is the point: a platform thread parked on readLine() costs
 * ~1MB of stack, so 50k connections would need ~50GB. A virtual thread parked
 * on the same call unmounts from its carrier and costs a few hundred bytes of
 * heap. Same code shape, three orders of magnitude less memory.
 *
 * Protocol (line-oriented, UTF-8):
 *   client -> HELLO|<nodeId>        server -> OK  | ERR|<reason>
 *   client -> T|<powerKw>|<soc>     server -> (silent)
 */
@Component
public class TelemetryIngestServer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestServer.class);

    private final NodeRegistry registry;
    private final ConnectionManager connections;

    @Value("${gridweaver.ingest.port:9099}")
    private int port;

    @Value("${gridweaver.ingest.backlog:8192}")
    private int backlog;

    private volatile boolean running;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    public TelemetryIngestServer(NodeRegistry registry, ConnectionManager connections) {
        this.registry = registry;
        this.connections = connections;
    }

    @PostConstruct
    public void start() throws Exception {
        serverSocket = new ServerSocket(port, backlog);
        running = true;

        // Accept loop gets its own virtual thread so @PostConstruct returns
        // and Spring finishes starting up.
        acceptThread = Thread.ofVirtual().name("ingest-accept").start(this::acceptLoop);

        log.info("Telemetry ingest listening on port {} (backlog {})", port, backlog);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                Thread.ofVirtual()
                        .name("ingest-conn-", connections.totalAccepted())
                        .start(() -> handle(socket));
            } catch (Exception e) {
                if (running) {
                    log.warn("accept failed: {}", e.toString());
                }
            }
        }
    }

    /** Runs on one virtual thread for the lifetime of the connection. */
    private void handle(Socket socket) {
        String nodeId = null;
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            socket.setTcpNoDelay(true);

            // --- handshake ---
            String hello = in.readLine();
            if (hello == null || !hello.startsWith("HELLO|")) {
                reply(socket, "ERR|expected HELLO\n");
                return;
            }
            nodeId = hello.substring(6);
            if (registry.get(nodeId) == null) {
                reply(socket, "ERR|unknown node\n");
                return;
            }
            reply(socket, "OK\n");
            connections.onConnect();

            // --- frame loop: blocks here for the life of the connection ---
            String line;
            while ((line = in.readLine()) != null) {
                try {
                    TelemetryFrame.parse(line, System.currentTimeMillis());
                    connections.onFrame();
                    // Day 4 wires this into registry.update(nodeId, ...)
                } catch (RuntimeException bad) {
                    connections.onBadFrame();
                }
            }
        } catch (Exception e) {
            if (running) {
                log.debug("connection {} dropped: {}", nodeId, e.toString());
            }
        } finally {
            if (nodeId != null) {
                connections.onDisconnect();
            }
        }
    }

    private void reply(Socket socket, String msg) throws Exception {
        OutputStream out = socket.getOutputStream();
        out.write(msg.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    @PreDestroy
    public void stop() throws Exception {
        running = false;
        if (serverSocket != null) serverSocket.close();
        if (acceptThread != null) acceptThread.join(2000);
        log.info("Telemetry ingest stopped. Peak connections: {}", connections.peak());
    }
}