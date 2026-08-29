package com.GridWeaver.ingestion;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.TelemetryFrame;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
  Identical protocol and logic to TelemetryIngestServer, but one PLATFORM
  thread per connection instead of one virtual thread.

  This exists solely for the mid-project audit. It is the control group: the
  blocking read pattern is the same, so any difference in memory or ceiling is
  attributable to the threading model and nothing else.

  The pool is unbounded (cached) on purpose. A fixed pool would simply queue
  connections and never fail, hiding the cost -- what we want to observe is the
  OS refusing to create more threads, and where that happens.
 */
@Component
@ConditionalOnProperty(name = "gridweaver.ingest.mode", havingValue = "platform")
public class PlatformThreadIngestServer {

    private static final Logger log = LoggerFactory.getLogger(PlatformThreadIngestServer.class);

    private final NodeRegistry registry;
    private final ConnectionManager connections;

    public PlatformThreadIngestServer(NodeRegistry registry, ConnectionManager connections) {
        this.registry = registry;
        this.connections = connections;
    }

    @Value("${gridweaver.ingest.port:9099}")     private int port;
    @Value("${gridweaver.ingest.backlog:8192}")  private int backlog;
    @Value("${gridweaver.ingest.acceptors:4}")   private int acceptors;

    private volatile boolean running;
    private ServerSocket serverSocket;
    private ExecutorService pool;
    private final AtomicInteger threadsCreated = new AtomicInteger();
    private final AtomicInteger creationFailures = new AtomicInteger();
    private final java.util.List<Thread> acceptThreads = new java.util.ArrayList<>();


    @PostConstruct
    public void start() throws Exception {
        serverSocket = new ServerSocket(port, backlog);
        running = true;

        // Cached pool: creates a new platform thread per task when none is idle.
        pool = new ThreadPoolExecutor(
                0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                r -> {
                    Thread t = new Thread(r, "plat-conn-" + threadsCreated.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                });

        for (int i = 0; i < acceptors; i++) {
            Thread a = new Thread(this::acceptLoop, "plat-accept-" + i);
            a.setDaemon(true);
            a.start();
            acceptThreads.add(a);
        }

        log.warn("PLATFORM THREAD MODE -- one OS thread per connection. Port {}", port);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                try {
                    pool.execute(() -> handle(socket));
                } catch (OutOfMemoryError | RuntimeException e) {
                    // The finding: the OS or JVM refuses to back another thread.
                    int n = creationFailures.incrementAndGet();
                    if (n == 1) {
                        log.error("THREAD CREATION FAILED at {} live connections ({} threads created): {}",
                                connections.active(), threadsCreated.get(), e.toString());
                    }
                    try { socket.close(); } catch (Exception ignored) { }
                }
            } catch (Exception e) {
                if (running) log.warn("accept failed: {}", e.toString());
            }
        }
    }

    /** Byte-for-byte the same handler as the virtual thread version. */
    private void handle(Socket socket) {
        String nodeId = null;
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            socket.setTcpNoDelay(true);

            String hello = in.readLine();
            if (hello == null || !hello.startsWith("HELLO|")) {
                reply(socket, "ERR|expected HELLO\n");
                return;
            }
            nodeId = hello.substring(6);
            NodeState node = registry.get(nodeId);
            if (node == null) {
                reply(socket, "ERR|unknown node\n");
                return;
            }
            reply(socket, "OK|" + node.type() + "\n");
            connections.onConnect();

            final String id = nodeId;
            String line;
            while ((line = in.readLine()) != null) {
                try {
                    TelemetryFrame f = TelemetryFrame.parse(line, System.currentTimeMillis());
                    registry.update(id, prev -> prev.withTelemetry(f.powerKw(), f.soc(), f.receivedAt()));
                    connections.onFrame();
                } catch (RuntimeException bad) {
                    connections.onBadFrame();
                }
            }
        } catch (Exception e) {
            if (running) log.debug("connection {} dropped: {}", nodeId, e.toString());
        } finally {
            if (nodeId != null) connections.onDisconnect();
        }
    }

    private void reply(Socket socket, String msg) throws Exception {
        OutputStream out = socket.getOutputStream();
        out.write(msg.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public int threadsCreated()   { return threadsCreated.get(); }
    public int creationFailures() { return creationFailures.get(); }

    @PreDestroy
    public void stop() throws Exception {
        running = false;
        if (serverSocket != null) serverSocket.close();
        if (pool != null) pool.shutdownNow();
        log.info("Platform ingest stopped. Peak: {}, threads created: {}, failures: {}",
                connections.peak(), threadsCreated.get(), creationFailures.get());
    }
}