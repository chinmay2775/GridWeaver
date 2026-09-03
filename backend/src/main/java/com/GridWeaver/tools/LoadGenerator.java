package com.GridWeaver.tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
  Mock IoT fleet. Opens N connections, each on its own virtual thread, and
  sends a telemetry frame every intervalMs.

  Reconnects on failure with exponential backoff. Without that, a backend
  restart kills the whole fleet and every test run needs both processes
  restarted in the right order.

  Usage: LoadGenerator [count] [host] [port] [intervalMs]
    -Dgw.sourceIps=127.0.0.1,127.0.0.2,...  spread source addresses so each
                                             gets its own ephemeral port pool*/
public class LoadGenerator {

    private static final Map<String, LongAdder> failures = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        int count      = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        String host    = args.length > 1 ? args[1] : "127.0.0.1";
        int port       = args.length > 2 ? Integer.parseInt(args[2]) : 9099;
        int intervalMs = args.length > 3 ? Integer.parseInt(args[3]) : 1000;

        // One client->server IP pair shares a single ephemeral port pool, which
        // caps out around 55k regardless of available memory.
        String[] sourceIps = System.getProperty("gw.sourceIps", "").isEmpty()
                ? new String[]{ null }
                : System.getProperty("gw.sourceIps").split(",");

        String[] zones = {"A", "B", "C", "D", "E"};
        int perZone = 2000;

        AtomicInteger connected = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(count);

        long t0 = System.nanoTime();

        for (int i = 0; i < count; i++) {
            // Paced ramp. A full burst overruns the OS accept queue -- Windows
            // silently clamps the listen backlog well below the requested value,
            // so pacing is the only lever that works.
            if (i > 0 && i % 100 == 0) Thread.sleep(50);

            final int idx = i;
            final String nodeId = "zone-%s/node-%04d"
                    .formatted(zones[i / perZone % 5], i % perZone);

            Thread.ofVirtual().name("gen-", i).start(() -> {
                int attempt = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    boolean first = (attempt == 0);
                    try {
                        runConnection(nodeId, host, port, intervalMs,
                                sourceIps, idx, connected, ready, first);
                        // Clean return means the server closed us (BYE or EOF).
                        // Back off and reconnect rather than treating it as fatal.
                    } catch (Exception e) {
                        if (first) {
                            failed.incrementAndGet();
                            failures.computeIfAbsent(
                                    e.getClass().getSimpleName() + ": " + e.getMessage(),
                                    k -> new LongAdder()).increment();
                            ready.countDown();
                        }
                    }
                    attempt++;

                    try {
                        // Exponential backoff with jitter, capped at 30s. Without
                        // jitter 10k clients reconnect in lockstep and overwhelm
                        // the accept queue the moment the server returns.
                        long backoff = Math.min(30_000, 500L * (1L << Math.min(attempt, 6)));
                        Thread.sleep(backoff + ThreadLocalRandom.current().nextInt(1000));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
        }

        ready.await();
        long ms = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("connected=%d failed=%d in %d ms%n",
                connected.get(), failed.get(), ms);
        failures.forEach((k, v) -> System.out.printf("  %6d  %s%n", v.sum(), k));
        System.out.println("Holding connections. Ctrl+C to stop.");
        Thread.currentThread().join();
    }

    /**
      One connection's full lifetime: connect, handshake, then emit telemetry
      until the socket closes. Returns normally when the server hangs up.

      @param first true only on the initial attempt -- the latch and the
                   connected counter must not be touched on reconnects, or the
                   startup summary becomes meaningless. */
    private static void runConnection(String nodeId, String host, int port, int intervalMs,
                                      String[] sourceIps, int idx,
                                      AtomicInteger connected, CountDownLatch ready,
                                      boolean first) throws Exception {
        try (Socket s = new Socket()) {
            String src = sourceIps[idx % sourceIps.length];
            if (src != null) {
                s.bind(new InetSocketAddress(InetAddress.getByName(src), 0));
            }
            s.connect(new InetSocketAddress(host, port), 10_000);
            s.setTcpNoDelay(true);

            OutputStream out = s.getOutputStream();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));

            out.write(("HELLO|" + nodeId + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();

            String ack = in.readLine();
            if (ack == null || !ack.startsWith("OK")) {
                throw new IllegalStateException("handshake rejected: " + ack);
            }

            // The server echoes the node's type so we emit telemetry consistent
            // with what the registry believes this node is. Guessing from the
            // node ID would disagree with the seeded topology for most nodes.
            String type = ack.contains("|")
                    ? ack.substring(ack.indexOf('|') + 1).trim()
                    : "SOLAR";

            if (first) {
                connected.incrementAndGet();
                ready.countDown();
            }

            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            double soc = rnd.nextDouble(0.3, 0.8);

            while (!Thread.currentThread().isInterrupted()) {
                double power;
                switch (type) {
                    case "SOLAR" -> power = rnd.nextDouble(0.0, 12.0);
                    case "LOAD"  -> power = -rnd.nextDouble(0.5, 8.0);
                    default -> {
                        power = rnd.nextDouble(-4.0, 4.0);
                        soc = Math.clamp(soc - power * 0.0005, 0.05, 1.0);
                    }
                }

                out.write("T|%.2f|%.3f\n".formatted(power, soc)
                        .getBytes(StandardCharsets.UTF_8));
                out.flush();

                Thread.sleep(intervalMs + rnd.nextInt(200));
            }
        }
    }
}