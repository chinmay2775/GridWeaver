package com.GridWeaver.tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock IoT fleet. Opens N connections, each on its own virtual thread,
 * and sends a telemetry frame every intervalMs.
 *
 * Usage: LoadGenerator [count] [host] [port] [intervalMs]
 */
public class LoadGenerator {

    public static void main(String[] args) throws Exception {
        int count      = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        String host    = args.length > 1 ? args[1] : "127.0.0.1";
        int port       = args.length > 2 ? Integer.parseInt(args[2]) : 9099;
        int intervalMs = args.length > 3 ? Integer.parseInt(args[3]) : 1000;

        String[] zones = {"A", "B", "C", "D", "E"};
        int perZone = 2000;

        AtomicInteger connected = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(count);

        long t0 = System.nanoTime();
        var failures = new java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder>();

        for (int i = 0; i < count; i++) {
            if (i > 0 && i % 250 == 0) Thread.sleep(25);
            String nodeId = "zone-%s/node-%04d".formatted(zones[i / perZone % 5], i % perZone);
            Thread.ofVirtual().name("gen-", i).start(() -> {
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress(host, port), 10_000);
                    s.setTcpNoDelay(true);
                    OutputStream out = s.getOutputStream();
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));

                    out.write(("HELLO|" + nodeId + "\n").getBytes(StandardCharsets.UTF_8));
                    out.flush();

                    String ack = in.readLine();
                    if (!"OK".equals(ack)) {
                        failed.incrementAndGet();
                        ready.countDown();
                        return;
                    }
                    connected.incrementAndGet();
                    ready.countDown();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    while (!Thread.currentThread().isInterrupted()) {
                        double power = rnd.nextDouble(-8.0, 12.0);
                        double soc = rnd.nextDouble(0.1, 1.0);
                        out.write("T|%.2f|%.3f\n".formatted(power, soc)
                                .getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        Thread.sleep(intervalMs + rnd.nextInt(200));  // jitter
                    }
                } catch (Exception e) {
                    failed.incrementAndGet();
                    failures.computeIfAbsent(e.getClass().getSimpleName() + ": " + e.getMessage(),
                                    k -> new java.util.concurrent.atomic.LongAdder())
                            .increment();
                    ready.countDown();
                }
            });
        }

        ready.await();
        long ms = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("connected=%d failed=%d in %d ms%n",
                connected.get(), failed.get(), ms);
        System.out.println("Holding connections. Ctrl+C to stop.");
        Thread.currentThread().join();
        failures.forEach((k, v) -> System.out.printf("  %6d  %s%n", v.sum(), k));
    }
}