package com.GridWeaver.model;

/**
 * One telemetry reading off the wire.
 *
 * Wire format is a pipe-delimited line, not JSON: at 50k connections the
 * per-frame allocation cost of JSON parsing dominates everything else.
 *   T|<powerKw>|<soc>
 */
public record TelemetryFrame(double powerKw, double soc, long receivedAt) {

    public static TelemetryFrame parse(String line, long now) {
        // expects: T|<powerKw>|<soc>
        int p1 = line.indexOf('|');
        int p2 = line.indexOf('|', p1 + 1);
        if (p1 < 0 || p2 < 0) {
            throw new IllegalArgumentException("malformed frame: " + line);
        }
        double power = Double.parseDouble(line.substring(p1 + 1, p2));
        double soc = Double.parseDouble(line.substring(p2 + 1));
        return new TelemetryFrame(power, soc, now);
    }
}