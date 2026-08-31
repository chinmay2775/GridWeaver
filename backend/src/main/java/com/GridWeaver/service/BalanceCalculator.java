package com.GridWeaver.service;

import com.GridWeaver.config.NodeRegistry;
import com.GridWeaver.model.NodeState;
import com.GridWeaver.model.NodeType;
import com.GridWeaver.model.Zone;
import com.GridWeaver.model.ZoneBalance;
import com.GridWeaver.model.ZoneStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;

/**
 Computes what each zone can export or needs to import.
 The key distinction is between net power and transferable power:
 net        = generation - consumption
 exportable = net, minus what local storage wants to absorb, minus a
 reserve margin, capped by the interconnect rating
 deficit    = shortfall, minus what local storage can supply

 A zone always serves itself first. Only what remains after local storage
 is considered gets offered to neighbours -- moving power across an
 interconnect to charge a battery that a neighbour also needs is worse
 than doing nothing. */

@Service
public class BalanceCalculator {

    private final NodeRegistry registry;

    @Value("${gridweaver.balance.interconnect-kw:2500}")
    private double interconnectKw;

    @Value("${gridweaver.balance.reserve-margin:0.10}")
    private double reserveMargin;

    @Value("${gridweaver.balance.soc-ceiling:0.95}")
    private double socCeiling;

    @Value("${gridweaver.balance.soc-floor:0.20}")
    private double socFloor;

    @Value("${gridweaver.balance.stale-after-ms:5000}")
    private long staleAfterMs;

    public BalanceCalculator(NodeRegistry registry) {
        this.registry = registry;
    }

    public Map<Zone, ZoneBalance> compute(Map<Zone, ZoneStatus> zoneStatus) {
        long cutoff = System.currentTimeMillis() - staleAfterMs;

        // [gen, con, headroom, reserve]
        Map<Zone, double[]> acc = new EnumMap<>(Zone.class);
        for (Zone z : Zone.values()) acc.put(z, new double[4]);

        for (NodeState n : registry.all()) {
            if (n.lastSeen() < cutoff) continue;
            double[] a = acc.get(n.zone());

            double p = n.powerKw();
            if (p >= 0) a[0] += p; else a[1] += -p;

            if (n.type() == NodeType.BATTERY) {
                // Headroom: kWh of charge this battery could still take, expressed
                // against its power rating -- a full battery offers no headroom
                // regardless of how big it is.
                a[2] += n.capacityKw() * Math.max(0, socCeiling - n.soc());
                a[3] += n.capacityKw() * Math.max(0, n.soc() - socFloor);
            }
        }

        Map<Zone, ZoneBalance> out = new EnumMap<>(Zone.class);
        acc.forEach((z, a) -> {
            double gen = a[0], con = a[1];
            double net = gen - con;
            double headroom = a[2], reserve = a[3];

            double exportable = 0, deficit = 0;

            if (net > 0) {
                // Surplus: local storage soaks up what it can first, and we hold
                // back a margin so the zone is not left exposed if its own demand
                // rises before the next tick.
                double afterStorage = net - headroom;
                double margin = net * reserveMargin;
                exportable = Math.max(0, Math.min(afterStorage - margin, interconnectKw));
            } else if (net < 0) {
                // Deficit: local batteries discharge first. Only the remainder
                // is a genuine import requirement.
                double shortfall = -net;
                deficit = Math.max(0, Math.min(shortfall - reserve, interconnectKw));
            }

            out.put(z, new ZoneBalance(
                    z,
                    zoneStatus.getOrDefault(z, ZoneStatus.NOMINAL),
                    round(gen), round(con), round(net),
                    round(headroom), round(reserve),
                    round(exportable), round(deficit)
            ));
        });
        return out;
    }

    private static double round(double d) {
        return Math.round(d * 100.0) / 100.0;
    }

    public double interconnectKw() { return interconnectKw; }
}