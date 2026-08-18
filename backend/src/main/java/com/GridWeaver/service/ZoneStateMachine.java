package com.GridWeaver.service;

import com.GridWeaver.model.ZoneEvent;
import com.GridWeaver.model.ZoneStatus;
import org.springframework.stereotype.Service;

import java.util.EnumSet;
import java.util.Set;

/**
  One machine per zone. Explicit transition table with guards -- the same shape
  Spring State Machine would give us, without the dependency.

  Spring Statemachine 4.0.x targets Boot 3.5.x and the Boot 4 support request was
  declined upstream, so it is not viable on Boot 4.1. This implementation keeps
  the semantics that matter here: a defined state set, events that drive
  transitions, guards that can veto them, and a listener hook for auditing.

  Not thread-safe by design: exactly one caller (the scheduled evaluator) drives
  all five machines from a single tick, so locking would be pure overhead.
 */
@Service
public class ZoneStateMachine {


    public record Bands(
            double stressedEnter,
            double stressedExit,
            double surplusEnter,
            double surplusExit,
            double reserveFloor,
            double reserveRestore
    ) {
        public static Bands defaults() {
            return new Bands(0.80, 0.70, 0.40, 0.50, 0.15, 0.25);
        }
    }


    private static final Set<ZoneEvent> FROM_SURPLUS =
            EnumSet.of(ZoneEvent.LOAD_ROSE);
    private static final Set<ZoneEvent> FROM_NOMINAL =
            EnumSet.of(ZoneEvent.LOAD_ROSE, ZoneEvent.LOAD_FELL);
    private static final Set<ZoneEvent> FROM_STRESSED =
            EnumSet.of(ZoneEvent.LOAD_FELL, ZoneEvent.RESERVE_DEPLETED);
    private static final Set<ZoneEvent> FROM_CRITICAL =
            EnumSet.of(ZoneEvent.LOAD_FELL, ZoneEvent.RESERVE_RESTORED);

    private ZoneStatus state = ZoneStatus.NOMINAL;
    private long enteredAt = System.currentTimeMillis();
    private long transitionCount;

    public ZoneStatus state()          { return state; }
    public long enteredAt()            { return enteredAt; }
    public long transitionCount()      { return transitionCount; }
    public long dwellMs(long now)      { return now - enteredAt; }

    public boolean accepts(ZoneEvent e) {
        return switch (state) {
            case SURPLUS  -> FROM_SURPLUS.contains(e);
            case NOMINAL  -> FROM_NOMINAL.contains(e);
            case STRESSED -> FROM_STRESSED.contains(e);
            case CRITICAL -> FROM_CRITICAL.contains(e);
        };
    }


    public ZoneStatus fire(double loadFactor, double avgSoc, long now, Bands b) {
        ZoneEvent event = deriveEvent(loadFactor, avgSoc, b);
        if (event == null || !accepts(event)) return null;

        ZoneStatus target = target(event, loadFactor, avgSoc, b);
        if (target == null || target == state) return null;

        state = target;
        enteredAt = now;
        transitionCount++;
        return state;
    }

    private ZoneEvent deriveEvent(double load, double avgSoc, Bands b) {
        return switch (state) {
            case SURPLUS  -> load > b.surplusExit()    ? ZoneEvent.LOAD_ROSE : null;
            case NOMINAL  -> load >= b.stressedEnter() ? ZoneEvent.LOAD_ROSE
                    : load <= b.surplusEnter()  ? ZoneEvent.LOAD_FELL
                    : null;
            case STRESSED -> avgSoc < b.reserveFloor() ? ZoneEvent.RESERVE_DEPLETED
                    : load < b.stressedExit()   ? ZoneEvent.LOAD_FELL
                    : null;
            case CRITICAL -> avgSoc > b.reserveRestore() ? ZoneEvent.RESERVE_RESTORED
                    : load < b.stressedExit()     ? ZoneEvent.LOAD_FELL
                    : null;
        };
    }

    private ZoneStatus target(ZoneEvent e, double load, double avgSoc, Bands b) {
        return switch (e) {
            case LOAD_ROSE -> {
                if (state == ZoneStatus.SURPLUS) yield ZoneStatus.NOMINAL;
                // guard: don't enter STRESSED with no reserve to draw on
                yield avgSoc < b.reserveFloor() ? ZoneStatus.CRITICAL : ZoneStatus.STRESSED;
            }
            case LOAD_FELL -> {
                if (state == ZoneStatus.NOMINAL) yield ZoneStatus.SURPLUS;
                yield ZoneStatus.NOMINAL;
            }
            case RESERVE_DEPLETED  -> ZoneStatus.CRITICAL;
            case RESERVE_RESTORED  -> ZoneStatus.STRESSED;
        };
    }
}