package com.gridweaver.model;

/**
 * Per-node lifecycle state. Week 2 drives transitions between these.
 */
public enum NodeStatus {
    IDLE,
    CHARGING,
    DISCHARGING,
    FAULT
}
