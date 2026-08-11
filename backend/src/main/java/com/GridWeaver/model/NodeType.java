package com.GridWeaver.model;

public enum NodeType {
    SOLAR,    // generation only, no storage
    BATTERY,  // charges / discharges, has state of charge
    LOAD      // consumption only
}