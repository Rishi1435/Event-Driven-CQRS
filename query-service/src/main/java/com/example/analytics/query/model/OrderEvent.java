package com.example.analytics.query.model;

import com.fasterxml.jackson.databind.JsonNode;

public class OrderEvent {
    private String eventType;
    private JsonNode payload;

    public OrderEvent() {}

    public OrderEvent(String eventType, JsonNode payload) {
        this.eventType = eventType;
        this.payload = payload;
    }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public JsonNode getPayload() { return payload; }
    public void setPayload(JsonNode payload) { this.payload = payload; }
}
