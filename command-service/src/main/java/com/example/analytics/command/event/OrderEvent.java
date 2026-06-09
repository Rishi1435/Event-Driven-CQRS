package com.example.analytics.command.event;

public class OrderEvent {
    private String eventType;
    private Object payload;

    public OrderEvent() {}

    public OrderEvent(String eventType, Object payload) {
        this.eventType = eventType;
        this.payload = payload;
    }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }
}
