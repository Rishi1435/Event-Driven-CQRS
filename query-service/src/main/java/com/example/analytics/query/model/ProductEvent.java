package com.example.analytics.query.model;

public class ProductEvent {
    private String eventType;
    private Product payload;

    public ProductEvent() {}

    public ProductEvent(String eventType, Product payload) {
        this.eventType = eventType;
        this.payload = payload;
    }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public Product getPayload() { return payload; }
    public void setPayload(Product payload) { this.payload = payload; }
}
