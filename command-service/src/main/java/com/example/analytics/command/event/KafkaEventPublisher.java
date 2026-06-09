package com.example.analytics.command.event;

import com.example.analytics.command.model.Order;
import com.example.analytics.command.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventPublisher {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.products:product-events}")
    private String productTopic;

    @Value("${kafka.topics.orders:order-events}")
    private String orderTopic;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void publishProductCreated(Product product) {
        try {
            ProductEvent event = new ProductEvent("ProductCreated", product);
            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(productTopic, String.valueOf(product.getId()), message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish ProductCreated event", e);
        }
    }

    public void publishOrderCreated(Order order) {
        try {
            OrderEvent event = new OrderEvent("OrderCreated", order);
            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(orderTopic, String.valueOf(order.getId()), message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish OrderCreated event", e);
        }
    }

    public void publishOrderUpdated(Long orderId, String newStatus) {
        try {
            OrderUpdatedPayload payload = new OrderUpdatedPayload(orderId, newStatus);
            OrderEvent event = new OrderEvent("OrderUpdated", payload);
            String message = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(orderTopic, String.valueOf(orderId), message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish OrderUpdated event", e);
        }
    }
}
