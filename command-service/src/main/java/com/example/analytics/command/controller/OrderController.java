package com.example.analytics.command.controller;

import com.example.analytics.command.model.Order;
import com.example.analytics.command.repository.OrderRepository;
import com.example.analytics.command.event.KafkaEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderRepository orderRepository;
    private final KafkaEventPublisher kafkaEventPublisher;

    public OrderController(OrderRepository orderRepository, KafkaEventPublisher kafkaEventPublisher) {
        this.orderRepository = orderRepository;
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order orderRequest) {
        Order order = new Order();
        order.setCustomerId(orderRequest.getCustomerId());
        order.setItems(orderRequest.getItems());
        order.setStatus("CREATED");

        Order savedOrder = orderRepository.save(order);
        kafkaEventPublisher.publishOrderCreated(savedOrder);

        return ResponseEntity.status(HttpStatus.CREATED).body(savedOrder);
    }

    @PutMapping("/{orderId}/status")
    public ResponseEntity<Order> updateOrderStatus(
            @PathVariable Long orderId,
            @RequestBody Map<String, String> statusUpdate) {
        String newStatus = statusUpdate.get("status");
        if (newStatus == null) {
            return ResponseEntity.badRequest().build();
        }

        return orderRepository.findById(orderId)
                .map(order -> {
                    order.setStatus(newStatus);
                    Order updatedOrder = orderRepository.save(order);
                    kafkaEventPublisher.publishOrderUpdated(orderId, newStatus);
                    return ResponseEntity.ok(updatedOrder);
                })
                .orElse(ResponseEntity.notFound().build());
    }
}
