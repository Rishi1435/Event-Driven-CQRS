package com.example.analytics.command.controller;

import com.example.analytics.command.model.Order;
import com.example.analytics.command.model.OrderItem;
import com.example.analytics.command.repository.OrderRepository;
import com.example.analytics.command.event.KafkaEventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class OrderControllerTest {

    private MockMvc mockMvc;

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private KafkaEventPublisher kafkaEventPublisher;

    @InjectMocks
    private OrderController orderController;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(orderController).build();
    }

    @Test
    public void testCreateOrder() throws Exception {
        List<OrderItem> items = Arrays.asList(new OrderItem(1L, 2, BigDecimal.valueOf(99.99)));
        Order request = new Order(null, 101, null, items);
        Order saved = new Order(1L, 101, "CREATED", items);

        when(orderRepository.save(any(Order.class))).thenReturn(saved);

        mockMvc.perform(post("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.customerId").value(101))
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.items[0].productId").value(1))
                .andExpect(jsonPath("$.items[0].quantity").value(2))
                .andExpect(jsonPath("$.items[0].price").value(99.99));

        verify(orderRepository, times(1)).save(any(Order.class));
        verify(kafkaEventPublisher, times(1)).publishOrderCreated(saved);
    }

    @Test
    public void testUpdateOrderStatus_Success() throws Exception {
        List<OrderItem> items = Arrays.asList(new OrderItem(1L, 2, BigDecimal.valueOf(99.99)));
        Order existing = new Order(1L, 101, "CREATED", items);
        Order updated = new Order(1L, 101, "PAID", items);

        when(orderRepository.findById(1L)).thenReturn(Optional.of(existing));
        when(orderRepository.save(any(Order.class))).thenReturn(updated);

        Map<String, String> statusUpdate = new HashMap<>();
        statusUpdate.put("status", "PAID");

        mockMvc.perform(put("/api/orders/1/status")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(statusUpdate)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.status").value("PAID"));

        verify(orderRepository, times(1)).findById(1L);
        verify(orderRepository, times(1)).save(any(Order.class));
        verify(kafkaEventPublisher, times(1)).publishOrderUpdated(1L, "PAID");
    }

    @Test
    public void testUpdateOrderStatus_NotFound() throws Exception {
        when(orderRepository.findById(1L)).thenReturn(Optional.empty());

        Map<String, String> statusUpdate = new HashMap<>();
        statusUpdate.put("status", "PAID");

        mockMvc.perform(put("/api/orders/1/status")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(statusUpdate)))
                .andExpect(status().isNotFound());

        verify(orderRepository, times(1)).findById(1L);
        verify(orderRepository, never()).save(any(Order.class));
        verify(kafkaEventPublisher, never()).publishOrderUpdated(anyLong(), anyString());
    }
}
