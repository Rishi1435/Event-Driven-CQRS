package com.example.analytics.command.event;

import com.example.analytics.command.model.Order;
import com.example.analytics.command.model.OrderItem;
import com.example.analytics.command.model.Product;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class KafkaEventPublisherTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaEventPublisher kafkaEventPublisher;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        kafkaEventPublisher = new KafkaEventPublisher(kafkaTemplate);
        ReflectionTestUtils.setField(kafkaEventPublisher, "productTopic", "product-events");
        ReflectionTestUtils.setField(kafkaEventPublisher, "orderTopic", "order-events");
    }

    @Test
    public void testPublishProductCreated() {
        Product product = new Product(123L, "Laptop", "Electronics", BigDecimal.valueOf(999.99));
        
        kafkaEventPublisher.publishProductCreated(product);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

        verify(kafkaTemplate, times(1)).send(topicCaptor.capture(), keyCaptor.capture(), messageCaptor.capture());

        assertEquals("product-events", topicCaptor.getValue());
        assertEquals("123", keyCaptor.getValue());
        assertTrue(messageCaptor.getValue().contains("ProductCreated"));
        assertTrue(messageCaptor.getValue().contains("Laptop"));
        assertTrue(messageCaptor.getValue().contains("Electronics"));
    }

    @Test
    public void testPublishProductCreated_Exception() {
        Product product = new Product(123L, "Laptop", "Electronics", BigDecimal.valueOf(999.99));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("Kafka error"));

        assertThrows(RuntimeException.class, () -> kafkaEventPublisher.publishProductCreated(product));
    }

    @Test
    public void testPublishOrderCreated() {
        List<OrderItem> items = Arrays.asList(new OrderItem(1L, 2, BigDecimal.valueOf(99.99)));
        Order order = new Order(456L, 101, "CREATED", items);

        kafkaEventPublisher.publishOrderCreated(order);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

        verify(kafkaTemplate, times(1)).send(topicCaptor.capture(), keyCaptor.capture(), messageCaptor.capture());

        assertEquals("order-events", topicCaptor.getValue());
        assertEquals("456", keyCaptor.getValue());
        assertTrue(messageCaptor.getValue().contains("OrderCreated"));
        assertTrue(messageCaptor.getValue().contains("CREATED"));
    }

    @Test
    public void testPublishOrderCreated_Exception() {
        List<OrderItem> items = Arrays.asList(new OrderItem(1L, 2, BigDecimal.valueOf(99.99)));
        Order order = new Order(456L, 101, "CREATED", items);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("Kafka error"));

        assertThrows(RuntimeException.class, () -> kafkaEventPublisher.publishOrderCreated(order));
    }

    @Test
    public void testPublishOrderUpdated() {
        kafkaEventPublisher.publishOrderUpdated(456L, "PAID");

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

        verify(kafkaTemplate, times(1)).send(topicCaptor.capture(), keyCaptor.capture(), messageCaptor.capture());

        assertEquals("order-events", topicCaptor.getValue());
        assertEquals("456", keyCaptor.getValue());
        assertTrue(messageCaptor.getValue().contains("OrderUpdated"));
        assertTrue(messageCaptor.getValue().contains("PAID"));
    }

    @Test
    public void testPublishOrderUpdated_Exception() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("Kafka error"));

        assertThrows(RuntimeException.class, () -> kafkaEventPublisher.publishOrderUpdated(456L, "PAID"));
    }

    @Test
    public void testEventPojos() {
        // ProductEvent
        ProductEvent pe = new ProductEvent();
        pe.setEventType("TypeA");
        Product p = new Product();
        pe.setPayload(p);
        assertEquals("TypeA", pe.getEventType());
        assertSame(p, pe.getPayload());

        ProductEvent peConstructor = new ProductEvent("TypeB", p);
        assertEquals("TypeB", peConstructor.getEventType());
        assertSame(p, peConstructor.getPayload());

        // OrderEvent
        OrderEvent oe = new OrderEvent();
        oe.setEventType("TypeC");
        Object payload = new Object();
        oe.setPayload(payload);
        assertEquals("TypeC", oe.getEventType());
        assertSame(payload, oe.getPayload());

        OrderEvent oeConstructor = new OrderEvent("TypeD", payload);
        assertEquals("TypeD", oeConstructor.getEventType());
        assertSame(payload, oeConstructor.getPayload());

        // OrderUpdatedPayload
        OrderUpdatedPayload oup = new OrderUpdatedPayload();
        oup.setOrderId(789L);
        oup.setNewStatus("SHIPPED");
        assertEquals(789L, oup.getOrderId());
        assertEquals("SHIPPED", oup.getNewStatus());

        OrderUpdatedPayload oupConstructor = new OrderUpdatedPayload(789L, "SHIPPED");
        assertEquals(789L, oupConstructor.getOrderId());
        assertEquals("SHIPPED", oupConstructor.getNewStatus());
    }
}
