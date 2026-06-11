package com.example.analytics.query.streams;

import com.example.analytics.query.model.*;
import com.example.analytics.query.serdes.JsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AnalyticsTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ProductEvent> productInputTopic;
    private TestInputTopic<String, OrderEvent> orderInputTopic;

    private KeyValueStore<String, Double> productSalesStore;
    private KeyValueStore<String, Double> categoryRevenueStore;
    private WindowStore<String, Double> hourlySalesStore;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setup() {
        StreamsBuilder builder = new StreamsBuilder();
        AnalyticsTopology topologyBean = new AnalyticsTopology();

        ReflectionTestUtils.setField(topologyBean, "productTopic", "product-events");
        ReflectionTestUtils.setField(topologyBean, "orderTopic", "order-events");

        topologyBean.kStream(builder);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-analytics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        testDriver = new TopologyTestDriver(topology, props);

        productInputTopic = testDriver.createInputTopic(
                "product-events",
                Serdes.String().serializer(),
                new JsonSerde<>(ProductEvent.class).serializer()
        );

        orderInputTopic = testDriver.createInputTopic(
                "order-events",
                Serdes.String().serializer(),
                new JsonSerde<>(OrderEvent.class).serializer()
        );

        productSalesStore = testDriver.getKeyValueStore("product-sales-store");
        categoryRevenueStore = testDriver.getKeyValueStore("category-revenue-store");
        hourlySalesStore = testDriver.getWindowStore("hourly-sales-store");
    }

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    public void testTopologyJoinsAndAggregations() {
        // 1. Create and send product events
        Product laptop = new Product(1L, "Laptop", "Electronics", BigDecimal.valueOf(1000.00));
        ProductEvent productEvent1 = new ProductEvent("ProductCreated", laptop);
        productInputTopic.pipeInput("1", productEvent1);

        Product shirt = new Product(2L, "T-Shirt", "Apparel", BigDecimal.valueOf(20.00));
        ProductEvent productEvent2 = new ProductEvent("ProductCreated", shirt);
        productInputTopic.pipeInput("2", productEvent2);

        // 2. Create and send order events
        OrderItem item1 = new OrderItem(1L, 2, BigDecimal.valueOf(1000.00)); // 2000.00 Electronics
        OrderItem item2 = new OrderItem(2L, 5, BigDecimal.valueOf(20.00));   // 100.00 Apparel
        Order order = new Order(1L, 101, "CREATED", Arrays.asList(item1, item2));

        OrderEvent orderEvent = new OrderEvent("OrderCreated", objectMapper.valueToTree(order));

        Instant orderTime = Instant.parse("2026-06-12T10:15:00Z");
        orderInputTopic.pipeInput("1", orderEvent, orderTime);

        // 3. Verify Product Sales Store
        Double laptopSales = productSalesStore.get("1");
        assertNotNull(laptopSales);
        assertEquals(2000.00, laptopSales);

        Double shirtSales = productSalesStore.get("2");
        assertNotNull(shirtSales);
        assertEquals(100.00, shirtSales);

        // 4. Verify Category Revenue Store
        Double electronicsRevenue = categoryRevenueStore.get("Electronics");
        assertNotNull(electronicsRevenue);
        assertEquals(2000.00, electronicsRevenue);

        Double apparelRevenue = categoryRevenueStore.get("Apparel");
        assertNotNull(apparelRevenue);
        assertEquals(100.00, apparelRevenue);

        // 5. Verify Hourly Sales Store
        Instant windowStart = Instant.parse("2026-06-12T10:00:00Z");
        Instant windowEnd = windowStart.plus(Duration.ofHours(1));

        var iterator = hourlySalesStore.fetch("ALL", windowStart, windowEnd);
        boolean found = false;
        while (iterator.hasNext()) {
            var next = iterator.next();
            assertEquals(windowStart.toEpochMilli(), next.key);
            assertEquals(2100.00, next.value);
            found = true;
        }
        iterator.close();
        assertEquals(true, found);
    }
}
