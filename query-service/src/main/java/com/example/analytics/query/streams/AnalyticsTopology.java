package com.example.analytics.query.streams;

import com.example.analytics.query.model.*;
import com.example.analytics.query.serdes.JsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class AnalyticsTopology {

    @Value("${kafka.topics.products:product-events}")
    private String productTopic;

    @Value("${kafka.topics.orders:order-events}")
    private String orderTopic;

    @Bean
    public KStream<String, OrderEvent> kStream(StreamsBuilder streamsBuilder) {
        // 1. Build KTable for products
        KTable<String, Product> productTable = streamsBuilder.table(
                productTopic,
                Consumed.with(Serdes.String(), new JsonSerde<>(ProductEvent.class))
        ).mapValues(
                ProductEvent::getPayload,
                Materialized.<String, Product, KeyValueStore<Bytes, byte[]>>as("products-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(Product.class))
        );

        // 2. Consume order-events as KStream
        KStream<String, OrderEvent> orderStream = streamsBuilder.stream(
                orderTopic,
                Consumed.with(Serdes.String(), new JsonSerde<>(OrderEvent.class))
        );

        // 3. Filter for OrderCreated events, flatMap to get individual order items keyed by productId
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        KStream<String, OrderItem> orderItemStream = orderStream
                .filter((key, value) -> value != null && "OrderCreated".equals(value.getEventType()))
                .flatMap((key, value) -> {
                    List<KeyValue<String, OrderItem>> result = new ArrayList<>();
                    try {
                        Order order = objectMapper.treeToValue(value.getPayload(), Order.class);
                        if (order != null && order.getItems() != null) {
                            for (OrderItem item : order.getItems()) {
                                result.add(KeyValue.pair(String.valueOf(item.getProductId()), item));
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to deserialize Order payload: " + e.getMessage());
                    }
                    return result;
                });

        // 4. Join the stream of items with the products table on productId
        KStream<String, EnrichedOrderItem> enrichedStream = orderItemStream.join(
                productTable,
                (orderItem, product) -> new EnrichedOrderItem(
                        orderItem.getProductId(),
                        orderItem.getQuantity(),
                        orderItem.getPrice(),
                        product.getCategory(),
                        product.getName()
                ),
                Joined.with(Serdes.String(), new JsonSerde<>(OrderItem.class), new JsonSerde<>(Product.class))
        );

        // 5. Aggregate Product Sales
        enrichedStream
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(EnrichedOrderItem.class)))
                .aggregate(
                        () -> 0.0,
                        (key, item, aggregate) -> aggregate + (item.getQuantity() * item.getPrice().doubleValue()),
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("product-sales-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        // 6. Aggregate Category Revenue
        enrichedStream
                .selectKey((key, item) -> item.getCategory())
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(EnrichedOrderItem.class)))
                .aggregate(
                        () -> 0.0,
                        (key, item, aggregate) -> aggregate + (item.getQuantity() * item.getPrice().doubleValue()),
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("category-revenue-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        // 7. Aggregate Hourly Sales
        TimeWindows hourlyWindow = TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(10));

        enrichedStream
                .selectKey((key, item) -> "ALL")
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(EnrichedOrderItem.class)))
                .windowedBy(hourlyWindow)
                .aggregate(
                        () -> 0.0,
                        (key, item, aggregate) -> aggregate + (item.getQuantity() * item.getPrice().doubleValue()),
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("hourly-sales-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        return orderStream;
    }
}
