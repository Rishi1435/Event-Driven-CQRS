package com.example.analytics.query.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public AnalyticsController(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    private <K, V> ReadOnlyKeyValueStore<K, V> getKeyValueStore(String storeName) {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new IllegalStateException("Kafka Streams is not initialized");
        }
        return kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    private <K, V> ReadOnlyWindowStore<K, V> getWindowStore(String storeName) {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new IllegalStateException("Kafka Streams is not initialized");
        }
        return kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.windowStore()
                )
        );
    }

    @GetMapping("/products/{productId}/sales")
    public ResponseEntity<?> getProductSales(@PathVariable String productId) {
        try {
            ReadOnlyKeyValueStore<String, Double> store = getKeyValueStore("product-sales-store");
            Double sales = store.get(productId);
            if (sales == null) {
                sales = 0.0;
            }
            return ResponseEntity.ok(Map.of(
                    "productId", Long.parseLong(productId),
                    "totalSales", sales
            ));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of(
                    "productId", Long.parseLong(productId),
                    "totalSales", 0.0
            ));
        }
    }

    @GetMapping("/categories/{categoryName}/revenue")
    public ResponseEntity<?> getCategoryRevenue(@PathVariable String categoryName) {
        try {
            ReadOnlyKeyValueStore<String, Double> store = getKeyValueStore("category-revenue-store");
            Double revenue = store.get(categoryName);
            if (revenue == null) {
                revenue = 0.0;
            }
            return ResponseEntity.ok(Map.of(
                    "category", categoryName,
                    "totalRevenue", revenue
            ));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of(
                    "category", categoryName,
                    "totalRevenue", 0.0
            ));
        }
    }

    @GetMapping("/hourly-sales")
    public ResponseEntity<?> getHourlySales(
            @RequestParam(required = false) String start,
            @RequestParam(required = false) String end) {
        try {
            Instant startInstant = start != null ? Instant.parse(start) : Instant.now().minus(Duration.ofDays(1));
            Instant endInstant = end != null ? Instant.parse(end) : Instant.now();

            ReadOnlyWindowStore<String, Double> store = getWindowStore("hourly-sales-store");
            var iterator = store.fetch("ALL", startInstant, endInstant);
            List<Map<String, Object>> result = new ArrayList<>();
            while (iterator.hasNext()) {
                var next = iterator.next();
                Instant windowStart = Instant.ofEpochMilli(next.key);
                Instant windowEnd = windowStart.plus(Duration.ofHours(1));
                result.add(Map.of(
                        "windowStart", windowStart.toString(),
                        "windowEnd", windowEnd.toString(),
                        "totalSales", next.value
                ));
            }
            iterator.close();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.ok(List.of());
        }
    }

    @GetMapping("/topology")
    public ResponseEntity<String> getTopology() {
        if (streamsBuilderFactoryBean.getTopology() != null) {
            return ResponseEntity.ok(streamsBuilderFactoryBean.getTopology().describe().toString());
        }
        return ResponseEntity.ok("Topology not initialized yet");
    }
}
