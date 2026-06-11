package com.example.analytics.query.controller;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

public class AnalyticsControllerTest {

    private MockMvc mockMvc;

    @Mock
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private ReadOnlyKeyValueStore<String, Double> keyValueStore;

    @Mock
    private ReadOnlyWindowStore<String, Double> windowStore;

    @InjectMocks
    private AnalyticsController analyticsController;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(analyticsController).build();
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
    }

    @Test
    public void testGetProductSales() throws Exception {
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(keyValueStore);
        when(keyValueStore.get("1")).thenReturn(150.0);

        mockMvc.perform(get("/api/analytics/products/1/sales"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.productId").value(1))
                .andExpect(jsonPath("$.totalSales").value(150.0));
    }

    @Test
    public void testGetProductSales_NullSales() throws Exception {
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(keyValueStore);
        when(keyValueStore.get("1")).thenReturn(null);

        mockMvc.perform(get("/api/analytics/products/1/sales"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.productId").value(1))
                .andExpect(jsonPath("$.totalSales").value(0.0));
    }

    @Test
    public void testGetProductSales_Exception() throws Exception {
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenThrow(new RuntimeException("State store not available"));

        mockMvc.perform(get("/api/analytics/products/1/sales"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.productId").value(1))
                .andExpect(jsonPath("$.totalSales").value(0.0));
    }

    @Test
    public void testGetCategoryRevenue() throws Exception {
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(keyValueStore);
        when(keyValueStore.get("Electronics")).thenReturn(500.0);

        mockMvc.perform(get("/api/analytics/categories/Electronics/revenue"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.category").value("Electronics"))
                .andExpect(jsonPath("$.totalRevenue").value(500.0));
    }

    @Test
    public void testGetCategoryRevenue_NullRevenue() throws Exception {
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(keyValueStore);
        when(keyValueStore.get("Electronics")).thenReturn(null);

        mockMvc.perform(get("/api/analytics/categories/Electronics/revenue"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.category").value("Electronics"))
                .andExpect(jsonPath("$.totalRevenue").value(0.0));
    }

    @Test
    public void testGetCategoryRevenue_Exception() throws Exception {
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenThrow(new RuntimeException("State store not available"));

        mockMvc.perform(get("/api/analytics/categories/Electronics/revenue"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.category").value("Electronics"))
                .andExpect(jsonPath("$.totalRevenue").value(0.0));
    }

    @Test
    public void testGetTopology() throws Exception {
        Topology topology = mock(Topology.class);
        org.apache.kafka.streams.TopologyDescription description = mock(org.apache.kafka.streams.TopologyDescription.class);
        when(streamsBuilderFactoryBean.getTopology()).thenReturn(topology);
        when(topology.describe()).thenReturn(description);
        when(description.toString()).thenReturn("Topologies: Sub-topology 0");

        mockMvc.perform(get("/api/analytics/topology"))
                .andExpect(status().isOk())
                .andExpect(content().string("Topologies: Sub-topology 0"));
    }

    @Test
    public void testGetTopology_NullTopology() throws Exception {
        when(streamsBuilderFactoryBean.getTopology()).thenReturn(null);

        mockMvc.perform(get("/api/analytics/topology"))
                .andExpect(status().isOk())
                .andExpect(content().string("Topology not initialized yet"));
    }

    @Test
    public void testGetHourlySales_WithoutStartAndEnd() throws Exception {
        WindowStoreIterator<Double> iterator = mock(WindowStoreIterator.class);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(windowStore);
        when(windowStore.fetch(eq("ALL"), any(java.time.Instant.class), any(java.time.Instant.class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(new KeyValue<>(1672531200000L, 250.0));

        mockMvc.perform(get("/api/analytics/hourly-sales"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].totalSales").value(250.0))
                .andExpect(jsonPath("$[0].windowStart").value("2023-01-01T00:00:00Z"))
                .andExpect(jsonPath("$[0].windowEnd").value("2023-01-01T01:00:00Z"));
    }

    @Test
    public void testGetHourlySales_WithStartAndEnd() throws Exception {
        WindowStoreIterator<Double> iterator = mock(WindowStoreIterator.class);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(windowStore);
        when(windowStore.fetch(eq("ALL"), any(java.time.Instant.class), any(java.time.Instant.class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(new KeyValue<>(1672531200000L, 250.0));

        mockMvc.perform(get("/api/analytics/hourly-sales")
                        .param("start", "2023-01-01T00:00:00Z")
                        .param("end", "2023-01-01T02:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].totalSales").value(250.0));
    }

    @Test
    public void testGetHourlySales_Exception() throws Exception {
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenThrow(new RuntimeException("State store not available"));

        mockMvc.perform(get("/api/analytics/hourly-sales"))
                .andExpect(status().isOk())
                .andExpect(content().string("[]"));
    }
}

