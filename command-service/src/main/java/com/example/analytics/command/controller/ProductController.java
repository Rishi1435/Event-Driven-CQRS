package com.example.analytics.command.controller;

import com.example.analytics.command.model.Product;
import com.example.analytics.command.repository.ProductRepository;
import com.example.analytics.command.event.KafkaEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/products")
public class ProductController {
    private final ProductRepository productRepository;
    private final KafkaEventPublisher kafkaEventPublisher;

    public ProductController(ProductRepository productRepository, KafkaEventPublisher kafkaEventPublisher) {
        this.productRepository = productRepository;
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    @PostMapping
    public ResponseEntity<Product> createProduct(@RequestBody Product productRequest) {
        Product product = new Product();
        product.setName(productRequest.getName());
        product.setCategory(productRequest.getCategory());
        product.setPrice(productRequest.getPrice());

        Product savedProduct = productRepository.save(product);
        kafkaEventPublisher.publishProductCreated(savedProduct);

        return ResponseEntity.status(HttpStatus.CREATED).body(savedProduct);
    }
}
