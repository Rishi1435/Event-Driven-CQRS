package com.example.analytics.query.model;

import java.math.BigDecimal;

public class EnrichedOrderItem {
    private Long productId;
    private Integer quantity;
    private BigDecimal price;
    private String category;
    private String productName;

    public EnrichedOrderItem() {}

    public EnrichedOrderItem(Long productId, Integer quantity, BigDecimal price, String category, String productName) {
        this.productId = productId;
        this.quantity = quantity;
        this.price = price;
        this.category = category;
        this.productName = productName;
    }

    public Long getProductId() { return productId; }
    public void setProductId(Long productId) { this.productId = productId; }
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
    public BigDecimal getPrice() { return price; }
    public void setPrice(BigDecimal price) { this.price = price; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }
}
