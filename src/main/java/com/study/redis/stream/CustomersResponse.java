package com.study.redis.stream;

import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
public class CustomersResponse {

    private final List<Customer> customers;
    private final String consumerId;

    public CustomersResponse(String consumerId, List<Customer> customers) {
        this.consumerId = consumerId;
        this.customers = customers;
    }
}
