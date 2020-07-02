package com.study.redis.stream;

import java.util.List;

interface CustomerRepository {

    List<Customer> findAll(String consumerId);
}
