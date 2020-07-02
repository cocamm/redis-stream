package com.study.redis.stream;

import java.util.List;

interface CustomerService {

    CustomersResponse getCustomers();

    CustomersResponse getCustomers(String consumerId);
}
