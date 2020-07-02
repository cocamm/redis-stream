package com.study.redis.stream;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.UUID;

@Service
@RequiredArgsConstructor
class DefaultCustomerService implements CustomerService {

    private final CustomerRepository customerRepository;

    @Override
    public CustomersResponse getCustomers() {
        return this.getCustomers(null);
    }

    @Override
    public CustomersResponse getCustomers(String consumerId) {
        consumerId = StringUtils.isEmpty(consumerId) ? UUID.randomUUID().toString() : consumerId;
        var customers = customerRepository.findAll(consumerId);

        return new CustomersResponse(consumerId, customers);
    }
}
