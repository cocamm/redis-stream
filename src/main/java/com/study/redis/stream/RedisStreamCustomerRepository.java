package com.study.redis.stream;

import org.springframework.data.redis.connection.stream.Record;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.study.redis.stream.Runner.CUSTOMER_GROUP;

@Service
class RedisStreamCustomerRepository implements CustomerRepository {

    private final StreamTemplate streamTemplate;

    RedisStreamCustomerRepository(StreamTemplate streamTemplate) {
        this.streamTemplate = streamTemplate;
    }

    @Override
    public List<Customer> findAll(String consumerId) {
        var messages = streamTemplate
                .readFromLast(CUSTOMER_GROUP, "customer-" + consumerId, Customer.class);

        return Optional.ofNullable(messages)
                .map(Collection::stream)
                .map(stream -> stream
                        .map(Record::getValue)
                        .collect(Collectors.toList()))
                .orElseGet(Collections::emptyList);
    }
}
