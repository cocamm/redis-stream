package com.study.redis.stream;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.study.redis.stream.Runner.CUSTOMER_GROUP;
import static com.study.redis.stream.Runner.CUSTOMER_STREAM;

@Service
@RequiredArgsConstructor
class RedisStreamCustomerRepository implements CustomerRepository {

    private static final int CONSUMER_COUNT_LIMIT = 5;

    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public List<Customer> findAll(String consumerId) {
        redisTemplate.opsForStream()
                .groups("customer-stream")
                .stream()
                .findFirst()
                .ifPresent(group -> {
                    var consumerCount = group.consumerCount();
                    if (consumerCount > CONSUMER_COUNT_LIMIT) {
                        throw new IllegalStateException("You cannot keep open more than " + CONSUMER_COUNT_LIMIT + " stream consumers");
                    }
                });

        var messages = redisTemplate.opsForStream(new Jackson2HashMapper(true))
                .read(Customer.class, Consumer.from(CUSTOMER_GROUP,
                        "customer-" + consumerId),
                        StreamReadOptions.empty().noack().count(10),
                        StreamOffset.create(CUSTOMER_STREAM, ReadOffset.lastConsumed()));

        return Optional.ofNullable(messages)
                .map(Collection::stream)
                .map(stream -> stream
                        .map(Record::getValue)
                        .collect(Collectors.toList()))
                .orElseGet(Collections::emptyList);
    }
}
