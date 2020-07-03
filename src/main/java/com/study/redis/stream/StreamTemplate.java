package com.study.redis.stream;

import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.hash.Jackson2HashMapper;

import java.util.List;

class StreamTemplate {

    private final StreamOperations<String, String, Object> streamOperations;
    private final String stream;

    private Integer maxNumberMessages;
    private Integer maxNumberConsumers;

    StreamTemplate(RedisTemplate<String, String> redisTemplate, String stream) {
        this.streamOperations = redisTemplate.opsForStream(new Jackson2HashMapper(true));
        this.stream = stream;
    }

    public <T> List<ObjectRecord<String, T>> readFromLast(String group, String consumer, Class<T> type) {
        if (this.maxNumberConsumers != null) {
            this.validateMaxConsumers();
        }

        return streamOperations
                .read(type, Consumer.from(group,
                        consumer),
                        StreamReadOptions.empty().noack().count(this.maxNumberMessages),
                        StreamOffset.create(stream, ReadOffset.lastConsumed()));
    }

    public void createGroup(String group) {
        if (streamOperations.groups(this.stream).stream().noneMatch(g -> g.groupName().equals(group))) {
            streamOperations.createGroup(this.stream, group);
        }
    }

    public <T> void add(ObjectRecord<String, T> record) {
        streamOperations.add(record);
    }

    private void validateMaxConsumers() {
        streamOperations.groups(this.stream)
                .stream()
                .findFirst()
                .ifPresent(group -> {
                    var consumerCount = group.consumerCount();
                    if (consumerCount > this.maxNumberConsumers) {
                        throw new IllegalStateException("You cannot keep open more than " + this.maxNumberConsumers + " stream consumers");
                    }
                });
    }

    protected void setMaxNumberMessages(Integer maxNumberMessages) {
        this.maxNumberMessages = maxNumberMessages;
    }

    protected void setMaxNumberConsumers(Integer maxNumberConsumers) {
        this.maxNumberConsumers = maxNumberConsumers;
    }
}
