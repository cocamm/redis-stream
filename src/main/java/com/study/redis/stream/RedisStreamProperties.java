package com.study.redis.stream;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@ConfigurationProperties("redis.stream")
class RedisStreamProperties {

    Map<String, StreamProperties> config;

    @Data
    static class StreamProperties {

        Integer maxNumberConsumers;

        Integer maxNumberMessages = 1;
    }

}
