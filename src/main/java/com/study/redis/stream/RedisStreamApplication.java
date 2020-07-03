package com.study.redis.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.nonNull;

@SpringBootApplication
@EnableConfigurationProperties({RedisStreamProperties.class})
public class RedisStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisStreamApplication.class, args);
    }

    @Bean
    StreamTemplateFactoryBean streamTemplateFactoryBean(RedisStreamProperties redisStreamProperties,
                                                        RedisTemplate<String, String> redisTemplate) {
        var entry = redisStreamProperties.getConfig().entrySet()
                .stream()
                .findFirst()
                .orElse(null);

        return new StreamTemplateFactoryBean(entry.getKey(), entry.getValue(), redisTemplate);
    }

}

class StreamTemplateFactoryBean implements FactoryBean<StreamTemplate> {

    private String stream;
    private RedisStreamProperties.StreamProperties streamProperties;
    private RedisTemplate<String, String> redisTemplate;

    StreamTemplateFactoryBean(String stream, RedisStreamProperties.StreamProperties streamProperties, RedisTemplate<String, String> redisTemplate) {
        this.stream = stream;
        this.streamProperties = streamProperties;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public StreamTemplate getObject() throws Exception {
        return createStreamTemplate();
    }

    @Override
    public Class<?> getObjectType() {
        return StreamTemplate.class;
    }

    private StreamTemplate createStreamTemplate() {
        var streamTemplate = new StreamTemplate(redisTemplate, stream);
        if (nonNull(streamProperties)) {
            streamTemplate.setMaxNumberConsumers(streamProperties.getMaxNumberConsumers());
            streamTemplate.setMaxNumberMessages(streamProperties.getMaxNumberMessages());
        }
        return streamTemplate;
    }
}

@Component
@RequiredArgsConstructor
class Runner {

    public static final String CUSTOMER_STREAM = "customer-stream";
    public static final String CUSTOMER_GROUP = "customer-group";

    private final StreamTemplate streamTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        IntStream.range(0, 1000)
                .forEach(i -> {
                    final var record = StreamRecords.newRecord()
                            .in(CUSTOMER_STREAM)
                            .ofObject(new Customer("Name Customer " + i));
                    streamTemplate.add(record);
                });
        streamTemplate.createGroup(CUSTOMER_GROUP);
    }
}

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("customers")
class CustomerController {

    private final CustomerService customerService;

    @GetMapping
    public ResponseEntity<List<Customer>> getCustomers() {
        log.info("Iniciando consulta stream customer");

        var response = customerService.getCustomers();
        log.info("Retornando Stream, response={}", response);

        return ResponseEntity.ok()
                .headers(httpHeaders -> httpHeaders.add("x-next", response.getConsumerId()))
                .body(response.getCustomers());
    }

    @GetMapping(value = "/{consumerId}")
    public ResponseEntity<List<Customer>> getCustomersByConsumerId(@PathVariable("consumerId") String consumerId) {
        log.info("Iniciando consulta stream customer, consumerId={}", consumerId);

        var response = customerService.getCustomers(consumerId);
        log.info("Retornando Stream para o consumer={}, response={}", consumerId, response);

        return ResponseEntity.ok()
                .headers(httpHeaders -> httpHeaders.add("x-next", response.getConsumerId()))
                .body(response.getCustomers());
    }
}

@ControllerAdvice
class ExceptionHandler extends ResponseEntityExceptionHandler {

    protected ResponseEntity<Object> handlerIllegalStateException(IllegalStateException ex, HttpHeaders headers, HttpStatus status, WebRequest webRequest) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
    }
}