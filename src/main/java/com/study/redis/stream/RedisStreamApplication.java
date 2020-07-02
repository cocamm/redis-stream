package com.study.redis.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;
import java.util.stream.IntStream;

@SpringBootApplication
public class RedisStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisStreamApplication.class, args);
    }

}

@Component
@RequiredArgsConstructor
class Runner {

    public static final String CUSTOMER_STREAM = "customer-stream";
    public static final String CUSTOMER_GROUP = "customer-group";

    private final RedisTemplate<String, String> redisTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        IntStream.range(0, 1000)
                .forEach(i -> {
                    final var record = StreamRecords.newRecord()
                            .in(CUSTOMER_STREAM)
                            .ofObject(new Customer("Name Customer " + i));
                    redisTemplate.opsForStream(new Jackson2HashMapper(true))
                            .add(record);
                });
        if (redisTemplate.opsForStream().groups(CUSTOMER_STREAM).stream().noneMatch(group -> group.groupName().equals(CUSTOMER_GROUP))) {
            redisTemplate.opsForStream().createGroup(CUSTOMER_STREAM, CUSTOMER_GROUP);
        }
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