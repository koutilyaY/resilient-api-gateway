package com.koutilya.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Resilient API Gateway.
 *
 * <p>A Spring Cloud Gateway (reactive/WebFlux) that shields downstream services with
 * distributed sliding-window rate limiting (Redis + Lua) and Resilience4j circuit
 * breaking, time limiting, bulkheading and retry-with-backoff.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ResilientApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ResilientApiGatewayApplication.class, args);
    }
}
