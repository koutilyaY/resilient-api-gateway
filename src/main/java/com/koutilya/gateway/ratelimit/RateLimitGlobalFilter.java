package com.koutilya.gateway.ratelimit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Edge rate-limit filter.
 *
 * <p>Runs very early in the filter chain ({@link #getOrder()} is a large negative number) so
 * abusive traffic is shed <em>before</em> it reaches routing, the circuit breaker, or any
 * downstream service. A rejected request never opens a downstream connection.
 *
 * <p>On Redis failure it applies the configured fail-open / fail-closed policy.
 */
@Component
public class RateLimitGlobalFilter implements GlobalFilter, Ordered {

    /**
     * Runs ahead of Spring Cloud Gateway's routing/circuit-breaker filters (which sit near
     * {@code Integer.MAX_VALUE}) but leaves room for tracing/security filters before it.
     */
    public static final int ORDER = -100;

    private static final Logger log = LoggerFactory.getLogger(RateLimitGlobalFilter.class);

    private final RateLimitProperties properties;
    private final TenantResolver tenantResolver;
    private final RateLimiter rateLimiter;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RateLimitGlobalFilter(RateLimitProperties properties,
                                 TenantResolver tenantResolver,
                                 RateLimiter rateLimiter) {
        this.properties = properties;
        this.tenantResolver = tenantResolver;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (!properties.isEnabled()) {
            return chain.filter(exchange);
        }
        // Never rate limit the internal fallback forward (avoids double-counting / loops).
        String path = exchange.getRequest().getPath().value();
        if (path.startsWith(properties.getFallbackPath())) {
            return chain.filter(exchange);
        }

        TenantResolver.Resolution resolution = tenantResolver.resolve(exchange.getRequest());
        RateLimitProperties.Bucket bucket = resolution.bucket();

        return rateLimiter
                .isAllowed(resolution.bucketKey(), bucket.getLimit(), bucket.windowMillis())
                .flatMap(decision -> {
                    writeRateLimitHeaders(exchange.getResponse().getHeaders(), decision);
                    if (decision.allowed()) {
                        return chain.filter(exchange);
                    }
                    return reject(exchange, decision);
                })
                .onErrorResume(error -> handleRedisFailure(exchange, chain, bucket, error));
    }

    private Mono<Void> handleRedisFailure(ServerWebExchange exchange,
                                          GatewayFilterChain chain,
                                          RateLimitProperties.Bucket bucket,
                                          Throwable error) {
        if (properties.isFailOpen()) {
            log.warn("Rate-limit backend unavailable, failing OPEN (allowing request): {}",
                    error.toString());
            RateLimitDecision decision = RateLimitDecision.failOpen(bucket.getLimit());
            HttpHeaders headers = exchange.getResponse().getHeaders();
            writeRateLimitHeaders(headers, decision);
            headers.add("X-RateLimit-Degraded", "fail-open");
            return chain.filter(exchange);
        }
        log.warn("Rate-limit backend unavailable, failing CLOSED (rejecting request): {}",
                error.toString());
        // Fail closed => backend protection cannot be evaluated, so shed load with 503.
        return writeError(exchange, HttpStatus.SERVICE_UNAVAILABLE,
                Map.of("error", "rate_limit_unavailable",
                        "message", "Rate limiter backend is unavailable and the gateway is configured to fail closed."));
    }

    private void writeRateLimitHeaders(HttpHeaders headers, RateLimitDecision decision) {
        headers.set("X-RateLimit-Limit", Long.toString(decision.limit()));
        headers.set("X-RateLimit-Remaining", Long.toString(decision.remaining()));
        headers.set("X-RateLimit-Reset", Long.toString(decision.resetEpochSeconds()));
    }

    private Mono<Void> reject(ServerWebExchange exchange, RateLimitDecision decision) {
        ServerHttpResponse response = exchange.getResponse();
        response.getHeaders().set("Retry-After", Long.toString(decision.retryAfterSeconds()));
        return writeError(exchange, HttpStatus.TOO_MANY_REQUESTS,
                Map.of("error", "rate_limit_exceeded",
                        "message", "Rate limit exceeded. Retry after " + decision.retryAfterSeconds() + "s.",
                        "limit", decision.limit(),
                        "retryAfterSeconds", decision.retryAfterSeconds()));
    }

    private Mono<Void> writeError(ServerWebExchange exchange, HttpStatus status, Map<String, Object> body) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(body);
        } catch (JsonProcessingException e) {
            bytes = ("{\"error\":\"" + status.getReasonPhrase() + "\"}").getBytes();
        }
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }
}
