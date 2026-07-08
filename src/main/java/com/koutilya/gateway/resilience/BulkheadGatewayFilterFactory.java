package com.koutilya.gateway.resilience;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-route bulkhead: caps the number of requests that may be <em>in flight to a downstream
 * at once</em>. This isolates a slow dependency so it cannot consume the whole gateway's
 * capacity (thread/connection/latency budget) and starve healthy routes.
 *
 * <p>The permit is held for the full lifetime of {@code chain.filter(exchange)} — i.e. until
 * the downstream response completes — so it genuinely bounds concurrency, not just admission.
 * When the bulkhead is saturated the request is shed immediately with 503 rather than queuing
 * unboundedly.
 *
 * <p>Usage in application.yml:
 * <pre>
 * - name: Bulkhead
 *   args: { name: flakyBh, maxConcurrentCalls: 20, maxWaitMillis: 0 }
 * </pre>
 */
@Component
public class BulkheadGatewayFilterFactory
        extends AbstractGatewayFilterFactory<BulkheadGatewayFilterFactory.Config> {

    private final BulkheadRegistry registry = BulkheadRegistry.ofDefaults();
    private final ConcurrentMap<String, Bulkhead> bulkheads = new ConcurrentHashMap<>();

    public BulkheadGatewayFilterFactory() {
        super(Config.class);
    }

    @Override
    public List<String> shortcutFieldOrder() {
        return List.of("name", "maxConcurrentCalls", "maxWaitMillis");
    }

    @Override
    public GatewayFilter apply(Config config) {
        Bulkhead bulkhead = bulkheads.computeIfAbsent(config.getName(), name ->
                registry.bulkhead(name, BulkheadConfig.custom()
                        .maxConcurrentCalls(config.getMaxConcurrentCalls())
                        .maxWaitDuration(Duration.ofMillis(config.getMaxWaitMillis()))
                        .build()));

        return (exchange, chain) -> chain.filter(exchange)
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .onErrorResume(BulkheadFullException.class, ex -> {
                    exchange.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
                    exchange.getResponse().getHeaders().add("X-Bulkhead-Rejected", config.getName());
                    return exchange.getResponse().setComplete();
                });
    }

    public static class Config {
        private String name = "default";
        private int maxConcurrentCalls = 25;
        private long maxWaitMillis = 0;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getMaxConcurrentCalls() {
            return maxConcurrentCalls;
        }

        public void setMaxConcurrentCalls(int maxConcurrentCalls) {
            this.maxConcurrentCalls = maxConcurrentCalls;
        }

        public long getMaxWaitMillis() {
            return maxWaitMillis;
        }

        public void setMaxWaitMillis(long maxWaitMillis) {
            this.maxWaitMillis = maxWaitMillis;
        }
    }
}
