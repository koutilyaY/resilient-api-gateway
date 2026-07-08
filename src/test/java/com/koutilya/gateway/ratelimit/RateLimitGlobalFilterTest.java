package com.koutilya.gateway.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the filter's control flow in isolation (no Redis): header emission, 429 rejection, and
 * the fail-open / fail-closed policy when the backend errors.
 *
 * <p>Deliberately uses a hand-written {@link RateLimiter} fake instead of a mocking framework:
 * the filter depends on the {@code RateLimiter} interface (dependency inversion), so no bytecode
 * instrumentation is needed. This keeps the test fast and portable across JDKs — it runs
 * identically on Java 17 and Java 26, where inline mock makers can lag new JDK releases.
 */
class RateLimitGlobalFilterTest {

    /** Canned rate limiter: returns a fixed decision, or errors to simulate a Redis outage. */
    private static final class FakeRateLimiter implements RateLimiter {
        private final RateLimitDecision decision;
        private final RuntimeException error;

        private FakeRateLimiter(RateLimitDecision decision, RuntimeException error) {
            this.decision = decision;
            this.error = error;
        }

        static FakeRateLimiter returning(RateLimitDecision decision) {
            return new FakeRateLimiter(decision, null);
        }

        static FakeRateLimiter failingWith(RuntimeException error) {
            return new FakeRateLimiter(null, error);
        }

        @Override
        public Mono<RateLimitDecision> isAllowed(String bucketKey, int limit, long windowMs) {
            return error != null ? Mono.error(error) : Mono.just(decision);
        }
    }

    private static final class RecordingChain implements GatewayFilterChain {
        final AtomicBoolean called = new AtomicBoolean(false);

        @Override
        public Mono<Void> filter(org.springframework.web.server.ServerWebExchange exchange) {
            called.set(true);
            return Mono.empty();
        }
    }

    private RateLimitProperties propsWithFailOpen(boolean failOpen) {
        RateLimitProperties props = new RateLimitProperties();
        props.setFailOpen(failOpen);
        props.setDefaultBucket(new RateLimitProperties.Bucket(10, 60));
        return props;
    }

    private MockServerWebExchange exchange() {
        return MockServerWebExchange.from(
                MockServerHttpRequest.get("/echo/thing").remoteAddress(
                        new java.net.InetSocketAddress("10.0.0.5", 12345)));
    }

    @Test
    void allowsAndWritesRateLimitHeaders() {
        RateLimiter limiter = FakeRateLimiter.returning(
                new RateLimitDecision(true, 10, 7, System.currentTimeMillis() + 60000, false));
        RateLimitProperties props = propsWithFailOpen(true);
        RateLimitGlobalFilter filter = new RateLimitGlobalFilter(props, new TenantResolver(props), limiter);

        MockServerWebExchange exchange = exchange();
        RecordingChain chain = new RecordingChain();

        filter.filter(exchange, chain).block();

        assertThat(chain.called).isTrue();
        assertThat(exchange.getResponse().getHeaders().getFirst("X-RateLimit-Limit")).isEqualTo("10");
        assertThat(exchange.getResponse().getHeaders().getFirst("X-RateLimit-Remaining")).isEqualTo("7");
        assertThat(exchange.getResponse().getHeaders().getFirst("X-RateLimit-Reset")).isNotNull();
    }

    @Test
    void rejectsWith429AndRetryAfterWhenOverLimit() {
        RateLimiter limiter = FakeRateLimiter.returning(
                new RateLimitDecision(false, 10, 0, System.currentTimeMillis() + 5000, false));
        RateLimitProperties props = propsWithFailOpen(true);
        RateLimitGlobalFilter filter = new RateLimitGlobalFilter(props, new TenantResolver(props), limiter);

        MockServerWebExchange exchange = exchange();
        RecordingChain chain = new RecordingChain();

        filter.filter(exchange, chain).block();

        assertThat(chain.called).isFalse(); // shed at the edge, downstream never reached
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After")).isNotNull();
        assertThat(exchange.getResponse().getHeaders().getFirst("X-RateLimit-Remaining")).isEqualTo("0");
    }

    @Test
    void failsOpenWhenBackendErrors() {
        RateLimiter limiter = FakeRateLimiter.failingWith(new RuntimeException("redis down"));
        RateLimitProperties props = propsWithFailOpen(true);
        RateLimitGlobalFilter filter = new RateLimitGlobalFilter(props, new TenantResolver(props), limiter);

        MockServerWebExchange exchange = exchange();
        RecordingChain chain = new RecordingChain();

        filter.filter(exchange, chain).block();

        assertThat(chain.called).isTrue(); // request allowed through
        assertThat(exchange.getResponse().getHeaders().getFirst("X-RateLimit-Degraded")).isEqualTo("fail-open");
    }

    @Test
    void failsClosedWhenBackendErrorsAndConfigured() {
        RateLimiter limiter = FakeRateLimiter.failingWith(new RuntimeException("redis down"));
        RateLimitProperties props = propsWithFailOpen(false);
        RateLimitGlobalFilter filter = new RateLimitGlobalFilter(props, new TenantResolver(props), limiter);

        MockServerWebExchange exchange = exchange();
        RecordingChain chain = new RecordingChain();

        filter.filter(exchange, chain).block();

        assertThat(chain.called).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }
}
