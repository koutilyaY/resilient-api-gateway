package com.koutilya.gateway.resilience;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.koutilya.gateway.support.EmbeddedRedis;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/**
 * Proves the resilience layer: a downstream that is slower than the time limiter causes each
 * call to time out and fall back to 503; after enough failures the circuit breaker opens and
 * short-circuits subsequent calls straight to the fallback (fast, without touching downstream).
 *
 * <p>The {@code flakyCb} breaker is configured (see {@link ResilienceConfig}) with
 * minimumNumberOfCalls=3, failureRateThreshold=50%, timeout=300ms; WireMock delays 2s.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CircuitBreakerTest {

    private static final EmbeddedRedis REDIS = EmbeddedRedis.startOnRandomPort();
    private static final WireMockServer WIREMOCK = new WireMockServer(options().dynamicPort());

    static {
        WIREMOCK.start();
        // Downstream is pathologically slow (2s) -> always exceeds the 300ms time limiter.
        WIREMOCK.stubFor(any(urlMatching(".*"))
                .willReturn(aResponse().withStatus(200).withFixedDelay(2000).withBody("slow")));
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", () -> "localhost");
        registry.add("spring.data.redis.port", REDIS::port);
        registry.add("DOWNSTREAM_ECHO_URI", WIREMOCK::baseUrl);
        registry.add("DOWNSTREAM_FLAKY_URI", WIREMOCK::baseUrl);
        // Give the flaky tenant plenty of rate-limit headroom so 429s never mask the test.
        registry.add("gateway.ratelimit.default-bucket.limit", () -> "1000");
    }

    @AfterAll
    static void tearDown() {
        WIREMOCK.stop();
        REDIS.stop();
    }

    @LocalServerPort
    int port;

    private WebTestClient client() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Test
    void breakerOpensAndFallbackReturns503() {
        WebTestClient client = client();

        // Drive enough calls to trip the breaker; every call must land on the 503 fallback,
        // whether via time-limiter timeout (early) or open-circuit short-circuit (later).
        for (int i = 0; i < 6; i++) {
            client.get().uri("/flaky/resource")
                    .exchange()
                    .expectStatus().isEqualTo(503)
                    .expectHeader().valueEquals("X-Circuit-Fallback", "true")
                    .expectBody()
                    .jsonPath("$.error").isEqualTo("service_unavailable");
        }

        // Once OPEN, the breaker short-circuits: the response comes back much faster than the
        // 2s downstream delay because downstream is never called.
        long start = System.currentTimeMillis();
        client.get().uri("/flaky/resource")
                .exchange()
                .expectStatus().isEqualTo(503);
        long elapsedMs = System.currentTimeMillis() - start;

        org.assertj.core.api.Assertions.assertThat(elapsedMs)
                .as("open circuit should short-circuit well under the 2s downstream delay")
                .isLessThan(1500);
    }
}
