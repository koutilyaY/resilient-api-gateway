package com.koutilya.gateway.ratelimit;

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
 * Full-stack test: real gateway (random port), embedded Redis, WireMock downstream. Proves the
 * end-to-end contract — N requests pass, then 429 with the documented headers, and quotas are
 * isolated per tenant. Docker-free, so it runs under the default {@code mvn test}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RateLimitFilterIntegrationTest {

    private static final EmbeddedRedis REDIS = EmbeddedRedis.startOnRandomPort();
    private static final WireMockServer WIREMOCK = new WireMockServer(options().dynamicPort());

    static {
        WIREMOCK.start();
        WIREMOCK.stubFor(any(urlMatching(".*"))
                .willReturn(aResponse().withStatus(200).withBody("{\"ok\":true}")
                        .withHeader("Content-Type", "application/json")));
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", () -> "localhost");
        registry.add("spring.data.redis.port", REDIS::port);
        registry.add("DOWNSTREAM_ECHO_URI", WIREMOCK::baseUrl);
        registry.add("DOWNSTREAM_FLAKY_URI", WIREMOCK::baseUrl);
        // Deterministic small quotas for the test tenants.
        registry.add("gateway.ratelimit.tenants.itest.limit", () -> "3");
        registry.add("gateway.ratelimit.tenants.itest.window-seconds", () -> "60");
        registry.add("gateway.ratelimit.tenants.itest-other.limit", () -> "3");
        registry.add("gateway.ratelimit.tenants.itest-other.window-seconds", () -> "60");
    }

    @AfterAll
    static void tearDown() {
        WIREMOCK.stop();
        REDIS.stop();
    }

    @LocalServerPort
    int port;

    private WebTestClient client;

    private WebTestClient client() {
        if (client == null) {
            client = WebTestClient.bindToServer()
                    .baseUrl("http://localhost:" + port)
                    .responseTimeout(Duration.ofSeconds(10))
                    .build();
        }
        return client;
    }

    @Test
    void allowsUpToLimitThenReturns429WithHeaders() {
        // First 3 requests succeed for tenant "itest" (limit 3).
        for (int i = 0; i < 3; i++) {
            client().get().uri("/echo/thing")
                    .header(TenantResolver.HEADER_TENANT, "itest")
                    .exchange()
                    .expectStatus().isOk()
                    .expectHeader().valueEquals("X-RateLimit-Limit", "3");
        }

        // 4th is rejected at the edge with 429 + Retry-After.
        client().get().uri("/echo/thing")
                .header(TenantResolver.HEADER_TENANT, "itest")
                .exchange()
                .expectStatus().isEqualTo(429)
                .expectHeader().valueEquals("X-RateLimit-Remaining", "0")
                .expectHeader().exists("Retry-After")
                .expectBody()
                .jsonPath("$.error").isEqualTo("rate_limit_exceeded");
    }

    @Test
    void quotasAreIsolatedPerTenant() {
        // Exhaust tenant "itest-other".
        for (int i = 0; i < 3; i++) {
            client().get().uri("/echo/thing")
                    .header(TenantResolver.HEADER_TENANT, "itest-other")
                    .exchange()
                    .expectStatus().isOk();
        }
        client().get().uri("/echo/thing")
                .header(TenantResolver.HEADER_TENANT, "itest-other")
                .exchange()
                .expectStatus().isEqualTo(429);

        // A different tenant is completely unaffected.
        client().get().uri("/echo/thing")
                .header(TenantResolver.HEADER_TENANT, "premium")
                .exchange()
                .expectStatus().isOk();
    }
}
