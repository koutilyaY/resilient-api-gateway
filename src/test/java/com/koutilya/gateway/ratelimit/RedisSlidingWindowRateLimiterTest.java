package com.koutilya.gateway.ratelimit;

import com.koutilya.gateway.support.EmbeddedRedis;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the atomic sliding-window limiter against a real (embedded) Redis.
 * No Docker, no network — the binary is bundled and started on a random port.
 */
class RedisSlidingWindowRateLimiterTest {

    private static EmbeddedRedis redis;
    private static LettuceConnectionFactory connectionFactory;
    private static RedisSlidingWindowRateLimiter limiter;

    @BeforeAll
    static void startRedis() {
        redis = EmbeddedRedis.startOnRandomPort();
        connectionFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration("localhost", redis.port()));
        connectionFactory.afterPropertiesSet();

        ReactiveStringRedisTemplate template = new ReactiveStringRedisTemplate(connectionFactory);
        limiter = new RedisSlidingWindowRateLimiter(template, loadScript(), new SimpleMeterRegistry());
    }

    @AfterAll
    static void stopRedis() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    @SuppressWarnings("rawtypes")
    private static RedisScript<List> loadScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/sliding_window.lua"));
        script.setResultType(List.class);
        return script;
    }

    private String freshKey() {
        return "test-" + UUID.randomUUID();
    }

    @Test
    void allowsUpToLimitThenReturns429() {
        String key = freshKey();
        int limit = 3;
        long windowMs = 60_000;

        StepVerifier.create(limiter.isAllowed(key, limit, windowMs))
                .assertNext(d -> {
                    assertThat(d.allowed()).isTrue();
                    assertThat(d.remaining()).isEqualTo(2);
                    assertThat(d.limit()).isEqualTo(3);
                })
                .verifyComplete();

        StepVerifier.create(limiter.isAllowed(key, limit, windowMs))
                .assertNext(d -> assertThat(d.remaining()).isEqualTo(1))
                .verifyComplete();

        StepVerifier.create(limiter.isAllowed(key, limit, windowMs))
                .assertNext(d -> {
                    assertThat(d.allowed()).isTrue();
                    assertThat(d.remaining()).isEqualTo(0);
                })
                .verifyComplete();

        // 4th request within the window is rejected.
        StepVerifier.create(limiter.isAllowed(key, limit, windowMs))
                .assertNext(d -> {
                    assertThat(d.allowed()).isFalse();
                    assertThat(d.remaining()).isEqualTo(0);
                    assertThat(d.retryAfterSeconds()).isGreaterThanOrEqualTo(1);
                })
                .verifyComplete();
    }

    @Test
    void windowRollsOverAndReAllows() throws InterruptedException {
        String key = freshKey();
        int limit = 2;
        long windowMs = 1_000;

        // Exhaust the window.
        limiter.isAllowed(key, limit, windowMs).block();
        limiter.isAllowed(key, limit, windowMs).block();

        StepVerifier.create(limiter.isAllowed(key, limit, windowMs))
                .assertNext(d -> assertThat(d.allowed()).isFalse())
                .verifyComplete();

        // Let the whole window slide past.
        Thread.sleep(1_200);

        StepVerifier.create(limiter.isAllowed(key, limit, windowMs))
                .assertNext(d -> {
                    assertThat(d.allowed()).isTrue();
                    assertThat(d.remaining()).isEqualTo(1);
                })
                .verifyComplete();
    }

    @Test
    void tenantsAreIsolatedFromEachOther() {
        String tenantA = freshKey();
        String tenantB = freshKey();
        int limit = 1;
        long windowMs = 60_000;

        // Tenant A consumes its only slot.
        limiter.isAllowed(tenantA, limit, windowMs).block();
        StepVerifier.create(limiter.isAllowed(tenantA, limit, windowMs))
                .assertNext(d -> assertThat(d.allowed()).isFalse())
                .verifyComplete();

        // Tenant B is unaffected.
        StepVerifier.create(limiter.isAllowed(tenantB, limit, windowMs))
                .assertNext(d -> assertThat(d.allowed()).isTrue())
                .verifyComplete();
    }

    @Test
    void propagatesErrorWhenRedisUnreachable() {
        // Point a limiter at a port with nothing listening -> the reactive call errors,
        // which the filter layer turns into fail-open / fail-closed behaviour.
        LettuceConnectionFactory dead = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration("localhost", 1));
        dead.afterPropertiesSet();
        RedisSlidingWindowRateLimiter deadLimiter = new RedisSlidingWindowRateLimiter(
                new ReactiveStringRedisTemplate(dead), loadScript(), new SimpleMeterRegistry());

        StepVerifier.create(deadLimiter.isAllowed(freshKey(), 5, 1_000))
                .expectError()
                .verify();

        dead.destroy();
    }
}
