package com.koutilya.gateway.ratelimit;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Distributed sliding-window-log rate limiter backed by Redis.
 *
 * <p>The entire read-modify-write is executed as a single Lua script inside Redis, which
 * makes the counter update <em>atomic</em>: two gateway instances evaluating the same
 * tenant concurrently can never both read a stale count and both admit a request that
 * pushes the tenant over its limit. There is deliberately no read-then-write in Java.
 *
 * <p>The decision latency (the overhead the limiter adds to each request) is recorded in a
 * Micrometer timer so the "p99 gateway overhead" claim is actually measurable.
 */
public class RedisSlidingWindowRateLimiter implements RateLimiter {

    private final ReactiveStringRedisTemplate redis;
    private final RedisScript<List> script;
    private final Timer decisionTimer;

    public RedisSlidingWindowRateLimiter(ReactiveStringRedisTemplate redis,
                                         RedisScript<List> script,
                                         MeterRegistry meterRegistry) {
        this.redis = redis;
        this.script = script;
        this.decisionTimer = Timer.builder("gateway.ratelimit.decision")
                .description("Latency of a single Redis-backed rate-limit decision (gateway overhead)")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    /**
     * Evaluate one request against a tenant bucket.
     *
     * @param bucketKey  logical identity the quota is keyed on (tenant id, or {@code anon:<ip>})
     * @param limit      max requests allowed within the window
     * @param windowMs   window size in milliseconds
     * @return a decision; errors (e.g. Redis down) are propagated so the caller can apply
     *         its fail-open / fail-closed policy
     */
    @Override
    public Mono<RateLimitDecision> isAllowed(String bucketKey, int limit, long windowMs) {
        long now = Instant.now().toEpochMilli();
        // {..} hash-tag keeps the key on a single slot if Redis is ever clustered.
        String redisKey = "ratelimit:{" + bucketKey + "}";
        String member = now + "-" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong());

        List<String> keys = List.of(redisKey);
        List<String> args = List.of(
                Long.toString(now),
                Long.toString(windowMs),
                Integer.toString(limit),
                member);

        Timer.Sample sample = Timer.start();
        return redis.execute(script, keys, args)
                .single()
                .map(raw -> toDecision(raw, limit))
                .doOnEach(signal -> {
                    if (signal.isOnNext() || signal.isOnError()) {
                        sample.stop(decisionTimer);
                    }
                });
    }

    @SuppressWarnings("unchecked")
    private RateLimitDecision toDecision(Object raw, int limit) {
        List<Object> result = (List<Object>) raw;
        long allowed = asLong(result.get(0));
        long remaining = asLong(result.get(1));
        long echoedLimit = asLong(result.get(2));
        long reset = asLong(result.get(3));
        return new RateLimitDecision(
                allowed == 1L,
                (int) echoedLimit,
                Math.max(0, remaining),
                reset,
                false);
    }

    /**
     * Redis integer replies come back as {@link Long} in most cases, but depending on the
     * serializer they may arrive as {@link String}; handle both defensively.
     */
    private static long asLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }
}
