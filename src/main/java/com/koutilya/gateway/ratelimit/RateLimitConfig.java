package com.koutilya.gateway.ratelimit;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

/**
 * Wires the Lua script and the Redis-backed rate limiter.
 */
@Configuration
public class RateLimitConfig {

    /** Loads the atomic sliding-window script; Spring caches its SHA and uses EVALSHA. */
    @Bean
    @SuppressWarnings("rawtypes")
    public RedisScript<List> slidingWindowScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/sliding_window.lua"));
        script.setResultType(List.class);
        return script;
    }

    @Bean
    public RedisSlidingWindowRateLimiter rateLimiter(ReactiveStringRedisTemplate redisTemplate,
                                                     @SuppressWarnings("rawtypes") RedisScript<List> slidingWindowScript,
                                                     MeterRegistry meterRegistry) {
        return new RedisSlidingWindowRateLimiter(redisTemplate, slidingWindowScript, meterRegistry);
    }
}
