package com.koutilya.gateway.ratelimit;

import reactor.core.publisher.Mono;

/**
 * A rate-limit strategy. Extracting this interface keeps {@link RateLimitGlobalFilter} decoupled
 * from the Redis-backed implementation (dependency inversion): the filter is unit-testable with a
 * simple hand-written fake, and the limiting algorithm/backend can be swapped without touching the
 * filter.
 */
public interface RateLimiter {

    /**
     * Evaluate one request against a bucket.
     *
     * @param bucketKey logical identity the quota is keyed on (tenant id, or {@code anon:<ip>})
     * @param limit     max requests allowed within the window
     * @param windowMs  window size in milliseconds
     * @return a decision; the Mono errors if the backend is unreachable, letting the caller apply
     *         its fail-open / fail-closed policy
     */
    Mono<RateLimitDecision> isAllowed(String bucketKey, int limit, long windowMs);
}
