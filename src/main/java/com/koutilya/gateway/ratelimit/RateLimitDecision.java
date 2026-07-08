package com.koutilya.gateway.ratelimit;

/**
 * Outcome of a single rate-limit evaluation.
 *
 * @param allowed     whether the request may proceed
 * @param limit       the configured limit for the matched bucket
 * @param remaining   requests still available in the current window (never negative)
 * @param resetEpochMs epoch millis at which capacity is expected to free up
 * @param failedOpen  true when this decision was granted because Redis was unreachable
 *                    and the gateway is configured to fail open
 */
public record RateLimitDecision(
        boolean allowed,
        int limit,
        long remaining,
        long resetEpochMs,
        boolean failedOpen) {

    public static RateLimitDecision failOpen(int limit) {
        return new RateLimitDecision(true, limit, limit, System.currentTimeMillis(), true);
    }

    /** Seconds a client should wait before retrying (for the Retry-After header). */
    public long retryAfterSeconds() {
        long deltaMs = resetEpochMs - System.currentTimeMillis();
        return Math.max(1, (long) Math.ceil(deltaMs / 1000.0));
    }

    /** Epoch seconds form of the reset instant (for X-RateLimit-Reset). */
    public long resetEpochSeconds() {
        return resetEpochMs / 1000L;
    }
}
