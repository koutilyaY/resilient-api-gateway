package com.koutilya.gateway.ratelimit;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.Map;

/**
 * Config-driven rate-limit policy.
 *
 * <pre>
 * gateway:
 *   ratelimit:
 *     enabled: true
 *     fail-open: true          # on Redis outage: true = allow, false = reject
 *     default-bucket: { limit: 100, window-seconds: 60 }
 *     tenants:
 *       tenant-a:  { limit: 5,    window-seconds: 10 }
 *       premium:   { limit: 1000, window-seconds: 60 }
 *     api-keys:
 *       key-abc:     tenant-a
 *       key-premium: premium
 * </pre>
 */
@Validated
@ConfigurationProperties(prefix = "gateway.ratelimit")
public class RateLimitProperties {

    /** Master switch for the whole rate-limit filter. */
    private boolean enabled = true;

    /**
     * Behaviour when Redis is unreachable. Fail-open favours availability (let traffic
     * through, protection temporarily off); fail-closed favours protection (reject).
     */
    private boolean failOpen = true;

    /** Path the circuit-breaker fallback forwards to; never rate limited. */
    private String fallbackPath = "/__fallback";

    /** Applied to any caller not matched to a configured tenant. */
    @NotNull
    private Bucket defaultBucket = new Bucket(100, 60);

    /** tenantId -> quota. */
    private Map<String, Bucket> tenants = new HashMap<>();

    /** X-API-Key value -> tenantId. */
    private Map<String, String> apiKeys = new HashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isFailOpen() {
        return failOpen;
    }

    public void setFailOpen(boolean failOpen) {
        this.failOpen = failOpen;
    }

    public String getFallbackPath() {
        return fallbackPath;
    }

    public void setFallbackPath(String fallbackPath) {
        this.fallbackPath = fallbackPath;
    }

    public Bucket getDefaultBucket() {
        return defaultBucket;
    }

    public void setDefaultBucket(Bucket defaultBucket) {
        this.defaultBucket = defaultBucket;
    }

    public Map<String, Bucket> getTenants() {
        return tenants;
    }

    public void setTenants(Map<String, Bucket> tenants) {
        this.tenants = tenants;
    }

    public Map<String, String> getApiKeys() {
        return apiKeys;
    }

    public void setApiKeys(Map<String, String> apiKeys) {
        this.apiKeys = apiKeys;
    }

    /** A single quota: {@code limit} requests per {@code windowSeconds}. */
    public static class Bucket {

        @Min(1)
        private int limit;

        @Min(1)
        private long windowSeconds;

        public Bucket() {
        }

        public Bucket(int limit, long windowSeconds) {
            this.limit = limit;
            this.windowSeconds = windowSeconds;
        }

        public int getLimit() {
            return limit;
        }

        public void setLimit(int limit) {
            this.limit = limit;
        }

        public long getWindowSeconds() {
            return windowSeconds;
        }

        public void setWindowSeconds(long windowSeconds) {
            this.windowSeconds = windowSeconds;
        }

        public long windowMillis() {
            return windowSeconds * 1000L;
        }
    }
}
