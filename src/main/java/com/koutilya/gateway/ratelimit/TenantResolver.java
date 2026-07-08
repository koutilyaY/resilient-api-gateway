package com.koutilya.gateway.ratelimit;

import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;

/**
 * Resolves the caller identity that a quota is keyed on.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>{@code X-Tenant-Id} header (explicit tenant).</li>
 *   <li>{@code X-API-Key} header, mapped to a tenant via {@code gateway.ratelimit.api-keys}.</li>
 *   <li>Neither present: treat as anonymous and key on the client IP with the default bucket,
 *       so one noisy anonymous source cannot exhaust a single shared global bucket.</li>
 * </ol>
 * A tenant id that is present but not configured falls back to the default bucket while still
 * being keyed on its own identity.
 */
@Component
public class TenantResolver {

    public static final String HEADER_TENANT = "X-Tenant-Id";
    public static final String HEADER_API_KEY = "X-API-Key";

    private final RateLimitProperties properties;

    public TenantResolver(RateLimitProperties properties) {
        this.properties = properties;
    }

    public Resolution resolve(ServerHttpRequest request) {
        String tenantId = request.getHeaders().getFirst(HEADER_TENANT);

        if (tenantId == null) {
            String apiKey = request.getHeaders().getFirst(HEADER_API_KEY);
            if (apiKey != null) {
                tenantId = properties.getApiKeys().get(apiKey);
            }
        }

        if (tenantId != null) {
            RateLimitProperties.Bucket configured = properties.getTenants().get(tenantId);
            RateLimitProperties.Bucket bucket = (configured != null) ? configured : properties.getDefaultBucket();
            return new Resolution(tenantId, bucket);
        }

        // Anonymous: key on client IP so quotas isolate per source.
        return new Resolution("anon:" + clientIp(request), properties.getDefaultBucket());
    }

    private String clientIp(ServerHttpRequest request) {
        String forwarded = request.getHeaders().getFirst("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        if (request.getRemoteAddress() != null && request.getRemoteAddress().getAddress() != null) {
            return request.getRemoteAddress().getAddress().getHostAddress();
        }
        return "unknown";
    }

    /**
     * @param bucketKey identity the quota is counted against
     * @param bucket    quota applied to it
     */
    public record Resolution(String bucketKey, RateLimitProperties.Bucket bucket) {
    }
}
