package com.koutilya.gateway.resilience;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Circuit-breaker fallback. Routes configure {@code fallbackUri: forward:/__fallback}; when a
 * downstream call fails, times out, or the breaker is open, the request is forwarded here and
 * we return a clean 503 instead of leaking the raw downstream error (or hanging).
 */
@RestController
public class FallbackController {

    @RequestMapping(value = "/__fallback", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Map<String, Object>>> fallback() {
        return Mono.just(ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("X-Circuit-Fallback", "true")
                .body(Map.of(
                        "error", "service_unavailable",
                        "message", "Downstream service is unavailable or the circuit is open. Please retry later.")));
    }
}
