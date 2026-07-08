// k6 load test for the Resilient API Gateway.
//
// Two things are exercised:
//   1. Steady throughput through the gateway (to measure added overhead / p99).
//   2. Rate-limit enforcement: a "free" tenant (5 req / 10s) should see a wave of 429s,
//      while a "premium" tenant (1000 req / 60s) sails through.
//
// Run (gateway + redis + echo up via docker-compose):
//   k6 run loadtest/ratelimit.js
//   k6 run -e BASE_URL=http://localhost:8080 -e RPS=3200 -e DURATION=30s loadtest/ratelimit.js
//
// NOTE: any throughput / latency figures you quote from this are LOCAL load-test numbers,
// not production SLAs.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RPS = parseInt(__ENV.RPS || '3200', 10);
const DURATION = __ENV.DURATION || '30s';

const rejected429 = new Counter('rate_limited_429');
const allowed2xx = new Counter('allowed_2xx');
const gatewayLatency = new Trend('gateway_latency_ms', true);

export const options = {
  scenarios: {
    // High steady load on the premium tenant: measures gateway overhead at target RPS.
    steady_premium: {
      executor: 'constant-arrival-rate',
      rate: RPS,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 200,
      maxVUs: 2000,
      exec: 'premiumTraffic',
    },
    // Bursty free tenant: deliberately trips the limiter so we see 429s.
    bursty_free: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 200,
      exec: 'freeTraffic',
    },
  },
  thresholds: {
    // The overhead claim lives or dies here. Tune to your hardware.
    'gateway_latency_ms': ['p(99)<6'],
    'http_req_failed': ['rate<0.5'], // 429s are expected on the free scenario
  },
};

export function premiumTraffic() {
  const res = http.get(`${BASE_URL}/echo/load`, {
    headers: { 'X-API-Key': 'key-premium' },
  });
  gatewayLatency.add(res.timings.duration);
  if (res.status >= 200 && res.status < 300) allowed2xx.add(1);
  check(res, { 'premium not rate limited': (r) => r.status !== 429 });
}

export function freeTraffic() {
  const res = http.get(`${BASE_URL}/echo/load`, {
    headers: { 'X-API-Key': 'key-free' },
  });
  if (res.status === 429) {
    rejected429.add(1);
    check(res, {
      'has Retry-After': (r) => r.headers['Retry-After'] !== undefined,
      'has X-RateLimit-Remaining': (r) => r.headers['X-Ratelimit-Remaining'] !== undefined,
    });
  } else if (res.status >= 200 && res.status < 300) {
    allowed2xx.add(1);
  }
}
