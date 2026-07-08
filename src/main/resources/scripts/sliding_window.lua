-- Atomic sliding-window-log rate limiter.
--
-- The whole read-modify-write happens inside Redis in a single script execution,
-- so there is no read-then-write race between concurrent gateway instances.
--
-- KEYS[1] = bucket key (e.g. "ratelimit:{tenant-a}")
-- ARGV[1] = now            (epoch milliseconds, supplied by the caller = the gateway)
-- ARGV[2] = window         (window size in milliseconds)
-- ARGV[3] = limit          (max requests permitted within the window)
-- ARGV[4] = member         (globally-unique id for THIS request; keeps ZSET members distinct)
--
-- Returns { allowed, remaining, limit, reset }
--   allowed   = 1 if the request is permitted, else 0
--   remaining = requests still available in the current window
--   limit     = the configured limit (echoed back for header building)
--   reset     = epoch millis at which the window will have room again

local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]

-- 1. Evict timestamps that have slid out of the trailing window.
local window_start = now - window
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- 2. How many requests are currently inside the window?
local count = redis.call('ZCARD', key)

local allowed = 0
local remaining = 0

if count < limit then
    -- 3a. Room available: record this request's timestamp.
    redis.call('ZADD', key, now, member)
    allowed = 1
    remaining = limit - count - 1
else
    -- 3b. Over the limit: do NOT record it (rejected requests must not extend the window).
    allowed = 0
    remaining = 0
end

-- 4. Let idle buckets expire so we never leak memory for quiet tenants.
--    +1s of slack guards against rounding at the edge of the window.
redis.call('PEXPIRE', key, window + 1000)

-- 5. Compute when the caller can expect capacity again.
--    When allowed  -> next reset is a full window out (worst case for a burst).
--    When rejected -> it is when the OLDEST in-window request expires.
local reset = now + window
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if oldest[2] then
    reset = tonumber(oldest[2]) + window
end

return { allowed, remaining, limit, reset }
