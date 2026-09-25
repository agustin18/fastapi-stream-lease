from __future__ import annotations

# Atomic Lua script to acquire a stream lease:
# Checks and clears expired leases, validates user and global limits, and adds the lease.
# Returns:
#   1 -> Acquired successfully
#   2 -> User stream limit reached
#   3 -> Global stream limit reached
ACQUIRE_SCRIPT = """
local user_key = KEYS[1]
local global_key = KEYS[2]
local now = tonumber(ARGV[1])
local expires = tonumber(ARGV[2])
local lease_id = ARGV[3]
local user_limit = tonumber(ARGV[4])
local global_limit = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])

redis.call('ZREMRANGEBYSCORE', user_key, '-inf', now)
if global_limit > 0 then
    redis.call('ZREMRANGEBYSCORE', global_key, '-inf', now)
end

if user_limit > 0 and redis.call('ZCARD', user_key) >= user_limit then
    return 2
end

if global_limit > 0 and redis.call('ZCARD', global_key) >= global_limit then
    return 3
end

redis.call('ZADD', user_key, expires, lease_id)
redis.call('EXPIRE', user_key, ttl)

if global_limit > 0 then
    redis.call('ZADD', global_key, expires, lease_id)
    redis.call('EXPIRE', global_key, ttl)
end

return 1
"""

# Atomic Lua script to renew an active stream lease:
# Verifies the lease is still active in sets and extends its expiration.
# Returns:
#   1 -> Renewed successfully
#   0 -> Lease not found or already expired
RENEW_SCRIPT = """
local user_key = KEYS[1]
local global_key = KEYS[2]
local lease_id = ARGV[1]
local new_expires = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local check_global = tonumber(ARGV[4]) or 1

if not redis.call('ZSCORE', user_key, lease_id) then
    return 0
end

if check_global > 0 and not redis.call('ZSCORE', global_key, lease_id) then
    return 0
end

redis.call('ZADD', user_key, new_expires, lease_id)
redis.call('EXPIRE', user_key, ttl)

if check_global > 0 then
    redis.call('ZADD', global_key, new_expires, lease_id)
    redis.call('EXPIRE', global_key, ttl)
end

return 1
"""

# Atomic Lua script to release a stream lease immediately:
# Removes the lease from both user and global sets.
RELEASE_SCRIPT = """
local user_key = KEYS[1]
local global_key = KEYS[2]
local lease_id = ARGV[1]

redis.call('ZREM', user_key, lease_id)
redis.call('ZREM', global_key, lease_id)

return 1
"""

# Atomic script to clean expired elements and return active stream count:
COUNT_SCRIPT = """
local target_key = KEYS[1]
local now = tonumber(ARGV[1])

redis.call('ZREMRANGEBYSCORE', target_key, '-inf', now)
return redis.call('ZCARD', target_key)
"""
