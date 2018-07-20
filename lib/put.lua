local current_timestamp_ms = tonumber(ARGV[1])
local new_content          = tonumber(ARGV[2])
local ttl                  = tonumber(ARGV[3])

redis.call('HMSET', KEYS[1],
            'last_drip', current_timestamp_ms,
            'content', new_content)
redis.call('EXPIRE', KEYS[1], ttl)

return { current_timestamp_ms, new_content }
