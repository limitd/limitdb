local current_timestamp_ms = tonumber(ARGV[1])
local tokens_to_add        = tonumber(ARGV[2])
local bucket_size          = tonumber(ARGV[3])
local ttl                  = tonumber(ARGV[4])

local current = redis.call('HMGET', KEYS[1], 'r')

if current[1] then
  tokens_to_add = math.min(current[1] + tokens_to_add, bucket_size)
end

if tokens_to_add < bucket_size then
  redis.call('HMSET', KEYS[1],
            'd', current_timestamp_ms,
            'r', tokens_to_add)
  redis.call('EXPIRE', KEYS[1], ttl)
else
  redis.call('DEL', KEYS[1])
end

return { tokens_to_add }
