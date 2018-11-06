local tokens_to_add        = tonumber(ARGV[1])
local bucket_size          = tonumber(ARGV[2])
local ttl                  = tonumber(ARGV[3])

local current_time = redis.call('TIME')
local current_timestamp_ms = current_time[1] * 1000 + current_time[2] / 1000

local current = redis.call('HMGET', KEYS[1], 'r')

if current[1] then
  tokens_to_add = math.min(current[1] + tokens_to_add, bucket_size)
end

redis.replicate_commands()
if tokens_to_add < bucket_size then
  redis.call('HMSET', KEYS[1],
            'd', current_timestamp_ms,
            'r', tokens_to_add)
  redis.call('EXPIRE', KEYS[1], ttl)
else
  redis.call('DEL', KEYS[1])
end

return { tokens_to_add, current_timestamp_ms }
