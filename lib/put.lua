local current_timestamp_ms = tonumber(ARGV[1])
local new_content          = tonumber(ARGV[2])
local bucket_size          = tonumber(ARGV[3])
local ttl                  = tonumber(ARGV[4])

new_content = math.min(new_content, bucket_size)

if new_content < bucket_size then
  redis.call('HMSET', KEYS[1],
            'd', current_timestamp_ms,
            'r', new_content)
  redis.call('EXPIRE', KEYS[1], ttl)
else
  redis.call('DEL', KEYS[1])
end

return { current_timestamp_ms, new_content }
