local prefix = ARGV[1]
local limit = tonumber(ARGV[2])
local matches = {}
local cursor = 0

repeat
  local results = redis.call('SCAN', cursor, 'MATCH', prefix, 'COUNT', 100)
  cursor = tonumber(results[1])
  local keys = results[2]
  for idx = 1, #keys do
    local key = keys[idx]
    local value = redis.call('HMGET', key, 'd', 'r')
    table.insert(value, key)
    table.insert(matches, value)
  end
until cursor == 0 or (limit ~= 0 and #matches > limit)

return matches
