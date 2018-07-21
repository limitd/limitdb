local prefix = ARGV[1]
local matches = {}
local cursor = 0

repeat
  local results = redis.call('SCAN', cursor, 'MATCH', prefix)
  cursor = tonumber(results[1])
  local keys = results[2]
  for idx = 1, #keys do
    local value = redis.call('HMGET', keys[idx], 'd', 'r')
    table.insert(value, keys[idx])
    table.insert(matches, value)
  end
until cursor == 0

return matches
