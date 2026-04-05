local args = {...}
local prefix = tostring(args[1])
local start_index = tonumber(args[2])
local end_exclusive = tonumber(args[3])
local value_bytes = tonumber(args[4])
local varbinary = require('varbinary')

if box.space.KV == nil then
  error("box.space.KV is not initialized")
end

local ascii_a = string.byte("A")

local function payload_for(index, size)
  local marker = string.format("%08d:", index)
  if size <= #marker then
    return string.sub(marker, 1, size)
  end
  local fill = string.char(ascii_a + (index % 26))
  return marker .. string.rep(fill, size - #marker)
end

for index = start_index, end_exclusive - 1 do
  box.space.KV:replace({
    string.format("%s-%08d", prefix, index),
    varbinary.new(payload_for(index, value_bytes))
  })
end

return end_exclusive - start_index
