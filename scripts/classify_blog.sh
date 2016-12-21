#!/bin/sh

redis-cli -s etc/.redis.sock eval '

local blog = KEYS[1]
local blog_set = blog .. "(set)"
local suffix = "." .. blog
local sub_idx = -#suffix
local n = 0
local nn = 0
local cursor = "0"
local tmp = {}

repeat
    local data
    cursor, data = unpack(redis.call("sscan", "hosts", cursor))
    for _, v in pairs(data) do
        if string.sub(v, sub_idx) == suffix then
            n = n + 1
            local prefix = string.sub(v, 1, sub_idx)
            table.insert(tmp, prefix)
        end
    end
until (cursor == "0")

return tmp

--[[
for _, prefix in pairs(tmp) do
    if redis.call("sadd", blog_set, prefix) == 1 then
        redis.call("rpush", blog, prefix)
        nn = nn + 1
   end
end

return {n, nn}
]]

' 1 $1
