#!/bin/sh

redis-cli -s etc/.redis.sock eval '

local ts = KEYS[1]

local ops = 0

for _, key in pairs(ARGV) do
    for _, t in pairs(redis.call("hkeys", key)) do
        if t < ts then
            redis.call("hdel", key, t)
            ops = ops + 1
        end
    end
end

return ops

' 1 $(date -d '3 days ago' '+%G%m%d')  cnt_found cnt_done
