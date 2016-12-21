#!/usr/bin/env python3

import sys
import redis

script_1 = """

local suffix = "." .. KEYS[1]
local sub_idx = -#suffix
local n = 0
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
"""

script_2 = """
local suffix = KEYS[1]
local blog = "blog:" .. suffix
local blog_set = blog .. "(set)"
local n = 0

for _, prefix in pairs(ARGV) do
    if redis.call("sadd", blog_set, prefix) == 1 then
        redis.call("rpush", blog, prefix)
        n = n + redis.call("srem", "hosts", prefix .. suffix)
   end
end

return n
"""

def main(blogs):
    redis_cli = redis.StrictRedis(unix_socket_path="etc/.redis.sock",
                                  decode_responses=True)

    if not blogs:
        blogs = redis_cli.smembers("blogs")

    for suffix in blogs:
        out = redis_cli.eval(script_1, 1, suffix)
        l = len(out)
        out = redis_cli.eval(script_2, 1, suffix, *out)
        print(suffix, out, l)


if __name__ == "__main__":
    main(sys.argv[1:])
