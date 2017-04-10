#!/usr/bin/env python3

import sys
import redis
import random

script_1 = """

local suffixes = {}

for _, v in pairs(ARGV) do
    table.insert(suffixes, "." .. v)
end

local n = 0
local cursor = "0"
local tmp = {}

repeat
    local data
    cursor, data = unpack(redis.call("sscan", "hosts", cursor))
    for _, v in pairs(data) do
        for _, suffix in pairs(suffixes) do
            if string.sub(v, -#suffix) == suffix then
                table.insert(tmp, v)
                break
            end
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

    for i in range(0, 25):
        redis_cli.hset("cnt_done", "20170312-{:02}".format(i), random.randint(20000, 26000))
    for i in range(0, 10):
        redis_cli.hset("cnt_done", "20170313-{:02}".format(i), random.randint(20000, 27000))
    redis_cli.hset("cnt_done", "20170313-09", 1000)
    return
    out = redis_cli.eval(script_1, 0, *redis_cli.smembers("ignored"))
    #for i in out:
        #print(i)
    print(len(out))
    redis_cli.srem("hosts", *out)
    return



if __name__ == "__main__":
    main(sys.argv[1:])
