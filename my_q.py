#!/usr/bin/env python3


import sys
import urllib.parse

import bs4
import requests
import redis


class MyQueue(object):
    _redis = redis.StrictRedis(unix_socket_path="etc/.redis.sock",
                               decode_responses=True)

    _script_add_sha = _redis.script_load("""
        local cmd, key = KEYS[1], KEYS[2]
        local set = key .. "(set)"
        local n = 0
        for _, v in pairs(ARGV) do
            if redis.call("sadd", set, v) == 1 then
                redis.call(cmd, key, v)
                n = n + 1
            end
        end
        return n
    """)

    def __init__(self, name):
        self._key = name
        self._key_set = name + "(set)"

    def __repr__(self):
        return "{} {}:{} {}:{}".format(
            self.__class__.__name__,
            self._key, self._redis.llen(self._key),
            self._key_set, self._redis.scard(self._key_set),
        )

    def __len__(self):
        return self._redis.llen(self._key)

    def append(self, *values):
        return self._redis.evalsha(self._script_add_sha, 2, "rpush", self._key, *values)

    def insert(self, *values):
        return self._redis.evalsha(self._script_add_sha, 2, "lpush", self._key, *values)

    def remove(self, *values):
        return self._redis.srem(self._key_set, *values)

    def pop(self, block=False):
        if block:
            return self._redis.blpop(self._key)[1]
        else:
            return self._redis.lpop(self._key)


class MyQueues(dict):
    def __missing__(self, key):
        self[key] = MyQueue(key)
        return self[key]


def main():
    q = MyQueue("test")
    q.append(*range(10))
    print(q)
    q.pop()
    print(q)


if __name__ == "__main__":
    main()
