#!/usr/bin/env python3

import fcntl
import os
import gc
import sys
import time

import leveldb

N = 0
T = int(time.time())
L = [N]


def f(k, v):
    global N, T
    t = int(time.time())
    if t != T:
        T = t
        print(N - L[-1], N)
        L.append(N)
    N += 1


def f(k, v):
    if v.count("撸") > 3:
        print(k)


def main():
    db = leveldb.LevelDB('./homepages.2')
    print(db.GetStats())
    for k, v in db.RangeIter():
        f(k.decode(), v.decode())


if __name__ == "__main__":
    main()
