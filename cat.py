#!/usr/bin/env python3

import fcntl
import os
import sys
import time

import leveldb


class Lock():
    def __init__(self, fn=".lock"):
        self.fd = open(fn, "w")

    def __enter__(self):
        fcntl.lockf(self.fd, fcntl.LOCK_EX)

    def __exit__(self, type, value, traceback):
        fcntl.lockf(self.fd, fcntl.LOCK_UN)


def main(key=None):
    with Lock():
        db = leveldb.LevelDB('./homepages')
        if key is None:
            for key in db.RangeIter():
                print(key[0].decode())
            print(db.GetStats())
        else:
            print(db.Get(key.encode()).decode())


if __name__ == "__main__":
    main(*sys.argv[1:])
