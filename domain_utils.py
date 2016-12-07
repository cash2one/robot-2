#!/usr/bin/env python3

import io
import time
import re


def _init():
    with open('public_suffix_list.dat') as f:
        l = [i.strip() for i in filter(re.compile("^[a-z.]+\n$").fullmatch, f)]

    s1 = set(filter(re.compile("^[a-z]+$").fullmatch, l))
    s2 = set()

    for i in l:
        fs = i.split(".")
        if len(fs) == 2 and len(fs[-1]) == 2 and fs[0] in s1:
            s2.add(i)

    return s1 | s2


suffixes = _init()


def tail(name):
    idx = name.rindex(".")  # raise if error
    s1 = name[idx + 1:]
    idx = name.rfind(".", 0, idx)  # -1 if not found
    s2 = name[idx + 1:]
    idx = name.rfind(".", 0, idx)
    s3 = name[idx + 1:]
    if s2 in suffixes:
        return s3
    elif s1 in suffixes:
        return s2


def main():
    suffixes
    assert tail("g.cn") == "g.cn"
    assert tail("a.b.c.g.com.cn") == "g.com.cn"
    assert tail("www.baidu.com") == "baidu.com"
    assert tail("baidu.com") == "baidu.com"
    existed = set()
    while True:
        i = input()
        if i not in existed:
            existed.add(i)
            if tail(i) == i:
                print(i)


if __name__ == "__main__":
    main()
