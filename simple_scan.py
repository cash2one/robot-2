#!/usr/bin/env python3

import re
import binascii
import os
import datetime
import gzip
import os.path
import json
import collections
import multiprocessing
import sys


def hash_name(s):
    crc = "{:08x}".format(binascii.crc32(s.encode()))
    return "fs/{}/{}/{}".format(crc[4:6], crc[6:], s)


domain_names = """
xxx
xnxx
sex
porn
\.sucks
hentai
gay
lesbian
nude
"""

kw = """
xxx
sex
porn
hentai
gay
lesbian
nude
炮友
性爱
做爱
一夜情
色情
情色
人妻
无码
撸一撸
咪咪色
エロ
炮友
性愛
做愛
壹夜情
色情
情色
人妻
無碼
18禁
精液
千百撸
激情
超碰
六合彩
小电影
"""


def to_pattern_sting(s):
    return "|".join(map("({})".format, set(s.split())))


domain_pattern = re.compile(to_pattern_sting(domain_names))
kw_pattern = re.compile(to_pattern_sting(kw))


def g():
    for p, _, fns in os.walk("fs"):
        for fn in fns:
            fn_full = os.path.join(p, fn)
            with open(fn_full) as f:
                data = f.read()
            yield fn_full, data


def g():
    for p, _, fns in os.walk("fs", followlinks=True):
        if len(p) != 8:
            continue  # fs/XX/XX
        for fn in fns:
            fn_full = os.path.join(p, fn)
            try:
                with gzip.open(fn_full, "rt") as f:
                    data = json.load(f)
            except Exception as e:
                print(e, type(e), fn_full, file=sys.stderr)
                continue
            yield fn_full, data

def g():
    with open("all_domains_from_xh.1") as f:
        for i in f:
            fn_full = hash_name(i.strip())
            try:
                with gzip.open(fn_full, "rt") as f:
                    data = json.load(f)
            except Exception as e:
                print(fn_full, file=sys.stderr)
                continue
            yield fn_full, data


def get_flag(s):
    if s:
        if kw_pattern.search(s.lower()):
            return 1
    return 0


def page1(data):
    flag_title = flag_keywords = flag_description = 0
    pages = data.get("pages")
    if pages:
        p = pages[0]
        flag_title = get_flag(p.get("title"))
        flag_keywords = get_flag(p.get("keywords"))
        flag_description = get_flag(p.get("description"))
    return flag_title, flag_keywords, flag_description


def pages(data):
    flag_title = flag_keywords = flag_description = 0
    idx = 0
    pages = data.get("pages")
    if pages:
        for idx, p in enumerate(pages):
            flag_title = get_flag(p.get("title"))
            flag_keywords = get_flag(p.get("keywords"))
            flag_description = get_flag(p.get("description"))
            if flag_title or flag_keywords or flag_description:
                break

    return tuple(i * (1 - idx*0.01) for i in [flag_title, flag_keywords, flag_description])


def get_info(args):
    fn_full, data = args
    try:
        data = json.loads(data)
    except json.JSONDecodeError:
        return

    f0 = int(bool(domain_pattern.search(fn_full)))
    f1, f2, f3 = page1(data)

    return f0, f1, f2, f3, f0+f1+f2+f3, fn_full[9:]


def get_pages_info(fn_full, data):
    f1, f2, f3 = pages(data)
    f0 = int(bool(domain_pattern.search(fn_full)))
    return fn_full[9:], f0, f1, f2, f3


def main():
    #for n, i in zip(range(10), g()):
    for i in g():
        out = get_pages_info(*i)
        print(*out, sep="\t")
    return
    n = 0
    with multiprocessing.Pool(os.cpu_count() - 4) as pool:
        for out in pool.imap_unordered(get_info, g()):
            if out:
                print(*out, sep="\t")
            n += 1
            if n > 10:
                break


if __name__ == "__main__":
    main()
