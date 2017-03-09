#!/usr/bin/env python3

import requests
import bs4
import urllib.parse
import re
import sys


ss = requests.Session()


valid_host = re.compile(
    r"([-a-z0-9]{1,64}\.)+"
    r"[a-z]{2,16}"
)


def find_host(s):
    o = valid_host.search(s)
    if o:
        return o.group()


def search_baidu(word):
    """baidu
    每页最多显示 50 条, rn=50
    取 200 条, 4 页
    """
    hosts = set()

    for i in range(4):
        resp = ss.get("http://www.baidu.com/s?wd={}&rn=50&pn={}".format(word, 50 * i))
        soup = bs4.BeautifulSoup(resp.text, 'lxml')
        for url in soup.select(".c-showurl"):
            host = find_host(url.text)
            if host:
                hosts.add(host)

    return hosts


def search_so(word):
    """360搜索: so.com
    每页最多显示 10 条, 暂不知道如何配置
    """
    hosts = set()

    for i in range(1, 21):
        resp = ss.get("https://www.so.com/s?q={}&pn={}".format(word, i))
        soup = bs4.BeautifulSoup(resp.text, 'lxml')
        for url in soup.select(".res-linkinfo cite"):
            host = find_host(url.text)
            if host:
                hosts.add(host)

    return hosts


def search_sogou(word):
    """搜狗搜索
    每页最多显示 100 条, num=100
    """
    hosts = set()

    for i in [1, 2]:
        resp = ss.get("https://www.sogou.com/web?num=100&query={}&page={}".format(word, i))
        soup = bs4.BeautifulSoup(resp.text, 'lxml')
        for url in soup.select(".fb cite"):
            host = find_host(url.text)
            if host:
                hosts.add(host)

    return hosts


def main(words):
    hosts = set()

    for word in words:
        hosts.update(search_baidu(word) | search_so(word) | search_sogou(word))

    for i in hosts:
        print(i)
    return

    for word in words:
        print(search_baidu(word))
        print()
        print(search_so(word))
        print()
        print(search_sogou(word))


if __name__ == "__main__":
    main(sys.argv[1:])
