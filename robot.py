#!/usr/bin/env python3

"""
robot.py
"""


import datetime
import json
import functools
import logging
import os
import random
import re
import signal
import socket
import sys
import time
import urllib.parse

try:
    import resource
except ImportError:
    class _R:
        def __getattr__(self, key):
            return lambda *args, **kwargs: None
    resource = _R()

try:
    import lxml
    HTML_PARSER = "html5lib"
    HTML_PARSER = "lxml"
except ImportError:
    HTML_PARSER = "html.parser"

import bs4
import click
import langid
import requests


# not now IMAGES_COUNT = int(os.environ.get("IMAGES_COUNT", 50))
TOO_LONG = 1 * 1024 * 1024


is_valid_host = re.compile(
    r"([-a-z0-9]{1,64}\.)+"
    r"[a-z]{2,8}"
    r"(:[0-9]{2,5})?"
).fullmatch

search_simple_home_url = re.compile(r"https?://[-a-z0-9.]+").search


def netloc_to_host(netloc):
    """获取到更符合规则的 HOST 字符串

    一般来说, `urllib.parse.urlparse(URL).netloc` 可以得到我们需要的 HOST,
    但还不够, 我们需要一点点的调整.

    >>> netloc_to_host("Q.q:80")
    'q.q'
    >>> netloc_to_host("Q.q.. ")
    'q.q'
    """

    host = netloc.strip(". ").lower()
    host_domain, _, host_port = host.partition(":")
    if host_port == "80":  # fucking explicit port 80
        host = host_domain
    return host


def filter_links(base_host, links) -> (set, set):
    """把链接分为两类: 站内链接, 站外链接

    站外链接的抓取任务不属于当前的爬虫, 仅提取出 HOST 记录下来.

    >>> filter_links("q.org", [
    ...     "http://q.net/1",
    ...     "http://q.org/2",
    ...     "http://q.cn/",
    ... ]) == ({'http://q.org/2'}, {'q.net', 'q.cn'})
    ...
    True

    """

    inner_links = set()
    other_hosts = set()

    for s in links:
        parsed = urllib.parse.urlparse(s)
        host = netloc_to_host(parsed.netloc)
        if host == base_host:
            inner_links.add(s)
        elif is_valid_host(host) and len(host) < 100:
            other_hosts.add(host)

    return inner_links, other_hosts


def get_meta_tag_content(soup: bs4.BeautifulSoup, name: str) -> str or None:
    """从 BeautifulSoup 对象中提取出指定名字的 meta 标签的内容

    keywords or Keywords or KEYWORDS
    description or Description or DESCRIPTION

    >>> soup = bs4.BeautifulSoup('''
    ... <meta name="Keywords" content="k1,k2" />
    ... <meta name="Description" content="NB!" />
    ... ''', 'html.parser')
    >>> get_meta_tag_content(soup, "keywords")
    'k1,k2'
    """

    assert name == name.lower(), name

    for name in [name, name.capitalize(), name.upper()]:
        tag = soup.select_one("meta[name={}]".format(name))
        if tag:
            return tag.get("content")


def get_meta_redirect(soup: bs4.BeautifulSoup) -> str or None:
    """从 BeautifulSoup 对象中提取重定向信息

    有些站点不是用到 301 302 重定向, 而是在返回的 HTML 中,
    用的带 `http-equiv` 属性的 meta 标签::

        <meta http-equiv="refresh" content="0; URL=http://foo.bar.com/">

    在这里我们简单认为主页的跳转最有处理价值, 因为实际场景中就是这样.

    >>> soup = bs4.BeautifulSoup('''
    ... <meta name="Keywords" content="k1,k2" />
    ... <meta http-equiv="refresh" content="0; URL=http://foo.bar.com/q/p/">
    ... ''', 'html.parser')
    >>> get_meta_redirect(soup)
    'http://foo.bar.com'
    """

    tag = soup.select_one("meta[http-equiv=refresh]")
    if tag:
        result = search_simple_home_url(tag.get("content"))
        return result and result.group()


def fetch(url: str, get: requests.Session.get) -> (dict, set, set, set) or None:
    """抓取指定 URL 的页面的信息

    返回的东西比较多: 页面主要内容, 找到的内部链接, 找到的外部的 HOSTS
    """

    page = {}
    resp = get(url, stream=True)

    parsed = urllib.parse.urlparse(resp.url)

    page["url"] = resp.url
    page["path"] = parsed.path
    page["code"] = resp.status_code

    if resp.status_code >= 300:
        return

    if int(resp.headers.get("Content-Length", 0)) > TOO_LONG:
        return

    if not resp.headers.get("Content-Type", "").startswith("text/html"):
        return

    if len(resp.content) > TOO_LONG:
        return

    markup = resp.content
    if resp.encoding != 'ISO-8859-1':
        try:
            markup = markup.decode(encoding=resp.encoding)
        except (LookupError, UnicodeDecodeError):  # unknown encoding or decode error
            pass

    soup = bs4.BeautifulSoup(markup, HTML_PARSER)

    abs_url = functools.partial(urllib.parse.urljoin, resp.url)

    encoding = soup.original_encoding or resp.encoding
    if encoding:
        encoding = encoding[:30]
    page["encoding"] = encoding
    page["title"] = soup.title and soup.title.text.strip()

    for meta_name in ["keywords", "description"]:
        meta_tag_content = get_meta_tag_content(soup, meta_name)
        page[meta_name] = meta_tag_content and meta_tag_content.lower()

    # see get_meta_tag_content.__doc__
    html_meta_redirect = get_meta_redirect(soup)
    if html_meta_redirect:
        page["url"] = html_meta_redirect

    for tag in soup.find_all(["script", "style"]):
        tag.clear()

    page["text"] = "\n".join(filter(None, map(
        str.strip, soup.text.split("\n")
    )))[:128*1024]  # trim

    all_tag_a = soup.find_all("a")

    links = list(filter(
        lambda url: url.startswith("http"),
        set(
            abs_url(url).partition("#")[0]
            for url in map(lambda a: a.get("href"), all_tag_a) if url
        )
    ))

    inner_links, other_hosts = filter_links(parsed.netloc, links)

    images = set()
    for img in soup.find_all("img"):
        src = img.get("src", "").strip()
        # how to prevent `data:image/jpeg;base64,...` ?
        if not src or len(src) > 256:
            continue
        src = abs_url(src)
        if src.startswith("http"):
            # fucking http:/a/b/c.jpg
            x = urllib.parse.urlparse(src)
            if x.scheme and x.netloc and not x.query:
                images.add(src)

    return page, inner_links, other_hosts, images


@click.command()
@click.option("--host", "-h", prompt="Host")
@click.option("--count", "-n", default=1, help="Number of pages")
@click.option("--proxy", "-x", help="Proxy")
def demo(host, count, proxy):
    """开始一个爬虫任务, 测试
    """

    # limit amount of processor time: 60s
    #resource.setrlimit(resource.RLIMIT_CPU, (60, -1))
    out = run(host=host, n_pages=count, proxy=proxy)
    import pprint, io, sys
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, errors="ignore")
    pprint.pprint(out)


def run(**config):
    host = config["host"]
    n_pages = config.get("n_pages", 1)
    proxy = config.get("proxy")
    proxies = {
        "http": proxy,
        "https": proxy,
    } if proxy else None

    try:
        signal.signal(signal.SIGALRM, raise_timeout)
        signal.alarm(n_pages * 60)  # 30s per-page average
    except AttributeError:
        pass

    schema = "http"  # guess http first
    url_root = "{}://{}".format(schema, host)
    other_hosts_found = set()
    images = set()
    urls_todo = [url_root]  # only one
    urls_done = {url_root, url_root + "/"}  # :)
    pages = []
    error_message = None
    ip = None

    base_host = host

    try:
        ip = socket.gethostbyname(host)  # prefetch

        session_for_fetch = requests.Session()
        session_for_fetch.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 6.2; WOW64)"
        get_method = functools.partial(session_for_fetch.get,
                                       timeout=(10, 20),
                                       proxies=proxies,
                                       verify=False)

        for idx, url in enumerate(urls_todo):
            if len(pages) >= n_pages:
                break

            try:
                page, inner_links, other_hosts, _images = fetch(url, get_method)
            except TypeError:
                continue

            url_strict = page["url"]

            urls_done.add(url)
            urls_done.add(url_strict)
            other_hosts_found.update(other_hosts)
            images.update(_images)
            urls_todo.extend(inner_links - urls_done)  # dangerous, hold it!
            pages.append(page)

            if idx == 0:  # home page, tiny special
                if url_strict.startswith("https"):
                    schema = "https"
                base_host_new = netloc_to_host(
                    urllib.parse.urlparse(url_strict).netloc)
                if base_host != base_host_new:
                    return {
                        "redirect": base_host_new,  # then "hub" should add base_host_new
                        "ip": ip,
                        "pages": pages,
                    }

    except Exception as e:
        error_message = str(e)
        error_type = type(e)
        logging.exception(e)

    try:
        signal.alarm(0)  # no alarm now
    except AttributeError:
        pass

    info = {
        "schema": schema,
        "other_hosts_found": list(other_hosts_found)[:1000],
        # DEPRECATED "images": list(images),
        "pages": pages,
    }

    if error_message:
        info["err"] = str(error_type)[8:-2]
        info["error"] = error_message

    if ip:
        info["ip"] = ip

    return info


def raise_timeout(*_):
    raise TimeoutError(os.times())


def main(spec_task=None):
    if spec_task:
        return do_it(spec_task)

    flag_loop = True

    def _sig_term(signum, frame):
        nonlocal flag_loop
        flag_loop = False

    signal.signal(signal.SIGTERM, _sig_term)

    while flag_loop:
        if do_it() == "break":
            break


from entities import db_session, select
from entities import Host

def do_it(host_name=None):
    with db_session(immediate=True):  # select with session lock
        if host_name is None:
            o = select(i for i in Host if i.crawler_started is None).limit(1)
        else:
            o = select(i for i in Host if i.name == host_name).limit(1)

        if not o:
            return "break"

        task, = o
        print(task.name, flush=True)
        task.crawler_started = datetime.datetime.now()
        host_id = task.id
        host_name = task.name

    info = run(host=host_name, n_pages=1)
    #print(info)
    homepage = ""  # store as file

    with db_session:
        host = Host[host_id]
        host.crawler_done = datetime.datetime.now()
        host.ip = info.get("ip")
        host.redirect = info.get("redirect")
        host.err = info.get("err")
        pages = info.get("pages")
        if pages:
            page1 = pages[0]
            host.url = page1["url"]
            host.title = page1.get("title")
            host.keywords = page1.get("keywords")
            host.description = page1.get("description")
            host.encoding = page1.get("encoding")
            homepage = page1.get("text", "")
            host.language, _ = langid.classify(homepage)

    if homepage:
        # fn has prefix like hash
        if host_name.startswith("www."):
            fn = "homepage.d/www/{}/{}".format(host_name[4], host_name)
        else:
            fn = "homepage.d/{}/{}".format(host_name[0], host_name)
        with open(fn, "w") as f:
            f.write(homepage)


if __name__ == "__main__":
    from entities import init
    init(".db")
    main(*sys.argv[1:])
