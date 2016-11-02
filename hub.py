#!/usr/bin/env python3

import collections
import contextlib
import datetime
import itertools
import json
import logging
import os
import gc
import resource
import signal
import time

import leveldb
import redis
import tornado.ioloop
import tornado.options
import tornado.web

import tasks_publisher
import public_suffix
import sqliteset


class BaseHandler(tornado.web.RequestHandler):
    tasks = tasks_publisher.Tasks("hosts/queue")
    db = leveldb.LevelDB("hosts.ldb")
    redis_cli = redis.StrictRedis(unix_socket_path="redis/sock",
                                  decode_responses=True)
    commands = {}

    def set_default_headers(self):
        self.set_header("Content-Type", "text/plain; charset=UTF-8")

    def write_json(self, obj):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.finish(json.dumps(obj, default=str, ensure_ascii=False, indent=4,
                               sort_keys=True, separators=(",", ": ")))


class MainHandler(BaseHandler):
    def get(self):
        gc.collect()


class CommandHandler(BaseHandler):
    def post(self):
        cmd = self.request.body.decode()
        if cmd == "renew":
            self.tasks.text.renew()
        else:
            raise tornado.web.HTTPError(404)


class HostHandler(BaseHandler):
    workers = collections.defaultdict(dict)

    def get(self):
        resp = {}
        data = self.request.body.decode()
        if data:
            info = json.loads(data)
            worker_id = info["id"]
            worker = self.workers[worker_id]
            worker["active"] = datetime.datetime.now()
            worker.update(info)
            command = self.commands.pop(worker_id, None)
            if command:
                resp["command"] = command

        host = self.tasks.get()
        if host is None:
            raise tornado.web.HTTPError(404)
        resp["host"] = host
        self.write_json(resp)

    def post(self):
        host = self.request.body.decode()
        self.tasks.add(host)


class HostInfoHandler(BaseHandler):
    ignored_hosts_set = sqliteset.Set(0x100, "hosts/ignored.set")
    with open("hosts/ignored_suffixes") as f:
        ignored_suffixes = set(i for i in f.read().split() if i)
        ignored_suffixes.add(None)  # ignore unknown host suffix
    del f

    def get(self, name):
        try:
            self.write(bytes(self.db.Get(name.encode())))
        except KeyError:
            raise tornado.web.HTTPError(404)

    def post(self, name):
        ts = time.strftime("%Y%m%d-%H%M")
        self.redis_cli.hincrby("cnt_done", ts)
        content = self.request.body
        info = json.loads(content.decode())
        other_hosts_found = info.get("other_hosts_found")
        if other_hosts_found:
            valued, ignored = [], []
            suffixes = collections.Counter()
            for i in other_hosts_found:
                suffix = public_suffix.get_independent_domain(i)
                if suffix not in self.ignored_suffixes:
                    valued.append(i)
                    suffixes[suffix] += 1
                else:
                    ignored.append(i)
            n_found = self.tasks.add(*valued)
            n_found += self.ignored_hosts_set.add(*ignored)
            if n_found:
                self.redis_cli.hincrby("cnt_found", ts, n_found)
            for k, v in suffixes.items():
                if v > 2:
                    n = self.redis_cli.hincrby("suffixes_warned", k, v)
                    if n > 99:
                        self.ignored_suffixes.add(k)
                        with open("hosts/ignored_suffixes", "a") as f:
                            print(k, file=f)
                        self.redis_cli.hdel("suffixes_warned", k)

        redirect = info.get("redirect")
        if redirect:
            self.tasks.add(redirect)

        self.db.Put(name.encode(), content)

        command = self.commands.pop(self.get_query_argument("id", None), None)
        if command:
            self.write(command)

    def delete(self, name):
        self.db.Delete(name.encode())


handlers = [
    (r"/host", HostHandler),
    (r"/host-info/(.+)", HostInfoHandler),
    (r"/_cmd", CommandHandler),
    (r"/", MainHandler),
]


def main():
    gc.disable()
    _, n = resource.getrlimit(resource.RLIMIT_NOFILE)
    if n < 2000:
        raise Warning("RLIMIT_NOFILE", n)
    resource.setrlimit(resource.RLIMIT_NOFILE, (n, n))

    tornado.options.parse_command_line()

    p = int(os.environ.get("PORT", 1033))
    tornado.web.Application(
        handlers,
    ).listen(p, xheaders=True)

    with contextlib.suppress(ImportError):
        import tornadospy
        tornadospy.listen(p + 1)

    io_loop = tornado.ioloop.IOLoop.instance()

    def _term(*_):
        #io_loop.close(True)
        io_loop.stop()
        BaseHandler.tasks.close()
        logging.info("stop")

    signal.signal(signal.SIGTERM, _term)

    logging.info("start")
    io_loop.start()


if __name__ == "__main__":
    main()
    """
    curl -d "renew" 'localhost:1033/_cmd'
    curl -d "rebuild" 'localhost:1033/_cmd'
    """
