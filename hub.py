#!/usr/bin/env python3

import contextlib
import itertools
import json
import logging
import os
import gc
import resource
import signal
import time

import tornado.ioloop
import tornado.options
import tornado.web
import leveldb

import tasks_publisher

gc.disable()


class BaseHandler(tornado.web.RequestHandler):
    tasks = tasks_publisher.Tasks("hosts")
    db = leveldb.LevelDB("hosts.ldb")

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
        if cmd == "rebuild":
            self.tasks.rebuild_filter()
        elif cmd == "renew":
            self.tasks.text.renew()
        else:
            raise tornado.web.HTTPError(404)


class HostHandler(BaseHandler):
    def get(self):
        host = self.tasks.get()
        if host is None:
            raise tornado.web.HTTPError(404)
        self.write(host)

    def post(self):
        host = self.request.body.decode()
        self.tasks.add(host)


class HostInfoHandler(BaseHandler):
    def get(self, name):
        try:
            self.write(bytes(self.db.Get(name.encode())))
        except KeyError:
            raise tornado.web.HTTPError(404)

    def post(self, name):
        content = self.request.body
        info = json.loads(content.decode())
        for host in info.get("other_hosts_found", []):
            self.tasks.add(host)
        redirect = info.get("redirect")
        if redirect:
            self.tasks.add(redirect)
        self.db.Put(name.encode(), content)

    def delete(self, name):
        self.db.Delete(name.encode())


handlers = [
    (r"/host", HostHandler),
    (r"/host-info/(.+)", HostInfoHandler),
    (r"/_cmd", CommandHandler),
    (r"/", MainHandler),
]


def main():
    tornado.options.parse_command_line()

    _, n = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (n, n))

    p = int(os.environ.get("PORT", 1033))
    tornado.web.Application(
        handlers,
    ).listen(p, xheaders=True)

    with contextlib.suppress(ImportError):
        import tornadospy
        tornadospy.listen(p + 1)

    io_loop = tornado.ioloop.IOLoop.instance()

    def _term(signal_number, stack_frame):
        #io_loop.close(True)
        io_loop.stop()
        HostHandler.tasks.finish()
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
