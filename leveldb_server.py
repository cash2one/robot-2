#!/usr/bin/env python3

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

gc.disable()


class BaseHandler(tornado.web.RequestHandler):
    db = leveldb.LevelDB('./homepages')

    def set_default_headers(self):
        self.set_header("Content-Type", "text/plain; charset=UTF-8")

    def write_json(self, obj):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.finish(json.dumps(obj, default=str, ensure_ascii=False, indent=4,
                               sort_keys=True, separators=(",", ": ")))


class MainHandler(BaseHandler):
    def get(self):
        gc.collect()
        self.write(self.db.GetStats())


class DataHandler(BaseHandler):
    def get(self, name):
        try:
            self.write(bytes(self.db.Get(name.encode())))
        except KeyError:
            raise tornado.web.HTTPError(404)

    def post(self, name):
        content = self.request.body
        self.db.Put(name.encode(), content)

    def delete(self, name):
        self.db.Delete(name.encode())


class KeysHandler(BaseHandler):
    def get(self):
        def argv(k):
            v = self.get_argument(k, None)
            return v and v.encode()
        n = int(self.get_argument("n", 100))
        it = self.db.RangeIter(argv("from"), argv("to"), include_value=False)
        self.write_json(list(itertools.islice(map(bytearray.decode, it), n)))


class IterHandler(BaseHandler):
    _iter = None

    def get(self):
        try:
            k, v = map(bytearray.decode, next(self._iter))
        except (StopIteration, TypeError):
            raise tornado.web.HTTPError(404)
        self.write_json([k, v])

    def post(self):
        def argv(k):
            v = self.get_argument(k, None)
            return v and v.encode()
        self.__class__._iter = self.db.RangeIter(argv("from"), argv("to"))


handlers = [
    (r"/data/(.+)", DataHandler),
    (r"/keys", KeysHandler),
    (r"/iter", IterHandler),
    (r"/", MainHandler),
]


def main():
    _, n = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (n, n))

    tornado.options.parse_command_line()

    tornado.web.Application(
        handlers,
    ).listen(int(os.environ.get("PORT", 1030)), xheaders=True)

    io_loop = tornado.ioloop.IOLoop.instance()

    def _term(signal_number, stack_frame):
        logging.info("stop")
        io_loop.stop()

    signal.signal(signal.SIGTERM, _term)

    logging.info("start")
    io_loop.start()


if __name__ == "__main__":
    main()
