#!/usr/bin/env python3

import logging
import os
import signal
import time

import tornado.ioloop
import tornado.options
import tornado.web

import leveldb


db = leveldb.LevelDB('./homepages')


class MainHandler(tornado.web.RequestHandler):
    def get(self, name):
        try:
            self.write(bytes(db.Get(name.encode())))
        except KeyError:
            raise tornado.web.HTTPError(404)

    def post(self, name):
        content = self.request.body
        #print(name, len(content))
        db.Put(name.encode(), content)


handlers = [
    (r"/(.+)", MainHandler),
]


def main():
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
