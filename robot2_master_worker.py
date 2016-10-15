#!/usr/bin/env python3

import collections
import datetime
import imp
import json
import multiprocessing
import os
import resource
import signal
import socket
import sys
import time

import requests
import socks

import robot2
import master_worker


"""
class SessionWithLock(requests.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = multiprocessing.Lock()

    def request(self, *args, **kwargs):
        with self._lock:
            return super().request(*args, **kwargs)
"""


class Cli(master_worker.MasterWorker):
    NUM_OF_WORKERS = 2
    RLIMIT_AS = 500 * 1024 * 1024

    def init(self):
        socks.set_default_proxy(socks.SOCKS4, "10.10.60.1")
        self.session = requests.Session()

    def get_command(self):
        url_task_ask = "http://gpu.tyio.net:1033/host"
        while True:
            try:
                task = self.session.get(url_task_ask)
                if task.status_code == 200:

                    notice = task.headers.get("Notice")
                    if notice:  # as hub's command
                        notice = json.loads(notice)
                        self.log(notice)
                        if "NUM_OF_WORKERS" in notice:
                            master_worker.NUM_OF_WORKERS = notice["NUM_OF_WORKERS"]

                    return task.text

                else:
                    self.log("have a rest")
                    time.sleep(0.1)

            except Exception as e:
                self.log(e)
                time.sleep(0.1)

    def work(self, host):
        socket.socket = socks.socksocket  # monkey patch
        out = robot2.run(host=host, n_pages=10)
        data = json.dumps(out, separators=(",", ":"), ensure_ascii=False).encode()
        return data

    def process_result(self, host, data):
        url = "http://gpu.tyio.net:1033/host-info/{}".format(host)
        self.session.post(url, data=data)


master_worker = Cli.instance()


def update_num_of_workers(*_):
    with open("NUM_OF_WORKERS") as f:
        n = int(f.read())
        master_worker.NUM_OF_WORKERS = n


def reload_code(*_):
    imp.reload(robot2)


def main():
    with open(".pid", "w") as f:
        f.write(str(os.getpid()))

    signal.signal(signal.SIGUSR1, update_num_of_workers)
    signal.signal(signal.SIGUSR2, reload_code)
    update_num_of_workers()
    master_worker.run()


if __name__ == "__main__":
    main()
