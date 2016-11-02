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
import uuid

import requests

import robot2
import master_worker
import random


class SessionWithLock(requests.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = multiprocessing.Lock()

    def request(self, *args, **kwargs):
        with self._lock:
            return super().request(*args, **kwargs)


class Cli(master_worker.MasterWorker):
    NUM_OF_WORKERS = 20
    RLIMIT_CPU = 240 - 3
    RLIMIT_AS = 500 * 1024 * 1024

    def init(self):
        self.session = requests.Session()
        self.id = "{}_{:012x}_{}".format(
            socket.gethostname(),
            uuid.getnode(),
            os.getpid(),
        )
        self.t = int(time.time())
        self.proxy = None

    def get_command(self):
        url_task_ask = "http://u146.tyio.net:1033/host"

        attrs = "id proxy NUM_OF_WORKERS RLIMIT_CPU RLIMIT_AS".split()
        while True:
            try:
                data = {k: getattr(self, k, None) for k in attrs}
                t = int(time.time())
                if t > self.t:  # every second
                    self.t = t
                    data["tasks"] = self.children
                data = json.dumps(data, default=str).encode()
                task = self.session.get(url_task_ask, data=data)
                if task.status_code == 200:
                    resp = task.json()
                    self._execute_command(resp.get("command"))
                    return resp["host"]
                else:
                    self.log("have a rest")
                    time.sleep(0.1)

            except Exception as e:
                self.log(e)
                time.sleep(0.1)

    def _execute_command(self, cmd):
        if cmd:
            try:
                exec(cmd, None, {"_": self})
            except Exception as e:
                self.log(e)

    def work(self, host):
        out = robot2.run(host=host, n_pages=10, proxy=self.proxy)
        data = json.dumps(out, separators=(",", ":"), ensure_ascii=False).encode()
        return data

    def process_result(self, host, data):
        url = "http://u146.tyio.net:1033/host-info/{}?id={}".format(host, self.id)
        resp = self.session.post(url, data=data)
        self._execute_command(resp.text)

    def cmd__reload(self):
        imp.reload(robot2)


master_worker = Cli.instance()


def main():
    master_worker.run()


if __name__ == "__main__":
    main()
