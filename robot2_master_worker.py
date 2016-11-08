#!/usr/bin/env python3

import collections
import datetime
import imp
import json
import multiprocessing
import threading
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
    NUM_OF_WORKERS = 1
    RLIMIT_CPU = 240 - 3
    RLIMIT_AS = 500 * 1024 * 1024

    def init(self):
        self.session = requests.Session()
        self.proxy = None

    def get_command(self):
        url_task_ask = "http://u146.tyio.net:1033/host"

        while True:
            try:
                task = self.session.get(url_task_ask)
                if task.status_code == 200:
                    resp = task.json()
                    return resp["host"]
                else:
                    self.log("have a rest")
                    time.sleep(0.1)

            except Exception as e:
                self.log(e)
                time.sleep(0.1)

    def work(self, host):
        out = robot2.run(host=host, n_pages=10, proxy=self.proxy)
        data = json.dumps(out, separators=(",", ":"), ensure_ascii=False).encode()
        return data

    def process_result(self, host, data):
        url = "http://u146.tyio.net:1033/host-info/{}".format(host)
        self.session.post(url, data=data)

    def cmd__reload(self):
        imp.reload(robot2)


master_worker = Cli.instance()

def mailer():
    ss = requests.Session()
    attrs = set("""
        loop_flag
        RLIMIT_CPU RLIMIT_AS
        proxy NUM_OF_WORKERS
    """.split())
    id = "{}_{:012x}_{}".format(
        socket.gethostname(),
        uuid.getnode(),
        os.getpid(),
    )
    url = "http://u146.tyio.net:1033/mail/{}".format(id)

    while True:
        try:

            data = {k: getattr(master_worker, k, None) for k in attrs}
            data["tasks"] = master_worker.children
            ss.post(url, data=json.dumps(data, default=str).encode())

            resp = ss.get(url, timeout=10)
            assert resp.status_code == 200

            if resp.content:
                print(resp.json(), flush=True)
                for k, v in resp.json().items():
                    if k == "kill":
                        if v in master_worker:
                            os.kill(v, signal.SIGTERM)
                    elif k in attrs:
                        setattr(master_worker, k, v)

        except Exception as e:
            print(e, flush=True)
            time.sleep(1)

        if not master_worker.loop_flag:
            break


def main():
    #return mailer()
    threading.Thread(target=mailer).start()
    master_worker.run()


if __name__ == "__main__":
    main()
