#!/usr/bin/env python3

import collections
import datetime
import gc
import os
import resource
import signal
import sys
import time


class MasterWorker(object):
    """Multiprocessing based master-worker model. Singleton(lock todo)
    """

    NUM_OF_WORKERS = 4
    RLIMIT_CPU = 60
    RLIMIT_AS = 300 * 1024 * 1024


    def __init__(self):
        self.children = set()
        signal.signal(signal.SIGCHLD, self._sig_chld)
        signal.signal(signal.SIGTERM, self._sig_term)
        self.init()

    def _sig_chld(self, signum, frame):
        while True:
            try:
                pid, status = os.waitpid(0, os.WNOHANG)
                if pid == 0:
                    break
                exit_status, signal_number = status.to_bytes(2, "big")
                self.children.discard(pid)
            except ChildProcessError:
                break

    def _sig_term(self, signum, frame):
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        self._wait_children()
        sys.exit()

    def _wait_children(self):
        while self.children:
            pid, status = os.wait()
            self.children.discard(pid)

    def run(self):
        gc.disable()

        while True:
            while True:
                if len(self.children) < self.NUM_OF_WORKERS:
                    break
                time.sleep(0.01)

            cmd = self.get_command()
            if cmd is None:
                self._wait_children()
                break

            pid = os.fork()
            if pid == 0:  # child
                signal.signal(signal.SIGCHLD, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                resource.setrlimit(resource.RLIMIT_CPU, (self.RLIMIT_CPU, -1))
                resource.setrlimit(resource.RLIMIT_AS, (self.RLIMIT_AS, -1))
                self.work(cmd)
                sys.exit()
            else:
                self.children.add(pid)
                gc.collect()

    def init(self):
        pass

    def get_command(self) -> object:
        """Just an example

        subclass should override this
        """

        try:
            return input("> ") or None
        except EOFError:
            pass

    def work(self, cmd) -> None:
        """Just an example

        subclass should override this
        """

        repr(cmd)


def log(*args):
    print(datetime.datetime.now(), *args, file=sys.stderr)


def main():
    import random

    class T(MasterWorker):
        def work(self, cmd):
            time.sleep(10)
            exec(cmd)

    T().run()


if __name__ == "__main__":
    main()
