#!/usr/bin/env python3

import collections
import contextlib
import datetime
import gc
import os
import pathlib
import pickle
import resource
import selectors
import signal
import sys
import time
import threading


def _fifo(name):
    with contextlib.suppress(FileExistsError):
        os.mkfifo(name)
    p = pathlib.Path(name)
    assert p.is_fifo()
    def w():
        with p.open("w"):
            pass
    threading.Thread(target=w).start()  # prevent blocking
    return p.open()


class MasterWorker(object):
    """Multiprocessing based master-worker model. Singleton
    """

    NUM_OF_WORKERS = 2
    RLIMIT_CPU = 60
    RLIMIT_AS = 300 * 1024 * 1024
    _lock = True

    def __init__(self):
        if self._lock:
            raise ValueError(self)
        self._children = set()
        self._selector = selectors.SelectSelector()
        self._foo = _fifo(".pipe." + type(self).__name__)
        signal.signal(signal.SIGCHLD, self._sig_chld)
        signal.signal(signal.SIGTERM, self._sig_term)

    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._lock = False
            cls._instance = cls()
            cls._lock = True
        return cls._instance

    @classmethod
    def clear_instance(cls):
        if hasattr(cls, "_instance"):
            del cls._instance

    def _sig_chld(self, signum, frame):
        while True:
            try:
                pid, status = os.waitpid(0, os.WNOHANG)
                if pid == 0:
                    break
                exit_status, signal_number = status.to_bytes(2, "big")
                self._children.discard(pid)
            except ChildProcessError:
                break

    def _sig_term(self, signum, frame):
        self.clean()
        sys.exit()

    def _wait_children(self):
        while self._children:
            #print(self._children)
            time.sleep(0.1)  # see _sig_chld

    def log(self, x):
        print(datetime.datetime.now(), x, file=sys.stderr, flush=True)

    def run(self):
        self.init()
        gc.disable()

        loop_flag = True
        while loop_flag:
            # loop
            if len(self._children) < self.NUM_OF_WORKERS:
                command = self.get_command()
                if command is None:
                    loop_flag = False
                else:
                    self._fork(command)

            self._select_and_process()
            self._have_a_rest()
            # loop

        self.clean()

    def _have_a_rest(self):
        text = self._foo.read()
        if not text:
            return
        cmd, *args = text.split()
        cmd = getattr(self, "cmd__{}".format(cmd), None)
        if cmd:
            try:
                cmd(*args)
            except Exception as e:
                self.log(e)

    def cmd__tune_num_of_workers(self, n):
        self.NUM_OF_WORKERS = int(n)

    def clean(self):
        while self._selector.get_map():
            self._select_and_process()
        self._wait_children()
        self._selector.close()
        self.clear_instance()

    def _select_and_process(self):
        events = self._selector.select(0.1)
        for key, _ in events:
            f = key.fileobj
            self._selector.unregister(f)
            command, result = pickle.loads(f.read())
            try:
                self.process_result(command, result)
            except Exception as e:
                self.log(e)
            f.close()

    def _fork(self, command):
        r, w = os.pipe()
        #print(r, w)
        pid = os.fork()
        if pid == 0:  # child
            self._foo.close()
            os.close(r)
            sender = os.fdopen(w, "wb")
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            resource.setrlimit(resource.RLIMIT_CPU, (self.RLIMIT_CPU, -1))
            resource.setrlimit(resource.RLIMIT_AS, (self.RLIMIT_AS, -1))
            try:
                result = self.work(command)
            except Exception as e:
                result = type(e)
            sender.write(pickle.dumps((command, result)))
            sender.close()
            sys.exit()
        else:
            os.close(w)
            f = os.fdopen(r, "rb")
            self._selector.register(f, selectors.EVENT_READ)
            self._children.add(pid)
            gc.collect()

    def init(self):
        pass

    def get_command(self) -> object:
        """Just an example

        Subclass should override this
        """

        try:
            return input("> ") or None
        except EOFError:
            pass

    def work(self, command) -> object:
        """Just an example

        Subclass should override this

        Returned value must be pickable
        """

        return eval(command, None, sys.modules)

    def process_result(self, command, result) -> None:
        """Just an example

        Subclass should override this
        """

        print("command:", command)
        print("result: ", repr(result))


def main():
    import random

    class T(MasterWorker):
        NUM_OF_WORKERS = 1
        def get_command(self):
            return time.time()
        def work(self, command) -> object:
            print(command)
            time.sleep(random.randint(1, 5))
        def cmd__test(self):
            print(self)

    T.instance().run()


if __name__ == "__main__":
    main()
