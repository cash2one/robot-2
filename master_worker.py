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
import struct
import socket
import multiprocessing
import sys
import time
import threading


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
        self._reader, self._writer = socket.socketpair()
        self._reader.settimeout(0.01)
        self._writer_lock = multiprocessing.Lock()
        self._struct_msg_header = struct.Struct("!I")
        self._loop_flag = False
        signal.signal(signal.SIGCHLD, self._sig_chld)
        signal.signal(signal.SIGUSR1, self._sig_usr1)
        signal.signal(signal.SIGTERM, self._sig_term)
        with open(".{}.pid".format(type(self).__name__), "w") as f:
            f.write(str(os.getpid()))

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
        self._loop_flag = False

    def _sig_usr1(self, signum, frame):
        try:
            with open(".cmd") as f:
                text = f.read()
            cmd, *args = text.split()
            cmd = getattr(self, "cmd__{}".format(cmd), None)
            if cmd:
                cmd(*args)
        except Exception as e:
            self.log(e)

    def log(self, x):
        print(datetime.datetime.now(), x, file=sys.stderr, flush=True)

    def run(self):
        self.init()
        gc.disable()

        self._loop_flag = True
        while self._loop_flag:
            # loop
            if len(self._children) < self.NUM_OF_WORKERS:
                try:
                    command = self.get_command()
                except Exception as e:
                    self.log(e)
                    command = None
                if command is None:
                    self._loop_flag = False
                else:
                    self._fork(command)

            self._recv_and_proc()
            # loop

        self.clean()

    def cmd__tune_num_of_workers(self, n):
        self.NUM_OF_WORKERS = int(n)

    def clean(self):
        while self._children:
            self._recv_and_proc()
        self._reader.close()
        self._writer.close()
        self.clear_instance()

    def _recv(self):
        header = b''
        while True:
            rest = self._struct_msg_header.size - len(header)
            if not rest:
                break
            header += self._reader.recv(rest)

        size, = self._struct_msg_header.unpack(header)

        body = b''
        while True:
            rest = size - len(body)
            if not rest:
                break
            body += self._reader.recv(rest)

        return body

    def _recv_and_proc(self):
        while True:
            try:
                msg = self._recv()
                command, result = pickle.loads(msg)
                try:
                    self.process_result(command, result)
                except Exception as e:
                    self.log(e)
            except socket.timeout:
                break

    def _fork(self, command):
        pid = os.fork()
        if pid == 0:  # child
            self._reader.close()
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            signal.signal(signal.SIGUSR1, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            resource.setrlimit(resource.RLIMIT_CPU, (self.RLIMIT_CPU, -1))
            resource.setrlimit(resource.RLIMIT_AS, (self.RLIMIT_AS, -1))
            try:
                result = self.work(command)
            except Exception as e:
                result = type(e)
            msg = pickle.dumps((command, result))
            with self._writer_lock:
                self._writer.sendall(self._struct_msg_header.pack(len(msg)) + msg)
            self._writer.close()
            sys.exit()
        else:
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
            time.sleep(random.random() * 10)
            return command, os.getpid()
        def cmd__test(self):
            print(self)

    T.instance().run()


if __name__ == "__main__":
    main()
