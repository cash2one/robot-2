#!/usr/bin/env python3

import contextlib
import hashlib
import itertools
import pathlib
import threading
import time

import bitarray

from sqliteset import Set

class BloomFilterMD5():
    BITARRAY_LENGTH = 2 ** 32
    NUM_OF_BITARRAY = 3

    def __init__(self, folder):
        self.folder = pathlib.Path(folder)
        if not self.folder.exists():
            self.folder.mkdir()

        self.bitarrays = []
        for idx in map(str, range(self.NUM_OF_BITARRAY)):
            fn = self.folder / idx
            if fn.exists():
                with fn.open("rb") as f:
                    ba = bitarray.bitarray()
                    ba.fromfile(f)
                    assert ba.length() == self.BITARRAY_LENGTH
            else:
                ba = bitarray.bitarray(self.BITARRAY_LENGTH)
                ba.setall(0)
            self.bitarrays.append(ba)
        assert len(self.bitarrays) == self.NUM_OF_BITARRAY

    def save(self):
        threading.Thread(target=self._save).start()

    def _save(self):
        for idx, ba in enumerate(self.bitarrays):
            with (self.folder / str(idx)).open("wb") as f:
                ba.tofile(f)

    def exists(self, key) -> bool:
        if isinstance(key, str):
            key = key.encode()
        return all(self.bitarrays[idx][offset]
                   for idx, offset in enumerate(self._hash_values(key)))

    def set(self, key):
        """set to true"""
        if isinstance(key, str):
            key = key.encode()
        for idx, offset in enumerate(self._hash_values(key)):
            self.bitarrays[idx][offset] = True

    def _hash_values(self, key):
        md5 = hashlib.md5(key).digest()
        from_bytes = int.from_bytes
        for idx in range(self.NUM_OF_BITARRAY):
            i, j = idx * 4, (idx + 1) * 4
            yield from_bytes(md5[i:j], "big")

    def clear(self):
        for ba in self.bitarrays:
            ba.setall(0)

    def close(self):
        self.save()


class RecordedText():
    PERSIST_INTERVAL = 0.1

    def __init__(self, fn):
        path = pathlib.Path(fn)
        self.file = path.open()
        self.tail = path.open("a")
        self.position_mark = mark = path.parent / ".pos.{}".format(path.name)
        self.position = 0
        if mark.exists():
            with mark.open() as f:
                self.position = self.file.seek(int(f.read() or "0"))
        self._th = threading.Thread(target=self._periodic_persist)
        self._th.start()

    def __iter__(self):
        while True:
            line = self.read()
            if line is None:
                raise StopIteration
            yield line

    def _periodic_persist(self):
        prev = self.position
        while threading.main_thread().is_alive() and self.position is not None:
            if self.position == prev:
                time.sleep(0.01)
                continue
            self._persist()
            prev = self.position
            time.sleep(self.PERSIST_INTERVAL)

    def _persist(self):
        with self.position_mark.open("w") as f:
            f.write(str(self.position))

    def read(self):
        """
        return None if empty line
        """

        line = self.file.readline()
        if line:
            self.position = self.file.tell()
            return line.rstrip()

    def write(self, line):
        if not line.endswith("\n"):
            line += "\n"
        self.tail.write(line)
        self.tail.flush()

    def renew(self, position=0):
        self.position = position
        self.file.seek(position)

    def close(self):
        self.file.close()
        self.tail.close()
        self._persist()
        self.position = None
        self._th.join()


class Tasks():
    def __init__(self, name):
        self.text = RecordedText(name)
        self.set = Set(0x100, name + ".set")
        self.name = name

    def get(self):
        task = self.text.read()
        return task

    def add(self, *tasks):
        tasks = set(t for t in tasks if t not in self.set)
        self.set.add(*tasks)
        for t in tasks:
            self.text.write(t)
        return len(tasks)

    def close(self):
        self.set.close()
        self.text.close()


def test():
    t = Tasks("hosts")
    #t.set.clear()

    print(len(t.set))
    #l = []
    while True:
        try:
            x = input()
        except EOFError:
            break
        assert x in t.set
        #t.add(x)
        #l.append(x)

    #t.add(*l)
    print(len(t.set))

    #for _ in range(10):
    #    print(t.get())

    #t.text.renew()
    t.close()


if __name__ == "__main__":
    test()
