#!/usr/bin/env python3

import contextlib
import hashlib
import itertools
import pathlib
import threading
import time

import bitarray


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
    def __init__(self, fn):
        path = pathlib.Path(fn)
        self.file = path.open()
        self.position_mark = mark = path.parent / ".pos.{}".format(path.name)
        self.position = 0
        if mark.exists():
            with mark.open() as f:
                self.position = self.file.seek(int(f.read()))
        threading.Thread(target=self._periodic_persist).start()

    def __iter__(self):
        while True:
            line = self.read()
            if line is None:
                raise StopIteration
            yield line

    def _periodic_persist(self):
        prev = self.position
        while True:
            if self.position is None:  # see method close
                break
            if self.position == prev:
                time.sleep(0.01)
                continue
            with self.position_mark.open("w") as f:
                f.write(str(self.position))
            prev = self.position
            time.sleep(0.1)

    def read(self):
        """
        return None if empty line
        """

        line = self.file.readline()
        if line:
            self.position = self.file.tell()
            return line.rstrip()

    def renew(self, position=0):
        self.file.seek(position)
        with self.position_mark.open("w") as f:
            f.write(str(position))

    def close(self):
        self.file.close()
        self.position = None
        time.sleep(0.2)


class Tasks():
    def __init__(self, name):
        self.text = RecordedText(name)
        self.tail = open(name, "a")
        self.filter = BloomFilterMD5(name + ".bf")
        self.name = name
        self._count_of_new_tasks = 0

    def get(self):
        task = self.text.read()
        return task

    def add(self, task):
        if not self.filter.exists(task):
            print(task, file=self.tail, flush=True)
            self.filter.set(task)
            self._count_of_new_tasks += 1
            if self._count_of_new_tasks > 100 * 1000:
                self.filter.save()
                self._count_of_new_tasks = 0

    def rebuild_filter(self):
        """
        when you were confused, do it
        """
        self.filter.clear()
        threading.Thread(target=self._rebuild_filter).start()

    def _rebuild_filter(self):
        with open(self.name) as f:
            for line in f:
                self.filter.set(line.rstrip())
        self.filter.save()

    def finish(self):
        self.text.close()
        self.tail.close()
        self.filter.close()


def test():
    t = Tasks("hosts")
    t.rebuild_filter()
    #t.finish()


if __name__ == "__main__":
    test()
