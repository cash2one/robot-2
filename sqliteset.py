#!/usr/bin/env python3

import sqlite3
import zlib
import functools
import threading


class Set(object):
    SQL_CREATE = """
    CREATE TABLE IF NOT EXISTS t (
        k TEXT PRIMARY KEY
    )
    """

    def __init__(self, base=0x10, name="set-dbs"):
        self.base = base
        self._cache = {}
        self._dbs = [sqlite3.connect("{}/{:02x}".format(name, i))
                     for i in range(base)]
        self._cursors = [db.cursor() for db in self._dbs]
        self._counter = [0] * base
        for db in self._dbs:
            db.execute("PRAGMA synchronous = off")
            db.execute("PRAGMA temp_store = memory")
            db.execute("PRAGMA journal_mode = memory")
            db.execute("PRAGMA secure_delete = false")
            db.execute(self.SQL_CREATE)

    def __del__(self):
        self.close()

    def __len__(self):
        n = 0
        for c in self._cursors:
            c.execute("SELECT COUNT(k) FROM t")
            n += c.fetchone()[0]
        return n

    def close(self):
        for db in self._dbs:
            db.close()

    @functools.lru_cache(maxsize=100000)
    def index(self, key):
        return zlib.adler32(key.encode()) % self.base

    def __contains__(self, key):
        if key in self._cache:
            o = self._cache[key]
        else:
            o = self._contains(key)
        return o

    @functools.lru_cache(maxsize=30000)
    def _contains(self, key):
        idx = self.index(key)
        c = self._cursors[idx]
        c.execute("SELECT 1 FROM t WHERE k = ? LIMIT 1", (key,))
        return bool(c.fetchone())

    def clear(self):
        for db in self._dbs:
            db.execute("DROP TABLE t")
            db.execute(self.SQL_CREATE)

    def _batched(sql, alive:bool):
        """
        """

        def method(self, *keys):
            if len(self._cache) > 50000:
                self._cache.clear()
                self._contains.cache_clear()
            for key in keys:
                idx = self.index(key)
                c = self._cursors[idx]
                c.execute(sql, (key,))
                if c.rowcount:
                    self._counter[idx] += c.rowcount
                    self._cache[key] = alive
            n_all = 0
            for idx, n in enumerate(self._counter):
                if n:
                    n_all += n
                    #print(idx, n)
                    self._dbs[idx].commit()
                    self._counter[idx] = 0
            assert not any(self._counter)
            return n_all

        return method

    add = _batched("INSERT OR IGNORE INTO t(k) VALUES(?)", True)
    remove = _batched("DELETE FROM t WHERE k = ?", False)
    discard = remove
    del _batched


def main():
    s = Set(256)
    #s.clear()
    l = []
    while True:
        try:
            x = input()
        except EOFError:
            break
        print(x in s)
        #l.append(x)
        s.add(x)
        assert x in s
        #s.remove(x)

    print(s.add(*l))
    print(len(s))


if __name__ == "__main__":
    main()
