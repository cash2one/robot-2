#!/usr/bin/env python3

import sys
import datetime
import re
import time

from pony.orm import *


db = Database()


is_valid_host = re.compile(
    r"([-a-z0-9]{1,64}\.)+"
    r"[a-z]{2,8}"
    r"(:[0-9]{2,5})?"
).fullmatch


class Host(db.Entity):
    """用于控制爬虫的任务状态
    """

    name = Required(str, 100, unique=True)
    crawler_started = Optional(datetime.datetime, index=True)
    crawler_done = Optional(datetime.datetime, index=True)
    ip = Optional(str, 15, nullable=True)
    redirect = Optional(str, 100, nullable=True)
    err = Optional(str, 80, nullable=True)
    url = Optional(str, nullable=True)
    title = Optional(str, nullable=True)
    keywords = Optional(str, nullable=True)
    description = Optional(str, nullable=True)
    encoding = Optional(str, 30, nullable=True)
    language = Optional(str, 10, nullable=True)


class Tmp(db.Entity):
    key = PrimaryKey(str)


def init(db_file=None, hosts_file=None):
    db_file = db_file or ":memory:"
    db.bind("sqlite", db_file, create_db=True)
    db.generate_mapping(create_tables=True)
    sql_debug(False)
    if hosts_file is None:
        return

    with db_session, open(hosts_file) as f:
        done = set()

        for i in f:

            name = i.strip()
            if name not in done:
                Host(name=name)
            done.add(name)

            if not name.startswith("www."):
                name = "www." + name
                if name not in done:
                    Host(name=name)
                done.add(name)


def main():
    return


if __name__ == "__main__":
    sql_debug(True)
    init(*sys.argv[1:])
    main()
    """
    select count(name) from host where crawler_started < datetime('now', 'localtime', '-5 minutes') and (crawler_done is null or crawler_done < crawler_started);
    update host set crawler_started = null where crawler_started < datetime('now', 'localtime', '-5 minutes') and (crawler_done is null or crawler_done < crawler_started);
    """
