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


class Alexa(db.Entity):
    """用于控制爬虫的任务状态
    """

    name = Required(str, 100, index=True)
    rank = Required(int)
    date = Required(datetime.date)


class Tmp(db.Entity):
    key = PrimaryKey(str)


def init(db_file=None, hosts_file=None):
    db_file = db_file or ":memory:"
    db.bind("sqlite", db_file, create_db=True)
    db.generate_mapping(create_tables=True)
    sql_debug(False)
    if hosts_file is None:
        return

    with open(hosts_file) as f:
        today = datetime.date.today()
        with db_session:
            done = set(select(i.name for i in Host))

        def add_host(name):
            if name not in done:
                Host(name=name)
                done.add(name)

        for rank, s in enumerate(f, 1):
            name = s.strip()
            with db_session:
                #Alexa(name=name, date=today, rank=rank)
                add_host(name)
                if not name.startswith("www."):
                    add_host("www." + name)


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
