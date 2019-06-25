#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen, PIPE
import sys
import re

proc = [
    "zfs",
    "get",
    "all"
]

o, e = Popen(proc, stdout=PIPE).communicate()

data = [x for x in o.decode().splitlines()]
data = filter(lambda x:re.search("local", x), data)
data = filter(lambda x:not re.search("@", x), data)
data = filter(lambda x:re.search("com.sun:auto-snapshot", x), data)
data = [x.split() for x in data]


for dataset, key, value, source in data:
    proc = ["zfs", "inherit", key, dataset]
    Popen(proc).communicate()