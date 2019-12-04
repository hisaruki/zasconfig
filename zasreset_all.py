#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen, PIPE
import sys
import re

proc = [
    "zfs",
    "list"
]

o, e = Popen(proc, stdout=PIPE).communicate()

keys = [
    "com.sun:auto-snapshot:hourly",
    "com.sun:auto-snapshot:frequent",
    "com.sun:auto-snapshot:daily",
    "com.sun:auto-snapshot:monthly", 
    "com.sun:auto-snapshot:weekly"
]

for l in o.decode().splitlines():
    dataset = l.split()[0]
    for key in keys:
        proc = ["zfs", "inherit", key, dataset]
        sys.stderr.write(" ".join(proc) + "\n")
        Popen(proc).communicate()
