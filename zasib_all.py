#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import sys
import re

f_root = sys.argv[1]
t_root = sys.argv[2]
commands = sys.argv[3:]

keys = []

proc = ["zfs", "list", f_root, "-r"]
o, e = Popen(proc, stdout=PIPE).communicate()
for l in filter(lambda x:x.find("NAME") < 0, o.decode().splitlines()):
    key = l.split()[0]
    keys.append(key)

for f in keys:
    t = re.sub(re.compile(r'^' + f_root), t_root, f)
    proc = ["zasib", f, t] + commands
    print(" ".join(proc))
    Popen(proc).communicate()
