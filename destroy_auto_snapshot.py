#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import sys
import re
import argparse

parser = argparse.ArgumentParser(description="Destroy Auto Snapshot")
parser.add_argument('target')
parser.add_argument('--autokey', default="znap")
parser.add_argument("-r", '--recursive', action="store_true")
args = parser.parse_args()

def fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

cmd = [
    "zfs", "list", "-t", "snapshot", "-p"
]
if args.target != "all":
    cmd.append(args.target)
if args.recursive:
    cmd.append("-r")
o, e = Popen(cmd, stdout=PIPE).communicate()
if e:
    sys.stdout.write(e.decode()+"\n")
    sys.exit()

lines = o.decode().splitlines()
f = filter(lambda x:re.search("@", x), lines)
f = filter(lambda x:re.search(args.autokey, x), f)
data = [(line.split()[0], int(line.split()[1]))for line in f]
for _name, size in data:
    sys.stdout.write("{}\t{}\n".format(_name, size))
total = fmt(sum([x[1] for x in data]))
sys.stdout.write("{} free space will be available.\n".format(total))
sys.stdout.write("Do you want to continue [y/N]?")
if re.match("y", input().lower()):
    for snapshot, _ in data:
        cmd = ["zfs", "destroy", snapshot]
        sys.stdout.write("{}\n".format(" ".join(cmd)))
        o, e = Popen(cmd, stdout=PIPE).communicate()
        if o:
            sys.stdout.write(o.decode()+"\n")
        if e:
            sys.stdout.write(e.decode()+"\n")
            sys.exit()
