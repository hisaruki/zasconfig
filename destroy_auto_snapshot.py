#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import sys
import re
import argparse
import collections

parser = argparse.ArgumentParser(description="Destroy Auto Snapshot")
parser.add_argument('target')
args = parser.parse_args()

proc = subprocess.Popen([
    "zfs", "list", "-t", "snapshot", "-p"
], stdout=subprocess.PIPE).communicate()


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

lines = proc[0].decode("utf-8").splitlines()
lines = map(lambda x: [x.split()[0], x.split()[1]], lines)
lines = [x for x in lines if re.search(r'znap', x[0])]
if args.target != "all":
    lines = [x for x in lines if re.search("^" + args.target + "@", x[0])]
lines = list(map(lambda x: [x[0], int(x[1])], lines))

gain = sum([x[1] for x in lines])

if len(lines):
    for line in lines:
        print(line)

    print(sizeof_fmt(gain) + " free space will be available.")
    sys.stdout.write("Do you want to continue [Y/n]?")
    if re.match("y", input().lower()):
        for line in lines:
            subprocess.Popen(["zfs", "destroy", line[0]])
