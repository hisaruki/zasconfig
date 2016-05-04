#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess,sys,collections,argparse

parser = argparse.ArgumentParser(description="ZFS Auto Snapshot Config")
parser.add_argument('--mode',default="view")
parser.add_argument('--reset',dest="mode",action="store_const",const="reset")
parser.add_argument('--on',dest="mode",action="store_const",const="on")
parser.add_argument('--levels')
parser.add_argument('target',nargs="?")
args = parser.parse_args()


def view():
  if len(status) > 0:
    i = 0
    for attr in attrs:
      key = "com.sun:auto-snapshot:"+attr
      proc = subprocess.Popen([
        "zfs",
        "get",
        key,
      ], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
      for line in proc[0].decode("utf-8").splitlines():
        if line.split()[2] == "true" and line.split()[0] in status:
          status[line.split()[0]][i] = "1"
      i += 1
  for path in status:
    line = list(map(lambda x:x.rjust(8),status[path]))
    print(path.ljust(max([len(x) for x in status]))+"".join(line))

def reset():
  for attr in attrs:
    key = "com.sun:auto-snapshot:"+attr
    for path in status:
      if path != "NAME":
        proc = subprocess.Popen([
          "zfs",
          "inherit",
          key,
          path
        ]).communicate()

def set():
  i = 0
  for level in args.levels:
    level = bool(int(level))
    key = "com.sun:auto-snapshot:"+attrs[i]+"="+str(level).lower()
    set = ["zfs","set",key,args.target]
    proc = subprocess.Popen(set).communicate()
    i += 1


attrs = ["frequent","hourly","daily","weekly","monthly"]
status = collections.OrderedDict()
get = ["zfs","list"]
if args.target:
  get.append("-r")
  get.append(args.target)

proc = subprocess.Popen(get, stdout=subprocess.PIPE).communicate()
for line in proc[0].decode("utf-8").splitlines():
  status[line.split()[0]] = ["0","0","0","0","0"]
  if line.split()[0] == "NAME":
    status[line.split()[0]] = attrs


if args.mode == "view":
  view()

if args.mode == "reset":
  reset()
  view()

if args.mode == "on":
  if not args.levels:args.levels = "11111"
  if args.target and len(args.levels) == 5:
    set()
    view()
