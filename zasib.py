#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess,sys,argparse,re

parser = argparse.ArgumentParser(description="ZFS Auto Snapshot Incremental Backup")
parser.add_argument('_from')
parser.add_argument('_to')
parser.add_argument('--path')
args = parser.parse_args()

def ref(o):
  result = {}
  for l in o:
    l = l.split()
    if not l[0] == "NAME":
      path,snapshot = l[0].split("@")
      path = re.sub("^"+args._from+"/","",path)
      path = re.sub("^"+args._to+"/","",path)
      if not path in result:
        result[path] = []
      if not re.search("zfs-auto-snap_frequent|zfs-auto-snap_hourly",snapshot):
        result[path].append(snapshot)
  return result

f = subprocess.Popen(["zfs","list","-t","snapshot","-r",args._from], stdout=subprocess.PIPE).communicate()[0].decode("utf-8").splitlines()
f = ref(f)
t = subprocess.Popen(["zfs","list","-t","snapshot","-r",args._to], stdout=subprocess.PIPE).communicate()[0].decode("utf-8").splitlines()
t = ref(t)

for path in f:
  if path in t:
    if args.path == path or not args.path:
      inc = t[path][-1]
      target = f[path][-1]
      if inc != target:
        send = ["sudo","zfs","send","-i",args._from+"/"+path+"@"+inc,args._from+"/"+path+"@"+target]
        recv = ["sudo","zfs","recv",args._to+"/"+path+"@"+target]
        print(" ".join(send)+" | "+" ".join(recv))
        p1 = subprocess.Popen(send, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(recv, stdin=p1.stdout)
        p1.stdout.close()
        p2.communicate()

t = subprocess.Popen(["zfs","list","-t","snapshot","-r",args._to], stdout=subprocess.PIPE).communicate()[0].decode("utf-8").splitlines()
t = ref(t)


for path in t:
  if len(t[path]) > 1 and t[path][0] in f[path] and t[path][-1] in f[path]:
    proc = ["sudo","zfs","destroy",args._to+"/"+path+"@"+t[path][0]]
    print(" ".join(proc))
    subprocess.Popen(proc, stdout=subprocess.PIPE).communicate()[0].decode("utf-8").splitlines()
    t[path].pop(0)



