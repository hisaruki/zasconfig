#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import sys
import re
import os
import datetime
import argparse

class Snapshot:
    def __init__(self, zfs, snapshotname):
        self.dataset = zfs
        self.name = snapshotname
        self.fullname = "{}@{}".format(zfs.fullname, snapshotname)

    def exists(self):
        o, e = Popen(["zfs", "list", self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        return bool(o)

class Zfs:
    def __init__(self, fullname):
        self.fullname = fullname
        self.name = fullname.split("/")[-1]
        self.depth = len(self.name.split("/"))

    def children(self):
        o, e = Popen(["zfs", "list", self.fullname, "-r"], stdout=PIPE, stderr=PIPE).communicate()
        res = [x.split()[0] for x in o.decode().splitlines()[1:]]
        res = map(lambda x:x.split("/"), res)
        res = filter(lambda x:len(x) == self.depth + 1, res)
        res = map(lambda x:Zfs("/".join(x)), res)
        return list(res)

    def parent(self):
        d = self.fullname.split("/")[0:-1]
        if not len(d):
            return None
        else:
            return Zfs("/".join(d))

    def exists(self):
        o, e = Popen(["zfs", "list", self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        return bool(o)

    def snapshots(self):
        o, e = Popen(["zfs", "list", "-t", "snapshot", "-r", self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        f = filter(lambda x:re.match('{}@'.format(self.fullname), x), o.decode().splitlines())
        return [Snapshot(self, x.split()[0].split("@")[-1]) for x in f]

    def samekeys(self, recv):
        fns = set([x.name for x in self.snapshots()])
        tns = set([x.name for x in recv.snapshots()])
        res = None
        for res in (fns and tns):
            pass
        return res


    def send(self, recv, recursive=False):
        def sender():
            proc = ["zfs", "send"]
            self.samekeys(recv)

            proc.append(f.fullname)
            if "ZASIB_PREF" in os.environ:
                proc = os.environ["ZASIB_PREF"].split() + proc
            return proc

        def reciever():
            proc = ["zfs", "recv", t.fullname]
            if "ZASIB_PRET" in os.environ:
                proc = os.environ["ZASIB_PRET"].split() + proc
            return proc

        f = self.snapshots()[-1]
        t = Snapshot(Zfs(recv.fullname + "/" + self.name), f.name)
        if t.dataset.parent().exists():
            l = "{} | {}\n".format(" ".join(sender()), " ".join(reciever()))
            sys.stderr.write(l)
            """
            ps = Popen(sender(), stdout=PIPE)
            pr = Popen(reciever(), stdin=ps.stdout)
            pr.communicate()

            if recursive:
                for child in self.children():
                    child.send(Zfs(recv.fullname + "/" + child.name), recursive)
            """

    def get(self, key):
        o, e = Popen(["zfs", "get", key, self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        res = o.decode().splitlines()[-1].split()
        return (res[2], res[3])

    def set(self, key, value):
        proc = ["zfs", "set", "{}={}".format(key, value), self.fullname]
        if value is False:
            proc = ["zfs", "inherit", key, self.fullname]
        if "ZASIB_PREF" in os.environ:
            proc = os.environ["ZASIB_PREF"].split() + proc
        o, e = Popen(proc, stdout=PIPE, stderr=PIPE).communicate()
        if e:
            sys.stderr.write("{}".format(e.decode()))
        if o:
            sys.stderr.write("{}".format(o.decode()))

    def zasconfig(self, attr=None):
        keys = ["frequent", "hourly", "daily", "weekly", "monthly"]
        for i in range(0, len(keys)):
            k = "com.sun:auto-snapshot:{}".format(keys[i])
            v = None
            if type(attr) == list:
                v = attr[i]
            if v is not None:
                self.set(k, v)
            print( self.fullname, k, self.get(k) )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('datasets', nargs="*", default=None)
    parser.add_argument('--level', default=None)
    parser.add_argument('--reset', action="store_true")
    parser.add_argument('--on', action="store_true")
    parser.add_argument('--off', action="store_true")
    args = parser.parse_args()

    if not args.datasets:
        o, e = Popen(["zpool", "list"], stdout=PIPE, stderr=PIPE).communicate()
        datasets = [l.split()[0] for l in o.decode().splitlines() if l.split()[0] != "NAME"]
    else:
        datasets = args.datasets
    if type(args.level) == str:
        args.level = [str(bool(int(x))).lower() for x in args.level]
    if args.on is True:
        args.level = ["true", "true", "true", "true", "true"]
    if args.off is True:
        args.level = ["false", "false", "false", "false", "false"]
    if args.reset is True:
        args.level = [False, False, False, False, False]

    for dataset in datasets:
        Zfs(dataset).zasconfig(args.level)