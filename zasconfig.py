#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import sys
import re
import os
import datetime
import argparse

class Snapshot:
    def __init__(self, zfs, snapshotname, pre=[]):
        self.dataset = zfs
        self.name = snapshotname
        self.fullname = "{}@{}".format(zfs.fullname, snapshotname)
        self.pre = pre

    def exists(self):
        o, e = Popen(self.pre + ["zfs", "list", self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        return bool(o)

    def rollback(self, dry_run):
        proc = self.pre + ["zfs", "rollback", self.fullname, "-r"]
        if dry_run is True:
            sys.stderr.write("# ")
        l = "{}\n".format(" ".join(proc))
        sys.stderr.write(l)
        if dry_run is False:
            o, e = Popen(proc, stdout=PIPE, stderr=PIPE).communicate()
        return not bool(e)

class Zfs:
    def __init__(self, fullname, pre=[]):
        self.fullname = fullname
        self.name = fullname.split("/")[-1]
        self.depth = self.fullname.split("/")
        self.pre = pre

    def children(self, recursive=False):
        o, _ = Popen(self.pre + ["zfs", "list", self.fullname, "-r"], stdout=PIPE, stderr=PIPE).communicate()
        res = [x.split()[0] for x in o.decode().splitlines()[1:]]
        res = map(lambda x:x.split("/"), res)
        if recursive is True:
            res = filter(lambda x:len(x) >= len(self.depth) + 1, res)
        if recursive is False:
            res = filter(lambda x:len(x) == len(self.depth) + 1, res)
        res = map(lambda x:Zfs("/".join(x), self.pre), res)
        for child in res:
            yield child

    def snapshot(self, key):
        proc = ["zfs", "snapshot", "{}@{}".format(self.fullname, key)]
        o, e = Popen(self.pre + proc, stdout=PIPE, stderr=PIPE).communicate()
        return o, e

    def parent(self):
        d = self.fullname.split("/")[0:-1]
        if not len(d):
            return None
        else:
            return Zfs("/".join(d), self.pre)

    def exists(self):
        o, e = Popen(self.pre + ["zfs", "list", self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        return bool(o)

    def snapshots(self):
        o, e = Popen(self.pre + ["zfs", "list", "-t", "snapshot", "-r", self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        f = filter(lambda x:re.match('{}@'.format(self.fullname), x), o.decode().splitlines())
        f = filter(lambda x:not re.search("@znap", x), f)
        return [Snapshot(self, x.split()[0].split("@")[-1], self.pre) for x in f]

    def samekeys(self, recv):
        fns = [x.name for x in self.snapshots()]
        tns = [x.name for x in recv.snapshots()]
        res = []
        for key in fns:
            if key in (set(fns) and set(tns)):
                res.append(key)
        return res

    def send(self, recv, dry_run=False):
        print(self.fullname)
        if len(self.snapshots()):
            samekeys = self.samekeys(recv)
            def sender():
                proc = self.pre + ["zfs", "send"]
                if not samekeys:
                    proc.append(self.snapshots()[0].fullname)
                else:    
                    proc += [
                        "-I",
                        "{}@{}".format(self.fullname, samekeys[-1]),
                        f.fullname
                    ]
                return proc
            def reciever():
                proc = self.pre + ["zfs", "recv", recv.fullname]
                return proc

            if not samekeys:
                f = self.snapshots()[0]
            else:
                f = self.snapshots()[-1]
                if samekeys[-1] == f.name:
                    sys.stderr.write("## {} and {} are already synced for each other.\n".format(self.fullname, recv.fullname))
                    f = None

            if f:
                try:
                    s = recv.snapshots()[-1]
                    s.pre = os.environ["ZASIB_PRET"].split()
                    s.rollback(dry_run)
                except:
                    pass
                if dry_run is True:
                    sys.stderr.write("# ")
                l = "{} | {}\n".format(" ".join(sender()), " ".join(reciever()))
                sys.stderr.write(l)
                if recv.parent().exists():
                    if dry_run is False:
                        ps = Popen(sender(), stdout=PIPE)
                        pr = Popen(reciever(), stdin=ps.stdout)
                        pr.communicate()
                else:
                    if dry_run is False:
                        sys.stderr.write("# [Error] parent dataset not found.\n")

    def get(self, key):
        o, e = Popen(self.pre + ["zfs", "get", key, self.fullname], stdout=PIPE, stderr=PIPE).communicate()
        res = o.decode().splitlines()[-1].split()
        return (res[2], res[3])

    def set(self, key, value):
        proc = self.pre + ["zfs", "set", "{}={}".format(key, value), self.fullname]
        if value is False:
            proc = self.pre + ["zfs", "inherit", key, self.fullname]
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
    if args.on is True:
        if type(args.level) == str:
            args.level = [str(bool(int(x))).lower() for x in args.level]
        else:
            args.level = ["true", "true", "true", "true", "true"]
    if args.off is True:
        args.level = ["false", "false", "false", "false", "false"]
    if args.reset is True:
        args.level = [False, False, False, False, False]

    for dataset in datasets:
        Zfs(dataset).zasconfig(args.level)