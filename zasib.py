#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import argparse
import sys
import re
import os
import datetime

parser = argparse.ArgumentParser(
    description="ZFS Auto Snapshot Incremental Backup"
)
parser.add_argument('fdataset')
parser.add_argument('tdataset')
parser.add_argument('-a', '--all', action="store_true")
parser.add_argument('-r', '--rename', action="store_true")
parser.add_argument('-s', '--send', action="store_true")
parser.add_argument('-c', '--compare', action="store_true")
parser.add_argument('--revisions', type=int, default=28)
parser.add_argument('--prefrom', nargs="*", default=[])
parser.add_argument('--preto', nargs="*", default=[])
args = parser.parse_args()


if "ZASIB_PREF" in os.environ:
    prefrom = os.environ["ZASIB_PREF"].split()
if "ZASIB_PRET" in os.environ:
    preto = os.environ["ZASIB_PRET"].split()
if args.prefrom:
    prefrom = args.prefrom
if args.preto:
    preto = args.preto
if args.all:
    args.rename, args.send, args.compare = True, True, True

class Snapshot:
    def __init__(self, l, dataset):
        self.dataset = dataset
        l = l.split()
        self.name = l[0]
        self.name = self.name.split("@")[-1]
        self.fullname = self.dataset.name + "@" + self.name
        self.used = l[1]
        self.avail = l[2]
        self.refer = l[3]
        self.mountpoint = l[4]

    def rename(self, newname=None):
        if newname is None:
            newname = re.sub(r'[^0-9]', '', self.name)[0:8]
        newfullname = "{}@{}".format(self.dataset.name, newname)

        if not self.dataset.search_snapshot(newname):
            sys.stderr.write("# {} => {}\n".format(self.fullname, newfullname))
            proc = [
                "zfs",
                "rename",
                self.fullname,
                newname
            ]
            proc = self.dataset.prefix + proc
            sys.stderr.write(" ".join(proc) + "\n")
            Popen(proc).communicate()
            self.dataset.create_status()
    def destroy(self):
        proc = [
            "zfs",
            "destroy",
            self.fullname
        ]
        proc = self.dataset.prefix + proc
        sys.stderr.write(" ".join(proc) + "\n")
        Popen(proc).communicate()
        self.dataset.create_status()

class Zfs:
    def __init__(self, datasetname):
        self.name = datasetname
        self._snapshots = []
        self.prefix = []
        self.create_status()

    def create_status(self):
        proc = ["zfs", "list", self.name, "-t", "snapshot"]
        o, e = Popen(proc, stdout=PIPE, stderr=PIPE).communicate()
        self.exists = True
        if e:
            self.exists = False
        for l in o.decode().splitlines():
            if l.find("NAME") == 0:
                continue
            snapshot = Snapshot(l, self)
            self._snapshots.append(snapshot)

    def _filter_snapshots(self, mode):
        f = self._snapshots
        if mode == "znap":
            f = filter(lambda x:x.name.find("znap") >= 0, self._snapshots)
        if mode == "static":
            f = filter(lambda x:x.name.find("znap") == -1, self._snapshots)
        f = list(f)
        return f

    def snapshot(self, eq, mode="all"):
        f = self._filter_snapshots(mode)
        if len(f):
            return f[eq]
    
    def snapshots(self, mode="all"):
        f = self._filter_snapshots(mode)
        return f

    def search_snapshot(self, text):
        res = [x for x in self._snapshots if x.name == text]
        if len(res):
            return res[0]
        else:
            return False

    def send(self, dataset1, dataset2=None):
        proc = ["zfs", "send", dataset1.fullname]
        if dataset2:
            proc = ["zfs", "send", "-I", dataset1.fullname, dataset2.fullname]
        proc = self.prefix + proc
        class Exec:
            def __init__(self, proc):
                self.proc = proc
        return Exec(proc)

    def recv(self):
        proc = ["zfs", "recv", self.name]
        proc = self.prefix + proc
        class Exec:
            def __init__(self, proc):
                self.proc = proc
        return Exec(proc)

if __name__ == '__main__':
    f = Zfs(args.fdataset)
    f.prefix = prefrom
    if f.exists is False:
        sys.stderr.write("dataset {} does not exist.\n".format(f.name))
        sys.exit(1)
    t = Zfs(args.tdataset)
    t.prefix = preto

    if args.rename:
        r = f.snapshot(-1, "znap")
        if r:
            r.rename()

    if args.send:
        if not t.exists:
            sys.stderr.write("#First time sending...\n")
            send = f.send(f.snapshot(0, "static"))
            recv = t.recv()
            l = "{} | {}\n".format(" ".join(send.proc), " ".join(recv.proc))
            sys.stderr.write(l)
            send = Popen(send.proc, stdout=PIPE)
            recv = Popen(recv.proc, stdin=send.stdout)
            recv.communicate()
            f.create_status()
            t.create_status()
        istart = t.snapshot(-1, "static").name
        istart = f.search_snapshot(istart)
        iend = f.snapshot(-1, "static")
        if istart and iend:
            if istart.name != iend.name:
                sys.stderr.write("#Incrimental sending...\n")
                send = f.send(istart, iend)
                recv = t.recv()
                l = "{} | {}\n".format(" ".join(send.proc), " ".join(recv.proc))
                sys.stderr.write(l)

                send = Popen(send.proc, stdout=PIPE)
                recv = Popen(recv.proc, stdin=send.stdout)
                recv.communicate()
                f.create_status()
                t.create_status()

    if args.compare:
        fa = set([x.name for x in f.snapshots("static")])
        ta = set([x.name for x in t.snapshots("static")])
        target = list(fa and ta)
        target.sort()
        while len(target) > args.revisions:
            target.pop(0)
        target = set(target)
       
        for x in f.snapshots("static") + t.snapshots("static"):
            if x.name not in target:
                x.destroy()