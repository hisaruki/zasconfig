#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import sys
import re
import os
import datetime
import argparse

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

    def get(self, attr):
        proc = ["zfs", "get", attr, self.name]
        proc = self.prefix + proc
        o, _ = Popen(proc, stdout=PIPE, stderr=PIPE).communicate()
        return o.decode().splitlines()[-1].split()[-2]

    def set(self, attr, value):
        proc = ["zfs", "set", "{}={}".format(attr, value), self.name]
        proc = self.prefix + proc
        sys.stderr.write(" ".join(proc) + "\n")
        o, _ = Popen(proc, stdout=PIPE, stderr=PIPE).communicate()
        if o:
            sys.stderr.write(o.decode()+"\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="ZFS Auto Snapshot Configure"
    )
    parser.add_argument('dataset')
    parser.add_argument('--on', action="store_true")
    parser.add_argument('--off', action="store_true")
    parser.add_argument('--inherit', action="store_true")
    parser.add_argument('--level', default=None)
    args = parser.parse_args()

    z = Zfs(args.dataset)
    bk = "com.sun:auto-snapshot"
    fks = ["frequent", "hourly", "daily", "weekly", "monthly"]

    if args.on:
        args.level = "11111"
    if args.off:
        args.level = "00000"

    if args.level and len(args.level) == 5:
        for seq, fk in enumerate(fks):
            v = ["false", "true"][int(args.level[seq])]
            k = "{}:{}".format(bk, fk)
            z.set(k, v)

    sys.stderr.write("\t".join(["NAME"] + fks) + "\n")
    sys.stderr.write(z.name + "\t")
    for fk in fks:
        k = "{}:{}".format(bk, fk)
        sys.stderr.write(z.get(k) + "\t")
