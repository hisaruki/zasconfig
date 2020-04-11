#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import argparse
import os
import sys
from zasconfig import Zfs, Snapshot

if __name__ == '__main__':
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
        t.set("tk.hisaruki.greed:ZASIB_RESTORE", f.name)

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