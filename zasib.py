#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import argparse
import os
import sys
import re
import datetime
from zasconfig import Zfs
from pathlib import Path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="ZFS Auto Snapshot Incremental Backup"
    )
    parser.add_argument('fdataset')
    parser.add_argument('tdataset')
    parser.add_argument('-n', '--dry_run', action="store_true")
    parser.add_argument('-r', '--recursive', action="store_true")
    parser.add_argument('-c', '--create', action="store_true")
    args = parser.parse_args()

    try:
        pret = os.environ["ZASIB_PRET"].split()
    except:
        pret = []
    f = Zfs(args.fdataset, pret)
    try:
        pref = os.environ["ZASIB_PREF"].split()
    except:
        pref = []
    t = Zfs(args.tdataset)


    if f.name != t.name:
        t = Zfs(t.fullname + "/" + f.name, pret)
    if args.create:
        k = datetime.datetime.now().strftime("%Y%m%d")
        f.snapshot(k)
    f.send(t, dry_run=args.dry_run)


    if args.recursive:
        for fc in f.children(args.recursive):
            tc = Zfs(t.fullname + re.sub("^{}".format(f.fullname), "", fc.fullname), pref)
            sys.stderr.write("# {} -> {}\n".format(fc.fullname, tc.fullname))
            if args.create:
                k = datetime.datetime.now().strftime("%Y%m%d")
                fc.snapshot(k)
            fc.send(tc, dry_run=args.dry_run)

