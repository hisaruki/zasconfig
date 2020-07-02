#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import argparse
import os
import sys
from zasconfig import Zfs

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
    parser.add_argument('-n', '--dry_run', action="store_true")
    parser.add_argument('--revisions', type=int, default=28)
    args = parser.parse_args()

    f = Zfs(args.fdataset)
    t = Zfs(args.tdataset)


    sys.stderr.write("{} => {}\n".format(f.name, t.name))
    f.send(t, recursive=False)
        