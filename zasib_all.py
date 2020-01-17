#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import re
from subprocess import Popen, PIPE

parser = argparse.ArgumentParser()
parser.add_argument('fpool')
parser.add_argument('tpool')
parser.add_argument('--zasib', default="/home/hisaruki/Public/zasconfig/zasib.py")
args = parser.parse_args()

o, e = Popen(["zfs", "list", "-r", args.fpool], stdout=PIPE).communicate()
fdatasets = [x.split()[0] for x in o.decode().splitlines() if x.find("NAME") != 0]

#fdatasets = filter(lambda x:x.find("store/Music") == 0, fdatasets)

rk = r'^' + args.fpool
for fdataset in fdatasets:
    tdataset = re.sub(rk, args.tpool, fdataset)
    tdataset = tdataset.split("/")
    tdataset = "{}/{}".format(tdataset[0], "_".join(tdataset[1:]))
    Popen([
        args.zasib,
        "-a",
        fdataset,
        tdataset,
    ]).communicate()