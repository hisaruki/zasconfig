#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import zasbackup,argparse,subprocess

parser = argparse.ArgumentParser(description="BaiduStreamer")
parser.add_argument('dataset',type=str)
parser.add_argument('newname',type=str)
args = parser.parse_args()


l = zasbackup.Local()

oldname = list(filter(lambda x:x.find("zfs-auto-snap") == 0,l.list[args.dataset]))[-1]

subprocess.Popen(["zfs","rename",args.dataset+"@"+oldname,args.dataset+"@"+args.newname]).communicate()