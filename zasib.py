#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import argparse
import re
import os
import datetime

parser = argparse.ArgumentParser(
    description="ZFS Auto Snapshot Incremental Backup")
parser.add_argument('name_from')
parser.add_argument('name_to')

parser.add_argument('-a', '--all', action="store_true")
parser.add_argument('-r', '--rename', action="store_true")
parser.add_argument('-s', '--send', action="store_true")
parser.add_argument('-c', '--compare', action="store_true")
parser.add_argument('--revisions', type=int, default=3)
parser.add_argument('-n', '--dry-run', action="store_true")
parser.add_argument('-v', '--verbose', action="store_true")

parser.add_argument('--prefrom', nargs="*")
parser.add_argument('--preto', nargs="*")
args = parser.parse_args()


class Zasib:

    def __init__(self, name_from, name_to, prefrom, preto, dry_run):
        self.name_from = name_from
        self.name_to = name_to
        self.prefrom = prefrom
        self.preto = preto
        self.dry_run = dry_run
        self.get()

    def get(self):
        list_from = ["zfs", "list", "-t", "snapshot", "-r", self.name_from]
        if type(self.prefrom) == list:
            list_from = self.prefrom + list_from
        list_from = subprocess.Popen(list_from, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).communicate()[
            0].decode("utf-8").splitlines()

        list_to = ["zfs", "list", "-t", "snapshot", "-r", self.name_to]
        if type(self.preto) == list:
            list_to = self.preto + list_to
        list_to = subprocess.Popen(list_to, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).communicate()[
            0].decode("utf-8").splitlines()

        zfs_root_from = ["zfs", "list", self.name_from.split("/")[0]]
        if type(self.prefrom) == list:
            zfs_root_from = self.prefrom + zfs_root_from
        self.zfs_root_from = bool(subprocess.Popen(
            zfs_root_from, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).communicate()[0].decode("utf-8"))

        zfs_root_to = ["zfs", "list", self.name_to.split("/")[0]]
        if type(self.preto) == list:
            zfs_root_to = self.preto + zfs_root_to
        self.zfs_root_to = bool(subprocess.Popen(
            zfs_root_to, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).communicate()[0].decode("utf-8"))

        def ref(l, n):
            l = filter(lambda x: not x.split()[0] == "NAME", l)
            l = map(lambda x: x.split()[0], l)
            l = filter(lambda x: re.search(n + "@", x), l)
            l = list(l)
            return l

        self.all_from, self.all_to = ref(
            list_from, self.name_from), ref(list_to, self.name_to)
        self.list_from = list(
            filter(lambda x: not re.search("@zfs-auto-snap", x), self.all_from))
        self.list_to = list(
            filter(lambda x: not re.search("@zfs-auto-snap", x), self.all_to))
        self.auto_from = list(
            filter(lambda x: re.search("@zfs-auto-snap", x), self.all_from))
        self.auto_to = list(
            filter(lambda x: re.search("@zfs-auto-snap", x), self.all_to))

    def send(self):
        rerun = False
        send, recv = None, None
        if len(self.list_from) > 0:
            if len(self.list_to) == 0:
                print("#First Time Sending")
                send = ["zfs", "send", self.list_from[0]]
                if type(self.prefrom) == list:
                    send = self.prefrom + send
                recv = ["zfs", "recv", self.name_to]
                if type(self.preto) == list:
                    recv = self.preto + recv
                rerun = True
            else:
                tk = self.list_to[-1].split("@")[-1]
                fks = list(map(lambda x: x.split("@")[-1], self.list_from))
                fk = fks[fks.index(tk)]
                if self.name_from + "@" + fk != self.list_from[-1]:
                    print("#Incremental Sending")
                    send = ["zfs", "send", "-I", self.name_from +
                            "@" + fk, self.list_from[-1]]
                    if type(self.prefrom) == list:
                        send = self.prefrom + send
                    recv = ["zfs", "recv", self.name_to]
                    if type(self.preto) == list:
                        recv = self.preto + recv

        if send and recv:
            parent = ["zfs", "list", recv[-1]]
            if type(self.preto) == list:
                parent = self.preto + parent
            parent = subprocess.Popen(parent, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).communicate()[
                0].decode("utf-8").splitlines()
            if len(parent) == 0:
                parent = recv[-1].split("/")
                parent.pop()
                parent = "/".join(parent)
                parent = ["zfs", "create", "-p", parent]
                if type(self.preto) == list:
                    parent = self.preto + parent
                    print(" ".join(parent))
                    if not self.dry_run:
                        subprocess.Popen(
                            parent, stdout=subprocess.PIPE).communicate()
            print(" ".join(send) + " | " + " ".join(recv))
            if not self.dry_run:
                p1 = subprocess.Popen(send, stdout=subprocess.PIPE)
                p2 = subprocess.Popen(recv, stdin=p1.stdout)
                p1.stdout.close()
                p2.communicate()
            self.get()
            if rerun and not self.dry_run:
                self.send()

    def compare(self):
        def destroy(key):
            df = ["zfs", "destroy", self.name_from + '@' + key]
            if type(self.prefrom) == list:
                df = self.prefrom + df
            print(" ".join(df))
            if not self.dry_run:
                subprocess.Popen(df, stdout=subprocess.PIPE).communicate()

            dt = ["zfs", "destroy", self.name_to + '@' + key]
            if type(self.preto) == list:
                dt = self.preto + dt
            print(" ".join(dt))
            if not self.dry_run:
                subprocess.Popen(dt, stdout=subprocess.PIPE).communicate()

        if len(self.list_from) and len(self.list_to):
            self.keys_from = map(lambda x: re.sub(self.name_from + "@", "", x), self.list_from)
            self.keys_to = map(lambda x: re.sub(self.name_to + "@", "", x), self.list_to)

            self.keys_from = list(self.keys_from)
            self.keys_to = list(self.keys_to)
            rmkeys = []

            for key in filter(lambda x:re.match("^[0-9]+$",x), self.keys_from):
                if key in self.keys_to and len(self.keys_from) - len(rmkeys) > args.revisions:
                    rmkeys.append(key)
            while rmkeys:
                destroy(rmkeys.pop(0))

    def rename(self):
        if len(self.all_from):
            if len(self.list_from) < 1 or self.all_from[-1] != self.list_from[-1]:
                newname = self.name_from + "@" + datetime.datetime.now().strftime("%Y%m%d")
                if newname not in self.all_from:
                    rename = [
                        "zfs",
                        "rename",
                        self.auto_from[-1],
                        self.name_from + "@" + datetime.datetime.now().strftime("%Y%m%d")
                    ]
                    if type(self.prefrom) == list:
                        rename = self.prefrom + rename
                    print(" ".join(rename))
                    if not self.dry_run:
                        subprocess.Popen(
                            rename, stdout=subprocess.PIPE).communicate()
                self.get()

if "ZASIB_PREF" in os.environ:
    prefrom = os.environ["ZASIB_PREF"].split()
if "ZASIB_PRET" in os.environ:
    preto = os.environ["ZASIB_PRET"].split()
if args.prefrom:
    prefrom = args.prefrom
if args.preto:
    preto = args.preto

if args.verbose:
    print(args.name_from, args.name_to)

z = Zasib(args.name_from, args.name_to, prefrom, preto, args.dry_run)

actions = {"r": False, "s": False, "c": False}

actions["r"] = args.rename
actions["s"] = args.send
actions["c"] = args.compare

if True not in [x[1] for x in actions.items()]:
    actions["s"] = True

if args.all:
    actions["r"], actions["s"], actions["c"] = True, True, True

if z.zfs_root_from and z.zfs_root_to:
    if actions["r"]:
        z.rename()
    if actions["s"]:
        z.send()
    if actions["c"]:
        z.compare()
else:
    print("#zfs root not found.")
