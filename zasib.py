#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess,argparse,re,os

parser = argparse.ArgumentParser(description="ZFS Auto Snapshot Incremental Backup")
parser.add_argument('nfrom')
parser.add_argument('nto')
parser.add_argument('-n','--dry-run',action="store_true")
parser.add_argument('--prefrom',nargs="*")
parser.add_argument('--preto',nargs="*")
args = parser.parse_args()

class Zasib:
  def __init__(self,nfrom,nto,prefrom,preto,dry_run):
    self.nfrom = nfrom
    self.nto = nto
    self.prefrom = prefrom
    self.preto = preto
    self.dry_run = dry_run

    listfrom = ["zfs","list","-t","snapshot","-r",self.nfrom]
    if type(self.prefrom) == list:
      listfrom = self.prefrom + listfrom
    listfrom = subprocess.Popen(listfrom,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL).communicate()[0].decode("utf-8").splitlines()


    listto = ["zfs","list","-t","snapshot","-r",self.nto]
    if type(self.preto) == list:
      listto = self.preto + listto
    listto = subprocess.Popen(listto,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL).communicate()[0].decode("utf-8").splitlines()

    def ref(l,n):
      l = filter(lambda x:not x.split()[0] == "NAME",l)
      l = map(lambda x:x.split()[0],l)
      l = filter(lambda x:re.search(n+"@",x),l)
      l = filter(lambda x:not re.search("@zfs-auto-snap",x),l)
      l = list(l)
      return l

    self.listfrom,self.listto = ref(listfrom,nfrom),ref(listto,nto)

  def send(self):
    send,recv = None,None
    if len(self.listfrom) > 0:
      if len(self.listto) == 0:
        send = ["zfs","send","-R",self.listfrom[-1]]
        if type(self.prefrom) == list:
          send = self.prefrom + send
        recv = ["zfs","recv",self.nto]
        if type(self.preto) == list:
          recv = self.preto + recv
      else:
        tk = self.listto[-1].split("@")[-1]
        fks = list(map(lambda x:x.split("@")[-1],self.listfrom))
        fk = fks[fks.index(tk)]
        if self.nfrom+"@"+fk != self.listfrom[-1]:
          send = ["zfs","send","-I",self.nfrom+"@"+fk,self.listfrom[-1]]
          if type(self.prefrom) == list:
            send = self.prefrom + send
          recv = ["zfs","recv",self.nto]
          if type(self.preto) == list:
            recv = self.preto + recv

    if send and recv:
      parent = ["zfs","list",recv[-1]]
      if type(self.preto) == list:
        parent = self.preto + parent
      parent = subprocess.Popen(parent,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL).communicate()[0].decode("utf-8").splitlines()
      if len(parent) == 0:
        parent = recv[-1].split("/")
        parent.pop()
        parent = "/".join(parent)
        parent = ["zfs","create","-p",parent]
        if type(self.preto) == list:
          parent = self.preto + parent
          print(" ".join(parent))
          if not self.dry_run:
            subprocess.Popen(parent, stdout=subprocess.PIPE)
      print(" ".join(send)+" | "+" ".join(recv))
      if not self.dry_run:
        p1 = subprocess.Popen(send, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(recv, stdin=p1.stdout)
        p1.stdout.close()
        p2.communicate()
    else:
      print("nothing to do.")



if "ZASIB_PREF" in os.environ:
  prefrom = os.environ["ZASIB_PREF"].split()
if "ZASIB_PRET" in os.environ:
  preto = os.environ["ZASIB_PRET"].split()
if args.prefrom:
  prefrom = args.prefrom
if args.preto:
  preto = args.preto

z = Zasib(args.nfrom,args.nto,prefrom,preto,args.dry_run)
z.send()

#./zasib.py pride/test ext0/test

