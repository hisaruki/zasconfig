#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess


class Local:
  def __init__(self):
    self.list = {}
    for line in subprocess.Popen(["zfs","list","-t","snapshot"], stdout=subprocess.PIPE).communicate()[0].decode("utf-8").splitlines():
      if line.split()[0] != "NAME":
        name,key = line.split()[0].split("@")
        if not name in self.list:self.list[name] = []
        self.list[name].append(key)

class Remote:
  def __init__(self,rcommand):
    self.list = {}
    for line in subprocess.Popen(rcommand+["zfs","list","-t","snapshot"], stdout=subprocess.PIPE).communicate()[0].decode("utf-8").splitlines():
      if line.split()[0] != "NAME":
        name,key = line.split()[0].split("@")
        if not name in self.list:self.list[name] = []
        self.list[name].append(key)

class Backup:
  def __init__(self,l,r):
    self.local = l
    self.remote = r

  def iprop(self,ldn,rdn):
    try:
      self.ikey = self.remote.list[rdn][-1]
      return self.ikey in self.local.list[ldn]
    except:
      return False

  def generate(self,ldn,rdn,rcommand):
    send,recv = None,None
    if self.iprop(ldn,rdn):
      self.lkey = list(filter(lambda x:x.find("zfs-auto-snap") != 0,self.local.list[ldn]))[-1]
      send = ["zfs","send","-i",ldn+"@"+self.ikey,ldn+"@"+self.lkey]
      recv = rcommand+["zfs","recv",rdn+"@"+self.lkey]
    if self.lkey == self.ikey:send,recv = None,None
    return send,recv
