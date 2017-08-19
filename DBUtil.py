# !/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import sys
import SSDB
import random
reload(sys)
sys.setdefaultencoding('utf-8')

def Ssdb(ip, port):
    return SSDB.SSDB(ip, port)

class Queue:
    def __init__(self, ssdb, queue_name):
        self.db = ssdb
        self.name = queue_name

    def pop(self):
        result = self.db.request('qpop_back', [self.name])
        if result.code != "ok":
            return ""
        return result.data

    def push(self, t):
        result = self.db.request('qpush_front', [self.name, t])
        if result.code != "ok":
            return ""
        return "ok"

    def popr(self):
        result = self.db.request('qpop_front', [self.name])
        if result.code != "ok":
            return ""
        return result.data

    def pushr(self, t):
        result = self.db.request('qpush_back', [self.name, t])
        if result.code != "ok":
            return ""
        return "ok"

    def update(self, l):
        t = [self.name]
        t.extend(l)
        result = self.db.request('qpush_front', t)
        if result.code != "ok":
            return ""
        return "ok"

    def updater(self, l):
        t = [self.name]
        t.extend(l)
        result = self.db.request('qpush_back', t)
        if result.code != "ok":
            return ""
        return "ok"

    def get(self, start, num):
        size = num
        result = self.db.request('qrange', [self.name, start, size])
        if result.code != "ok":
            return []
        return result.data
 
    def get1000(self, start):
        size = 1000
        result = self.db.request('qrange', [self.name, start, size])
        if result.code != "ok":
            return []
        return result.data

    def get10000(self, start):
        size = 10000
        result = self.db.request('qrange', [self.name, start, size])
        if result.code != "ok":
            return []
        return result.data

    def dump(self):
        l = []
        start = 0
        size = 1000
        while True:
            result = self.db.request('qrange', [self.name, start, size])
            start += size
            if result.code != "ok":
                return []
            l.extend(result.data)
            if len(result.data) < size:
                break
        print l
        return l

class Proxy:
    def __init__(self, ssdb, proxy_pool_name):
        self.db = ssdb
        self.name = proxy_pool_name

    def get(self):
        result = self.db.request('hgetall', [self.name])
        if result.code != "ok" or 'items' not in result.data:
            return ""
        while True:
            l = []
            for k in result.data['items']:
                v = int(result.data['items'][k])
                if v < 4:
                    l.append(k)
            if len(l) < 10 and len(result.data['items']) >= 10:
                self.proxy_flush()
            if len(l) > 0:
                break
            if len(result.data['items']) == 0:
                return ""
        num = random.randint(0, len(l)-1)
        try:
            value = int(result.data['items'][l[num]])
            if value > 0:
                self.db.request('hset', [self.name, l[num], value-1])
        except:
            pass
        print "proxy get: %s" % l[num]
        return l[num]

    def flush(self):
        print "proxy flush"
        result = self.db.request('hgetall', [self.name])
        if result.code != "ok" or 'items' not in result.data:
            return
        for k in result.data['items']:
            v = int(result.data['items'][k])
            if v > 0:
                self.db.request('hset', [self.name, k, v-1])
        return
                
    def update(self, l):
        if len(l) == 0:
            return ""
        result = self.db.request('hgetall', [self.name])
        if result.code != "ok" or 'items' not in result.data:
            return ""
        for k in l:
            if k in result.data['items']:
                v = int(result.data['items'][k])
                if v > 0:
                    self.db.request('hset', [self.name, k, v-1])
            else:
                self.db.request('hset', [self.name, k, 0])
        return "ok"

    def error(self, p):
        result = self.db.request('hget', [self.name, p])
        if result.code != "ok":
            return  ""
        v = int(result.data)
        result = self.db.request('hset', [self.name, p, v+2])
        if result.code != "ok":
            return  ""
        print "proxy error: %s" % p
        return "ok"

    def dump(self):
        result = self.db.request('hgetall', [self.name])
        if result.code != "ok" or 'items' not in result.data:
            print "None"
            return {}
        print result.data['items']
        return result.data['items']

if __name__ == "__main__":
    ssdb = Ssdb('192.168.212.13', 8888)
    queue = Queue(ssdb, 'task_id')
    for i in range(0, 20):
        print queue.pop()
'''
    proxy = Proxy(ssdb, 'proxy_pool')
    proxy.update(['192.168.0.1', '192.168.0.2', '192.168.0.3', '192.168.0.4'])
    proxy.dump()
    p = proxy.get()
    print p
    proxy.error(p)
    print "error: ", p
    p = proxy.get()
    print p
    proxy.error(p)
    print "error: ", p
    for i in range(0, 1):
        p = proxy.get()
        print p
    proxy.dump()
'''

