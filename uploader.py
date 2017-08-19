#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import json
from DBUtil import Ssdb, Proxy, Queue
reload(sys)
sys.setdefaultencoding('UTF-8') 

#--------- user define begin -------------------

SSDB_IP = '192.168.1.1'
SSDB_PORT = 8888
QUEUE = 'task_test1'

#--------- user define end -------------------

ssdb = Ssdb(SSDB_IP, SSDB_PORT)
queue = Queue(ssdb, QUEUE)

#--------- user define begin -------------------

with open(sys.argv[1], 'r') as input:
    l = []
    count = 0
    for line in input:
        line = line.strip()
        l.append(line)
        count += 1
        if count%10000 == 0:
            ret = queue.update(l)
            if not ret:
                print "update error"
            else:
                print "update success"
            l = []
        
    ret = queue.update(l)
    if not ret:
        print "update error"
    else:
        print "update success"
    l = []

#--------- user define end -------------------

