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

with open(sys.argv[1], 'w') as output:
    start = 0
    while True:
        list = queue.get10000(start)
        print len(list)
        start += 10000
        if list == []:
            break
        for l in list:
            output.write("%s\n"%l)

#--------- user define end -------------------

print "Over"
