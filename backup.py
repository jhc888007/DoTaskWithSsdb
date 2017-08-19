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
QUEUE_ORIGIN = 'task_test1'
QUEUE_BACKUP = 'task_test2'

#--------- user define end -------------------

if QUEUE_ORIGIN[-3:] == "bak":
    print "origin db end with bak"
    sys.exit(0)

if QUEUE_BACKUP[-3:] != "bak":
    print "backup db not end with bak"
    sys.exit(0)

ssdb = Ssdb(SSDB_IP, SSDB_PORT)
queue_origin = Queue(ssdb, QUEUE_ORIGIN)
queue_backup = Queue(ssdb, QUEUE_BACKUP)

start = 0
while True:
    list = queue_origin.get10000(start)
    print len(list)
    start += 10000
    if list == []:
        break
    queue_backup.updater(list)

