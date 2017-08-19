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
SRC_QUEUE = 'task_test1'
DST_QUEUE = 'task_test2'

#--------- user define end -------------------

ssdb = Ssdb('SSDB_IP', SSDB_PORT)
src_queue = Queue(ssdb, SRC_QUEUE)
dst_queue = Queue(ssdb, DST_QUEUE)

#--------- user define begin -------------------

while True:
    text = src_queue.popr()
    if not text:
        break
    dst_queue.pushr(text)
    
#--------- user define end -------------------
