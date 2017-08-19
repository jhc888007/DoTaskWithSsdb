#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import json
import re
from DBUtil import Ssdb, Proxy, Queue
reload(sys)
sys.setdefaultencoding('UTF-8') 

#JSON
def json_decode(data, coding="utf-8"):
    result = ""
    try:
        result = json.loads(data,encoding=coding,strict=False)
    except Exception,e:
        print e
        result = ""
    return result

def json_encode(data, coding="utf-8"):
    result = ""
    try:
        result = json.dumps(data, encoding=coding,ensure_ascii=False)
    except Exception,e:
        print e
        result = ""
    return result

#--------- user define begin -------------------

with open(sys.argv[1], 'r') as input:
    with open(sys.argv[2], 'w') as output:
        for line in input:
            js = json_decode(line.strip())
            if not js:
                continue

            try:
                sch_key = js['m_sch_key']
                false_id = str(js['m_false_id'])
                title = js['m_title']
                img = js['m_img']
                playcnt = str(js['m_playcnt'])
                time = str(js['m_behot_time'])
            except:
                continue

            text = json_encode(js)
            if not text:
                continue
            output.write(text)
            
#--------- user define end -------------------
                

