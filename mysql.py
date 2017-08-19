#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import commands
import MySQLdb
import json
reload(sys)
sys.setdefaultencoding('UTF-8') 

#--------- user define begin -------------------

USER = 'root'
PASSWD = 'test'
HOST = '192.168.1.1'
DBNAME = 'test'

#--------- user define end -------------------

conn = MySQLdb.connect(user=USER, passwd=PASSWD ,host=HOST, db=DBNAME, charset='utf8')
cursor = conn.cursor()
cursor.execute("set names utf8")

def escape(str):
    return MySQLdb.escape_string(str)
                
def execute(sql):
    result = None
    try :
        cursor.execute(sql)
        result = cursor.fetchall()
    except Exception, e:
        print sql 
        print e
        return None
    return result

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

err = 0
num = 0
for line in sys.stdin:
    num+=1
    js = json_decode(line.strip())

    try:
        id = str(js['m_false_id'])
        name = str(js['m_title'])
        playcnt = str(js['m_playcnt'])
        sch_key = str(js['m_sch_key'])
        img = str(js['image_url'])
        time = str(js['m_behot_time'])
    except:
        err += 1
        continue

    cmd = "insert into fa_artist_video(id, `key`, title, image, playcnt, create_timestamp) values (\"%s\",\"%s\",\"%s\",\"%s\",%s,%s);"%(escape(id),escape(sch_key),escape(name),escape(img),playcnt,time)
    ret = execute(cmd)
    if ret == None:
        err+=1

    '''
    num+=1
    v = line.strip().split("\t")
    if len(v) != 6:
        err+=1
        continue 
    
    id = v[0]
    name = v[2]
    playcnt = v[4]
    sch_key = v[1]
    img = v[3]
    time = v[5]

    cmd = "insert into fa_artist_video(id, `key`, title, image, playcnt, create_timestamp) values (\"%s\",\"%s\",\"%s\",\"%s\",%s,%s);"%(escape(id),escape(sch_key),escape(name),escape(img),playcnt,time)
    ret = execute(cmd)
    if ret == None:
        err+=1
    '''
#--------- user define end -------------------

cursor.close()
conn.close()
print "done"
