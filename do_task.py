#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import commands
import MySQLdb
import time
import urllib2
import urllib
import os
import gzip
import StringIO
import re
import threading
import socket
import json
import random
from DBUtil import Ssdb, Proxy, Queue
reload(sys)
sys.setdefaultencoding('UTF-8') 

#--------- user define begin -------------------
#THREAD
THREAD_NUM = 8
THREAD_SLEEP_TIME = 0
THREAD_REST_TIME = 30

#HTTP
MAX_TRY_TIME = 5
FAIL_SLEEP_TIME = 1

#SSDB
SSDB_IP = '192.168.1.1'
SSDB_PORT = 8888

#queue
INPUT = "task_test1"
OUTPUT = "task_test2"
ERROR = "task_test3"

#log
LOG_PATH = "./data/task.log"

#--------- user define end -------------------

#LOG
log_file = open(LOG_PATH, 'ab')
log_lock = threading.Lock()
def log(str):
    log_lock.acquire()
    log_file.write("%s, %s\n" % (str, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))))
    log_file.flush()
    log_lock.release()

#JSON
def json_decode(data, coding="utf-8"):
    result = {}
    try:
        result = json.loads(data,encoding=coding,strict=False)
    except Exception,e:
        print e
        result = {}
    return result

def json_encode(data, coding="utf-8"):
    result = ""
    try:
        result = json.dumps(data, encoding=coding,ensure_ascii=False)
    except Exception,e:
        print e
        result = ""
    return result

#Downloader
class Downloader(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.ssdb = Ssdb(SSDB_IP, SSDB_PORT)
        self.queue_input = Queue(self.ssdb, INPUT)
        self.queue_output = Queue(self.ssdb, OUTPUT)
        self.queue_error = Queue(self.ssdb, ERROR)

    def do(self, task):
        log("task begin:%s"%task)
        js = json_decode(task)
        if not js:
            log("json decode error:%s"%task)
            return ""
        result = do_work(js)
        if len(result) == 0:
            return ""
        text_list = []
        for r in result:
            text = json_encode(r)
            if not text:
                log("json encode error:%s"%task)
                continue
            text_list.append(text)
        if len(text_list) == 0:
            return ""
        return text_list

    def run(self):
        count = 0
        while 1:
            if not INPUT:
                task = self.queue_input.pop()
                if task == "":
                    time.sleep(THREAD_REST_TIME)
                    continue
            else:
                task = "{}"
            ret = self.do(task)
            if not ret:
                if not ERROR:
                    self.queue_error.push(task)
            else:
                if not OUTPUT:
                    for r in ret:
                        self.queue_output.push(r)
            count += 1
            time.sleep(THREAD_SLEEP_TIME)

#HTTP
def request(url, hds = ""):
    data =""
    try:
        if not hds:
            req = urllib2.Request(url)
        else:
            req = urllib2.Request(url=url, headers=hds)
        res = urllib2.urlopen(req)
        data = res.read()
        head = res.info()
        if ('Content-Encoding' in head and head['Content-Encoding']) or \
            ('content-encoding' in head and head['content-encoding']):
            gz = gzip.GzipFile(fileobj=StringIO.StringIO(data))
            data = gz.read()
            gz.close()
    except urllib2.URLError, e:
        return data
    return data

def request_with_proxy(url, hds):
    times = MAX_TRY_TIME
    while times > 0:
        times -= 1
                
        res = request(url, hds)

        if not res:
            return res

        log("url get error: %s" % url)
        proxyerror()
        time.sleep(FAIL_SLEEP_TIME)
   
    log("try too many times")
    return ""

socket.setdefaulttimeout(10)

#--------- user define begin -------------------

#PROXY
ssdb = Ssdb(SSDB_IP, SSDB_PORT)
proxy = Proxy(ssdb, "proxy_pool")
proxy_current = ""
proxy_lock = threading.Lock()
def proxychange():
    global proxy
    global proxy_current
    global proxy_lock
    proxy_current = proxy.get()
    proxy_lock.acquire()
    if not proxy_current:
        proxy_dic = {}
    else:
        proxy_dic = {'http':proxy_current}
    try:
        p = urllib2.ProxyHandler(proxy_dic)
        opener = urllib2.build_opener(p)
        urllib2.install_opener(opener)
    except Exception, e:
        pass
    proxy_lock.release()
def proxyerror():
    global proxy
    global proxy_current
    global proxy_lock
    proxy.error(proxy_current)
    proxychange()
proxychange()

#URL
headers_list = [
{ 
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding': 'gzip,deflate,br',
'Accept-Language': 'zh-CN,zh;q=0.8',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Cookie': 'UM_distinctid=15d1b17c894243-002ea993efdff2-474a0521-bf680-15d1b17c895ae2; uuid="w:53da0d0269814d9795ea6dcbaf8f11df"; _ga=GA1.2.105033893.1499398131; csrftoken=610102a9f1685f77069bc34da706bd30; WEATHER_CITY=%E5%8C%97%E4%BA%AC; tt_webid=6439865913038915086; CNZZDATA1259612802=261791786-1499393798-null%7C1501816136',
'Host': 'www.365yg.com',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
},
{ 
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding': 'gzip,deflate,br',
'Accept-Language': 'zh-CN,zh;q=0.8',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Cookie': 'tt_webid=6444465730176468494; uuid="w:1362244d4ffd4682948966da64a33e72"; UM_distinctid=15d5aed8c8c190-0c18a3b347d455-39034859-cb976-15d5aed8c8d466; csrftoken=09b9e4e727cf9b305fa33a6a6b0579e2; CNZZDATA1259612802=1241813877-1500468482-http%253A%252F%252Fwww.baidu.com%252F%7C1500468482',
'Host': 'www.365yg.com',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko'
},
{ 
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding': 'gzip,deflate,br',
'Accept-Language': 'zh-CN,zh;q=0.8',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Cookie': 'uuid="w:9e7653eadc7245dcb91e2cebe177dd93"; sid_guard="fc5b639927ba1e6f65b84c0507138f65|1481113884|2592000|Fri\054 06-Jan-2017 12:31:24 GMT"; UM_distinctid=15c1f277b2b159-0443ec3e-3b664008-1fa400-15c1f277b2c28; _ga=GA1.2.573314528.1481110643; tt_webid=58997154152; csrftoken=fb894548384f464252b6ec3d4938cecd; WEATHER_CITY=%E5%8C%97%E4%BA%AC; utm_source=toutiao; CNZZDATA1259612802=982301074-1495170840-%7C1501219083',
'Host': 'www.365yg.com',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'
},
{ 
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding': 'gzip,deflate,br',
'Accept-Language': 'zh-CN,zh;q=0.8',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Cookie': 'UM_distinctid=15d1b17c894243-002ea993efdff2-474a0521-bf680-15d1b17c895ae2; uuid="w:53da0d0269814d9795ea6dcbaf8f11df"; _ga=GA1.2.105033893.1499398131; csrftoken=610102a9f1685f77069bc34da706bd30; WEATHER_CITY=%E5%8C%97%E4%BA%AC; tt_webid=6439865913038915086; CNZZDATA1259612802=261791786-1499393798-null%7C1501816136',
'Host': 'www.365yg.com',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
},
{ 
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding': 'gzip,deflate,br',
'Accept-Language': 'zh-CN,zh;q=0.8',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Cookie': 'tt_webid=6444465730176468494; uuid="w:1362244d4ffd4682948966da64a33e72"; UM_distinctid=15d5aed8c8c190-0c18a3b347d455-39034859-cb976-15d5aed8c8d466; csrftoken=09b9e4e727cf9b305fa33a6a6b0579e2; CNZZDATA1259612802=1241813877-1500468482-http%253A%252F%252Fwww.baidu.com%252F%7C1500468482',
'Host': 'www.365yg.com',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko'
}
]
def get_url(text):
    url = "http://www.365yg.com/group/"+text+"/"
    hds = headers_list[random.randint(0, len(headers_list)-1)]
    return url, hds

#INFO
def get_duration(text):
    try:
        v = text.split(":")
        if len(v) == 2:
            return int(v[0])*60+int(v[1])
        if len(v) == 3:
            return int(v[0])*60*60+int(v[1])*60+int(v[2])
        return 200
    except:
        return 200

id_reg1 = re.compile(r'(?<=\/)[^\/]+(?=\/)')
def get_false_id(text):
    try:
        content = id_reg1.findall(text)
        if content:
            return content[-1]
    except:
        return ""

pic_reg1 = re.compile(r'[^\/]+')
def get_pic_url(text):
    try:
        content = pic_reg1.findall(text)
        if content:
            return "http://p1.pstatp.com/video1609/"+content[-1]
        return text
    except Exception,e:
        return text

run_cnt = 0
def do_work(in_dic):
    out_dic_list = []
    if "m_false_id" not in in_dic:
        return out_dic_list

    global run_cnt
    if run_cnt % 40 == 0:
        proxychange()

    root_id = str(in_dic["m_false_id"])
    url, hds = get_url(root_id)
    log("begin url: %s, %s" %(root_id, url))

    #GET URL
    res = request_with_proxy(url, hds)
    if len(res) == 0:
        return out_dic_list

    #GET CONTENT
    out_dic = in_dic
    content = re.findall(r'(?<=videoid\:\')[^\']+', res)
    if not content:
        log("vid get error: %s" % root_id)
        out_dic["m_real_id"] = "0"
    else:
        out_dic["m_real_id"] = content[0]
    out_dic_list.append(out_dic)
    return out_dic_list

'''
run_cnt = 0
def do_work(in_dic):
    out_dic_list = []
    if "m_false_id" not in in_dic:
        return out_dic_list

    global run_cnt
    if run_cnt % 40 == 0:
        proxychange()

    root_id = str(in_dic["m_false_id"])
    url, hds = get_url(root_id)
    log("begin url: %s, %s" %(root_id, url))

    #GET URL
    res = request_with_proxy(url, hds)
    if len(res) == 0:
        return out_dic_list

    #GET CONTENT
    content = re.findall(r'(?<=siblingList = \[).*?(?=\]\;)', res)
    if not content:
        log("content get error: %s" % root_id)
        return out_dic_list
    text = content[0]+","
    
    #GET PIECE
    content = re.findall(r'\{.*?\}(?=\,)', text)
    if not content:
        log("text get error: %s" % root_id)
        return out_dic_list

    #GET JSON
    for c in content:
        js = json_decode(c)
        if not js:
            log("json decode error: %s" % root_id)
            continue
        if 'title' not in js or 'video_duration_str' not in js or 'source_url' not in js or \
            'image_url' not in js or 'video_play_count' not in js:
            continue
        js['m_false_id'] = get_false_id(js['source_url'])
        if js['m_false_id'] == "":
            continue
        js['m_title'] = js['title']
        js['m_duration'] = get_duration(js['video_duration_str'])
        js['m_img'] = get_pic_url(js['image_url'])
        js['m_playcnt'] = js['video_play_count']
        js['m_root_id'] = root_id
        js['m_sch_key'] = ""
        js['m_root_name'] = ""
        try:
            js['m_sch_key'] = in_dic['m_sch_key']
            js['m_root_name'] = in_dic['m_title']
        except:
            pass
        out_dic_list.append(js)

    return out_dic_list
'''

#--------- user define end -------------------

for i in range(THREAD_NUM):
    t = Downloader()
    t.start()


