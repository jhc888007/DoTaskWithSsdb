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
THREAD_NUM = 5
THREAD_SLEEP_TIME = 0
THREAD_REST_TIME = 30

#HTTP
MAX_TRY_TIME = 8
FAIL_SLEEP_TIME = 1

#SSDB
SSDB_IP = '192.168.1.1'
SSDB_PORT = 8888

#queue
INPUT = "task_test1"
OUTPUT = "task_test2"
ERROR = "task_test3"

#log
LOG_PATH = "./do_yinyuetai_id.log"

#stop sign
STOP_PATH = "./stop.sign"

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
            return []
        result = do_work(js)
        if not result:
            return []
        text_list = []
        for r in result:
            text = json_encode(r)
            if not text:
                log("json encode error:%s"%task)
                continue
            text_list.append(text)
        return text_list

    def run(self):
        count = 0
        while 1:
            if STOP_PATH:
                if os.path.exists(STOP_PATH):
                    break
            if INPUT:
                task = self.queue_input.pop()
                if task == "":
                    time.sleep(THREAD_REST_TIME)
                    continue
            else:
                task = "{}"
            ret = self.do(task)
            if not ret:
                if ERROR:
                    self.queue_error.push(task)
            else:
                if OUTPUT:
                    for r in ret:
                        self.queue_output.push(r)
            count += 1
            time.sleep(THREAD_SLEEP_TIME)

#HTTP
def request(url, hds = ""):
    data =""
    try:
        if len(hds) == 0:
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
    except:
        return ""
    return data

def request_with_proxy(url, hds):
    times = MAX_TRY_TIME
    while times > 0:
        times -= 1
                
        res = request(url, hds)

        if res != "":
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
    if proxy_lock.acquire(False) == False:
        return
    proxy_current = proxy.get()
    if len(proxy_current) == "":
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
    if proxy_lock.acquire(False) == False:
        return
    proxy.error(proxy_current)
    proxy_lock.release()
    proxychange()
proxychange()

#URL
headers_list = [
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH92d',
'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G6'
#'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH93a',
'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH92f',
'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G6'
#'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH93e',
'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH91c',
'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G6'
#'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH91e',
'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH92t',
'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G6'
#'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH92y',
'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH91f',
'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G6'
#'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
},
{ 
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Connection': 'keep-alive',
'Host': 'ext.yinyuetai.com',
'Cookie': 'JSESSIONID=aaa0LJOZKMfu9hGyHH91b',
'User-Agent': 'Dalvik/1.6.0 (Linux; U; Android 4.3; GT-N7100 Build/JSS15J)'
}
]
def get_url(vid):
    url="http://ext.yinyuetai.com/main/get-h-mv-info?json=true&videoId=%s" %(vid)
    hds = headers_list[random.randint(0, len(headers_list)-1)]
    return url, hds

run_cnt = 0
def do_work(in_dic):
    out_dic_list = []
    if "m_artist_id" not in in_dic or "m_artist_name" not in in_dic \
        or "m_video_id" not in in_dic or "m_video_name" not in in_dic:
        return out_dic_list

    global run_cnt
    run_cnt += 1
    if run_cnt % 10 == 0:
        proxychange()

    vid = str(in_dic["m_video_id"])
    url, hds = get_url(vid)
    log("begin url: %s, %s" %(vid, url))

    #GET URL
    res = request_with_proxy(url, hds)
    if res == "":
        log("url request error: %s" %vid)
        return out_dic_list
        
    #GET JSON
    js = json_decode(res)
    if not js or "videoInfo" not in js or "coreVideoInfo" not in js["videoInfo"]:
        log("js error: %s, %s" %(vid, js))
        return out_dic_list
    
    out_dic = js["videoInfo"]["coreVideoInfo"]
    #GET CONTENT
    try:
        out_dic["m_video_id"] = out_dic["videoId"]
        out_dic["m_video_name"] = out_dic["videoName"].replace("\t","")
        out_dic["m_img"] = out_dic["headImage"]
        out_dic["m_artist_ids"] = out_dic["artistIds"]
        out_dic["m_artist_names"] = out_dic["artistNames"].replace("\t","")
        out_dic["m_duration"] = out_dic["duration"]
        out_dic["m_tag"] = out_dic["source"]
        video_list = out_dic["videoUrlModels"]
        video_info = video_list[len(video_list)-1]
        out_dic["m_video_url"] = video_info["videoUrl"]
        out_dic["m_video_size"] = video_info["fileSize"]
        out_dic_list.append(out_dic)
    except:
        log("content error: %s" %vid)
        pass

    return out_dic_list

#--------- user define end -------------------

for i in range(THREAD_NUM):
    t = Downloader()
    t.start()


