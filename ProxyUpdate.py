# !/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import urllib2
from ProxyGetFreeProxy import GetFreeProxy
from ProxyUtilFunction import validUsefulProxy
from DBUtil import Ssdb, Proxy
reload(sys)
sys.setdefaultencoding('utf-8')

ssdb = Ssdb("192.168.1.1", 8888)
proxy = Proxy(ssdb, "proxy_pool")

print "get proxy begin"
proxy_list = []
gg = GetFreeProxy()
for e in gg.freeProxyFirst():
    proxy_list.append(e)
for e in gg.freeProxySecond():
    proxy_list.append(e)
for e in gg.freeProxyThird():
    proxy_list.append(e)
for e in gg.freeProxyFourth():
    proxy_list.append(e)
for e in gg.freeProxyFifth():
    proxy_list.append(e)

print "get url begin"
proxy_url = "http://111.206.73.111:8888/proxy_server_crawler/proxy.txt.success"
try:
    req = urllib2.Request(url=proxy_url)
    res = urllib2.urlopen(req)
    data = res.read()
    for item in data.split("\n"):
        if item:
            pos = item.find("//")
            if pos == -1:
                continue
            item = item[pos + len("//"):]
            proxy_list.append(item)
except:
    pass

print "valid proxy begin, len:%d"% len(proxy_list)
proxy_valid_list = []
for e in proxy_list:
    if not validUsefulProxy(e):
        continue
    proxy_valid_list.append(e)
    if (len(proxy_valid_list)) >= 20:
        s = proxy.size()
        if s > 1000:
            proxy.clear()
        print "update proxy %d"% len(proxy_valid_list)
        proxy.update(proxy_valid_list)
        proxy_valid_list = []
proxy.update(proxy_valid_list)
            
