# !/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import requests
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

from ProxyUtilFunction import robustCrawl, getHtmlTree, validUsefulProxy
from ProxyWebRequest import WebRequest

# for debug to disable insecureWarning
requests.packages.urllib3.disable_warnings()


class GetFreeProxy(object):
    """
    proxy getter
    """

    def __init__(self):
        pass

    @staticmethod
    @robustCrawl  # decoration print error if exception happen
    def freeProxyFirst(page=10):
        """
        抓取无忧代理 http://www.data5u.com/
        :param page: 页数
        :return:
        """
        url_list = ['http://www.data5u.com/',
                    'http://www.data5u.com/free/',
                    'http://www.data5u.com/free/gngn/index.shtml',
                    'http://www.data5u.com/free/gnpt/index.shtml']
        for url in url_list:
            html_tree = getHtmlTree(url)
            ul_list = html_tree.xpath('//ul[@class="l2"]')
            for ul in ul_list:
                yield ':'.join(ul.xpath('.//li/text()')[0:2])

    @staticmethod
    @robustCrawl
    def freeProxySecond(proxy_number=100):
        """
        抓取代理66 http://www.66ip.cn/
        :param proxy_number: 代理数量
        :return:
        """
        url = "http://www.66ip.cn/mo.php?sxb=&tqsl={0}&port=&export=&ktip=&sxa=&submit=%CC%E1++%C8%A1&textarea=".format(proxy_number)
        request = WebRequest()
        html = request.get(url).content
        for proxy in re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}', html):
            yield proxy

    @staticmethod
    @robustCrawl
    def freeProxyThird(days=1):
        """
        抓取ip181 http://www.ip181.com/
        :param days:
        :return:
        """
        url = 'http://www.ip181.com/'
        html_tree = getHtmlTree(url)
        tr_list = html_tree.xpath('//tr')[1:]
        for tr in tr_list:
            yield ':'.join(tr.xpath('./td/text()')[0:2])

    @staticmethod
    @robustCrawl
    def freeProxyFourth():
        """
        抓取西刺代理 http://api.xicidaili.com/free2016.txt
        :return:
        """
        url_list = ['http://www.xicidaili.com/nn',  # 高匿
                    'http://www.xicidaili.com/nt',  # 透明
                    ]
        for each_url in url_list:
            tree = getHtmlTree(each_url)
            proxy_list = tree.xpath('.//table[@id="ip_list"]//tr')
            for proxy in proxy_list:
                yield ':'.join(proxy.xpath('./td/text()')[0:2])

    @staticmethod
    @robustCrawl
    def freeProxyFifth():
        """
        抓取guobanjia http://www.goubanjia.com/free/gngn/index.shtml
        :return:
        """
        url = "http://www.goubanjia.com/free/gngn/index{page}.shtml"
        for page in range(1, 10):
            page_url = url.format(page=page)
            tree = getHtmlTree(page_url)
            proxy_list = tree.xpath('//td[@class="ip"]')
            for each_proxy in proxy_list:
                yield ''.join(each_proxy.xpath('.//text()'))

if __name__ == "__main__":
    proxy_set = set()
    gg = GetFreeProxy()
    for e in gg.freeProxyFirst():
        proxy_set.add(e)

    for e in gg.freeProxySecond():
        proxy_set.add(e)

    for e in gg.freeProxyThird():
        proxy_set.add(e)

    for e in gg.freeProxyFourth():
        proxy_set.add(e)

    for e in gg.freeProxyFifth():
        proxy_set.add(e)

    print len(proxy_set)

    result_set = set()
    for e in proxy_set:
        if validUsefulProxy(e):
            result_set.add(e)
        
    print len(result_set)

