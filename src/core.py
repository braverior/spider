# !/usr/bin/env python
# -*- coding:utf-8 -*-
#filename spider.py

import Queue
from Queue import Empty
import threading
import time
import re

import urllib2
import urllib
import chardet
import urlparse
import BeautifulSoup
import sys
from log import*
reload(sys)
sys.setdefaultencoding('utf-8')


class Core(threading.Thread):
    def __init__(self, work_queue, out_queue, log_hdr, deep, key):
        """初始化工作线程
        deep:        深度
        log_hdr:     日志句柄
        Key:         关键字
        work_queue   输入队列
        out_queue    输出队列
        """
        threading.Thread.__init__(self)
        self.work_queue = work_queue  # 工作队列 (url,deep)
        self.out_queue = out_queue  # 输出队列
        self.log_hdr = log_hdr    # 日志句柄
        self.deep = deep       # 深度
        self.key = key        # 寻找的关键字
        self.url = ''         # current url
        self.status = True       # 进程的状态  Ture是运行状态，Flase 等待任务队列
        self.start()

    def is_valid_url(self, url):
        """判断是否为有效url
        判断 url中是否含有http:// 或者 https://
        """
        if not url:
            return False
        p = re.compile('^(http://|https://)[^\b]+$', re.I | re.S)
        if p.search(url):
            return True
        return False

    def get_abs_url(self, url, base):
        """转换成绝对url
        转换方式如下：
        base:                 url                     target
        http://a.com          http://c.com       ===> http://c.com
        http://a.com          /p/i            ===> http://a.com/p/i
        http://a.com          pp/ddd             ===> http//a.com/pp/ddd
        """
        try:
            if not url:     # 空字符串
                return
            if base and not self.is_valid_url(base):
                base = None
            # 带有协议的绝对地址,直接返回  http://a.com 无需处理
            if urlparse.urlparse(url).scheme:
                return url
            # 指向根目录的地址  例如： /a/index.html
            if url[0] is '/':
                ps = urlparse.urlparse(base)
                newurl = ps.scheme+'://'+ps.netloc+url
                return newurl
            #相对地址  例如： ssss/inx.html
            url = urllib2.urlparse.urljoin(base, url)
            return url
        except Exception, e:
            self.log_hdr.error('%s' % e)

    def get_content(self, url):
        """获取网页内容
        利用urllib2库获取网页内容
        如果失败，则返回空
        """
        try:
            f = urllib2.urlopen(url)
            headers = f.info()
            content = f.read()
            if headers['Content-Encoding'] or \
               headers['content-encoding']:
                import gzip
                import StringIO
                data = StringIO.StringIO(content)
                gz = gzip.GzipFile(fileobj=data)
                content = gz.read()
                gz.close()
            return content
        except Exception, e:
            self.log_hdr.error('%s' % e)
            return

    def get_title(self, content):
        """获取文章标题
        查找title标签。
        content 页面内容
        """
        isLower = 0
        Title_Start_pos = content.find('<title>')
        if Title_Start_pos < 0:
            isLower = 1
            Title_Start_pos = content.find('<TITLE>')
            if Title_Start_pos < 0:
                return ''
        if isLower == 1:
            Title_End_pos = content.find('</TITLE>')
        else:
            Title_End_pos = content.find('</title>')
        return content[len('<title>') + Title_Start_pos:Title_End_pos]

    def parse_url(self, content):
        """解析页面url
        提取页面所有的链接，放到数组中，返回此数组
        content 页面内容
        """
        try:
            soup = BeautifulSoup.BeautifulSoup(content)
        except Exception, e:
            self.log_hdr.error('%s' % e)
        links = []
        for link in soup('a'):
            for attr in link.attrs:
                if attr[0] == 'href':
                    links.append(attr[1].strip())
        return links

    def remove_repeat_url(self, urls):
        """去掉重复和无效的url
        1.去掉无用的后缀  比如：
        http://a.com/s.php?id=22#top   =>  http://a.com/s.php?id=22
        2.去掉错误的url
        3.去掉重复的url  set集合是不允许有重复的
        urls  链接集合

        """

        urls = [urllib2.urlparse.urldefrag(url)[0] for url in urls
                if self.is_valid_url(url)]

        urls = [self.get_abs_url(url, self.url) for url in urls]
        urls = list(set(urls))
        try:
            urls = [url for url in urls if self.is_valid_url(url)]
        except Exception, e:
            self.log_hdr.error('%s' % e)
        return urls

    def convert_utf8_charset(self, content):
        """转换编码
        判断编码，如果是 gbk, gb2312等编码
        统一成 utf-8编码
        返回转换编码后的文档
        content 页面内容
        """
        charset = chardet.detect(content)
        try:

            if charset['encoding'].lower() == 'utf-8':
                return content
            else:
                content = content.decode('gbk', 'ignore').encode('utf-8')
                return content
        except Exception, e:
            self.log_hdr.error('%s' % e)
        return content

    def crawl(self, url, deep):
        """开始爬行页面
        先获取页面内容
        统一编码
        保存页面
        解析页面所有链接
        将非重复链接放入队列内

        url   目标url
        deep  深度

        """
        self.log_hdr.info('now crawl url:%s' % url)
        content = self.convert_utf8_charset(self.get_content(url))
        page = {}
        page['url'] = url
        page['title'] = self.get_title(content)
        page['content'] = content
        page['lastmodified'] = time.strftime('%Y-%m-%d %H:%M:%S')
        # 如果key页面中 或者没有定义key 保存此页面
        if (self.key and content.find(self.key)) or not self.key:
            self.log_hdr.info("sava page %s :" % url)
            self.out_queue.put(page)

        if not content:
            self.log_hdr.error('content is null skipping..')
            return

        # 如果深度为0或者等于1 表示已经完成对词页面的获取，直接返回
        if deep <= 1:
            return

        urls = self.parse_url(content)
        urls = self.remove_repeat_url(urls)

        for url in urls:
            self.work_queue.put((url, deep - 1))

    def run(self):
        #死循环，从而让创建的线程在一定条件下关闭退出
        while True:
            try:
                self.status = True
                # 任务异步出队，Queue内部实现了同步机制
                url, deep = self.work_queue.get(True, 1)
                self.url = url
                self.crawl(url, deep)
                # 通知系统任务完成
                self.work_queue.task_done()
                self.status = False
            except Empty, e:
                #如果输出队列为空，则一直等待
                #self.log_hdr.warning('wait work Queue: %s' % e)
                continue
            except Exception, e:
                if self.work_queue.qsize():  # 如果还有没有完成的任务，继续。
                    continue
                else:
                    self.log_hdr.info("thread will exit:%s" % e)
                    # 告诉输出数据线程，我已经完成了对页面的爬取，需要退出了
                    self.out_queue.put(None)
                    break

# if __name__ == '__main__':
#     work_queue = Queue.Queue()
#     out_queue = Queue.Queue()
#     work_queue.put(('http://qiusuo.nyist.net',2))
#     threads = []
#     log_hdr = Logger('/tmp/spider.log',5).get_hdr() #获取日志句柄
#     for i in range(10):
#           threads.append(Core(work_queue,out_queue,log_hdr,2,None))
