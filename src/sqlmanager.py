# !/usr/bin/env python
# -*- coding:utf-8 -*-

import sqlite3
from log import*
import threading
import Queue
import time
from Queue import Empty


class SQLManager(threading.Thread):
    def __init__(self, dbfile, in_queue, log_hdr):
        """初始化
        参数：
        dbfile   数据库文件
        in_queue 保存数据队列
        log_hdr  日志文件句柄
        """
        threading.Thread.__init__(self)

        self.dbfile = dbfile
        self.in_queue = in_queue
        self.log_hdr = log_hdr
        self.connection = None
        self.start()

    def get_connection(self):
        """创建数据库连接
        """
        try:
            self.connection = sqlite3.connect(self.dbfile)
            self.connection.text_factory = str
            self.init_db()
            return self.connection
        except Exception, e:
            self.log_hdr.error("can't open the database file:%s" % e)
            return

    def init_db(self):
        """创建数据库
        """
        if not self.connection:
            self.log_hdr.error("the connection has been closed..")
            return
        curs = self.connection.cursor()
        sql = """
               CREATE TABLE IF NOT EXISTS pages(
               url,
               title,
               content,
               last_modified);"""
        curs.executescript(sql)
        curs.close()
        self.log_hdr.info('database Initialization done')

    def save_page(self, page):
        """
        保存网页内容

        """
        try:
            curs = self.connection.cursor()
            self.log_hdr.info('save page: %s' % page['url'])
            #注：execute执行脚本，参数要放到元组中
            curs.execute('INSERT INTO pages VALUES (?,?,?,?);',
                         (page['url'],
                          page['title'],
                          page['content'],
                          page['lastmodified']))
            self.connection.commit()
            curs.close()
        except Exception, e:
            self.log_hdr.error("can not save the page:%s :REASON:%s"
                               % (page['url'], e))
            return

    def run(self):
        """数据存储线程运行函数
           打开数据库连接
           初始化数据库
           等待输出队列
        """

        self.get_connection()
        self.log_hdr.info('database thread running')
        while True:
            try:
                page = self.in_queue.get(True, 1)
                if not page:
                    # 对应 ***put(None),已收到结束信号。
                    self.log_hdr.info('the task is none %s' % 'T T')
                    break
               # print 'func:::::>',page['content']
                self.save_page(page)
            except Empty, e:
                #如果输出队列为空，则一直等待
                self.log_hdr.error('Queue is Empty: %s' % e)
                continue

            except Exception, e:
                #遇到错误抛出去
                self.log_hdr.error('Exception: %s' % e)
                break
        self.connection.close()
        #数据存储线程结束，关闭数据库连接

#if __name__ == '__main__':
#   out_queue = Queue.Queue()
#   page = {}
#   page['url'] = 'http://www.baidu.com'
#   page['title'] = '百度'
#   page['content'] = '百度一下 你就知道了，这仅仅是一个测试'
#   page['lastmodified'] = time.strftime('%Y-%m-%d %H:%M:%S')
#
#   out_queue.put(page)
#
#   log = Logger('/tmp/spider.log',5).get_hdr()
#   sm = SQLManager('/tmp/spider.db',out_queue,log)
