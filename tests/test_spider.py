#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import unittest
import logging
import Queue
import sqlite3
import pep8

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(TESTS_DIR)
SPIDER_DIR = os.path.join(ROOT_DIR, 'src')

if not SPIDER_DIR in sys.path:
    sys.path.append(SPIDER_DIR)

from core import *
from log import *
from sqlmanager import *
from threadpool import *

import unittest
import chardet


class PEP8Test(unittest.TestCase):
    def test_core_pep8(self):
        core_pep8 = pep8.Checker(os.path.join(SPIDER_DIR, 'core.py'))
        var = core_pep8.check_all()
        self.assertEqual(var, 0)

    def test_threadpool_pep8(self):
        threadpool_pep8 = pep8.Checker(os.path.join(SPIDER_DIR,
                                                    'threadpool.py'))
        var = threadpool_pep8.check_all()
        self.assertEqual(var, 0)

    def test_sqlmanager_pep8(self):
        sqlmanager_pep8 = pep8.Checker(os.path.join(SPIDER_DIR,
                                                    'sqlmanager.py'))
        var = sqlmanager_pep8.check_all()
        self.assertEqual(var, 0)

    def test_log_pep8(self):
        log_pep8 = pep8.Checker(os.path.join(SPIDER_DIR, 'log.py'))
        var = log_pep8.check_all()
        self.assertEqual(var, 0)

    def test_spider_pep8(self):
        spider_pep8 = pep8.Checker(os.path.join(SPIDER_DIR, 'spider.py'))
        var = spider_pep8.check_all()
        self.assertEqual(var, 0)


class LoggerTest(unittest.TestCase):
    def setUp(self):
        self.log = Logger('test_logger.log', 5)
        self.log_hdr = None

    def tearDown(self):
        pass

    def test_get_logger(self):
        self.log_hdr = self.log.get_hdr()
        self.log_hdr.error('This is a error test')
        file_hdr = open('test_logger.log', 'r')
        content = file_hdr.readline()
        var = False
        if content.find('This is a error test'):
            var = True
        os.remove('test_logger.log')
        self.assertTrue(var)


class SQLManagerTest(unittest.TestCase):
    def setUp(self):
        self.out_queue = Queue.Queue()
        self.log_hdr = Logger('spider.log', 5).get_hdr()
        self.sqlmg = SQLManager('test.db', self.out_queue, self.log_hdr)
        self.sqlmg.get_connection()

    def tearDown(self):
        os.remove('spider.log')
        os.remove('test.db')
        pass

    def test_save_page(self):
        page = {'url': 'http://www.baidu.com', 'title': '测试',
                'content': 'testcontent', 'lastmodified': '2013-06'}
        self.out_queue.put(page)
        self.out_queue.put(None)
        time.sleep(0.5)  # 线程程退出
        connection = sqlite3.connect('test.db')
        curs = connection.cursor()
        curs.execute("select content from \
                      pages where url='http://www.baidu.com';")
        var = curs.fetchone()
        curs.close()
        connection.close()
        self.assertEqual(var, ('testcontent',))


class CoreTest(unittest.TestCase):
    def setUp(self):
        self.work_queue = Queue.Queue()
        self.out_queue = Queue.Queue()
        self.work_queue.put(('http://qiusuo.nyist.net', 2))
        self.log_hdr = Logger('spider.log', loglevel=5).get_hdr()
        self.core = Core(self.work_queue, self.out_queue,
                         self.log_hdr, deep=2, key=None)

    def tearDown(self):
        pass

    def test_is_valid_url(self):
        var = self.core.is_valid_url('http://www.baidu.com')
        self.assertTrue(var)
        var = self.core.is_valid_url('https://www.alipay.com')
        self.assertTrue(var)
        var = self.core.is_valid_url('baidu')
        self.assertFalse(var)
        var = self.core.is_valid_url('ht//www.alipay.com')
        self.assertFalse(var)

    def test_get_abs_url(self):
        var = self.core.get_abs_url('http://c.com', 'http://a.com')
        self.assertEqual(var, 'http://c.com')
        var = self.core.get_abs_url('/path/index.html', 'http://a.com')
        self.assertEqual(var, 'http://a.com/path/index.html')

        var = self.core.get_abs_url('pp/ddd', 'http://a.com')
        self.assertEqual(var, 'http://a.com/pp/ddd')

    def test_get_title(self):
        content = "<header><title>this is a test</title></header>"
        var = self.core.get_title(content)
        self.assertEqual(var, 'this is a test')
        content = "<header><TITLE>this is a test</TITLE></header>"
        self.assertEqual(var, 'this is a test')

    def test_parse_url(self):
        content = ''.join(('<html><body>',
                           '<a href="http://a.com"></a>',
                           '<a href="/test"></a>',
                           '<a href="test/index.html"></a>',
                           '<a href="test"></a>',
                           '<a href="#top"></a>',
                           '<a href="javascript"></a>',
                          '</body></html>'))

        var = self.core.parse_url(content)
        tmp = ['http://a.com', '/test', 'test/index.html',
               'test', '#top', 'javascript']
        self.assertEqual(tmp, var)

    def test_remove_repeat_url(self):
        tmp = ['http://a.com', 'http://a.com', None, 'http://a.com#top']
        tar = ['http://a.com']
        var = self.core.remove_repeat_url(tmp)
        self.assertEqual(tar, var)

    def test_convert_utf8_charset(self):
        content = '测试转换'
        con = content.decode('utf-8', 'ignore').encode('gbk')
        tmp_content = self.core.convert_utf8_charset(con)
        charset = chardet.detect(tmp_content)
        self.assertEqual('utf-8', charset['encoding'].lower())

        content = '测试转换'
        con = content.decode('utf-8', 'ignore').encode('gb2312')
        tmp_content = self.core.convert_utf8_charset(con)
        charset = chardet.detect(tmp_content)
        self.assertEqual('utf-8', charset['encoding'].lower())


def run_tests():
    unittest.main()

if __name__ == '__main__':
    unittest.main()
