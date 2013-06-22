# !/usr/bin/env python
# -*- coding:utf-8 -*-


from core import *
from log import*
from sqlmanager import*

from threading import Timer


class Threadpool(object):
    def __init__(self, url, deep, thread_num,
                 log_hdr, key, work_queue, out_queue):
        """线程池初始化
        参数列表：
        url:         入口链接
        deep:        深度
        thread_num:  线程数
        log_hdr:     日志句柄
        Key:         关键字
        work_queue   输入队列
        out_queue    输出队列
        """
        self.work_queue = work_queue  # 工作队列 (url,deep)
        self.out_queue = out_queue    # 输出队列
        self.threads = []             # 线程集合
        self.url = url                # 入口
        self.log_hdr = log_hdr        # 日志句柄
        self.deep = deep              # 深度
        self.key = key                # 寻找的关键字

        # 初始化
        self.__init_work_queue(self.url, self.deep)
        self.__init_thread_pool(thread_num)

        self.set_timer = threading.Timer(10.0, self.show_info)
        self.set_timer.start()

    def __init_thread_pool(self, thread_num):
        """
        初始化线程
        参数：
        thread_num  线程数量
        """
        for i in range(thread_num):
            self.threads.append(Core(self.work_queue, self.out_queue,
                                     self.log_hdr, self.deep, self.key))

    def show_info(self):
        """每隔10秒显示状态信息
        """
        last_urls = self.check_queue()     # 队列中还剩多少任务
        running_threads = self.runing_threads()  # 正在进行的线程有多少
        print 'current time: %s' % time.strftime('%Y-%m-%d %H:%M:%S'),
        print 'now urls = %d threading = %d' % (last_urls, running_threads)

        self.set_timer = threading.Timer(10.0, self.show_info)
        self.set_timer.start()

    def runing_threads(self):
        """查询进行中的线程数量

        """
        num = 0
        for thread in self.threads:
            if thread.status:
                num += 1
        #if r:
        #    r += self.tasks.qsize()
        return num

    def __init_work_queue(self, url, deep):
        """
        初始化工作队列
        将命令行传递的链接和深度放到工作队列中，
        参数：
        url   入口链接
        deep  深度
        """
        self.work_queue.put((url, deep))

    def check_queue(self):
        """
        检查剩余队列任务
        """
        return self.work_queue.qsize()

    def wait_allcomplete(self):
        """
        等待所有线程运行完毕
        """
        for item in self.threads:
            if item.isAlive():
                item.join()         # 加入正在进行的线程

        self.set_timer.cancel()    # 取消定时
