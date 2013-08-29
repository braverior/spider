spider
======

a spider in python


1.这是个一个简单的python爬虫程序
2.程序采用了线程池来对线程进行管理
3.程序有一些bug会在以后修改的
4.程序需要一些库的支持： BeautifulSoup chardet  等。
5.用法：

进入src目录：
python spider.py -u http://***** -d 2 --dbfile /tmp/test.db   -l 5      -f /tmp/test.log  --thread 10  --key '关键字'
                 入口链接         深度   数据库文件             日志等级    日志文件          线程数        关键字
             
python spider.py --testself
程序测试


6.coverage 程序覆盖率测试
