# coding: utf-8

import random
import threading
import time
from Queue import Queue

import requests
from bs4 import BeautifulSoup

thread_local = threading.local()

class LG:
    def __init__(self, threadNum):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
        }
        self.URL = 'http://www.lagou.com/'
        self.position = []
        self.pos_que = Queue()
        self.threadNum = threadNum
        """数据库"""

    def getPosition(self):
        page = self.__getPage()
        bs = BeautifulSoup(page.content, 'lxml')
        for s in bs.find_all('div', 'menu_sub'):
            for i in s.select('dl dd a'):
                self.position.append(i.string)

        print('职业列表获取成功')

    def __getPage(self):
        time.sleep(random.randint(0, 5))
        try:
            return requests.get(self.URL)
        except Exception:
            print('connect failed ...')
            return None

    def work(self):
        pass

    def workThread(self):
        thread_local.db = ''
        self.work()

    def run(self):
        print('获取职业列表 ...')
        self.__getPosition()
        for p in self.position:
            self.pos_que.put(p)

        print('Parent process start ...')
        for i in range(self.threadNum):
            t = threading.Thread(target=self.workThread())
            t.start()
        self.pos_que.join()
        print('Over......')


if __name__ == '__main__':
    lg = LG(1)
    lg.getPosition()
    print(len(lg.position))
    for i in range(len(lg.position)):
        print(lg.position[i])

