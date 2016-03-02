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

    def __getPosition(self):
        page = self.__getPage()
        if page is None:
            return None
        bs = BeautifulSoup(page.content, 'lxml')
        for s in bs.find_all('div', 'menu_sub'):
            for i in s.select('dl dd a'):
                self.position.append(i.string)
        if not len(self.position):
            print('职业列表获取完成')
        else:
            return self.position

    def __getPage(self):
        time.sleep(random.randint(0, 5))
        try:
            return requests.get(self.URL)
        except Exception:
            print('connect failed ...')
            return None

    def __getJobsList(self, kd):
        data = {'kd': kd,
                'pn': 1,
                'px': 'new'}
        jsonData = requests.post('http://www.lagou.com/jobs/positionAjax.json?',
                                 data=data,
                                 headers=self.headers)
        print(str(kd) + u' 职位总数目： ' + str(jsonData.json()['content']['totalCount']))
        # 获取该关键字的职位数目
        totalCount = jsonData.json()['content']['totalCount']
        # 获取第一页职位数据
        jobs = jsonData.json()['content']['result']
        if not len(jobs):
            return None
        else:
            # 实例化成Job来存入数据库
            for job in jobs:
                pass

    def work(self):
        while True:
            kd = self.pos_que.get()
            self.__getJobsList(kd)
            time.sleep(1)
            self.pos_que.task_done()

    def workThread(self):
        thread_local.db = ''
        self.work()

    def run(self):
        print('获取职业列表 ...')
        if self.__getPosition() is None:
            print('职业列表获取失败 ...')
            return None
        for p in self.position:
            print(p)
            self.pos_que.put(p)

        print('Parent process start ...')
        for i in range(self.threadNum):
            t = threading.Thread(target=self.workThread())
            t.start()
        self.pos_que.join()
        print('Over......')


if __name__ == '__main__':
    lg = LG(10)
    lg.run()

