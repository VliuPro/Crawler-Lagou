# coding: utf-8

import time
import threading
from Queue import Queue

import requests
from bs4 import BeautifulSoup

from lagouDb import DbTools, DB, count


class LG:
    def __init__(self, threadNum):
        self.URL = 'http://www.lagou.com/'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
        }
        self.num_thread = threadNum
        self.positions = []
        self.pos_que = Queue()
        self.db = DB(host='127.0.0.1', user='vliupro', passwd='liujida', database='ponytest')

    def getPositions(self):
        try:
            page = requests.get(self.URL)
        except Exception:
            print('connect failed ...')
            return None
        bs = BeautifulSoup(page.content, 'lxml')
        for s in bs.find_all('div', 'menu_sub'):
            for i in s.select('dl dd a'):
                self.positions.append(i.string)
        if len(self.positions):
            print('职业列表获取完成')
            return self.positions
        else:
            print('职业列表获取失败')
            return None

    def __getPosAjax(self, kd, pn):
        data = {'kd': kd,
                'pn': pn,
                'px': 'new'}
        jsonData = requests.post('http://www.lagou.com/jobs/positionAjax.json?',
                                 data=data,
                                 headers=self.headers)
        return jsonData.json()

    def savePositionsInfo(self, positions):
        if not len(positions):
            return None
        else:
            # 实例化成Job来存入数据库
            dbt = DbTools(positions, self.db)
            dbt.save()

    def __getJobsList(self, kd):
        pn = 1
        jsonData = self.__getPosAjax(kd, pn)
        # 获取该关键字的职位数目
        totalCount = jsonData['content']['totalCount']
        print(kd.decode('utf-8') + u' 职位总数目： ' + str(totalCount))

        # 如果总页数大于1，则循环请求 totalCount-pn 次 数据并存入数据库
        if totalCount >= pn:
            for i in range(pn, totalCount + 1):
                print(u'开始获取' + kd.decode('utf-8', 'ignore') + u' 的数据-----pn = ' + str(i))
                jsonData = self.__getPosAjax(kd, i)
                print(u'获取' + kd.decode('utf-8', 'ignore') + u' 的数据成功-----pn = ' + str(i))
                jobs = jsonData['content']['result']
                if not len(jobs):
                    break
                print(u'开始存入 ' + kd.decode('utf-8', 'ignore') + u' 的数据 pn=' + str(i))
                self.savePositionsInfo(jobs)
                print(u'存入 ' + kd.decode('utf-8', 'ignore') + u' 的数据成功 pn=' + str(i))
        else:
            return None

    def work_thread(self):
        while True:
            if not self.pos_que.empty():
                kd = self.pos_que.get()
                print(u'开始获取关键字： ' + kd.decode('utf-8', 'ignore') + u" 的招聘信息")
                self.__getJobsList(kd=kd)
                time.sleep(1)
                self.pos_que.task_done()
                print(u'获取关键字： ' + kd.decode('utf-8', 'ignore') + u" -- 完成")
            else:
                break

    def run(self):
        print('获取职业列表 ...')
        self.getPositions()
        if self.positions is None:
            return None
        for p in self.positions:
            # print(p)
            self.pos_que.put(p)

        for i in range(self.num_thread):
            t = threading.Thread(target=self.work_thread())
            t.start()

        self.pos_que.join()


if __name__ == '__main__':
    start = LG(10)
    start.run()

