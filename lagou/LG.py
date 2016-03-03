# coding: utf-8

import random
import threading
import time
from Queue import Queue

import requests
from bs4 import BeautifulSoup

from lagouDb import *


class LG(threading.Thread):
    URL = 'http://www.lagou.com/'

    def __init__(self, que):
        threading.Thread.__init__(self)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
        }
        self.pos_que = que
        self.db = DB(host='127.0.0.1', user='vliupro', passwd='liujida', database='ponytest')
        self.db.mappingTables()

    def __getPosAjax(self, kd, pn):
        data = {'kd': kd,
                'pn': pn,
                'px': 'new'}
        jsonData = requests.post('http://www.lagou.com/jobs/positionAjax.json?',
                                 data=data,
                                 headers=self.headers)
        return jsonData.json()

    def __savePositions(self, positions):
        if not len(positions):
            return None
        else:
            # 实例化成Job来存入数据库
            for position in positions:
                with db_session:
                    if not self.db.check_city(name=position['city']):
                        city = City(name=position['city'])

                    if not self.db.check_positiontype(typeName=position['positionFirstType']):
                        positionFirstType = PositionType(typeName=position['positionFirstType'], typeNo=0)

                    if not self.db.check_positiontype(typeName=position['positionType']):
                        positiontype = PositionType(typeName=position['positionType'], typeNo=1)

                    for l in position['companyLabelList']:
                        if not self.db.check_advantage(name=l):
                            advantage = Advantage(name=l)

                    if not self.db.check_company(companyId=position['companyId']):
                        com = {
                            'companyId': int(position['companyId']),
                            'companyShortName': position['companyName'],
                            'companyName': position['companyShortName'],
                            'companySize': position['companySize'],
                            'companyLogo': position['companyLogo'],
                            'financeStage': position['financeStage'],
                            'industryField': position['industryField']
                        }
                        company = Company(**com)
                        ads = []
                        for l in position['companyLabelList']:
                            Advantage.get(name=l).companys = company
                            ads.append(Advantage.get(name=l))
                        company.companyAdvlist = ads

                    if not self.db.check_job(positionId=position['positionId']):
                        pt = {
                            'positionId': position['positionId'],
                            'positionName': position['positionName'],
                            'positionFirstType': position['positionFirstType'],
                            'positionType': position['positionType'],
                            'positionAdvantage': position['positionAdvantage'],
                            'salary': position['salary'],
                            'workYear': position['workYear'],
                            'education': position['education'],
                            'createTime': position['createTime'],
                            'nature': position['jobNature'],
                            'leader': position['leaderName'],
                            'city': City.get(name=position['city']),
                            'company': Company.get(companyId=position['companyId'])
                        }
                        job = Job(**pt)
                        Company.get(companyId=position['companyId']).jobs = job

    def __getJobsList(self, kd):
        pn = 1
        # print(u'开始获取' + kd.decode('utf-8') + u' 的数据，-----    pn = ' + str(pn))
        jsonData = self.__getPosAjax(kd, pn)
        # print(u'获取' + kd.decode('utf-8') + u' 的数据成功 -----       pn = ' + str(pn))

        # 获取该关键字的职位数目
        totalCount = jsonData['content']['totalCount']
        print(kd.decode('utf-8') + u' 职位总数目： ' + str(totalCount))

        # 获取第一页职位数据
        # jobs = jsonData['content']['result']
        # print(u'开始存入 ' + kd.decode('utf-8') + u' 的数据： pn=' + str(pn))
        # 存入第一页职位数据
        # self.__savePositions(jobs)
        # print(u'存入 ' + kd.decode('utf-8') + u' 的数据成功 -----      pn=' + str(pn))
        # 如果总页数大于1，则循环请求 totalCount-pn 次 数据并存入数据库
        if totalCount >= pn:
            for i in range(pn, totalCount + 1):
                print(u'开始获取' + kd.decode('utf-8') + u' 的数据，-----pn = ' + str(i))
                jsonData = self.__getPosAjax(kd, i)
                print(u'获取' + kd.decode('utf-8') + u' 的数据成功 -----pn = ' + str(i))
                jobs = jsonData['content']['result']
                if not len(jobs):
                    break
                print(u'开始存入 ' + kd.decode('utf-8') + u' 的数据 pn=' + str(i))
                self.__savePositions(jobs)
                print(u'存入 ' + kd.decode('utf-8') + u' 的数据成功 pn=' + str(i))
        else:
            return None

    def run(self):
        while True:
            if not self.pos_que.empty():
                kd = self.pos_que.get()
                print(u'开始获取关键字： ' + kd.decode('utf-8') + u" 的招聘信息")
                self.__getJobsList(kd=kd)
                time.sleep(random.randint(0, 2))
                self.pos_que.task_done()
                print(u'获取关键字： ' + kd.decode('utf-8') + u" -- 完成")
            else:
                break


def getPositions():
    try:
        page = requests.get(LG.URL)
    except Exception:
        print('connect failed ...')
        return None
    positions = []
    bs = BeautifulSoup(page.content, 'lxml')
    for s in bs.find_all('div', 'menu_sub'):
        for i in s.select('dl dd a'):
            positions.append(i.string)
    if len(positions):
        print('职业列表获取完成')
        return positions
    else:
        print('职业列表获取失败')
        return None


class Down:
    def __init__(self, tNum):
        self.__threadNum = tNum
        self.__que = Queue()
        self.positions = []

    def run(self):
        print('获取职业列表 ...')
        self.__getPositions()
        if self.positions is None:
            return None
        for p in self.positions:
            # print(p)
            self.__que.put(p)
        for i in range(self.__threadNum):
            lg = LG(self.__que)
            lg.start()

        self.__que.join()

    def __getPositions(self):
        try:
            page = requests.get(LG.URL)
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


if __name__ == '__main__':
    start = Down(10)
    start.run()

