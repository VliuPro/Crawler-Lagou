# coding: utf-8

import random
import threading
import time
from Queue import Queue

import requests
from bs4 import BeautifulSoup

from lagouDb import *


class LG:
    def __init__(self, threadNum):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
        }
        self.URL = 'http://www.lagou.com/'
        self.position = []
        self.pos_que = Queue()
        self.threadNum = threadNum
        self.db = DB(host='127.0.0.1', user='vliupro', passwd='liujida', database='ponytest')
        self.db.mappingTables()

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
                    if not self.db.check_city(position['city']):
                        city = City(name=position['city'])

                    if not self.db.check_positiontype(position['positionFirstType'], 0):
                        positiontype = PositionType(typeName=position['positionFirstType'], typeNo=0)

                    for l in position['companyLabelList']:
                        if not self.db.check_advantage(l):
                            advantage = Advantage(name=l)

                    if not self.db.check_company(position['companyId']):
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


                    if not self.db.check_job(position['positionId']):
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
        jsonData = self.__getPosAjax(kd, pn)

        # 获取该关键字的职位数目
        totalCount = jsonData['content']['totalCount']
        print(str(kd) + u' 职位总数目： ' + str(totalCount))

        # 获取第一页职位数据
        jobs = jsonData['content']['result']
        print(u'开始存入 ' + str(kd) + u' 的数据： pn=' + str(pn))
        # 存入第一页职位数据
        self.__savePositions(jobs)
        # 如果总页数大于1，则循环请求 totalCount-pn 次 数据并存入数据库
        if totalCount >= pn:
            for i in range(pn + 1, totalCount + 1):
                jsonData = self.__getPosAjax(kd, i)
                jobs = jsonData['content']['result']
                print(u'开始存入 ' + str(kd) + u' 的数据： pn=' + str(i))
                self.__savePositions(jobs)
        else:
            return None

    def __workThread(self):
        while True:
            kd = self.pos_que.get()
            print(u'开始获取关键字： ' + str(kd) + u" 的招聘信息")
            self.__getJobsList(kd)
            time.sleep(1)
            self.pos_que.task_done()

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
            print(u'启动线程')
            t = threading.Thread(target=self.__workThread(), name=(u'进程' + str(i)))
            print(u'启动线程： ' + t.getName())
            t.setDaemon(True)
            t.start()

        self.pos_que.join()
        print('Over......')


if __name__ == '__main__':
    lg = LG(10)
    lg.run()

