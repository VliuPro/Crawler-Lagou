# coding: utf-8

import threading
import time
import random
from queue import Queue

import requests
from bs4 import BeautifulSoup

from lagouDb import DB, DbTools


kd_queue = Queue()
ps_queue = Queue()
job_queue = Queue()
lock = threading.Lock()
db = DB(host='127.0.0.1', user='vliupro', passwd='liujida', database='ponytest')


class Lagou:
    def __init__(self):
        self.URL = 'http://www.lagou.com/'

    def getTypes(self):
        positions = []
        page = self.getPagecode(self.URL)
        if page == None:
            print('No content ...')
            return []
        bs = BeautifulSoup(page.content, 'lxml')
        for s in bs.find_all('div', 'menu_sub'):
            for i in s.select('dl dd a'):
                positions.append(i.string.strip())
        if len(positions):
            print('职业列表获取完成')
            return positions
        else:
            print('职业列表获取失败')

    def getCities(self):
        cities = []
        page = self.getPagecode('http://www.lagou.com/jobs/list_Java')
        if page == None:
            print('No content city')
            return []
        bs = BeautifulSoup(page.content, 'lxml')
        for s in bs.find_all('div', 'more-positions'):
            for i in s.select('li a'):
                cities.append(i.string.strip())
        cities.pop(0)
        return cities

    def getPagecode(self, url, time=0):
        try:
            return requests.get(url)
        except Exception:
            print('connect failed ...')
            return None



class ThreadCrawl(threading.Thread):
    def __init__(self, kqueue, pqueue):
        threading.Thread.__init__(self)
        self.kdqueue = kqueue
        self.psqueue = pqueue
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
        }
        self.url = 'http://www.lagou.com/jobs/positionAjax.json?px=new'

    def getJsonData(self, kd, pn):
        data = {
            'kd': kd,
            'pn': pn,
            'px': 'new'
        }
        print('get jsonData , kd = ' + str(kd) + ' , pn = ' + str(pn))
        return requests.post(self.url, data=data, headers=self.headers).json()

    def run(self):
        while True:
            item = self.kdqueue.get()
            totalPageCount = int(self.getJsonData(item, 1)['content']['totalPageCount'])
            print('get ' + str(item) + ' data , total = ' + str(totalPageCount))
            for i in range(1, totalPageCount + 1):
                jd = self.getJsonData(item, i)
                self.psqueue.put((item, jd['content']['result']))
                time.sleep(round(random.uniform(0.5, 1), 2))
            self.kdqueue.task_done()


class ThreadSave(threading.Thread):
    def __init__(self, queue, job_queue, lock, dt):
        threading.Thread.__init__(self)
        self.queue = queue
        self.job_queue = job_queue
        self.lock = lock
        self.dt = dt

    def makeJobIdList(self, jobs):
        idlist = []
        for i in range(len(jobs)):
            idlist.append(jobs[i]['positionId'])
        self.job_queue.put(idlist)

    def run(self):
        while True:
            kd, jobs = self.queue.get()
            print('put jobId into job_queue..., kd = ' + str(kd))
            self.makeJobIdList(jobs)
            print('save data , kd = ' + str(kd))
            with lock:
                self.dt.save(kd, jobs)
            self.queue.task_done()


class ThreadJobCrawl(threading.Thread):
    def __init__(self, queue, lock, dt):
        threading.Thread.__init__(self)
        self.queue = queue
        self.lock = lock
        self.dt = dt
        self.url = 'http://www.lagou.com/jobs/'

    def makeUrl(self, jid):
        return str(self.url) + str(jid) + '.html'

    def getPageCode(self, url):
        try:
            return requests.get(url)
        except:
            print('connected failed ...')
            return None

    def filterPage(self, jobids):
        texts = []
        for jobid in jobids:
            rurl = self.makeUrl(jobid)
            page = self.getPageCode(rurl)
            if page != None:
                bs = BeautifulSoup(page.content, 'lxml')
                dd = bs.find_all('dd', class_='job_bt')
                if len(dd) == 0:
                    text = None
                else:
                    dd = dd[0]
                    for i in range(len(dd.select('p br'))):
                        dd.select('p br')[0].extract()
                    for i in range(len(dd.select('p strong'))):
                        dd.select('p strong')[0].unwrap()
                    for i in range(len(dd.select('p span'))):
                        dd.select('p span')[0].unwrap()
                    text = "__".join([str(i).replace('\xa0', '') for i in dd.strings if i != '\n' and i != '\xa0'])
            else:
                text = '404'
            texts.append(text)
        return texts

    def run(self):
        while True:
            print('begin deal jobinfo ...')
            jobids = self.queue.get()
            texts = self.filterPage(jobids)
            if texts != None:
                print('save jobinfo ... ... ...')
                with lock:
                    self.dt.info_save(jobids, texts)
                self.queue.task_done()


if __name__ == '__main__':
    # num = int(input('please input num of thread: '))
    num = 10
    lg = Lagou()
    dt = DbTools(db)
    print('get positions list ...')
    plist = lg.getTypes()
    if len(plist) != 0:
        for p in plist:
            dt.positiontype_save(p)
            kd_queue.put(p)
        print('put kd successfully, num = ' + str(len(plist)))
    cities = lg.getCities()
    if len(cities) != 0:
        for city in cities:
            dt.city_save(city)
        print('save cities successfully, num = ' + str(len(cities)))

    for i in range(num):
        t = ThreadCrawl(kd_queue, ps_queue)
        t.setDaemon(True)
        t.start()

    for i in range(num):
        t = ThreadSave(ps_queue, job_queue, lock, dt)
        t.setDaemon(True)
        t.start()

    for i in range(num):
        t = ThreadJobCrawl(job_queue, lock, dt)
        t.setDaemon(True)
        t.start()

    kd_queue.join()
    ps_queue.join()
    job_queue.join()

    print('crawl ending ...')

