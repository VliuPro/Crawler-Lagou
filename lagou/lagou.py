# coding: utf-8

import threading
from queue import Queue

import requests
from bs4 import BeautifulSoup

from lagouDb import DB


kd_queue = Queue()
db = DB(host='127.0.0.1', user='vliupro', passwd='liujida', database='ponytest')

class ThreadCrawl(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            item = self.queue.get()
            self.queue.task_done()


