# coding: utf-8

from manage import LG


def run():
    threadNum = input('请输入创建线程数： ')
    start = LG(int(threadNum))
    start.run()

run()
