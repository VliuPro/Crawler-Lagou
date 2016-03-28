# coding: utf-8

from pony.orm import *
from datetime import datetime


db = Database()


class PositionType(db.Entity):
    _table_ = 'db_type'
    typeName = Required(str, unique=True)


class City(db.Entity):
    _table_ = 'db_city'
    name = Required(str, unique=True)
    # job = Set('Job')


class Job(db.Entity):
    _table_ = 'db_job'
    positionId = Required(int, size=32, unique=True)
    positionName = Required(str)
    positionKey = Required(str)
    positionFirstType = Optional(str)
    positionType = Optional(str)
    positionAdvantage = Optional(str)
    salary = Required(str)
    workYear = Optional(str)
    education = Optional(str)
    createTime = Optional(datetime)
    nature = Optional(str)
    leader = Optional(str)
    city = Required(str)
    company = Required(int, size=32)
    info = Optional('JobInfo')


class Company(db.Entity):
    _table_ = 'db_company'
    companyId = Required(int, size=32, unique=True)
    companyShortName = Required(str)
    companyName = Required(str)
    companySize = Optional(str)
    companyLogo = Optional(str, 80)
    financeStage = Optional(str)
    industryField = Optional(str)
    companyAdvlist = Optional(str)
    # jobs = Set('Job')


class JobInfo(db.Entity):
    _table_ = 'db_jobinfo'
    describe = Optional(LongStr)
    jobId = Optional(int, unique=True)
    job = Optional(Job)


class DB:
    def __init__(self, host, user, passwd, database):
        self.db = db
        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database
        self.db.bind('mysql', host=self.host, user=self.user, passwd=self.passwd, database=self.database)
        self.db.generate_mapping(create_tables=True)

    @db_session
    def check_city(self, city):
        return City.exists(name=city)

    @db_session
    def check_type(self, p):
        return PositionType.exists(name=p)

    @db_session
    def check_job(self, positionId):
        return Job.exists(positionId=positionId)

    @db_session
    def check_company(self, companyId):
        return Company.exists(companyId=companyId)

    @db_session
    def check_jobinfo(self, jobId):
        return JobInfo.exists(jobId=jobId)


class DbTools():
    def __init__(self, dbs):
        self.db = dbs

    @db_session
    def city_save(self, city):
        if City.get(name=city) == None:
            return City(name=city)

    @db_session
    def positiontype_save(self, typename):
        if PositionType.get(typeName=typename) == None:
            ptype = PositionType(typeName=typename)

    @db_session
    def company_save(self, cp):
        return Company(**cp)

    @db_session
    def position_save(self, pt):
        return Job(**pt)

    @db_session
    def info_save(self, jobids, texts):
        for i in range(len(jobids)):
            jobid = jobids[i]
            text = texts[i]
            job = Job.get(positionId=jobid)
            if not self.db.check_jobinfo(jobid) and job != None :
                if text == None:
                    job.delete()
                    return None
                elif text == '' or text == '404':
                    text = 'none'
                info = JobInfo(jobId=jobid, describe=text)
                info.job = job
                job.info = info
            else:
                pass

    @db_session
    def save(self, kd, positions):
        for position in positions:
            company = Company.get(companyId=int(position['companyId']))
            if company == None:
                s = ','
                if len(position['companyLabelList']) == 0:
                    ads = ''
                else:
                    ads = ','.join([i.strip() for i in position['companyLabelList']])
                com = {
                    'companyId': int(position['companyId']),
                    'companyShortName': position['companyName'].strip(),
                    'companyName': position['companyShortName'].strip(),
                    'companySize': position['companySize'].strip(),
                    'companyLogo': position['companyLogo'].strip(),
                    'financeStage': position['financeStage'].strip(),
                    'industryField': position['industryField'].strip(),
                    'companyAdvlist': ads
                }
                company = self.company_save(com)
            else:
                pass

            if not self.db.check_job(positionId=int(position['positionId'])):
                pt = {
                    'positionId': int(position['positionId']),
                    'positionName': position['positionName'].strip(),
                    'positionKey': kd,
                    'positionFirstType': position['positionFirstType'].strip(),
                    'positionType': position['positionType'].strip(),
                    'positionAdvantage': position['positionAdvantage'].strip(),
                    'salary': position['salary'].strip(),
                    'workYear': position['workYear'].strip(),
                    'education': position['education'].strip(),
                    'createTime': datetime.strptime(position['createTime'].strip(), '%Y-%m-%d %H:%M:%S'),
                    'nature': position['jobNature'].strip(),
                    'leader': position['leaderName'].strip(),
                    'city': position['city'],
                    'company': int(position['companyId'])
                }
                job = self.position_save(pt)

            else:
                pass
