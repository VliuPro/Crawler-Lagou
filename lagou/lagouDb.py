# coding: utf-8

from pony.orm import *

db = Database()


class PositionType(db.Entity):
    _table_ = 'db_type'
    typeName = Required(str, unique=True)
    # 区别职位类型和总类型 比如： Java， 技术
    typeNo = Required(int)


class City(db.Entity):
    _table_ = 'db_city'
    name = Required(str, unique=True)
    job = Set('Job')


class Job(db.Entity):
    _table_ = 'db_job'
    positionId = Required(int, size=32, unique=True)
    positionName = Required(str)
    positionFirstType = Optional(str)
    positionType = Optional(str)
    positionAdvantage = Optional(str)
    salary = Optional(str)
    workYear = Optional(str)
    education = Optional(str)
    createTime = Optional(str)
    nature = Optional(str)
    leader = Optional(str)
    city = Required(City)
    company = Required('Company')


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
    jobs = Set(Job)


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
    def check_job(self, positionId):
        return Job.exists(positionId=int(positionId))

    @db_session
    def check_company(self, companyId):
        return Company.exists(companyId=int(companyId))


class DbTools():
    def __init__(self, dbs):
        self.db = dbs

    @db_session
    def city_save(self, cities):
        for city in cities:
            c = City(name=city)

    @db_session
    def positiontype_save(self, typename, typeno):
        ptype = PositionType(typeName=typename, typeNo=typeno)

    @db_session
    def company_save(self, cp):
        return Company(**cp)

    @db_session
    def position_save(self, pt):
        return Job(**pt)

    @db_session
    def save(self, positions):
        for position in positions:
            if not self.db.check_company(companyId=position['companyId']):
                s = ','
                if len(position['companyLabelList']) == 0:
                    ads = ''
                else:
                    ads = ','.join([i for i in position['companyLabelList'])
                com = {
                    'companyId': int(position['companyId']),
                    'companyShortName': position['companyName'],
                    'companyName': position['companyShortName'],
                    'companySize': position['companySize'],
                    'companyLogo': position['companyLogo'],
                    'financeStage': position['financeStage'],
                    'industryField': position['industryField'],
                    'companyAdvList': ads
                }
                company = self.company_save(com)
            else:
                pass

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
                    'leader': position['leaderName']
                }
                job = self.position_save(pt)
                job.city = City.get(name=position['city'])
                job.company = Company.get(companyId=position['companyId'])
                Company.get(companyId=position['companyId']).jobs = job
            else:
                pass
