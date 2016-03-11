# coding: utf-8

from pony.orm import *

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
    positionId = Required(str, unique=True)
    positionName = Required(str)
    positionFirstType = Optional(str)
    positionType = Optional(str)
    positionAdvantage = Optional(str)
    salary = Required(str)
    workYear = Optional(str)
    education = Optional(str)
    createTime = Optional(str)
    nature = Optional(str)
    leader = Optional(str)
    city = Optional(str)
    company = Optional('Company')


class Company(db.Entity):
    _table_ = 'db_company'
    companyId = Required(str, unique=True)
    companyShortName = Required(str)
    companyName = Required(str)
    companySize = Optional(str)
    companyLogo = Optional(str, 80)
    financeStage = Optional(str)
    industryField = Optional(str)
    companyAdvlist = Optional(str)
    jobs = Set('Job')


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
        return City.exists(name=p)

    @db_session
    def check_job(self, positionId):
        return Job.exists(positionId=positionId)

    @db_session
    def check_company(self, companyId):
        return Company.exists(companyId=companyId)


class DbTools():
    def __init__(self, dbs):
        self.db = dbs

    @db_session
    def city_save(self, cities):
        for city in cities:
            if City.get(name=city) == None:
                c = City(name=city)

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
    def save(self, positions):
        for position in positions:
            if not self.db.check_company(companyId=str(position['companyId'])):
                s = ','
                if len(position['companyLabelList']) == 0:
                    ads = ''
                else:
                    ads = ','.join([i for i in position['companyLabelList']])
                com = {
                    'companyId': str(position['companyId']),
                    'companyShortName': position['companyName'],
                    'companyName': position['companyShortName'],
                    'companySize': position['companySize'],
                    'companyLogo': position['companyLogo'],
                    'financeStage': position['financeStage'],
                    'industryField': position['industryField'],
                    'companyAdvlist': ads
                }
                company = self.company_save(com)
            else:
                pass

            if not self.db.check_job(positionId=str(position['positionId'])):
                pt = {
                    'positionId': str(position['positionId']),
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
                    'city': position['city']
                }
                job = self.position_save(pt)
                # city = City.get(name=position['city'])
                # if city != None:
                #     job.city = city
                # else:
                #     job.city = position['city']

                company = Company.get(companyId=str(position['companyId']))
                if company != None:
                    job.company = company
                    company.jobs = job
                else:
                    job.company = position['companyId']
            else:
                pass
