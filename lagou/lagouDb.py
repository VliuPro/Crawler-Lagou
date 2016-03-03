# coding: utf-8

from pony.orm import *

db = Database()


class PositionType(db.Entity):
    _table_ = 'db_type'
    typeName = Optional(str, unique=True)
    # 区别职位类型和总类型 比如： Java， 技术
    typeNo = Required(int)


class City(db.Entity):
    _table_ = 'db_city'
    name = Required(str, unique=True)
    job = Set('Job')


class Advantage(db.Entity):
    _table_ = 'db_advantage'
    name = Required(str, unique=True)
    companys = Set('Company')


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
    city = Optional(City)
    company = Optional('Company')


class Company(db.Entity):
    _table_ = 'db_company'
    companyId = Required(int, size=32, unique=True)
    companyShortName = Required(str)
    companyName = Required(str)
    companySize = Optional(str)
    companyLogo = Optional(str, 80)
    financeStage = Optional(str)
    industryField = Optional(str)
    companyAdvlist = Set(Advantage)
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

    # @classmethod
    # def mappingTables(cls, host, user, passwd, database):
    #     db.bind('mysql', host=host, user=user, passwd=passwd, database=database)
    #     # sql_debug(True)    开启后会在控制台输出SQL语句
    #     db.generate_mapping(create_tables=True)
    #     return db

    @db_session
    def check_job(self, positionId):
        return Job.exists(positionId=int(positionId))

    @db_session
    def check_company(self, companyId):
        return Company.exists(companyId=int(companyId))

    @db_session
    def check_positiontype(self, typeName):
        return typeName != '' and PositionType.exists(typeName=typeName)

    @db_session
    def check_city(self, name):
        return name != '' and City.exists(name=name)

    @db_session
    def check_advantage(clselfs, name):
        return name != '' and Advantage.exists(name=name)

global count
count = 0


class DbTools():
    def __init__(self, positions, dbs):
        self.positions = positions
        self.db = dbs

    @db_session
    def save(self):
        for position in self.positions:
            # with db_session:

            if not self.db.check_city(name=position['city']):
                city = City(name=position['city'])
                commit()

            if not self.db.check_positiontype(typeName=position['positionFirstType']):
                positionFirstType = PositionType(typeName=position['positionFirstType'], typeNo=0)
                commit()

            if not self.db.check_positiontype(typeName=position['positionType']):
                positiontype = PositionType(typeName=position['positionType'], typeNo=1)
                commit()

            for l in position['companyLabelList']:
                if not self.db.check_advantage(name=l):
                    advantage = Advantage(name=l)
                    commit()

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
                commit()
                ads = []
                for l in position['companyLabelList']:
                    Advantage.get(name=l).companys = company
                    ads.append(Advantage.get(name=l))
                company.companyAdvlist = ads
                commit()

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
                # sql_debug(True)
                job = Job(**pt)
                job.city = City.get(name=position['city'])
                job.company = Company.get(companyId=position['companyId'])
                commit()
                # sql_debug(False)
                Company.get(companyId=position['companyId']).jobs = job
                commit()
            global count
            count += 1
        print(u'职位总数为： ' + str(count))
