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


class Advantage(db.Entity):
    _table_ = 'db_advantage'
    name = Required(str, unique=True)
    companys = Set('Company')


class Job(db.Entity):
    _table_ = 'db_job'
    positionId = Required(int, size=32, unique=True)
    positionName = Required(str)
    positionFirstType = Required(str)
    positionType = Required(str)
    positionAdvantage = Required(str)
    salary = Required(str)
    workYear = Required(str)
    education = Required(str)
    createTime = Required(str)
    nature = Required(str)
    leader = Optional(str)
    city = Required(City)
    company = Required('Company')


class Company(db.Entity):
    _table_ = 'db_company'
    companyId = Required(int, size=32, unique=True)
    companyShortName = Required(str)
    companyName = Required(str)
    companySize = Required(str)
    companyLogo = Required(str, 80)
    financeStage = Required(str)
    industryField = Required(str)
    companyAdvlist = Set(Advantage)
    jobs = Set(Job)


class DB:
    def __init__(self, host, user, passwd, database):
        self.db = db
        self.db.bind('mysql', host=host, user=user, passwd=passwd, database=database)

    def mappingTables(self):
        # sql_debug(True)    开启后会在控制台输出SQL语句
        self.db.generate_mapping(create_tables=True)

    @db_session
    def check_job(self, positionId):
        return Job.exists(positionId=positionId)

    @db_session
    def check_company(self, companyId):
        return Company.exists(companyId=companyId)

    @db_session
    def check_positiontype(self, typename, typeno):
        return PositionType.exists(typeName=typename, typeNo=typeno)

    @db_session
    def check_city(self, name):
        return City.exists(name=name)

    @db_session
    def check_advantage(self, name):
        return Advantage.exists(name=name)


if __name__ == '__main__':
    mdb = DB(host='127.0.0.1', user='vliupro', passwd='liujida', database='ponytest')
    # sql_debug(True)
    mdb.mappingTables()
    # with db_session:
    #     com = {
    #         'companyId': int('11356'),
    #         'companyShortName': u'花花',
    #         'companyName': u'花花有限公司',
    #         'companySize': u'12-20人',
    #         'companyLogo': u'sasasasasasasasas',
    #         'financeStage': u'Ｃ轮',
    #         'industryField': u'大数据'
    #     }
    #     company = Company(**com)
    print(mdb.check_city('hahaha'))
