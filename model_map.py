#coding=utf-8
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

import pickle
from System.Xml.Linq import *

import os
import sqlite3

SQL_CREATE_TABLE_ACCOUNT = '''
    create table if not exists account(
        account_id TEXT, 
        nickname TEXT,    
        username TEXT,
        password TEXT, 
        photo TEXT, 
        telephone TEXT, 
        email TEXT, 
        gender TEXT, 
        age INT, 
        country TEXT,
        province TEXT,
        city TEXT,
        address TEXT, 
        birthday TEXT, 
        signature TEXT,
        install_time INT,
        last_login_time INT,
        recent_visit BLOB,
        source TEXT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0
        )'''

SQL_INSERT_TABLE_ACCOUNT = '''
    insert into account(account_id, nickname, username, password, photo, telephone, email, gender, age, country, province, city, address, birthday, signature, install_time, last_login_time, recent_visit, source, sourceApp, sourceFile, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_SEARCH = '''
    create table if not exists search(
        account_id TEXT, 
        keyword TEXT,
        create_time INT,
        delete_time INT,
        adcode TEXT,
        address TEXT,
        district TEXT,
        pos_x REAL,
        pos_y REAL,
        item_type INT DEFAULT 0,
        source TEXT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0
        )'''

SQL_INSERT_TABLE_SEARCH = '''
    insert into search(account_id, keyword, create_time, delete_time, adcode, address, district, pos_x, pos_y, item_type, source, sourceApp, sourceFile, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_ADDRESS = '''
    create table if not exists address(
        account_id TEXT,
        city_code INT,
        city_name TEXT,
        from_name TEXT,
        from_posX REAL,
        from_posY REAL,
        from_addr TEXT,
        to_name TEXT,
        to_posX REAL,
        to_posY REAL,
        to_addr TEXT,
        create_time INT,
        source TEXT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0,
        repeated INT DEFAULT 0
        )'''

SQL_INSERT_TABLE_ADDRESS = '''
    insert into address(account_id, city_code, city_name, from_name, from_posX, from_posY, from_addr, to_name, to_posX, to_posY, to_addr, create_time, source, sourceApp, sourceFile, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''



class Map(object):
    
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)

        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()

        self.db_create_table()

    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_create_table(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_ACCOUNT)
            self.cursor.execute(SQL_CREATE_TABLE_ADDRESS)
            self.cursor.execute(SQL_CREATE_TABLE_SEARCH)

    def db_insert_table_account(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_ACCOUNT, column.get_values())

    def db_insert_table_address(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_ADDRESS, column.get_values())

    def db_insert_table_search(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_SEARCH, column.get_values())



class Column(object):

    def __init__(self):
        self.source = ""
        self.sourceApp = ""
        self.sourceFile = ""
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return self.source, self.sourceApp, self.sourceFile, self.deleted, self.repeated


class Account(Column):

    def __init__(self):
        super(Account, self).__init__()      
        self.account_id = None      # 账户id
        self.nickname = None        # 昵称
        self.username = None        # 用户名
        self.password = None        # 密码
        self.photo = None           # 头像
        self.telephone = None       # 电话
        self.email = None           # 邮箱
        self.gender = None          # 性别
        self.age = None             # 年龄
        self.country = None         # 国家
        self.province = None        # 省份
        self.city = None            # 城市
        self.address = None         # 地址
        self.birthday = None        # 生日
        self.signature = None       # 签名
        self.install_time = None    # 安装时间
        self.last_login_time = None # 最后一次登陆时间
        self.recent_visit = None    # 最近一段时间访问次数

    def get_values(self):
        return (self.account_id, self.nickname, self.username, self.password, self.photo, self.telephone, self.email, self.gender, self.age, self.country, self.province, self.city, self.address, self.birthday, self.signature, self.install_time, self.last_login_time, self.recent_visit) + super(Account, self).get_values()


class Address(Column):
    
    def __init__(self):
        super(Address, self).__init__()
        self.account_id = None      # 账户id
        self.city_code = None       # 城市代码
        self.city_name = None       # 城市
        self.from_name = None       # 起点
        self.from_posX = None       # 起点经度
        self.from_posY = None       # 起点纬度
        self.from_addr = None       # 起点地址
        self.to_name = None         # 目的地
        self.to_posX = None         # 目的地经度
        self.to_posY = None         # 目的地纬度
        self.to_addr = None         # 目的地地址
        self.create_time = None     # 目的地搜索时间

    def get_values(self):
        return (self.account_id,self.city_code,self.city_name,self.from_name,self.from_posX,self.from_posY,self.from_addr,self.to_name,self.to_posX,self.to_posY,self.to_addr,self.create_time) + super(Address, self).get_values()


class Search(Column):
    
    def __init__(self):
        super(Search, self).__init__()
        self.account_id = None      # 账户id
        self.keyword = None         # 搜索关键词
        self.create_time = None     # 搜索时间
        self.delete_time = None     # 删除时间
        self.adcode = None          # 城市代码
        self.address = None         # 搜索地址
        self.district = None        # 地区
        self.pos_x = None           # 搜索经度
        self.pos_y = None           # 搜索纬度
        self.item_type = None       # 搜索类别[0.搜索 2.收藏]

    def get_values(self):
        return (self.account_id, self.keyword, self.create_time, self.delete_time, self.adcode, self.address, self.district, self.pos_x, self.pos_y, self.item_type) + super(Search, self).get_values()



class Genetate(object):

    def __init__(self, cache_db):
        self.cache_db = cache_db

    def get_models(self):
        models = []

        self.db = sqlite3.connect(self.cache_db)
        self.cursor = self.db.cursor()

        models.extend(self._get_accout_models())
        models.extend(self._get_address_models())
        models.extend(self._get_search_models())

        self.cursor.close()
        self.db.close()
        return models

        """
        account_id TEXT,   0
        nickname TEXT,     1    昵称
        username TEXT,     2    用户名
        password TEXT,     3    密码
        photo TEXT,        4    照片url
        telephone TEXT,    5
        email TEXT,        6
        gender TEXT,       7
        age INT,           8    
        country TEXT,      9
        province TEXT,     10
        city TEXT,         11
        address TEXT,      12 
        birthday TEXT,     13
        signature TEXT,    14   签名
        install_time INT,    15     
        last_login_time      16     
        recent_visit BLOB,   17   最近一段时间访问次数 类型：dict
        source TEXT,         18
        sourceApp            19
        sourceFile           20   
        deleted INT DEFAULT 0,   21 
        repeated INT DEFAULT 0   22
        """
    
    def _get_accout_models(self):
        models = []

        sql = '''
            select account_id, nickname, username, password, photo, telephone, email, gender, age, country, 
                        province, city, address, birthday, signature, install_time, last_login_time, recent_visit, source, sourceApp, sourceFile, deleted, repeated
            from account'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)
       
        while row is not None:
            user = Common.User()
            user.Source.Value = row[18]
            user.SourceApp.Value = row[19]
            if row[20]:
                user.SourceFile.Value = row[20]
            user.ID.Value = row[0]
            if row[1]:
                user.Name.Value = row[1]
            if row[2]:
                user.Username.Value = row[2]
            if row[3]:
                user.Password.Value = row[3]
            if row[5]:
                user.PhoneNumber.Value= row[5]
            if row[4]:
                user.PhotoUris.Add(Uri(row[4]))
            user.Email.Value = row[6]
            if row[7] == 0:
                user.Sex.Value = Common.SexType.Men 
            if row[7] == 1:
                user.Sex.Value = Common.SexType.Women
            if row[8]:
                user.Age.Value = row[8]
            if row[13]:
                user.Birthday.Value = row[13]
            if row[14]:
                user.Signature.Value = row[14]
            if row[15]:
                user.RegisterTime.Value = self._get_timestamp(row[15])
                user.LastLoginTime.Value = self._get_timestamp(row[16])
            if row[17]:
                data = pickle.loads(row[17])
                for k,v in data.items():
                    datecount = DateCount()
                    datecount.DateTime.Value = self._get_timestamp(k)
                    datecount.Count.Value = v
                    user.RecentlyDateCount.Add(datecount)
            address = Contacts.StreetAddress()
            if row[9]:
                address.Country.Value = row[9]
            if row[10]:
                address.Neighborhood.Value = row[10]
            if row[11]:
                address.City.Value = row[11]
            if row[12]:
                address.FullName.Value = row[12]
            user.Addresses.Add(address)
            user.Deleted = DeletedState.Intact if row[21] == 0 else DeletedState.Deleted 
            models.append(user)

            row = self.cursor.fetchone()

        return models


        """
        account_id TEXT,   0
        city_code INT,     1
        city_name TEXT,    2     
        from_name TEXT,    3
        from_posX REAL,    4
        from_posY REAL,    5 
        from_addr TEXT,    6 
        to_name TEXT,      7 
        to_posX REAL,      8 
        to_posY REAL,      9 
        to_addr TEXT,      10 
        create_time INT,   11 
        source TEXT,       12
        sourceApp          13
        SourceFile         14
        deleted INT DEFAULT 0,  15
        repeated INT DEFAULT 0  16 
        """

    def _get_address_models(self):
        models = []

        sql = '''
            select account_id, city_code, city_name, from_name, from_posX, from_posY, from_addr, to_name, to_posX, to_posY, to_addr, create_time, source, sourceApp, sourceFile, deleted,  repeated  from address 
            '''

        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            journey = Journey()
            journey.Source.Value = row[12]
            journey.SourceApp.Value = row[13]
            journey.SourceFile.Value = row[14]
            if row[11]:
                starttime = self._get_timestamp(row[11])
            journey.StartTime.Value = starttime
            frompoint = Location()
            topoint = Location()
            fromcoo = Coordinate()
            tocoo = Coordinate()
            if row[12]:
                fromcoo.Source.Value = row[12]
            if row[4]:
                fromcoo.Longitude.Value = row[4]
            if row[5]:
                fromcoo.Latitude.Value = row[5]
            if row[3]:
                fromcoo.PositionAddress.Value = row[3]
            tocoo.Source.Value = row[12]
            if row[7]:
                tocoo.PositionAddress.Value = row[7]
            if row[8]:
                tocoo.Longitude.Value = row[8]
            if row[9]:
                tocoo.Latitude.Value = row[9]

            frompoint.Position.Value = fromcoo
            topoint.Position.Value = tocoo
            if row[3]:
                frompoint.PositionAddress.Value = row[3]
            if row[7]:
                topoint.PositionAddress.Value = row[7]
            
            frompoint.Map.Value = row[12]
            topoint.Map.Value = row[12]

            journey.FromPoint.Value = frompoint 
            journey.ToPoint.Value = topoint 
            journey.StartTime.Value = starttime

            journey.Deleted = DeletedState.Intact if row[15] == 0 else DeletedState.Deleted

            models.append(journey)

            row = self.cursor.fetchone()

        return models

    
    """
        account_id TEXT    0
        keyword TEXT,      1   搜索关键字
        create_time INT,   2    
        delete_time INT,   3     
        adcode TEXT,       4
        address TEXT,      5    
        district TEXT,     6   地区
        pos_x REAL,        7
        pos_y REAL,        8
        item_type INT,     9
        source TEXT,        10
        sourceApp           11
        sourceFile          12
        deleted INT DEFAULT 0,    13 
        repeated INT DEFAULT 0    14 
    """

    def _get_search_models(self):
        models = []

        sql = '''
            select account_id, keyword, create_time, delete_time, adcode, address, district, pos_x, pos_y, item_type, source, sourceApp, sourceFile, deleted, repeated from search
        '''

        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            searchitem = SearchedItem()
            searchitem.Source.Value = row[10]
            searchitem.SourceApp.Value = row[11]
            searchitem.SourceFile.Value = row[12]
            if row[2]:
                searchitem.TimeStamp.Value = self._get_timestamp(row[2])
            searchitem.Value.Value = row[1]
            searchitem.PositionAddress.Value = row[5]
            coo = Coordinate()
            coo.PositionAddress.Value = row[6]
            if row[7]:
                coo.Longitude.Value = row[7]
            if row[8]:
                coo.Latitude.Value = row[8]
            if row[9] == 0:
                searchitem.ItemType.Value = ItemType.Search
            else:
                searchitem.ItemType.Value = ItemType.Favorites
            searchitem.Position.Value = coo
            searchitem.Deleted = DeletedState.Intact if row[13] == 0 else DeletedState.Deleted
            models.append(searchitem)

            row = self.cursor.fetchone()

        return models
    
    
    def _get_timestamp(self, timestamp):
        if len(str(timestamp)) == 13:
            timestamp = int(str(timestamp)[0:10])
        elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
            timestamp = 0
        ts = TimeStamp.FromUnixTime(timestamp, False)
        if not ts.IsValidForSmartphone():
            ts = None
        return ts
             


