#coding=utf-8
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('coordTransform_utils')
except:
    pass
del clr

import pickle
from System.Xml.Linq import *
import System.Data.SQLite as SQLite

import os
import sqlite3
import hashlib
import coordTransform_utils

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

SQL_CREATE_TABLE_JOURNEY = '''
    create table if not exists journey(
        account_id TEXT,
        start_time INT,
        depart TEXT,
        depart_address TEXT,
        start_longitude REAL,
        start_latitude REAL,
        start_above_sealevel REAL,
        end_time INT,
        destination TEXT,
        destination_address TEXT,
        destination_longitude REAL,
        destination_latitude REAL,
        destination_above_sealevel REAL,
        flightid TEXT,
        purchase_price TEXT,
        aircom TEXT,
        order_num TEXT,
        ticket_status TEXT,
        order_time INT,
        deleted_time INT,
        latest_mod_time INT,
        name TEXT,
        sequence_name TEXT,
        deliver_tool TEXT,
        materials_name TEXT,
        remark TEXT,
        source TEXT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0,
        repeated INT DEFAULT 0
        )'''


SQL_INSERT_TABLE_JOURNEY = '''
    insert into journey(account_id,start_time,depart,depart_address,start_longitude,
    start_latitude,start_above_sealevel,end_time,destination,destination_address,
    destination_longitude,destination_latitude,destination_above_sealevel,
    flightid,purchase_price,aircom,order_num,ticket_status,order_time,deleted_time,
    latest_mod_time,name,sequence_name,deliver_tool,materials_name,remark,
    source,sourceApp,sourceFile,deleted,repeated)
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''

SQL_CREATE_TABLE_PASSENGER = '''
    create table if not exists passenger(
        account_id TEXT,
        account TEXT,
        sequence_id TEXT,
        name TEXT,
        certificate_type TEXT,
        certificate_code TEXT,
        phone TEXT,
        people_type TEXT,
        source TEXT,
        sourceApp TEXT,
        sourceFile TEXT,
        deleted INT DEFAULT 0,
        repeated INT DEFAULT 0)
'''

SQL_INSERT_TABLE_PASSENGER = '''
    insert into passenger(account_id,account,sequence_id,name,certificate_type,certificate_code,phone,people_type,
        source,sourceApp,sourceFile,deleted,repeated)
        values(?,?,?,?,?,?,?,?,?,?,?,?,?)
'''


class Map(object):
    
    def __init__(self):
        self.db = None
        self.db_command = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                print("{0} remove failed!".format(db_path))
        
        self.db = SQLite.SQLiteConnection("Data Source = {0}".format(db_path))
        self.db.Open()
        self.db_command = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_create_table()
        self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_command is not None:
            self.db_command.Dispose()
            self.db_command = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            try:
                self.db_trans.Commit()
            except Exception as e:
                self.db_trans.RollBack()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_command is not None:
            self.db_command.CommandText = SQL_CREATE_TABLE_ACCOUNT
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_ADDRESS
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_SEARCH
            self.db_command.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        if self.db_command is not None:
            self.db_command.CommandText = sql
            self.db_command.Parameters.Clear()
            for value in values:
                param = self.db_command.CreateParameter()
                param.Value = value
                self.db_command.Parameters.Add(param)
            self.db_command.ExecuteNonQuery()

    def db_insert_table_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_ACCOUNT, column.get_values())

    def db_insert_table_address(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_ADDRESS, column.get_values())

    def db_insert_table_search(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_SEARCH, column.get_values())



class Column(object):

    def __init__(self):
        self.source = ""
        self.sourceApp = ""
        self.sourceFile = ""
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            if isinstance(value, str):
                # 过滤控制字符, 防止断言失败
                value = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', value)               
            self.__dict__[name] = value


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
        self.item_type = 0       # 搜索类别[0.搜索 2.收藏]

    def get_values(self):
        return (self.account_id, self.keyword, self.create_time, self.delete_time, self.adcode, self.address, self.district, self.pos_x, self.pos_y, self.item_type) + super(Search, self).get_values()

# 行程信息
class LocationJourney(Column):
    def __init__(self):
        super(LocationJourney, self).__init__()
        self.account_id = None
        self.start_time = None
        self.depart = None
        self.depart_address = None
        self.start_longitude = None
        self.start_latitude = None
        self.start_above_sealevel = None
        self.end_time = None
        self.destination = None
        self.destination_address = None
        self.destination_longitude = None
        self.destination_latitude = None
        self.destination_above_sealevel = None
        self.flightid = None
        self.purchase_price = None
        self.aircom = None
        self.order_num = None
        self.ticket_status = None
        self.order_time = None
        self.deleted_time = None
        self.latest_mod_time = None
        self.name = None
        self.sequence_name = None
        self.deliver_tool = None
        self.materials_name = None
        self.remark = None

    def get_values(self):
        return (self.account_id,self.start_time,self.depart,self.depart_address,self.start_longitude,self.start_latitude,
        self.start_above_sealevel,self.end_time,self.destination,self.destination_address,self.destination_longitude,self.destination_latitude,
        self.destination_above_sealevel,self.flightid,self.purchase_price,self.aircom,self.order_num,self.ticket_status,self.order_time,self.deleted_time,
        self.latest_mod_time,self.name,self.sequence_name,self.deliver_tool,self.materials_name,self.remark) + super(LocationJourney, self).get_values()

# 乘客信息
class Passenger(Column):
    def __init__(self):
        super(Passenger, self).__init__()
        self.account_id = None
        self.account = None
        self.sequence_id = None
        self.name = None
        self.certificate_type = None
        self.certificate_code = None
        self.phone = None
        self.people_type = None

    def get_values(self):
        return (self.account_id,self.account,self.sequence_id,self.name,self.certificate_type,self.certificate_code,self.phone,self.people_type) + super(Passenger, self).get_values()


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
        models.extend(self._get_trip_models())
        models.extend(self._get_passenger())

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
            pass
       
        while row is not None:
            if canceller.IsCancellationRequested:
                return
            user = Common.User()
            user.Source.Value = row[18]
            user.SourceApp.Value = row[19]
            if row[20]:
                user.SourceFile.Value =self._get_source_file(row[20])
            user.ID.Value = row[0]
            if row[0]:
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
            pass

        while row is not None:
            if canceller.IsCancellationRequested:
                return
            journey = Journey()
            journey.Source.Value = row[12]
            journey.SourceApp.Value = row[13]
            journey.SourceFile.Value = self._get_source_file(row[14])
            if row[11]:
                starttime = self._get_timestamp(row[11])
                journey.StartTime.Value = starttime
            frompoint = Location()
            topoint = Location()
            fromcoo = Coordinate()
            tocoo = Coordinate()
            if row[12]:
                if row[12] == "百度地图":
                    fromcoo.Type.Value = CoordinateType.Baidumc
                    tocoo.Type.Value = CoordinateType.Baidumc
                elif row[12] == "高德地图":
                    fromcoo.Type.Value = CoordinateType.Google
                    tocoo.Type.Value = CoordinateType.Google
                elif row[12] == "搜狗地图" or row[12] == "腾讯地图":
                    fromcoo.Type.Value = CoordinateType.Google
                    tocoo.Type.Value = CoordinateType.Google
            if row[12]:
                fromcoo.Source.Value = row[12]
            if row[4] and row[12] == "高德地图":
                fromcoo.Longitude.Value = coordTransform_utils.pixelXTolng(row[4])
            elif row[4] and row[12] == "腾讯地图":
                fromcoo.Longitude.Value = self._convert_coordinate(row[4],0)
            elif row[4]:
                fromcoo.Longitude.Value = row[4]
            if row[5] and row[12] == "高德地图":
                fromcoo.Latitude.Value = coordTransform_utils.pixelYToLat(row[5])
            elif row[5] and row[12] == "腾讯地图":
                fromcoo.Latitude.Value = self._convert_coordinate(row[5],1)
            elif row[5]:
                fromcoo.Latitude.Value = row[5]
            if row[3]:
                fromcoo.PositionAddress.Value = row[3]
            tocoo.Source.Value = row[12]
            if row[7]:
                tocoo.PositionAddress.Value = row[7]
            if row[8] and row[12] == "高德地图":
                tocoo.Longitude.Value = coordTransform_utils.pixelXTolng(row[8])
            elif row[8] and row[12] == "腾讯地图":
                tocoo.Longitude.Value = self._convert_coordinate(row[8],0)
            elif row[8]:
                tocoo.Longitude.Value = row[8]
            if row[9] and row[12] == "高德地图":
                tocoo.Latitude.Value = coordTransform_utils.pixelYToLat(row[9])
            elif row[9] and row[12] == "腾讯地图":
                tocoo.Latitude.Value = self._convert_coordinate(row[9],1)
            elif row[9]:
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
            pass

        while row is not None:
            if canceller.IsCancellationRequested:
                return
            searchitem = SearchedItem()
            searchitem.Source.Value = row[10]
            searchitem.SourceApp.Value = row[11]
            searchitem.SourceFile.Value = self._get_source_file(row[12])
            if row[2]:
                searchitem.TimeStamp.Value = self._get_timestamp(row[2])
            searchitem.Value.Value = row[1]
            if row[5]:
                searchitem.PositionAddress.Value = row[5]
            coo = Coordinate()
            if row[6]:
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
    
    """
        account_id TEXT,                        0
        start_time INT,                         1
        depart TEXT,                            2
        depart_address TEXT,                    3
        start_longitude REAL,                   4
        start_latitude REAL,                    5
        start_above_sealevel REAL,              6
        end_time INT,                           7
        destination TEXT,                       8
        destination_address TEXT,               9
        destination_longitude REAL,             10
        destination_latitude REAL,              11
        destination_above_sealevel REAL,        12
        flightid TEXT,                          13
        purchase_price TEXT,                    14
        aircom TEXT,                            15
        order_num TEXT,                         16
        ticket_status TEXT,                     17
        order_time INT,                         18
        deleted_time INT,                       19
        latest_mod_time INT,                    20
        name TEXT,                              21
        sequence_name TEXT,                     22
        deliver_tool TEXT,                      23
        materials_name TEXT,                    24
        remark TEXT,                            25
        source TEXT,                            26
        sourceApp TEXT,                         27
        sourceFile TEXT,                        28
        deleted INT DEFAULT 0,                  29
        repeated INT DEFAULT 0                  30
    """
    def _get_trip_models(self):
        model = []

        sql = """
            select account_id,start_time,depart,depart_address,start_longitude,
                start_latitude,start_above_sealevel,end_time,destination,destination_address,
                destination_longitude,destination_latitude,destination_above_sealevel,
                flightid,purchase_price,aircom,order_num,ticket_status,order_time,deleted_time,
                latest_mod_time,name,sequence_name,deliver_tool,materials_name,remark,
                source,sourceApp,sourceFile,deleted,repeated from journey
        """
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            if canceller.IsCancellationRequested:
                return
            trip = Trip()
            if row[0]:
                trip.OwnerUserID.Value = row[0]
            journey = Journey()
            frompoint = Location()
            topoint = Location()
            fromcoord = Coordinate()
            tocoord = Coordinate()
            if row[1]:
                journey.StartTime.Value = self._get_timestamp(row[1])
            if row[2]:
                frompoint.Name.Value = row[2]
            if row[3]:
                fromcoord.PositionAddress.Value = row[3]
            if row[4]:
                fromcoord.Longitude.Value = row[4]
            if row[5]:
                fromcoord.Latitude.Value = row[5]
            if row[6]:
                fromcoord.Elevation.Value = row[6]
            if row[7]:
                journey.EndTime.Value = self._get_timestamp(row[7])
            if row[8]:
                frompoint.Name.Value = row[8]
            if row[9]:
                tocoord.PositionAddress.Value = row[9]
            if row[10]:
                tocoord.Longitude.Value = row[10]
            if row[11]:
                tocoord.Latitude.Value = row[11]
            if row[12]:
                tocoord.Elevation.Value = row[12]
            if row[13]:
                trip.Flightid.Value = row[13]
            if row[14]:
                trip.PurchasePrice.Value = row[14]
            if row[17]:
                pass
            if row[18]:
                trip.OrderTime.Value = self._get_timestamp(row[18])
            if row[19]:
                trip.DeleteTime.Value = self._get_timestamp(row[19])
            if row[20]:
                trip.LastModTime.Value = self._get_timestamp(row[20])
            if row[21]:
                trip.Name.Value = row[21]
            if row[24]:
                trip.MaterialsName.Value = row[24]
            if row[26]:
                trip.Source.Value = row[26]
                journey.Source.Value = row[26]
                frompoint.Source.Value = row[26]
                topoint.Source.Value = row[26]
                fromcoord.Source.Value = row[26]
                tocoord.Source.Value = row[26]
            if row[27]:
                trip.SourceApp.Value = row[27]
                journey.SourceApp.Value = row[27]
                frompoint.SourceApp.Value = row[27]
                topoint.SourceApp.Value = row[27]
                fromcoord.SourceApp.Value = row[27]
                tocoord.SourceApp.Value = row[27]
            if row[28]:
                trip.SourceFile.Value = row[28]
                journey.SourceFile.Value = row[28]
                frompoint.SourceFile.Value = row[28]
                topoint.SourceFile.Value = row[28]
                fromcoord.SourceFile.Value = row[28]
                tocoord.SourceFile.Value = row[28]
            journey.Deleted = DeletedState.Intact if row[29] == 0 else DeletedState.Deleted
            frompoint.Deleted = DeletedState.Intact if row[29] == 0 else DeletedState.Deleted
            topoint.Deleted = DeletedState.Intact if row[29] == 0 else DeletedState.Deleted
            fromcoord.Deleted = DeletedState.Intact if row[29] == 0 else DeletedState.Deleted
            tocoord.Deleted = DeletedState.Intact if row[29] == 0 else DeletedState.Deleted
            
            frompoint.Position.Value = fromcoord
            topoint.Position.Value = tocoord            
            journey.FromPoint.Value = frompoint 
            journey.ToPoint.Value = topoint
            trip.Journey.Value = journey

            model.append(trip)
            row = self.cursor.fetchone()
        return model


    def _get_passenger(self):
        # if "passenger" not in  self.table_list:
        #     return []

        model = []
        sql = '''
            select account_id,account,sequence_id,name,certificate_type,certificate_code,phone,people_type,
        source,sourceApp,sourceFile,deleted,repeated from passenger
        '''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            if canceller.IsCancellationRequested:
                return
            passenger = Common.PassengerInfo()
            passenger.Source.Value = row[8]
            passenger.SourceApp.Value = row[9]
            passenger.SourceFile.Value = self._get_source_file(row[10])

            if row[0]:
                passenger.OwnerUserID.Value = row[0]
            if row[2]:
                passenger.SequenceID.Value = row[2]
            if row[3]:
                passenger.FullName.Value = row[3]
            if row[4]:
                pass
                # passenger.CertificateType.Value = row[4]
            if row[5]:
                passenger.CertificateCode.Value = row[5]
            if row[6]:
                passenger.PhoneNumber.Value = row[6]
            if row[7]:
                pass
            passenger.Deleted = DeletedState.Intact if row[11] == 0 else DeletedState.Deleted
            model.append(passenger)

            row = self.cursor.fetchone()
        return model

    def _get_timestamp(self, timestamp):
        if len(str(timestamp)) == 13:
            timestamp = int(str(timestamp)[0:10])
        elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
            timestamp = 0
        elif len(str(timestamp)) == 10:
            timestamp = timestamp
        ts = TimeStamp.FromUnixTime(timestamp, False)
        if not ts.IsValidForSmartphone():
            ts = None
        return ts
             
    def _get_source_file(self, source_file):
        if source_file:
            return source_file.replace('/', '\\')
        
    
    def _convert_coordinate(self, value, ctype):
        try:
            if value and ctype == 0:
                # 经度
                return float(str(value)[:3]+"."+str(value)[3:])
            elif value and ctype == 1:
                # 维度
                return float(str(value)[:2]+"."+str(value)[2:])
        except Exception as e:
            pass

def md5(cache_path, node_path):
    m = hashlib.md5()   
    m.update(node_path.encode(encoding = 'utf-8'))
    db_path = cache_path + "\\" + m.hexdigest() + ".db"
    return db_path

