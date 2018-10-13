#coding=utf-8
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

import pickle
from System.Xml.Linq import *
import System.Data.SQLite as SQLite

import os
import sqlite3

# 账号信息
SQL_CREATE_TABLE_WA_MFORENSICS_060100 = '''
    create table if not exists WA_MFORENSICS_060100(
        COLLECT_TARGET_ID TEXT,  
        BROWSE_TYPE TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        REGIS_NICKNAME TEXT,
        PASSWORD TEXT,
        INSTALL_TIME INT,
        AREA TEXT,
        CITY_CODE TEXT,
        FIXED_PHONE TEXT,
        MSISDN TEXT,
        EMAIL_ACCOUNT TEXT,
        CERTIFICATE_TYPE TEXT,
        CERTIFICATE_CODE TEXT,
        SEXCODE TEXT,
        AGE INT,
        POSTAL_ADDRESS TEXT,
        POSTAL_CODE TEXT,
        OCCUPATION_NAME TEXT,
        BLOOD_TYPE TEXT,
        NAME TEXT,
        SIGN_NAME TEXT,
        PERSONAL_DESC TEXT,
        REG_CITY TEXT,
        GRADUATESCHOOL TEXT,
        ZODIAC TEXT,
        CONSTALLATION TEXT,
        BIRTHDAY INT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT,
        NETWORK_APP TEXT,
        HASH_TYPE TEXT,
        USER_PHOTO TEXT,
        ACCOUNT_REG_DATE INT,
        LAST_LOGIN_TIME INT,
        LATEST_MOD_TIME INT)
    '''

SQL_INSERT_TABLE_WA_MFORENSICS_060100 = '''
    insert into WA_MFORENSICS_060100(COLLECT_TARGET_ID,BROWSE_TYPE,ACCOUNT_ID,ACCOUNT,REGIS_NICKNAME,PASSWORD,INSTALL_TIME,AREA,
    CITY_CODE,FIXED_PHONE,MSISDN,EMAIL_ACCOUNT,CERTIFICATE_TYPE,CERTIFICATE_CODE,SEXCODE,AGE,POSTAL_ADDRESS,POSTAL_CODE,
    OCCUPATION_NAME,BLOOD_TYPE,NAME,SIGN_NAME,PERSONAL_DESC,REG_CITY,GRADUATESCHOOL,ZODIAC,CONSTALLATION,BIRTHDAY,DELETE_STATUS,DELETE_TIME,
    NETWORK_APP,HASH_TYPE,USER_PHOTO,ACCOUNT_REG_DATE,LAST_LOGIN_TIME,LATEST_MOD_TIME)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

# 地图定位信息:包括但不限于GPS、基站等本地定位和搜索、导航、订酒店等非本地定位
SQL_CREATE_TABLE_WA_MFORENSICS_060200 = '''
    create table if not exists WA_MFORENSICS_060200(
        COLLECT_TARGET_ID TEXT,
        BROWSE_TYPE TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        LOCATE_TYPE TEXT DEFAULT "1",
        LOGIN_TIME INT,
        CITY_CODE TEXT,
        COMPANY_ADDRESS TEXT,
        LONGITUDE TEXT,
        LATITUDE TEXT,
        ABOVE_SEALEVEL TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT,
        NETWORK_APP TEXT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_060200 = '''
    insert into WA_MFORENSICS_060200(COLLECT_TARGET_ID,BROWSE_TYPE,ACCOUNT_ID,ACCOUNT,LOCATE_TYPE,LOGIN_TIME,CITY_CODE,
    COMPANY_ADDRESS,LONGITUDE,LATITUDE,ABOVE_SEALEVEL,DELETE_STATUS,DELETE_TIME,NETWORK_APP)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

# 行程信息:包括但不限于：导航记录、航班记录、乘车记录、打车记录、旅行记录等
SQL_CREATE_TABLE_WA_MFORENSICS_060300 = '''
    create table if not exists WA_MFORENSICS_060300(
        COLLECT_TARGET_ID TEXT,
        BROWSE_TYPE TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        START_TIME INT,
        DEPART TEXT,
        DEPART_ADDRESS TEXT,
        START_LONGITUDE TEXT,
        START_LATITUDE TEXT,
        START_ABOVE_SEALEVEL TEXT,
        END_TIME INT,
        DESTINATION TEXT,
        DESTINATION_ADDRESS TEXT,
        DESTINATION_LONGITUDE TEXT,
        DESTINATION_LATITUDE TEXT,
        DESTINATION_ABOVE_SEALEVEL TEXT,
        FLIGHTID TEXT,
        PURCHASE_PRICE INT,
        AIRCOM TEXT,
        ORDER_NUM TEXT,
        TICKET_STATUS TEXT,
        ORDER_TIME INT,
        LATEST_MOD_TIME INT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME TEXT,
        NETWORK_APP TEXT,
        NAME TEXT,
        SEQUENCE_NAME TEXT,
        DELIVER_TOOL TEXT,
        MATERIALS_NAME TEXT,
        REMARK TEXT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_060300 = '''
    insert into WA_MFORENSICS_060300(COLLECT_TARGET_ID,BROWSE_TYPE,ACCOUNT_ID,ACCOUNT,START_TIME,DEPART,DEPART_ADDRESS,START_LONGITUDE,START_LATITUDE,START_ABOVE_SEALEVEL,END_TIME,
    DESTINATION,DESTINATION_ADDRESS,DESTINATION_LONGITUDE,DESTINATION_LATITUDE,DESTINATION_ABOVE_SEALEVEL,FLIGHTID,PURCHASE_PRICE,AIRCOM,ORDER_NUM,TICKET_STATUS,ORDER_TIME,
    LATEST_MOD_TIME,DELETE_STATUS,DELETE_TIME,NETWORK_APP,NAME,SEQUENCE_NAME,DELIVER_TOOL,MATERIALS_NAME,REMARK)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''


# 搜索记录信息
SQL_CREATE_TABLE_WA_MFORENSICS_060400 = '''
    create table if not exists WA_MFORENSICS_060400(
        COLLECT_TARGET_ID  TEXT,
        BROWSE_TYPE  TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        CREATE_TIME INT,
        KEYWORD TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT,
        NETWORK_APP TEXT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_060400 = '''
    insert into WA_MFORENSICS_060400(COLLECT_TARGET_ID,BROWSE_TYPE,ACCOUNT_ID,ACCOUNT,CREATE_TIME,KEYWORD,DELETE_STATUS,DELETE_TIME,NETWORK_APP)
    values(?,?,?,?,?,?,?,?,?)
'''

# 地址信息：导航地址、收藏地址、收货地址等。
SQL_CREATE_TABLE_WA_MFORENSICS_060500 = '''
    create table if not exists WA_MFORENSICS_060500(
        COLLECT_TARGET_ID TEXT,
        NETWORK_APP TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        CITY_CODE TEXT,
        COMPANY_ADDRESS TEXT,
        LONGITUDE TEXT,
        LATITUDE TEXT,
        ABOVE_SEALEVEL TEXT,
        IDENTIFICATION_TYPE TEXT DEFAULT "0059999",
        NAME TEXT,
        RELATIONSHIP_ACCOUNT TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_060500 = '''
    insert into WA_MFORENSICS_060500(COLLECT_TARGET_ID,NETWORK_APP,ACCOUNT_ID,ACCOUNT,CITY_CODE,COMPANY_ADDRESS,LONGITUDE,LATITUDE,ABOVE_SEALEVEL,
    IDENTIFICATION_TYPE,NAME,RELATIONSHIP_ACCOUNT,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

# 人员信息：驾驶员、乘客、寄件人、收件人、配送人等
SQL_CREATE_TABLE_WA_MFORENSICS_060600 = '''
    create table if not exists WA_MFORENSICS_060600(
        COLLECT_TARGET_ID TEXT,
        NETWORK_APP TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        SEQUENCE_NAME TEXT,
        NAME TEXT,
        CERTIFICATE_TYPE TEXT,
        CERTIFICATE_CODE TEXT,
        MSISDN TEXT,
        PEOPLE_TYPE TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_060600 = '''
    insert into WA_MFORENSICS_060600(COLLECT_TARGET_ID,NETWORK_APP,ACCOUNT_ID,ACCOUNT,SEQUENCE_NAME,NAME,CERTIFICATE_TYPE,CERTIFICATE_CODE,
    MSISDN,PEOPLE_TYPE,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?,?,?)
'''

# 交通工具信息
SQL_CREATE_TABLE_WA_MFORENSICS_060700 = '''
    create table if not exists WA_MFORENSICS_060700(
        COLLECT_TARGET_ID TEXT,
        NETWORK_APP TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        SEQUENCE_NAME TEXT,
        DELIVER_TOOL TEXT,
        VEHICLE_CODE TEXT,
        VEHICLE_BRAND TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_060700 = '''
    insert into WA_MFORENSICS_060700(COLLECT_TARGET_ID,NETWORK_APP,ACCOUNT_ID,ACCOUNT,SEQUENCE_NAME,DELIVER_TOOL,VEHICLE_CODE,VEHICLE_BRAND,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?)
'''

# 住店信息
SQL_CREATE_TABLE_WA_MFORENSICS_060800 = '''
    create table if not exists WA_MFORENSICS_060800(
        COLLECT_TARGET_ID TEXT,
        NETWORK_APP TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        NAME TEXT,
        CERTIFICATE_TYPE TEXT,
        CERTIFICATE_CODE TEXT,
        MSISDN TEXT,
        START_TIME INT,
        END_TIME INT,
        PLACE_NAME TEXT,
        ROOMCOUNT INT,
        ROOM_ID TEXT,
        CITY_CODE TEXT,
        COMPANY_ADDRESS TEXT,
        LONGITUDE TEXT,
        LATITUDE TEXT,
        ABOVE_SEALEVEL TEXT,
        ORDER_STATUS TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_060800 = '''
    insert into WA_MFORENSICS_060800(COLLECT_TARGET_ID,NETWORK_APP,ACCOUNT_ID,ACCOUNT,NAME,CERTIFICATE_TYPE,CERTIFICATE_CODE,MSISDN,START_TIME,END_TIME,PLACE_NAME,ROOMCOUNT,
    ROOM_ID,CITY_CODE,COMPANY_ADDRESS,LONGITUDE,LATITUDE,ABOVE_SEALEVEL,ORDER_STATUS,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''



  
# 地图类工具代码 BROWSE_TYPE
MAP_TYPE_GOOGLE = "04001"
MAP_TYPE_BAIDU  = "04002"
MAP_TYPE_GAODE  = "04003"
MAP_TYPE_OTHER  = "04999"

# 旅行工具代码表
TOUR_TYPE_TRAVELSKY = "05001"  # 航旅纵横
TOUR_TYPE_DIDICHUXING = "05002" # 滴滴打车
TOUR_TYPE_TAXIFAST = "05003" # 快的打车
TOUR_TYPE_CTRIP = "05004" # 携程旅游
TOUR_TYPE_QUNAR = "05005" # 去哪儿旅游
TOUR_TYPE_OTHER = "05999" # 其他

# 地图应用代码表 NETWORK_APP
NETWORK_APP_MAP_GOOGLE  = "1440001"
NETWORK_APP_MAP_TENCENT = "1440002"
NETWORK_APP_MAP_GAODE   = "1440003"
NETWORK_APP_MAP_BAIDU   = "1440004"
NETWORK_APP_MAP_SOGOU   = "1440005"
NETWORK_APP_MAP_OTHER   = "1449999"
NETWORK_APP_MAP_TIGER   = "1440009"  # 老虎地图

# 导航信息代码表
NETWORK_APP_NAVIGATION_CARELAND = "1510008"  # 凯立德导航
NETWORK_APP_NAVIGATION_OTHER    = "1519999"  # 其他

# 票务预定类信息代码表
NETWORK_APP_TICKET_CTRIP = "1260004" # 携程
NETWORK_APP_TICKET_CSAIR = "1260005" # 南方航空
NETWORK_APP_TICKET_ELONG = "1260006" # 艺龙
NETWORK_APP_TICKET_QUNAR = "1260007" # 去哪儿
NETWORK_APP_TICKET_12306 = "1260008" # 12306
NETWORK_APP_TICKET_TRAVELSKY  = "1260010"  # 航旅纵横
NETWORK_APP_TICKET_VARIFLIGHT = "1260011"  # 非常准
NETWORK_APP_TICKET_LY = "1260014"  # 同城
NETWORK_APP_TICKET_JD = "1260015"  # 京东
NETWORK_APP_TICKET_OTHER = "1269999" # 其他

# 快递类信息代码表
NETWORK_APP_EXPRESS_SF = "1240003"  # 顺丰快递
NETWORK_APP_EXPRESS_KUAIDI100 = "1240010" # 快递100
NETWORK_APP_EXPRESS_CAINIAO = "1240021" # 菜鸟裹裹
NETWORK_APP_EXPRESS_OTHER = "1249999" # 其他

# 旅业预定信息代码表
NETWORK_APP_TOUR_TUNIU = "1230009"  # 途牛
NETWORK_APP_TOUR_OTHER = "1239999"  # 其他

# 网络订购类信息代码表
NETWORK_APP_ONLINE_ORDER_DIDICHUXING = "1520001" # 滴滴出行 
NETWORK_APP_ONLINE_ORDER_TAXIFAST = "1520002" # 快的打车
NETWORK_APP_ONLINE_ORDER_SZZUANCHE = "1520004"  # 神州专车
NETWORK_APP_ONLINE_ORDER_DIDACHUXING = "1520008" # 滴答出行/滴答拼车
NETWORK_APP_ONLINE_ORDER_SZZUCHE = "1520017" # 神州租车
NETWORK_APP_ONLINE_ORDER_UBER = "1520020" # Uber优步
NETWORK_APP_ONLINE_ORDER_MOBIKE = "1520301" # 摩拜单车
NETWORK_APP_ONLINE_ORDER_OFO = "1520303" # ofo
NETWORK_APP_ONLINE_ORDER_BLUEGOGO = "1520302" # 小蓝单车
NETWORK_APP_ONLINE_ORDER_OTHER = "1529999" # 其他

# 票务使用状态
TICKET_STATUS_UNKNOWN   = "0"     # 未知
TICKET_STATUS_USED      = "1"     # 已使用
TICKET_STATUS_UNUSE     = "2"     # 未使用
TICKET_STATUS_REFUNDED  = "3"     # 已退票
TICKET_STATUS_OTHER     = "9"     # 其他

# 性别
SEXCODE_UNKNOWN = "0"  # 未知
SEXCODE_MALE    = "1"  # 男性
SEXCODE_FEMALE  = "2"  # 女性
SEXCODE_OTHER   = "9"  # 未说明的性别

# 血型
BLOOD_TYPE_UNKNOWN = "0"  # 不明
BLOOD_TYPE_A = "1"  # A型
BLOOD_TYPE_B = "2"  # B型
BLOOD_TYPE_O = "3"  # O型
BLOOD_TYPE_AB = "4"  # AB型
BLOOD_TYPE_OTHER = "9"  # 其他

# 加密算法
HASH_TYPE_MD5  = "001"  # 标准的MD5算法
HASH_TYPE_SHA1  = "002"  # 标准的SHA1算法
HASH_TYPE_MD5_1  = "003"  # 快速MD5-1算法
HASH_TYPE_MD5_2  = "004"  # 快速MD5-2算法
HASH_TYPE_360  = "005"  # 360公司私有的文件特征值算法
HASH_TYPE_XUNLEI  = "006"  # 迅雷公司私有的文件特征值算法
HASH_TYPE_BAIDU  = "007"  # 百度公司私有的文件特征值算法
HASH_TYPE_JX  = "008"  # 金灏公司私有的文件特征值算法
HASH_TYPE_KUWO  = "009"  # 酷我公司私有的文件特征值算法
HASH_TYPE_SHA_256  = "010"  # SHA-256
HASH_TYPE_OTHER  = "999"  # 其他算法

# 定位方式
LOCATE_TYPE_UNKNOWN           = "0"  # 未知
LOCATE_TYPE_LOCAL_ORIENTATION = "1"  # 本地
LOCATE_TYPE_OTHER             = "9"  # 其他

# 删除状态
DELETE_STATUS_UNDELETED = "0"
DELETE_STATUS_DELETED = "1"

# 地址类型
IDENTIFICATION_TYPE_HOME = "0050001" # 家庭地址
IDENTIFICATION_TYPE_COMPANY = "0050002" # 单位地址
IDENTIFICATION_TYPE_OTHER = "0059999" # 其他

# 行程人员类型
PEOPLE_TYPE_DRIVER = "01" # 驾驶员
PEOPLE_TYPE_PASSENGER = "02" # 乘客
PEOPLE_TYPE_SENDER = "03" # 寄件人
PEOPLE_TYPE_RECIPIENTS = "04" # 收件人
PEOPLE_TYPE_DELIVERY = "05" # 配送人
PEOPLE_TYPE_OTHER = "99" # 其他

# 交通工具类别
DELIVER_TOOL_OTHER ="01"	# 其他
DELIVER_TOOL_BICYCLE ="02"	# 单车
DELIVER_TOOL_MOTORBIKE ="03"	# 摩托车
DELIVER_TOOL_CAR ="04"	# 汽车
DELIVER_TOOL_TRAIN ="05"	# 火车
DELIVER_TOOL_AIRPLANE ="06"	# 飞机
DELIVER_TOOL_SHIP ="07"	# 轮船

# 订单状态
ORDER_STATUS_UNKNOWN = "0"    # 未知
ORDER_STATUS_NOTSTARTED = "1" # 交易未开始
ORDER_STATUS_DEALING = "2"    # 交易中
ORDER_STATUS_FINISH = "3"     # 交易完成
ORDER_STATUS_CLOSE = "4"      # 交易关闭
ORDER_STATUS_OTHER = "9"      # 其他



class GIS(object):

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
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060100
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060200
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060300
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060400
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060500
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060600
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060700
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_060800
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
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060100, column.get_values())

    def db_insert_table_location(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060200, column.get_values())

    def db_insert_table_journey(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060300, column.get_values())

    def db_insert_table_search(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060400, column.get_values())

    def db_insert_table_address(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060500, column.get_values())

    def db_insert_table_person(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060600, column.get_values())

    def db_insert_table_vehicle(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060700, column.get_values())
    
    def db_insert_table_hotel(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_060800, column.get_values())


class Column(object):
    
    def __init__(self, collect_target_id, network_app, account_id, account):
        self.collect_target_id = collect_target_id  # 电子数据取证目标编号
        self.network_app = network_app # 账号类型
        self.account_id = account_id # 用户id
        self.account = account # 账户
        self.delete_status = DELETE_STATUS_UNDELETED # 删除状态
        self.delete_time = None # 删除时间

    def get_values(self):
        return None

# 账号
class Account(Column):

    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Account, self).__init__(collect_target_id, network_app, account_id, account)
        self.browse_type = None # 软件名称
        self.regis_nickname = None  # 昵称
        self.password = None  # 密码
        self.install_time = None  # 安装日期
        self.area = None  # 国家代号
        self.city_code = None  # 行政区划
        self.fixed_phone = None  # 电话号码
        self.msisdn = None  # 手机号码
        self.email_account = None  # 邮箱地址
        self.certificate_type = None  # 对象证件类型
        self.certificate_code = None  # 对象证件号码
        self.sexcode = SEXCODE_UNKNOWN  # 性别
        self.age = None  # 年龄
        self.postal_address = None  # 联系地址
        self.postal_code = None  # 邮政编码
        self.occupation_name = None  # 职业名称
        self.blood_type = BLOOD_TYPE_UNKNOWN  # 血型
        self.name = None  # 真实名
        self.sign_name = None  # 个性签名
        self.personal_desc = None  # 个人说明
        self.reg_city = None  # 城市
        self.graduateschool = None  # 毕业院校
        self.zodiac = None  # 生肖
        self.constallation = None  # 星座
        self.birthday = None  # 出生年月
        self.hash_type = None # 密码算法类型
        self.user_photo = None # 头像
        self.account_reg_date = None # 账号注册时间
        self.last_login_time = None # 账号登录时间
        self.latest_mod_time =  None # 账号更新时间

    def get_values(self):
        return (self.collect_target_id, self.browse_type, self.account_id, self.account,
                self.regis_nickname, self.password, self.install_time, self.area, self.city_code,
                self.fixed_phone, self.msisdn, self.email_account, self.certificate_type,
                self.certificate_code, self.sexcode, self.age, self.postal_address, self.postal_code,
                self.occupation_name, self.blood_type, self.name, self.sign_name, self.personal_desc,
                self.reg_city, self.graduateschool, self.zodiac, self.constallation, self.birthday,
                self.delete_status, self.delete_time,self.network_app,self.hash_type,self.user_photo,
                self.account_reg_date,self.last_login_time,self.latest_mod_time)

# 地图定位
class Location(Column):

    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Location, self).__init__(collect_target_id, network_app, account_id, account)
        self.locate_type = LOCATE_TYPE_UNKNOWN # 地点类别
        self.login_time = None # 时间
        self.city_code = None # 地点名称
        self.company_address = None # 详细地址
        self.longitude = None # 经度
        self.latitude = None # 纬度
        self.above_sealevel = None # 海拔
        self.browse_type = None # 软件名称

    def get_values(self):
        return (self.collect_target_id, self.browse_type, self.account_id, self.account,
                self.locate_type,self.login_time,self.city_code,self.company_address,
                self.longitude,self.latitude,self.above_sealevel,self.delete_status,self.delete_time,self.network_app)

# 行程信息
class Journey(Column):

    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Journey, self).__init__(collect_target_id, network_app, account_id, account)
        self.browse_type = None # 软件名称
        self.start_time = None # 出发时间
        self.depart = None # 出点地名称
        self.depart_address = None # 出发地详细地址
        self.start_longitude = None # 出发点经度
        self.start_latitude = None # 出发地纬度
        self.start_above_sealevel = None # 出发地海拔
        self.end_time = None # 到达时间
        self.destination = None # 目的地名称
        self.destination_address = None # 目的地详细地址
        self.destination_longitude = None # 目的地经度
        self.destination_latitude = None # 目的地纬度
        self.destination_above_sealevel = None # 目的地海拔
        self.flightid = None # 航班号或车次号
        self.purchase_price = None # 价格
        self.aircom = None # 运输公司 
        self.order_num = None # 票号
        self.ticket_status = None # 票务使用状态
        self.order_time = None # 创建时间
        self.latest_mod_time = None # 更改时间
        self.name = None # 行程名称
        self.sequence_name = None # 行程id
        self.deliver_tool = None # 交通工具
        self.materials_name = None # 物品名称
        self.remark = None # 备注


    def get_values(self):
        return (self.collect_target_id, self.browse_type, self.account_id, self.account,
            self.start_time,self.depart,self.depart_address,self.start_longitude,self.start_latitude,self.start_above_sealevel,
            self.end_time,self.destination,self.destination_address,self.destination_longitude,self.destination_latitude,
            self.destination_above_sealevel,self.flightid,self.purchase_price,self.aircom,self.order_num,self.ticket_status,
            self.order_time,self.latest_mod_time,self.delete_status, self.delete_time,self.network_app,self.name,self.sequence_name,
            self.deliver_tool,self.materials_name,self.remark)

# 搜索信息
class Search(Column):

    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Search, self).__init__(collect_target_id, network_app, account_id, account)
        self.create_time = None # 搜索时间
        self.keyword = None # 搜索关键字
        self.browse_type = None

    def get_values(self):
        return (self.collect_target_id, self.browse_type, self.account_id, self.account,
        self.create_time,self.keyword,self.delete_status,self.delete_time,self.network_app)

# 地址信息
class Address(Column):

    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Address, self).__init__(collect_target_id, network_app, account_id, account)
        self.city_code = None # 地点名称
        self.company_address = None # 详细地址
        self.longitude = None # 经度 
        self.latitude = None # 纬度
        self.above_sealevel = None # 海拔
        self.identification_type = IDENTIFICATION_TYPE_OTHER # 地址类型
        self.name = None # 姓名
        self.relationship_account = None # 联系方式

    def get_values(self):
        return (self.collect_target_id, self.network_app, self.account_id, self.account,
        self.city_code,self.company_address,self.longitude,self.latitude,self.above_sealevel,
        self.identification_type,self.name,self.relationship_account,self.delete_status,self.delete_time)


class Person(Column):

    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Person, self).__init__(collect_target_id, network_app, account_id, account)
        self.sequence_name = None # 行程id
        self.name = None # 姓名
        self.certificate_type = None # 证件类型
        self.certificate_code = None # 证件号码
        self.msisdn = None # 手机号码
        self.people_type = None # 行程人员类型

    def get_values(self):
        return (self.collect_target_id, self.network_app, self.account_id, self.account,
        self.sequence_name,self.name,self.certificate_type,self.certificate_code,self.msisdn,self.people_type,
        self.delete_status,self.delete_time)

    
class Vehicle(Column):
    
    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Vehicle, self).__init__(collect_target_id, network_app, account_id, account)
        self.sequence_name = None # 行程id
        self.deliver_tool = None # 交通工具类别
        self.vehicle_code = None # 牌照号
        self.vehicle_brand = None # 品牌型号

    def get_values(self):
        return (self.collect_target_id, self.network_app, self.account_id, self.account,
        self.sequence_name,self.deliver_tool,self.vehicle_code,self.vehicle_brand,
        self.delete_status,self.delete_time)

class Hotel(Column):

    def __init__(self, collect_target_id, network_app, account_id, account):
        super(Hotel, self).__init__(collect_target_id, network_app, account_id, account)
        self.name = None # 姓名
        self.certificate_type = None # 证件类型
        self.certificate_code = None # 证件号码
        self.msisdn = None # 手机号码
        self.start_time = None # 开始时间
        self.end_time = None # 结束时间
        self.place_name = None # 酒店名称
        self.roomcount = None # 房间数
        self.room_id = None # 房间号
        self.city_code = None # 地点名称
        self.company_address = None # 详细地址
        self.longitude = None # 经度
        self.latitude = None # 纬度
        self.above_sealevel = None # 海拔
        self.order_status = None # 订单状态

    def get_values(self):
        return (self.collect_target_id, self.network_app, self.account_id, self.account,
        self.name,self.certificate_type,self.certificate_code,self.msisdn,self.start_time,self.end_time,
        self.place_name,self.roomcount,self.room_id,self.city_code,self.company_address,self.longitude,
        self.latitude,self.above_sealevel,self.order_status,self.delete_status,self.delete_time)



class BuildBCP(object):

    def __init__(self, cache_db, bcp_db, collect_target_id, network_app):
        self.cache_db = cache_db
        self.bcp_db = bcp_db
        self.collect_target_id = collect_target_id
        self.network_app = network_app
        self.gis = GIS()
        self.db = None

    def genetate(self):
        self.gis.db_create(self.bcp_db)
        self.db = sqlite3.connect(self.cache_db)
        self._get_account()
        self._get_address()
        self._get_journey()
        self._get_location()
        self._get_search()

        self.db.close()
        self.gis.db_close()


    def _get_account(self):
        cursor = self.db.cursor()
        SQL = """
            select account_id, nickname, username, password, photo, telephone, email, gender, age, country, 
                        province, city, address, birthday, signature, install_time, last_login_time, recent_visit, source, sourceApp, sourceFile, deleted, repeated
            from account
        """
        try:
            cursor.execute(SQL)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if canceller.IsCancellationRequested:
                return
            account = Account(self.collect_target_id, self.network_app, row[0], row[2])
            if row[1]:
                account.regis_nickname = row[1]
            if row[3]:
                account.password = row[3]
            if row[4]:
                account.user_photo = row[4]
            if row[5]:
                account.msisdn = row[5]
            if row[6]:
                account.email_account = row[6]
            account.sexcode = self._convert_sex_type(row[7])
            if row[8]:
                account.age = row[8]
            if row[11]:
                account.reg_city = row[11]
            if row[12]:
                account.postal_address = row[12]
            if row[13]:
                account.birthday = row[13]
            if row[14]:
                account.personal_desc = row[14]
            account.browse_type = self._convert_browse_type(row[18])
            account.delete_status = self._convert_deleted_type(row[21])

            self.gis.db_insert_table_account(account)
            row = cursor.fetchone()
        self.gis.db_commit()
        cursor.close()

    def _get_location(self):
        cursor = self.db.cursor()
        SQL_SEARCH = """
            select account_id, keyword, create_time, delete_time, adcode, address, district, pos_x, pos_y, 
            item_type, source, sourceApp, sourceFile, deleted, repeated from search where item_type = 0
        """
        try:
            cursor.execute(SQL_SEARCH)
            search_row = cursor.fetchone()
        except Exception as e:
            print(e)
        while search_row is not None:
            if canceller.IsCancellationRequested:
                return
            search_location = Location(self.collect_target_id, self.network_app, search_row[0], None)
            search_location.locate_type = LOCATE_TYPE_OTHER
            if search_row[2]:
                search_location.login_time = search_row[2]
            if search_row[3]:
                search_location.delete_time =search_row[3]
            if search_row[5]:
                search_location.company_address = search_row[5]
            else:
                search_location.company_address = search_row[1]
            if search_row[7]:
                search_location.longitude = search_row[7]
            if search_row[8]:
                search_location.latitude = self._convert_coordinate_type(search_row[8])
            if search_row[10]:
                search_location.browse_type = self._convert_browse_type(search_row[10])
            if search_row[13]:
                search_location.delete_status = self._convert_deleted_type(search_row[13])
            
            self.gis.db_insert_table_location(search_location)
            search_row = cursor.fetchone()
        self.gis.db_commit()
        cursor.close()

        cursor2 = self.db.cursor()
        SQL_ROUTE = '''
            select account_id, city_code, city_name, from_name, from_posX, from_posY, from_addr, to_name, 
            to_posX, to_posY, to_addr, create_time, source, sourceApp, sourceFile, deleted,  repeated  from address 
            '''
        try:
            cursor2.execute(SQL_ROUTE)
            route_row = cursor2.fetchone()
        except Exception as e:
            print(e)
        while route_row is not None:
            if canceller.IsCancellationRequested:
                return
            route_location = Location(self.collect_target_id, self.network_app, route_row[0], None)
            route_location.locate_type = LOCATE_TYPE_OTHER
            if route_row[8]:
                route_location.longitude = self._convert_coordinate_type(route_row[8])
            if route_row[9]:
                route_location.latitude = self._convert_coordinate_type(route_row[9])
            if route_row[10]:
                route_location.company_address = route_row[10]
            else:
                route_location.company_address = route_row[7]
            if route_row[11]:
                route_location.login_time = route_row[11]
            if route_row[12]:
                route_location.browse_type = self._convert_browse_type(route_row[12])
            if route_row[13]:
                route_location.delete_status = self._convert_deleted_type(route_row[13])

            self.gis.db_insert_table_location(route_location)
            route_row = cursor2.fetchone()
        self.gis.db_commit()
        cursor2.close()

                    
    def _get_journey(self):
        cursor = self.db.cursor()
        SQL = """
            select account_id, city_code, city_name, from_name, from_posX, from_posY, from_addr, to_name, 
            to_posX, to_posY, to_addr, create_time, source, sourceApp, sourceFile, deleted,  repeated  from address 
            """
        try:
            cursor.execute(SQL)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        while row is not None:
            if canceller.IsCancellationRequested:
                return
            journey = Journey(self.collect_target_id, self.network_app, row[0], None)
            if row[6]:
                journey.depart_address = row[6]
            else:
                journey.depart_address = row[3]
            if row[4]:
                journey.start_longitude = self._convert_coordinate_type(row[4])
            if row[5]:
                journey.start_latitude = self._convert_coordinate_type(row[5])
            if row[10]:
                journey.destination_address = row[10]
            else:
                journey.destination_address = row[7]
            if row[8]:
                journey.destination_longitude = self._convert_coordinate_type(row[8])
            if row[9]:
                journey.destination_latitude = self._convert_coordinate_type(row[9])
            if row[11]:
                journey.start_time = row[11]
            if row[12]:
                journey.browse_type = self._convert_browse_type(row[12])
            if row[15]:
                journey.delete_status = self._convert_deleted_type(row[15])
            
            self.gis.db_insert_table_journey(journey)
            row = cursor.fetchone()
        self.gis.db_commit()
        cursor.close()

    
    def _get_search(self):
        cursor = self.db.cursor()
        sql = '''
            select account_id, keyword, create_time, delete_time, adcode, address, district, pos_x, pos_y, item_type, source, sourceApp, sourceFile, deleted, repeated from search
        '''
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)
        while row is not None:
            if canceller.IsCancellationRequested:
                return
            search = Search(self.collect_target_id, self.network_app, row[0], None)
            if row[1]:
                search.keyword = row[1]
            if row[2]:
                search.create_time = row[2]
            if row[3]:
                search.delete_time = row[3]
            if row[10]:
                search.browse_type = self._convert_browse_type(row[10])
            if row[13]:
                search.delete_status = self._convert_deleted_type(row[13])
            
            self.gis.db_insert_table_search(search)
            row = cursor.fetchone()
        self.gis.db_commit()
        cursor.close()

    
    def _get_address(self):
        cursor = self.db.cursor()
        ROUTE_SQL = """
            select account_id, city_code, city_name, from_name, from_posX, from_posY, from_addr, to_name, 
            to_posX, to_posY, to_addr, create_time, source, sourceApp, sourceFile, deleted,  repeated  from address 
            """
        try:
            cursor.execute(ROUTE_SQL)
            route_row = cursor.fetchone()
        except Exception as e:
            print(e)
        while route_row is not None:
            if canceller.IsCancellationRequested:
                return
            route_address = Address(self.collect_target_id, self.network_app, route_row[0], None)
            if route_row[10]:
                route_address.company_address = route_row[10]
            else:
                route_address.company_address = route_row[7]
            if route_row[8]:
                route_address.longitude = route_row[8]
            if route_row[9]:
                route_address.latitude = route_row[9]
            if route_row[15]:
                route_address.delete_status = self._convert_deleted_type(route_row[15])
            
            self.gis.db_insert_table_address(route_address)
            route_row = cursor.fetchone()
        self.gis.db_commit()
        cursor.close()

        cursor2 = self.db.cursor()
        SEARCH_SQL = '''
            select account_id, keyword, create_time, delete_time, adcode, address, district, pos_x, pos_y, item_type, 
            source, sourceApp, sourceFile, deleted, repeated from search where item_type = 1
        '''
        try:
            cursor2.execute(SEARCH_SQL)
            search_row = cursor2.fetchone()
        except Exception as e:
            print(e)

        while search_row is not None:
            if canceller.IsCancellationRequested:
                return
            search_address = Address(self.collect_target_id, self.network_app, search_row[0], None)
            if search_row[5]:
                search_address.company_address = search_row[5]
            else:
                search_address.company_address = search_row[1]
            if search_row[7]:
                search_address.longitude = self._convert_coordinate_type(search_row[7])
            if search_row[8]:
                search_address.latitude = self._convert_coordinate_type(search_row[8])
            if search_row[13]:
                search_address.delete_status = self._convert_deleted_type(search_row[13])

            self.gis.db_insert_table_address(search_address)
            search_row = cursor2.fetchone()
        self.gis.db_commit()
        cursor2.close()


    def _convert_sex_type(self, value):
        if value == "0":
            return SEXCODE_MALE
        elif value == "1":
            return SEXCODE_FEMALE
        elif value is None:
            return SEXCODE_UNKNOWN
        else:
            return SEXCODE_OTHER

    def _convert_browse_type(self, value):
        if value == "谷歌地图":
            return MAP_TYPE_GOOGLE
        elif value == "百度地图":
            return MAP_TYPE_BAIDU
        elif value == "高德地图":
            return MAP_TYPE_GAODE
        else:
            return MAP_TYPE_OTHER
        
    def _convert_deleted_type(self, value):
        if value == 0:
            return DELETE_STATUS_UNDELETED
        elif value == 1:
            return DELETE_STATUS_DELETED
        else:
            return DELETE_STATUS_UNDELETED

    def _convert_coordinate_type(self, value):
        try:
            return str(value)
        except Exception as e:
            pass   
