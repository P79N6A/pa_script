# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_browser')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite

import os
import sqlite3
import traceback
import model_browser

def exc():
    # pass
    traceback.print_exc()

DELETE_STATUS_NOT_DELETED = '0'
DELETE_STATUS_DELETED = '1'

# 6.5.1　保存的网站密码信息(WA_MFORENSICS_050100)
SQL_CREATE_TABLE_WA_MFORENSICS_050100 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_050100(
        COLLECT_TARGET_ID    TEXT,
        BROWSE_TYPE  TEXT,
        URL  TEXT,
        ACCOUNT  TEXT,
        PASSWORD     TEXT,
        DELETE_STATUS    TEXT DEFAULT '0',
        DELETE_TIME  INT,
        NETWORK_APP  TEXT,
        ACCOUNT_ID   TEXT,
        FRIEND_ACCOUNT   TEXT,
        LOGIN_COUNT  TEXT
   )'''

SQL_INSERT_TABLE_WA_MFORENSICS_050100 = '''
    INSERT INTO WA_MFORENSICS_050100(
        COLLECT_TARGET_ID,
        BROWSE_TYPE,
        URL,
        ACCOUNT,
        PASSWORD,
        DELETE_STATUS,
        DELETE_TIME,
        NETWORK_APP,
        ACCOUNT_ID,
        FRIEND_ACCOUNT,
        LOGIN_COUNT
    ) values(?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?,
             ?)
    '''

# 6.5.2　收藏夹信息(WA_MFORENSICS_050200)
SQL_CREATE_TABLE_WA_MFORENSICS_050200 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_050200(
        COLLECT_TARGET_ID   TEXT,
        BROWSE_TYPE         TEXT,
        NETWORK_APP         TEXT,
        DELETE_STATUS	 TEXT DEFAULT '0',
        DELETE_TIME         INT,

        NAME                TEXT,
        URL             	TEXT,
        CREATE_TIME         INT,
        ACCOUNT_ID          TEXT,
        ACCOUNT             TEXT,
        LATEST_MOD_TIME     INT,
        VISITS              TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_050200 = '''
    INSERT INTO WA_MFORENSICS_050200(
        COLLECT_TARGET_ID,
        BROWSE_TYPE,
        NAME,
        URL,
        CREATE_TIME,
        DELETE_STATUS,
        DELETE_TIME,
        NETWORK_APP,
        ACCOUNT_ID,
        ACCOUNT,
        LATEST_MOD_TIME,
        VISITS
    ) values(?, ?, ?, ?, ?, 
             ?, ?, ?, ?, ?, 
             ?, ?)
    '''

# 6.5.3　历史记录信息(WA_MFORENSICS_050300)
SQL_CREATE_TABLE_WA_MFORENSICS_050300 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_050300(
        COLLECT_TARGET_ID	 TEXT,
        BROWSE_TYPE	     TEXT,
        NETWORK_APP	     TEXT,
        DELETE_STATUS	 TEXT DEFAULT '0',
        DELETE_TIME	     INT,

        WEB_TITLE	     TEXT,
        URL	 TEXT, 
        VISIT_TIME	     INT,
        VISITS	         TEXT,
        ACCOUNT_ID	     TEXT,
        ACCOUNT	         TEXT,
        DUAL_TIME	     INT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_050300 = '''
    INSERT INTO WA_MFORENSICS_050300(
        COLLECT_TARGET_ID,
        BROWSE_TYPE,
        WEB_TITLE,
        URL,
        VISIT_TIME,
        VISITS,
        DELETE_STATUS,
        DELETE_TIME,
        NETWORK_APP,
        ACCOUNT_ID,
        ACCOUNT,
        DUAL_TIME
    ) values(?, ?, ?, ?, ?, 
             ?, ?, ?, ?, ?, 
             ?, ?)
    '''
# 6.5.4　COOKIES信息(WA_MFORENSICS_050400)
SQL_CREATE_TABLE_WA_MFORENSICS_050400 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_050400(
        COLLECT_TARGET_ID	 TEXT,
        BROWSE_TYPE	 TEXT,
        NETWORK_APP	 TEXT,
        DELETE_STATUS	 TEXT default '0',
        DELETE_TIME	 INT,

        URL	 TEXT,
        KEY_NAME	 TEXT,
        KEY_VALUE	 TEXT,
        CREATE_TIME	 INT,
        EXPIRE_TIME	 INT,
        VISIT_TIME	 INT,
        VISITS	 TEXT,
        ACCOUNT_ID	 TEXT,
        ACCOUNT	 TEXT,
        LATEST_MOD_TIME	 INT,
        NAME	 TEXT   
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_050400 = '''
    INSERT INTO WA_MFORENSICS_050400(
        COLLECT_TARGET_ID,
        BROWSE_TYPE,
        URL,
        KEY_NAME,
        KEY_VALUE,
        CREATE_TIME,
        EXPIRE_TIME,
        VISIT_TIME,
        VISITS,
        DELETE_STATUS,
        DELETE_TIME,
        NETWORK_APP,
        ACCOUNT_ID,
        ACCOUNT,
        LATEST_MOD_TIME,
        NAME
    ) values (?, ?, ?, ?, ?, 
              ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?,   
              ?)
    '''

# 6.5.5　账号信息(WA_MFORENSICS_050500)
SQL_CREATE_TABLE_WA_MFORENSICS_050500 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_050500(
        COLLECT_TARGET_ID TEXT,
        NETWORK_APP TEXT,
        ACCOUNT_ID  TEXT,
        ACCOUNT TEXT,
        REGIS_NICKNAME TEXT	,
        PASSWORD	TEXT,
        INSTALL_TIME	TEXT,
        AREA	TEXT,
        CITY_CODE	TEXT,
        FIXED_PHONE	TEXT,
        MSISDN	TEXT,
        EMAIL_ACCOUNT TEXT	,
        CERTIFICATE_TYPE	TEXT,
        CERTIFICATE_CODE	TEXT,
        SEXCODE	 TEXT,
        AGE	TEXT,
        POSTAL_ADDRESS	TEXT,
        POSTAL_CODE	TEXT,
        OCCUPATION_NAME	TEXT,
        BLOOD_TYPE	TEXT,
        NAME	TEXT,
        SIGN_NAME	TEXT,
        PERSONAL_DESC	TEXT,
        REG_CITY	TEXT,
        GRADUATESCHOOL	TEXT,
        ZODIAC	TEXT,
        CONSTALLATION TEXT,
        BIRTHDAY TEXT,
        HASH_TYPE TEXT,
        USER_PHOTO	TEXT,
        ACCOUNT_REG_DATE TEXT,
        LAST_LOGIN_TIME	INT,
        LATEST_MOD_TIME	INT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT      
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_050500 = '''
    INSERT INTO WA_MFORENSICS_050500(
        COLLECT_TARGET_ID,
        NETWORK_APP,
        ACCOUNT_ID,
        ACCOUNT,
        REGIS_NICKNAME,
        PASSWORD,
        INSTALL_TIME,
        AREA,
        CITY_CODE,
        FIXED_PHONE,
        MSISDN,
        EMAIL_ACCOUNT,
        CERTIFICATE_TYPE,
        CERTIFICATE_CODE,
        SEXCODE,
        AGE,
        POSTAL_ADDRESS,
        POSTAL_CODE,
        OCCUPATION_NAME,
        BLOOD_TYPE,
        NAME,
        SIGN_NAME,
        PERSONAL_DESC,
        REG_CITY,
        GRADUATESCHOOL,
        ZODIAC,
        CONSTALLATION,
        BIRTHDAY,
        HASH_TYPE,
        USER_PHOTO,
        ACCOUNT_REG_DATE,
        LAST_LOGIN_TIME,
        LATEST_MOD_TIME,
        DELETE_STATUS,
        DELETE_TIME
    ) values(
        ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?
    )
    '''

# 6.5.6　搜索记录信息(WA_MFORENSICS_050600)
SQL_CREATE_TABLE_WA_MFORENSICS_050600 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_050600(
        COLLECT_TARGET_ID	 TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME	 INT,
        CONTACT_ACCOUNT_TYPE	 TEXT,
        ACCOUNT_ID	 TEXT,
        ACCOUNT	 TEXT,
        CREATE_TIME	 INT,
        KEYWORD	 TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_050600 = '''
    INSERT INTO WA_MFORENSICS_050600(
        COLLECT_TARGET_ID,
        CONTACT_ACCOUNT_TYPE,
        ACCOUNT_ID,
        ACCOUNT,
        CREATE_TIME,
        KEYWORD,
        DELETE_STATUS,
        DELETE_TIME
    ) values(
        ?, ?, ?, ?, ?,
        ?, ?, ? 
    )
    '''

# 6.5.7　历史表单信息(WA_MFORENSICS_050700)
SQL_CREATE_TABLE_WA_MFORENSICS_050700 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_050700(
        COLLECT_TARGET_ID text,
        NETWORK_APP text,
        ACCOUNT_ID text,
        ACCOUNT text,
        URL text,
        KEY_NAME text,
        KEY_VALUE text,
        VISITS text,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME	 INT
    )
    '''

SQL_INSERT_TABLE_WA_MFORENSICS_050700 = '''
    INSERT INTO WA_MFORENSICS_050700(
        COLLECT_TARGET_ID,
        NETWORK_APP,
        ACCOUNT_ID,
        ACCOUNT,
        URL,
        KEY_NAME,
        KEY_VALUE,
        VISITS,
        DELETE_STATUS,
        DELETE_TIME
    ) 
    values(?, ?, ?, ?, ?,  
           ?, ? ,? ,?, ?)
'''

# SEXCODE_UNKNOWN = 0
# SEXCODE_MALE    = 1
# SEXCODE_FEMALE  = 2
# SEXCODE_OTHER   = 9


DELETE_STATUS_NOT_DELETED = '0'
DELETE_STATUS_DELETED = '1'

'''
WACODE_0070_03：浏览器类型代码表
    代码类型        代码	
    火狐           03100	
    360           03200	
    Chrome        03300	
    Opera         03400	
    IE            03600	
    遨游           03601	
    腾讯TT         03602	
    世界之窗浏览器  03603	
    手机自带       03618	
    UC            03619	
    QQ            03620	
    百度           03621	
    海豚           03622	
    天天           03623	
    其他          03999	
'''
BROWSER_TYPE_FIREFOX   = '03100'
BROWSER_TYPE_360       = '03200'
BROWSER_TYPE_CHROME    = '03200'
BROWSER_TYPE_OPERA     = '03200'
BROWSER_TYPE_IE        = '03200'
BROWSER_TYPE_MAXTHON   = '03200'
BROWSER_TYPE_TENCENTTT = '03602'
BROWSER_TYPE_THEWORLD  = '03603'
BROWSER_TYPE_PHONE     = '03618'
BROWSER_TYPE_UC        = '03619'
BROWSER_TYPE_QQ        = '03620'
BROWSER_TYPE_BAIDU     = '03621'
BROWSER_TYPE_DOLPHIN   = '03622'
BROWSER_TYPE_TIANTIAN  = '03623'
BROWSER_TYPE_OTHER     = '03999'

class IM(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except:
                exc()

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_create_table()
        self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_050100
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_050200
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_050300
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_050400
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_050500
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_050600
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_050700
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()
    '''
        - 050100 password
        050200 bookmark
        050300 browserecords
        050400 cookies
        050500 account
        050600 searchhistory
        - 050700 formrecoreds
    '''
    # def db_insert_table_password(self, column):
    #     self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_050100, column.get_values())

    def db_insert_table_bookmark(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_050200, column.get_values())

    def db_insert_table_browserecords(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_050300, column.get_values())

    def db_insert_table_cookies(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_050400, column.get_values())

    def db_insert_table_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_050500, column.get_values())

    def db_insert_table_searchhistory(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_050600, column.get_values())

    # def db_insert_table_formrecoreds(self, column):
    #     self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_050700, column.get_values())

class Column(object):
    def __init__(self, collect_target_id, browse_type):
        self.collect_target_id = collect_target_id         # 手机取证采集目标编号
        self.browse_type       = browse_type               # 浏览器类型
        # self.network_app       = network_app      # 账号类型

        self.delete_status     = DELETE_STATUS_NOT_DELETED # 删除状态
        self.delete_time       = None                      # 删除时间

    def get_values(self):
        return None

class Bookmark(Column):
    '''6.5.2　收藏夹信息(WA_MFORENSICS_050200)
        collect_target_id   手机取证采集目标编号
        browse_type         浏览器类型
        network_app         账号类型
        delete_status       删除状态
        delete_time         删除时间

        name                名称
        url             	url地址
        create_time         创建时间
        account_id          用户id
        account             账号
        latest_mod_time     最后更新时间
        visits              访问次数
    '''    
    def __init__(self, collect_target_id, contact_account_type):
        super(Bookmark, self).__init__(collect_target_id, contact_account_type)
        self.name            = None            # 名称
        self.url             = None             # url地址
        self.create_time     = None     # 创建时间
        self.account_id      = None      # 用户id
        self.account         = None         # 账号
        self.latest_mod_time = None # 最后更新时间
        self.visits          = None          # 访问次数

    def get_values(self):
        return (
            self.name,            
            self.url,             
            self.create_time,     
            self.account_id,      
            self.account,         
            self.latest_mod_time, 
            self.visits,          
        ) + super(Bookmark, self).get_values()

class Browserecord(Column):
    '''6.5.3　历史记录信息(WA_MFORENSICS_050300)
        collect_target_id	手机取证采集目标编号
        browse_type	    浏览器类型
        network_app	    账号类型
        delete_status	删除状态
        delete_time	    删除时间

        web_title	    网页标题
        url	url地址 
        visit_time	    访问时间
        visits	        访问次数
        account_id	    用户id
        account	        账号
        dual_time	    访问时长
    '''
    def __init__(self, collect_target_id, contact_account_type):
        super(Browserecord, self).__init__(collect_target_id, contact_account_type)
        self.web_title         = None #	网页标题
        self.url               = None #	url地址
        self.visit_time        = None #	访问时间
        self.visits            = None #	访问次数
        self.account_id        = None #	用户id
        self.account           = None #	账号
        self.dual_time         = None #	访问时长

    def get_values(self):
        return (
                self.web_title, 
                self.url,       
                self.visit_time,
                self.visits,    
                self.account_id,
                self.account,   
                self.dual_time,             
            ) + super(Browserecord, self).get_values()        

class Cookies(Column):
    ''' 6.5.4　COOKIES信息(WA_MFORENSICS_050400)
        collect_target_id	手机取证采集目标编号
        browse_type	浏览器类型
        network_app	账号类型
        delete_status	删除状态
        delete_time	删除时间

        url	url地址
        key_name	cookie键名
        key_value	cookie键值
        create_time	创建时间
        expire_time	过期时间
        visit_time	最后访问时间
        visits	访问次数
        account_id	用户id
        account	账号
        latest_mod_time	最后修改时间
        name	名称
    '''
    def __init__(self, collect_target_id, contact_account_type):
        super(Cookies, self).__init__(collect_target_id, contact_account_type)
        self.url             = None # url地址
        self.key_name        = None # cookie键名
        self.key_value       = None # cookie键值
        self.create_time     = None # 创建时间
        self.expire_time     = None # 过期时间
        self.visit_time      = None # 最后访问时间
        self.visits          = None # 访问次数
        self.account_id      = None # 用户id
        self.account         = None # 账号
        self.latest_mod_time = None # 最后修改时间
        self.name            = None # 名称

    def get_values(self):
        return (
            self.url,            
            self.key_name,       
            self.key_value,      
            self.create_time,    
            self.expire_time,    
            self.visit_time,     
            self.visits,         
            self.account_id,     
            self.account,        
            self.latest_mod_time,
            self.name,           
            ) + super(Cookies, self).get_values()

class Account(Column):
    '''6.5.5　账号信息(WA_MFORENSICS_050500)
    collect_target_id 手机取证采集目标编号

    network_app 账号类型

    account_id 用户id 
    account 账号
    regis_nickname	昵称
    password	密码
    install_time	安装日期
    area	国家代号
    city_code	行政区划
    fixed_phone	电话号码
    msisdn	手机号码
    email_account	邮箱地址
    certificate_type	对象证件类型
    certificate_code	对象证件号码
    sexcode	性别
    age	年龄
    postal_address	联系地址
    postal_code	邮政编码
    occupation_name	职业名称
    blood_type	血型
    name	真实名
    sign_name	个性签名
    personal_desc	个人说明
    reg_city	城市
    graduateschool	毕业院校
    zodiac	生肖
    constallation 星座
    birthday 出生年月
    hash_type 密码算法类型
    user_photo	头像
    account_reg_date 账号注册时间
    last_login_time	账号最后登录时间
    latest_mod_time	账号最后更新时间

    delete_status 删除状态	
    delete_time 删除时间	
    '''
    def __init__(self, collect_target_id, contact_account_type):
        super(Account, self).__init__(collect_target_id, contact_account_type)
        self.account_id       = None  # 用户ID
        self.account          = None  # 账号
        self.regis_nickname   = None  # 昵称
        self.password         = None  # 密码
        self.install_time     = None  # 安装日期
        self.area             = None  # 国家代号
        self.city_code        = None  # 行政区划
        self.fixed_phone      = None  # 电话号码
        self.msisdn           = None  # 手机号码
        self.email_account    = None  # 邮箱地址
        self.certificate_type = None  # 对象证件类型
        self.certificate_code = None  # 对象证件号码
        self.sexcode          = None  # 性别
        self.age              = None  # 年龄
        self.postal_address   = None  # 联系地址
        self.postal_code      = None  # 邮政编码
        self.occupation_name  = None  # 职业名称
        self.blood_type       = None  # 血型
        self.name             = None  # 真实名
        self.sign_name        = None  # 个性签名
        self.personal_desc    = None  # 个人说明
        self.reg_city         = None  # 城市
        self.graduateschool   = None  # 毕业院校
        self.zodiac           = None  # 生肖
        self.constallation    = None  # 星座
        self.birthday         = None  # 出生年月
        self.hash_type        = None  # 密码算法类型
        self.user_photo       = None  # 头像
        self.account_reg_date = None  # 账号注册时间
        self.last_login_time  = None  # 账号最后登录时间
        self.latest_mod_time  = None  # 账号最后更新时间

    def get_values(self):
        return (
        self.account_id,
        self.account,
        self.regis_nickname,
        self.password,
        self.install_time,
        self.area,
        self.city_code,
        self.fixed_phone,
        self.msisdn,
        self.email_account,
        self.certificate_type,
        self.certificate_code,
        self.sexcode,
        self.age,
        self.postal_address,
        self.postal_code,
        self.occupation_name,
        self.blood_type,
        self.name,
        self.sign_name,
        self.personal_desc,
        self.reg_city,
        self.graduateschool,
        self.zodiac,
        self.constallation,
        self.birthday,
        self.hash_type,
        self.user_photo,
        self.account_reg_date,
        self.last_login_time,
        self.latest_mod_time,
        ) + super(Account, self).get_values()


class SearchHistory(Column):
    ''' 6.5.6　搜索记录信息(WA_MFORENSICS_050600)
        collect_target_id	手机取证采集目标编号
        delete_status	删除状态
        delete_time	删除时间

        contact_account_type	账号类型
        account_id	用户id
        account	账号
        create_time	时间
        keyword	关键词
    '''
    def __init__(self, collect_target_id, contact_account_type):
        super(SearchHistory, self).__init__(collect_target_id, contact_account_type)
        self.contact_account_type = None # 账号类型
        self.account_id           = None # 用户id
        self.account              = None # 账号
        self.create_time          = None # 时间
        self.keyword              = None # 关键词

    def get_values(self):
        return (
            self.contact_account_type,
            self.account_id,          
            self.account,             
            self.create_time,         
            self.keyword,             
            ) + super(SearchHistory, self).get_values()


class GenerateBcp(object):
    def __init__(self, 
                 bcp_path, 
                 cache_db, 
                 bcp_db, 
                 collect_target_id, 
                 browser_type,      
            ):

        self.collect_target_id = collect_target_id # 手机取证采集目标编号
        self.browse_type       = browser_type      # 浏览器类型

        self.cache_db = cache_db # 中间数据库
        self.bcp_path = bcp_path
        self.bcp_db   = bcp_db
        self.cache_path = os.path.join(self.bcp_path, 'browser')
        self.im = IM()

    def generate(self):
        self.im.db_create(self.bcp_db)
        self.db = sqlite3.connect(self.cache_db)
        self._generate_bookmark()
        self._generate_browserecords()
        self._generate_cookies()
        self._generate_account()
        self._generate_searchhistory()

        self.db.close()
        self.im.db_close()

    def _generate_bookmark(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = ''' select * from bookmark'''
        '''
        0    id INTEGER,
        1    time INTEGER,
        2    title TEXT,
        3    url TEXT,
        4    owneruser TEXT,
        5    source TEXT,
        6    deleted INTEGER,
        7    repeated INTEGER     
        '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except:
            exc()

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            bm = Bookmark(self.collect_target_id, self.browse_type)
            bm.name          = row[2]
            bm.url           = row[3]
            bm.create_time   = row[3]
            bm.account       = row[4]
            bm.delete_status = self._convert_delete_status(row[6])
            self.im.db_insert_table_bookmark(bm)
            row = curscor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_browserecords(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = ''' select * from browse_records'''
        '''
        0    id INTEGER,
        1    name TEXT,
        2    url TEXT,
        3    datetime INTEGER,
        4    owneruser TEXT,
        5    source TEXT,
        6    deleted INTEGER,
        7    repeated INTEGER   
        '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except:
            exc()

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            br = Browserecord(self.collect_target_id, self.browse_type)
            br.web_title     = row[1]
            br.url           = row[2]
            br.visit_time    = row[3]
             # br.visits       = row[3]
            br.account       = row[4]
            br.delete_status = self._convert_delete_status(row[6])
            self.im.db_insert_table_browserecords(br)
            row = curscor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_cookies(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = ''' select * from cookies'''
        '''
        0    id INTEGER PRIMARY KEY AUTOINCREMENT,
        1    host_key TEXT,
        2    name TEXT,
        3    value TEXT,
        4    createdate INTEGER,
        5    expiredate INTEGER,
        6    lastaccessdate INTEGER,
        7    hasexipred INTEGER,
        8    owneruser TEXT,
        9    source TEXT,
        10   deleted INTEGER,
        11   repeated INTEGER    
        '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except:
            exc()

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            cookies = Cookies(self.collect_target_id, self.browse_type)
            cookies.url             = row[1]
            cookies.key_name        = row[2]
            cookies.key_value       = row[3]
            cookies.create_time     = row[4]
            cookies.expire_time     = row[5]
            # cookies.VISIT_TIME     = row[5]
            # cookies.VISITS         = row[5]
            # cookies.ACCOUNT_ID     = row[5]
            cookies.account         = row[8]
            cookies.latest_mod_time = row[6]
            cookies.name            = row[2]
            cookies.delete_status   = self._convert_delete_status(row[10])
            self.im.db_insert_table_cookies(cookies)
            row = curscor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_account(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = ''' select * from account'''
        '''
        0    id INTEGER PRIMARY KEY AUTOINCREMENT,
        1    name TEXT,
        2    logindate INTEGER,
        3    source TEXT,
        4    deleted INTEGER,
        5    repeated INTEGER        
        '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except:
            exc()

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            account = Account(self.collect_target_id, self.browse_type)
            account.account         = row[1]
            account.regis_nickname  = row[1]
            account.last_login_time = row[2]
            account.delete_status   = self._convert_delete_status(row[4])
            self.im.db_insert_table_account(account)
            row = curscor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_searchhistory(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = ''' select * from search_history'''
        '''
        0    id INTEGER,
        1    name TEXT,
        2    url TEXT,
        3    datetime INTEGER,
        4    owneruser TEXT,
        5    source TEXT,
        6    deleted INTEGER,
        7    repeated INTEGER
        '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except:
            exc()

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            sh = SearchHistory(self.collect_target_id, self.browse_type)
            sh.account       = row[4]
            sh.create_time   = row[3]
            sh.keyword       = row[1]
            sh.delete_status = self._convert_delete_status(row[6])
            self.im.db_insert_table_searchhistory(sh)
            row = curscor.fetchone()
        self.im.db_commit()
        cursor.close()

    @staticmethod
    def _convert_delete_status(status):
        if status == 0:
            return DELETE_STATUS_NOT_DELETED
        else:
            return DELETE_STATUS_DELETED

