# -*- coding: utf-8 -*-
__author__ = "TaoJianping"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
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
import model_im

# 微博信息

# 4.4.1　微博账号信息(WA_FORENSICS_010401)
SQL_CREATE_TABLE_WA_FORENSICS_010401 = '''
    create table if not exists WA_FORENSICS_010401(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        REGIS_NICKNAME text,
        PASSWORD text,
        FANS_COUNTER int,
        FOLLOW_COUNTER int,
        MESSAGE_COUNTER int,
        PREVACY_MESSAGE_COUNTER int,
        INSTALL_TIME text,
        AREA text,
        CITY_CODE text,
        FIXED_PHONE text,
        MSISDN text,
        EMAIL_ACCOUNT text,
        CERTIFICATE_TYPE text,
        CERTIFICATE_CODE text,
        SEXCODE text,
        AGE int,
        POSTAL_ADDRESS text,
        POSTAL_CODE text,
        OCCUPATION_NAME text,
        BLOOD_TYPE text,
        NAME text,
        SIGN_NAME text,
        PERSONAL_DESC text,
        REG_CITY text,
        GRADUATESCHOOL text,
        ZODIAC text,
        CONSTALLATION text,
        BIRTHDAY text,
        DELETE_STATUS text,
        DELETE_TIME int,
        HASH_TYPE text,
        USER_PHOTO text,
        ACCOUNT_REG_DATE int,
        LAST_LOGIN_TIME int,
        LATEST_MOD_TIME int)'''
SQL_INSERT_TABLE_WA_FORENSICS_010401 = '''
    insert into WA_FORENSICS_010401(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            REGIS_NICKNAME, PASSWORD, FANS_COUNTER, FOLLOW_COUNTER, MESSAGE_COUNTER, PREVACY_MESSAGE_COUNTER, INSTALL_TIME, 
            AREA, CITY_CODE, FIXED_PHONE, MSISDN, EMAIL_ACCOUNT, CERTIFICATE_TYPE, 
            CERTIFICATE_CODE, SEXCODE, AGE, POSTAL_ADDRESS, POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, 
            NAME, SIGN_NAME, PERSONAL_DESC, REG_CITY, GRADUATESCHOOL, ZODIAC, CONSTALLATION, 
            BIRTHDAY, DELETE_STATUS, DELETE_TIME, HASH_TYPE, USER_PHOTO, ACCOUNT_REG_DATE, LAST_LOGIN_TIME, LATEST_MOD_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ? ,? ,?)'''

# 4.4.2　微博好友信息(WA_FORENSICS_010402)
SQL_CREATE_TABLE_WA_FORENSICS_010402 = '''
    create table if not exists WA_FORENSICS_010402(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        FRIEND_NICKNAME text,
        FRIEND_GROUP text,
        FRIEND_REMARK text,
        URL text,
        WEIBO_FOLLOW_TYPE text,
        AREA text,
        CITY_CODE text,
        FIXED_PHONE text,
        MSISDN text,
        EMAIL_ACCOUNT text,
        CERTIFICATE_TYPE text,
        CERTIFICATE_CODE text,
        SEXCODE text,
        AGE int,
        POSTAL_ADDRESS text,
        POSTAL_CODE text,
        OCCUPATION_NAME text,
        BLOOD_TYPE text,
        NAME text,
        SIGN_NAME text,
        PERSONAL_DESC text,
        REG_CITY text,
        GRADUATESCHOOL text,
        ZODIAC text,
        CONSTALLATION text,
        BIRTHDAY text,
        DELETE_STATUS text,
        DELETE_TIME int,
        USER_PHOTO text,
        ACCOUNT_REG_DATE int,
        LAST_LOGIN_TIME int,
        LATEST_MOD_TIME int,
        FANS_COUNTER int, 
        FOLLOW_COUNTER int, 
        MESSAGE_COUNTER int)'''
SQL_INSERT_TABLE_WA_FORENSICS_010402 = '''
    insert into WA_FORENSICS_010402(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, FRIEND_GROUP, FRIEND_REMARK, URL, WEIBO_FOLLOW_TYPE, 
            AREA, CITY_CODE, FIXED_PHONE, MSISDN, EMAIL_ACCOUNT, CERTIFICATE_TYPE, 
            CERTIFICATE_CODE, SEXCODE, AGE, POSTAL_ADDRESS, POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, 
            NAME, SIGN_NAME, PERSONAL_DESC, REG_CITY, GRADUATESCHOOL, ZODIAC, CONSTALLATION, 
            BIRTHDAY, DELETE_STATUS, DELETE_TIME, USER_PHOTO, ACCOUNT_REG_DATE, 
            LAST_LOGIN_TIME, LATEST_MOD_TIME, FANS_COUNTER,  
            FOLLOW_COUNTER,  MESSAGE_COUNTER) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ?, ?, ? ,? ,? ,?)'''

# 4.4.3　博文信息(WA_FORENSICS_010403)
SQL_CREATE_TABLE_WA_FORENSICS_010403 = '''
    create table if not exists WA_FORENSICS_010403(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        FRIEND_NICKNAME text,
        WEIBO_MESSAGE_TYPE text,
        MAIL_SEND_TIME int,
        WEIBO_TOPIC text,
        WEIBO_MESSAGE text,
        WEIBO_REPLY_COUNTER int,
        WEIBO_RETWEET_COUNTER int,
        WEIBO_LIKE_COUNTER int,
        MBLOG_ID text,
        RELEVANT_MBLOG_ID text,
        IDROOT_MBLOG_ID text,
        DELETE_STATUS text,
        DELETE_TIME int,
        MAINFILE text,
        MEDIA_TYPE text,
        CITY_CODE text,
        COMPANY_ADDRESS text,
        LONGITUDE text,
        LATITUDE text,
        ABOVE_SEALEVEL text)'''
SQL_INSERT_TABLE_WA_FORENSICS_010403 = '''
    insert into WA_FORENSICS_010403(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, WEIBO_MESSAGE_TYPE, MAIL_SEND_TIME, WEIBO_TOPIC, WEIBO_MESSAGE, 
            WEIBO_REPLY_COUNTER, WEIBO_RETWEET_COUNTER, WEIBO_LIKE_COUNTER, MBLOG_ID, RELEVANT_MBLOG_ID, IDROOT_MBLOG_ID, 
            DELETE_STATUS, DELETE_TIME, MAINFILE, MEDIA_TYPE, CITY_CODE, COMPANY_ADDRESS, 
            LONGITUDE, LATITUDE, ABOVE_SEALEVEL) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 4.4.4　私信信息(WA_FORENSICS_010404)
SQL_CREATE_TABLE_WA_FORENSICS_010404 = '''
    create table if not exists WA_FORENSICS_010404(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        REGIS_NICKNAME text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        FRIEND_NICKNAME text,
        CONTENT text,
        MAIL_SEND_TIME int,
        LOCAL_ACTION text,
        TALK_ID text,
        DELETE_STATUS text,
        DELETE_TIME int,
        MAINFILE text,
        MEDIA_TYPE text,
        CITY_CODE text,
        COMPANY_ADDRESS text,
        LONGITUDE text,
        LATITUDE text,
        ABOVE_SEALEVEL text)'''
SQL_INSERT_TABLE_WA_FORENSICS_010404 = '''
    insert into WA_FORENSICS_010404(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            REGIS_NICKNAME, FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, CONTENT, MAIL_SEND_TIME, LOCAL_ACTION, 
            TALK_ID, DELETE_STATUS, DELETE_TIME, MAINFILE, MEDIA_TYPE, CITY_CODE,
            COMPANY_ADDRESS, LONGITUDE, LATITUDE, ABOVE_SEALEVEL) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 4.4.5　搜索记录信息(WA_FORENSICS_010405)
SQL_CREATE_TABLE_WA_FORENSICS_010405 = '''
    create table if not exists WA_FORENSICS_010405(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        CREATE_TIME int,
        KEYWORD text, 
        DELETE_STATUS text,
        DELETE_TIME int)'''
SQL_INSERT_TABLE_WA_FORENSICS_010405 = '''
    insert into WA_FORENSICS_010405(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            CREATE_TIME, KEYWORD, DELETE_STATUS, DELETE_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?)'''


# 性别
SEXCODE_UNKNOWN = 0
SEXCODE_MALE = 1
SEXCODE_FEMALE = 2
SEXCODE_OTHER = 9
# 本地动作
LOCAL_ACTION_RECEIVER = '01'
LOCAL_ACTION_SENDER = '02'
LOCAL_ACTION_OTHER = '99'
# 删除状态
DELETE_STATUS_NOT_DELETED = '0'
DELETE_STATUS_DELETED = '1'
# 媒体类型
MEDIA_TYPE_TEXT = '01'
MEDIA_TYPE_IMAGE = '02'
MEDIA_TYPE_VOICE = '03'
MEDIA_TYPE_VIDEO = '04'
MEDIA_TYPE_OTHER = '99'
# 微博类型
WEIBO_MESSAGE_TYPE_ORIGINAL = "0"
WEIBO_MESSAGE_TYPE_RETWEET = "1"
WEIBO_MESSAGE_TYPE_COMMENT_OR_LIKE = "2"
WEIBO_MESSAGE_TYPE_OTHER = "9"
# 朋友类型
FRIEND_TYPE_FRIEND = 1
FRIEND_TYPE_GROUP_FRIEND = 2
FRIEND_TYPE_FANS = 3
FRIEND_TYPE_FOLLOW = 4
FRIEND_TYPE_SPECAIL_FOLLOW = 5
FRIEND_TYPE_MUTUAL_FOLLOW = 6
FRIEND_TYPE_RECENT = 7
FRIEND_TYPE_SUBSCRIBE = 8
FRIEND_TYPE_STRANGER = 9


class Weibo(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                print('model_im db_create() remove %s error: %s' % (db_path, e))

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
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_FORENSICS_010401
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_FORENSICS_010402
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_FORENSICS_010403
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_FORENSICS_010404
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_FORENSICS_010405
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

    def db_insert_table_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_FORENSICS_010401, column.get_values())

    def db_insert_table_friends(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_FORENSICS_010402, column.get_values())

    def db_insert_table_feed(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_FORENSICS_010403, column.get_values())

    def db_insert_table_message(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_FORENSICS_010404, column.get_values())

    def db_insert_table_search(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_FORENSICS_010405, column.get_values())


class Column(object):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        self.collect_target_id = collect_target_id  # 手机取证采集目标编号
        self.contact_account_type = contact_account_type  # 账号类型
        self.account_id = account_id  # 用户ID
        self.account = account  # 账号
        self.delete_status = DELETE_STATUS_NOT_DELETED  # 删除状态
        self.delete_time = None  # 删除时间

    def get_values(self):
        return None


class Account(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Account, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.regis_nickname = None  # 昵称
        self.password = None  # 密码
        self.fans_counter = None  # 关注本机用户的用户数量
        self.follow_counter = None  # 本机用户关注的用户数量
        self.message_counter = None  # 本机用户发表的博文数量
        self.prevace_message_counter = None  # 本机用户收发的私信数量
        self.install_time = None  # 安装时间
        self.area = None 
        self.city_code = None  
        self.fixed_phone = None  # 固定电话
        self.msisdn = None  # 手机
        self.email_account = None 
        self.certificate_type = None 
        self.certificate_code = None 
        self.sex_code = None 
        self.age = None  
        self.postal_address = None 
        self.postal_code = None 
        self.occupation_name = None 
        self.blood_type = None 
        self.name = None 
        self.sign_name = None 
        self.personal_desc = None 
        self.reg_city = None  
        self.graduate_school = None  
        self.zodiac = None 
        self.constallation = None  
        self.birthday = None  
        self.hash_type = None
        self.user_photo = None
        self.account_reg_date = None
        self.last_login_time = None
        self.latest_mod_time = None

    def get_values(self):
        return (
            self.collect_target_id, 
            self.contact_account_type, 
            self.account_id, 
            self.account,
            self.regis_nickname, 
            self.password, 
            self.fans_counter, 
            self.follow_counter, 
            self.message_counter,
            self.prevace_message_counter, 
            self.install_time, 
            self.area, 
            self.city_code,
            self.fixed_phone, 
            self.msisdn, 
            self.email_account, 
            self.certificate_type, 
            self.certificate_code,
            self.sex_code, 
            self.age, 
            self.postal_address, 
            self.postal_code, 
            self.occupation_name,
            self.blood_type, 
            self.name, 
            self.sign_name, 
            self.personal_desc, 
            self.reg_city,
            self.graduate_school,
            self.zodiac,
            self.constallation,  
            self.birthday,  
            self.delete_status, 
            self.delete_time, 
            self.hash_type,
            self.user_photo,
            self.account_reg_date,
            self.last_login_time,
            self.latest_mod_time,
        )


class Friends(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Friends, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.friend_id = None  # 微博好友的用户ID
        self.friend_account = None  # 微博好友的账号
        self.friend_nickname = None  # 微博好友的昵称
        self.friend_group = None  # 好友的默认或者自定义分组
        self.friend_remark = None  # 好友备注
        self.url = None  # 好友主页的url
        self.weibo_follow_type = None  # 0未知、1关注人、2粉丝、3相互关注、9其他
        self.area = None  
        self.city_code = None 
        self.fixed_phone = None  
        self.msisdn = None  # 手机号码
        self.email_account = None
        self.certificate_type = None  # 好友信息里提取的有效身份证件类型
        self.certificate_code = None  # 好友信息里提取的有效身份证件号
        self.sex_code = None  # 性别
        self.age = None  
        self.postal_address = None  # 联系地址
        self.postal_code = None  # 邮政编码
        self.occupation_name = None  # 职业名称
        self.blood_type = None  
        self.name = None  # 真实姓名
        self.sign_name = None  # 个性签名
        self.personal_desc = None  # 个人说明
        self.reg_city = None  # 城市
        self.graduate_school = None  # 毕业院校
        self.zodiac = None  # 生肖
        self.constallation = None  # 星座
        self.birthday = None
        self.delete_status = None
        self.delete_time = None
        self.user_photo = None  # 附件文件路径
        self.account_reg_date = None  # 时间戳
        self.last_login_time = None  # 账户最后登录时间
        self.last_mod_time = None  # 账户最后更新时间
        self.fans_counter = None
        self.follow_counter = None
        self.message_counter = None  # 博文数量

    def get_values(self):
        return (
            self.collect_target_id, 
            self.contact_account_type, 
            self.account_id, 
            self.account,
            self.friend_id,
            self.friend_account,
            self.friend_nickname,
            self.friend_group,
            self.friend_remark,
            self.url,
            self.weibo_follow_type,
            self.area,
            self.city_code,
            self.fixed_phone,
            self.msisdn,
            self.email_account,
            self.certificate_type,
            self.certificate_code,
            self.sex_code,
            self.age,
            self.postal_address,
            self.postal_code,
            self.occupation_name,
            self.blood_type,
            self.name,
            self.sign_name,
            self.personal_desc,
            self.reg_city,
            self.graduate_school,
            self.zodiac,
            self.constallation,
            self.birthday,
            self.delete_status, 
            self.delete_time, 
            self.user_photo,
            self.account_reg_date,
            self.last_login_time,
            self.last_mod_time,
            self.fans_counter,
            self.follow_counter,
            self.message_counter,
        )


class Feed(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Feed, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.friend_id = None  # 发布动态消息的用户ID
        self.friend_account = None  # 发布动态消息的用户账号
        self.friend_nickname = None  # 发布者昵称
        self.weibo_message_type = None 
        self.mail_send_time = None
        self.weibo_topic = None
        self.weibo_message = None
        self.weibo_reply_counter = None  # 回复数
        self.weibo_retweet_counter = None  # 转发数
        self.weibo_like_counter = None  # 点赞数
        self.mblog_id = None  # 标识一条微博博文的唯一性ID 
        self.relevant_mblog_id = None  # 消息类型为原创时，此项为原创发布的内容；消息类型为转发或评论时，此项为评论内容；消息类型为点赞时，此项为空。
        self.id_root_mblog_id = None  # 转发、评论、点赞时的最原始的原创博文ID，消息类型为原创时此项为空
        self.mainfile = None  # 文件的名称
        self.media_type = None
        self.city_code = None
        self.company_address = None
        self.longitude = None
        self.latitude = None
        self.above_sealevel = None

    def get_values(self):
        return (
            self.collect_target_id, 
            self.contact_account_type, 
            self.account_id, 
            self.account,
            self.friend_id,
            self.friend_account,
            self.friend_nickname,
            self.weibo_message_type,
            self.mail_send_time,
            self.weibo_topic,
            self.weibo_message,
            self.weibo_reply_counter,
            self.weibo_retweet_counter,
            self.weibo_like_counter,
            self.mblog_id,
            self.relevant_mblog_id,
            self.id_root_mblog_id,
            self.delete_status, 
            self.delete_time, 
            self.mainfile,
            self.media_type,
            self.city_code,
            self.company_address,
            self.longitude,
            self.latitude,
            self.above_sealevel,
        )


class Message(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Message, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.regis_nickname = None
        self.friend_id = None  # 对方用户id
        self.friend_account = None  # 对方账号
        self.friend_nickname = None  # 对方昵称
        self.content = None 
        self.mail_send_time = None
        self.local_action = None  # 标示本机是收方还是发方，01接收方、02发送方、99其他
        self.talk_id = None  # 单条私信的全局唯一性标识ID
        self.delete_status = None
        self.delete_time = None
        self.mainfile = None
        self.media_type = None
        self.city_code = None
        self.company_address = None
        self.longitude = None
        self.latitude = None
        self.above_sealevel = None

    def get_values(self):
        return (
            self.collect_target_id, 
            self.contact_account_type, 
            self.account_id, 
            self.account,
            self.regis_nickname,
            self.friend_id,
            self.friend_account,
            self.friend_nickname,
            self.content,
            self.mail_send_time,
            self.local_action,
            self.talk_id,
            self.delete_status, 
            self.delete_time, 
            self.mainfile,
            self.media_type,
            self.city_code,
            self.company_address,
            self.longitude,
            self.latitude,
            self.above_sealevel,
        )


class Search(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Search, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.create_time = None
        self.keyword = None 

    def get_values(self):
        return (
            self.collect_target_id, 
            self.contact_account_type, 
            self.account_id, 
            self.account,
            self.create_time,
            self.keyword,
            self.delete_status, 
            self.delete_time, 
        )


class GenerateBcp(object):
    def __init__(self, cache_db, bcp_db, collect_target_id, contact_account_type):
        self.cache_db = cache_db
        self.bcp_db = bcp_db
        self.collect_target_id = collect_target_id
        self.contact_account_type = contact_account_type
        self.weibo = Weibo()

    def generate(self):
        self.weibo.db_create(self.bcp_db)
        self.db = sqlite3.connect(self.cache_db)
        self._generate_account()
        self._generate_friends()
        self._generate_feed()
        self._generate_message()
        self._generate_search()
        self.db.close()
        self.weibo.db_close()

    def _generate_account(self):
        """生成account数据"""
        cursor = self.db.cursor()
        sql = '''select account_id, nickname, username, password, photo, telephone, email, gender, age, country, 
                        province, city, address, birthday, signature, source, deleted, repeated
                 from account'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            account = Account(self.collect_target_id, self.contact_account_type, row[0], row[2])
            account.regis_nickname = row[1]
            account.password = row[3]
            account.fans_counter = None
            account.follow_counter = None   
            account.message_counter = self._query_message_number(account.account_id)  # TODO 通过feed表和feed_comment的计算得出结果
            account.prevace_message_counter = None  # TODO 通过message表的计算得出结果
            account.install_time = None
            account.area = row[9]
            account.city_code = row[11]  # 这个和下面的reg_city做一下比较 
            account.fixed_phone = None
            account.msisdn = row[5]
            account.email_account = row[6]
            account.certificate_type = None
            account.certificate_code = None
            account.sex_code = self._convert_sexcode(row[7])
            account.age = row[8]
            account.postal_address = row[12]
            account.postal_code = None
            account.occupation_name = None
            account.blood_type = None
            account.name = None
            account.sign_name = None
            account.personal_desc = None
            account.reg_city = row[11]
            account.graduate_school = None
            account.zodiac = None   # TODO 可以根据生日算出来
            account.constallation = None    # TODO 可以根据生日算出来
            account.birthday = row[13]
            account.delete_status = row[16]
            account.delete_time = None
            account.hash_type = None
            account.user_photo = None
            account.account_reg_date = None
            account.last_login_time = None
            account.latest_mod_time = None

            self.weibo.db_insert_table_account(account)
            row = cursor.fetchone()
        self.weibo.db_commit()
        cursor.close()

    def _generate_friends(self):
        """生成微博好友信息(WA_MFORENSICS_030200)的数据"""
        cursor = self.db.cursor()
        sql = '''select account_id,
                        friend_id,
                        nickname,
                        remark,
                        photo,
                        type,
                        telephone,
                        email,
                        gender,
                        age,
                        address,
                        birthday,
                        signature,
                        source,
                        deleted,
                        repeated
                 from friend'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            account_info = self._query_account_info(row[0])
            account = account_info["username"] if account_info is not None else None
            friend = Friends(self.collect_target_id, self.contact_account_type, row[0], account)
            friend.friend_id = row[1]
            friend.friend_nickname = row[2]
            friend.friend_remark = row[3]
            friend.sex_code = self._convert_sexcode(row[8])
            friend.message_counter = self._query_message_number(friend.friend_id)

            # 通过好友的account_id找到剩下的资料
            friend_account_info = self._query_account_info(row[1])
            if friend_account_info is not None:
                friend.friend_account = None
                friend.friend_group = None
                friend.url = None
                friend.weibo_follow_type = None
                friend.area = None
                friend.city_code = None
                friend.fixed_phone = None
                friend.msisdn = friend_account_info['telephone']
                friend.email_account = friend_account_info['email']
                friend.certificate_type = None
                friend.certificate_code = None
                friend.age = friend_account_info['age']
                friend.postal_address = friend_account_info['address']
                friend.postal_code = None
                friend.occupation_name = None
                friend.blood_type = None
                friend.name = None
                friend.sign_name = None
                friend.personal_desc = None
                friend.reg_city = friend_account_info['city']
                friend.graduate_school = None
                friend.zodiac = None
                friend.constallation = None
                friend.birthday = friend_account_info['birthday']
                friend.delete_status = None
                friend.delete_time = None
                friend.user_photo = None
                friend.account_reg_date = None
                friend.last_login_time = None
                friend.last_mod_time = None
                friend.fans_counter = None
                friend.follow_counter = None

            self.weibo.db_insert_table_friends(friend)
            row = cursor.fetchone()
        self.weibo.db_commit()
        cursor.close()

    def _generate_feed(self):
        """
        生成博文信息(WA_MFORENSICS_030300)
        下列依次为：
            1. 原始博文
            2. 评论
            3. 点赞
        """
        self._generate_feed_original()
        self._generate_feed_comment()
        self._generate_feed_like()

    def _generate_feed_original(self):
        """
        获取微博的原创博文
        """
        cursor = self.db.cursor()
        sql = '''select account_id, sender_id, type, content, media_path, urls, preview_urls, 
                        attachment_title, attachment_link, attachment_desc, send_time, likes,
                        likecount, rtcount, comments, commentcount, device, location, source, deleted, repeated
                 from feed'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            account_info = self._query_account_info(row[0])
            friend_info = self._query_friend_info(row[1])
            if account_info:
                account = account_info["username"]
            else:
                account = None
            if friend_info:
                friend_nickname = friend_info["nickname"]
            else:
                friend_nickname = None
            feed = Feed(self.collect_target_id, self.contact_account_type, row[0], account)
            feed.friend_id = row[1]
            feed.friend_account = None
            feed.friend_nickname = friend_nickname
            feed.weibo_message_type = WEIBO_MESSAGE_TYPE_ORIGINAL
            feed.mail_send_time = row[10]
            feed.weibo_topic = None
            feed.weibo_message = row[3]
            feed.weibo_reply_counter = row[15]
            feed.weibo_retweet_counter = row[13]
            feed.weibo_like_counter = row[12]
            feed.mblog_id = None
            feed.relevant_mblog_id = None
            feed.id_root_mblog_id = None  # 转发、评论、点赞时的最原始的原创博文ID，消息类型为原创时此项为空
            feed.mainfile = row[7]  # 文件的名称
            feed.media_type = self._convert_media_type(row[2])
            feed.city_code = None
            locationinfo = self._query_location_info(row[17])
            if locationinfo is not None:
                feed.longitude = locationinfo["longitude"]
                feed.latitude = locationinfo["latitude"]
                feed.above_sealevel = locationinfo["elevation"]
                feed.company_address = locationinfo["address"]
            else:
                feed.longitude = None
                feed.latitude = None
                feed.above_sealevel = None
                feed.company_address = None

            self.weibo.db_insert_table_feed(feed)
            row = cursor.fetchone()
        self.weibo.db_commit()
        cursor.close()

    def _generate_feed_comment(self):
        """
        获取微博的评论博文
        """
        cursor = self.db.cursor()
        sql = '''select comment_id,
                        sender_id,
                        sender_name,
                        ref_user_id,
                        ref_user_name,
                        content,
                        create_time,
                        source,
                        deleted,
                        repeated
                from feed_comment '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            account = None
            feed = Feed(self.collect_target_id, self.contact_account_type, row[1], account)
            feed.friend_id = row[1]
            feed.friend_account = None
            feed.friend_nickname = row[2]
            feed.weibo_message_type = WEIBO_MESSAGE_TYPE_COMMENT_OR_LIKE
            feed.mail_send_time = row[6]
            feed.weibo_topic = None
            feed.weibo_message = row[5]
            feed.weibo_reply_counter = None
            feed.weibo_retweet_counter = None
            feed.weibo_like_counter = None
            feed.mblog_id = None
            feed.relevant_mblog_id = None
            feed.id_root_mblog_id = None  # 转发、评论、点赞时的最原始的原创博文ID，消息类型为原创时此项为空
            feed.mainfile = None  # 文件的名称
            feed.media_type = None
            feed.city_code = None
            feed.company_address = None
            feed.longitude = None
            feed.latitude = None
            feed.above_sealevel = None

            self.weibo.db_insert_table_feed(feed)
            row = cursor.fetchone()
        self.weibo.db_commit()
        cursor.close()

    def _generate_feed_like(self):
        """
        获取微博的点赞数据
        """
        cursor = self.db.cursor()
        sql = '''select like_id,
                        sender_id,
                        sender_name,
                        create_time,
                        source,
                        deleted,
                        repeated
                from feed_like'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            account = None
            feed = Feed(self.collect_target_id, self.contact_account_type, row[1], account)
            feed.friend_id = row[1]
            feed.friend_account = None
            feed.friend_nickname = row[2]
            feed.weibo_message_type = WEIBO_MESSAGE_TYPE_COMMENT_OR_LIKE
            feed.mail_send_time = row[3]
            feed.weibo_topic = None
            feed.weibo_message = None
            feed.weibo_reply_counter = None
            feed.weibo_retweet_counter = None
            feed.weibo_like_counter = None
            feed.mblog_id = None
            feed.relevant_mblog_id = None
            feed.id_root_mblog_id = None  # 转发、评论、点赞时的最原始的原创博文ID，消息类型为原创时此项为空
            feed.mainfile = None  # 文件的名称
            feed.media_type = None
            feed.city_code = None
            feed.company_address = None
            feed.longitude = None
            feed.latitude = None
            feed.above_sealevel = None

            self.weibo.db_insert_table_feed(feed)
            row = cursor.fetchone()
        self.weibo.db_commit()
        cursor.close()

    def _generate_message(self):
        """生成message表私信的数据"""
        cursor = self.db.cursor()
        sql = '''select account_id,
                        talker_id,
                        talker_name,
                        sender_id,
                        sender_name,
                        is_sender,
                        msg_id,
                        type,
                        content,
                        media_path,
                        send_time,
                        extra_id,
                        status,
                        talker_type,
                        source,
                        deleted, 
                        repeated 
                from message'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            account_info = self._query_account_info(row[0])
            if account_info is not None:
                account = account_info["username"]
                regis_nickname = account_info["nickname"]
            else:
                account = None
                regis_nickname = None
            message = Message(self.collect_target_id, self.contact_account_type, row[0], account)
            message.local_action = self._convert_local_action(row[5])
            if message.local_action == LOCAL_ACTION_RECEIVER:
                message.friend_id = row[3]
                message.friend_nickname = row[4]
            else:
                message.friend_id = None
                message.friend_nickname = None
            message.regis_nickname = regis_nickname
            message.friend_account = None
            message.content = row[8]
            message.mail_send_time = row[10]
            message.talk_id = row[6]
            message.delete_status = row[16]
            message.delete_time = None
            message.mainfile = None
            message.media_type = self._convert_media_type(row[7])
            message.city_code = None
            # 添加地理信息
            locationinfo = self._query_location_info(row[11])
            if locationinfo is not None:
                message.longitude = locationinfo["longitude"]
                message.latitude = locationinfo["latitude"]
                message.above_sealevel = locationinfo["elevation"]
                message.company_address = locationinfo["address"]
            else:
                message.longitude = None
                message.latitude = None
                message.above_sealevel = None
                message.company_address = None


            self.weibo.db_insert_table_message(message)
            row = cursor.fetchone()
        self.weibo.db_commit()
        cursor.close()

    def _generate_search(self):
        """生成search搜索记录的数据的数据"""
        cursor = self.db.cursor()
        sql = '''select search.account_id as account_id, 
                        account.username as username, 
                        search.key as key, 
                        search.create_time as create_time, 
                        search.source as source, 
                        search.deleted as deleted, 
                        search.repeated as repeated 
                from search left join account on search.account_id=account.account_id'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            search_record = Search(self.collect_target_id, self.contact_account_type, row[0], row[1])
            search_record.create_time = row[3]
            search_record.keyword = row[2]
            search_record.delete_status = row[5]
            search_record.delete_time = None

            self.weibo.db_insert_table_search(search_record)
            row = cursor.fetchone()
        self.weibo.db_commit()
        cursor.close()

    def _query_account_info(self, account_id):
        """
        通过用户id拿到用户的信息
        :param account_id: (str) 用户id
        :return: (dict) key是account表所有的字段
        """
        cursor = self.db.cursor()
        sql = '''select account_id, nickname, username, password, photo, telephone, email, gender, age, country, 
                        province, city, address, birthday, signature, source, deleted, repeated
                 from account where account_id={account_id}'''.format(account_id=account_id)
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return {
                "account_id": row[0],
                "nickname": row[1],
                "username": row[2],
                "password": row[3],
                "photo": row[4],
                "telephone": row[5],
                "email": row[6],
                "gender": row[7],
                "age": row[8],
                "country": row[9],
                "province": row[10],
                "city": row[11],
                "address": row[12],
                "birthday": row[13],
                "signature": row[14],
                "source": row[15],
                "deleted": row[16],
                "repeated": row[17],
            }
        except Exception as e:
            print(e)
            return None

    def _query_friend_info(self, friend_id):
        """
        通过friend_id拿到friend表中的信息
        :param friend_id: (str) id
        :return: (dict) {"nickname": "haha"}
        """
        cursor = self.db.cursor()
        sql = '''select nickname
                 from friend where friend_id={friend_id}'''.format(friend_id=friend_id)
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return {
                "nickname": row[0],
            }
        except Exception as e:
            print(e)
            return None

    def _query_location_info(self, location_id):
        """
        通过location_id拿到地理位置的信息
        :param account_id: (str) location的id
        :return: (dict) key是location表所有的字段
        """
        cursor = self.db.cursor()
        sql = '''SELECT latitude, 
                        longitude, 
                        elevation, 
                        address 
                    FROM location WHERE location_id="{location_id}"'''.format(location_id=location_id)
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return {
                "latitude": row[0],
                "longitude": row[1],
                "elevation": row[2],
                "address": row[3],
            }
        except Exception as e:
            print(e)
            return None

    def _query_message_number(self, account_id):
        """
        通过account_id拿到这个用户发过的所有的博文，再计算他的数量
        :param account_id: (str) 
        :return: (int) 博文的数量 
        """
        cursor = self.db.cursor()
        sql = '''select count(sender_id) 
                from (select sender_id FROM feed union all select sender_id from feed_comment)
                where sender_id={sender_id};'''.format(sender_id=account_id)
        cursor.execute(sql)
        row = cursor.fetchone()
        return int(row[0])

    @staticmethod
    def _convert_sexcode(sexcode):
        if sexcode == model_im.GENDER_NONE:
            return SEXCODE_UNKNOWN
        elif sexcode == model_im.GENDER_MALE:
            return SEXCODE_MALE
        elif sexcode == model_im.GENDER_FEMALE:
            return SEXCODE_FEMALE
        else:
            return SEXCODE_OTHER
            
    @staticmethod
    def _convert_delete_status(status):
        if status == 0:
            return DELETE_STATUS_NOT_DELETED
        else:
            return DELETE_STATUS_DELETED

    @staticmethod
    def _convert_local_action(is_send):
        if is_send == 0:
            return LOCAL_ACTION_RECEIVER
        else:
            return LOCAL_ACTION_SENDER

    @staticmethod
    def _convert_media_type(media_type):
        if media_type == model_im.MESSAGE_CONTENT_TYPE_TEXT:
            return MEDIA_TYPE_TEXT
        elif media_type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
            return MEDIA_TYPE_IMAGE
        elif media_type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
            return MEDIA_TYPE_VOICE
        elif media_type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
            return MEDIA_TYPE_VIDEO
        else:
            return MEDIA_TYPE_OTHER
