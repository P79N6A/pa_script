# -*- coding: utf-8 -*-
__author__ = "sumeng"

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

# 即时通信类信息

# 账号信息
SQL_CREATE_TABLE_WA_MFORENSICS_020100 = '''
    create table if not exists WA_MFORENSICS_020100(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        REGIS_NICKNAME text,
        PASSWORD text,
        INSTALL_TIME int,
        AREA text,
        CITY_CODE text,
        FIXED_PHONE text,
        MSISDN text,
        EMAIL_ACCOUNT text,
        CERTIFICATE_TYPE text,
        CERTIFICATE_CODE text,
        SEXCODE int,
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
        BIRTHDAY int,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        HASH_TYPE text,
        USER_PHOTO text,
        ACCOUNT_REG_DATE int,
        LAST_LOGIN_TIME int,
        LATEST_MOD_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020100 = '''
    insert into WA_MFORENSICS_020100(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            REGIS_NICKNAME, PASSWORD, INSTALL_TIME, AREA, CITY_CODE, FIXED_PHONE, MSISDN, 
            EMAIL_ACCOUNT, CERTIFICATE_TYPE, CERTIFICATE_CODE, SEXCODE, AGE, POSTAL_ADDRESS, 
            POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, NAME, SIGN_NAME, PERSONAL_DESC, REG_CITY, 
            GRADUATESCHOOL, ZODIAC, CONSTALLATION, BIRTHDAY, DELETE_STATUS, DELETE_TIME, HASH_TYPE, 
            USER_PHOTO, ACCOUNT_REG_DATE, LAST_LOGIN_TIME, LATEST_MOD_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?)'''

# 好友信息
SQL_CREATE_TABLE_WA_MFORENSICS_020200 = '''
    create table if not exists WA_MFORENSICS_020200(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        FRIEND_NICKNAME text,
        FRIEND_GROUP text,
        FRIEND_REMARK text,
        AREA text,
        CITY_CODE text,
        FIXED_PHONE text,
        MSISDN text,
        EMAIL_ACCOUNT text,
        CERTIFICATE_TYPE text,
        CERTIFICATE_CODE text,
        SEXCODE int,
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
        BIRTHDAY int,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        USER_PHOTO text,
        ACCOUNT_REG_DATE int,
        LAST_LOGIN_TIME int,
        LATEST_MOD_TIME int,
        ADD_FRIEDN_TIME int,
        BLACKLIST_TIME int,
        DELETE_FRIEND_TIME int,
        FRIEND_RELATION_TYPE text)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020200 = '''
    insert into WA_MFORENSICS_020200(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, FRIEND_GROUP, FRIEND_REMARK, AREA, 
            CITY_CODE, FIXED_PHONE, MSISDN, EMAIL_ACCOUNT, CERTIFICATE_TYPE,  CERTIFICATE_CODE, 
            SEXCODE, AGE, POSTAL_ADDRESS, POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, NAME, 
            SIGN_NAME, PERSONAL_DESC, REG_CITY, GRADUATESCHOOL, ZODIAC, CONSTALLATION, BIRTHDAY, 
            DELETE_STATUS, DELETE_TIME, USER_PHOTO, ACCOUNT_REG_DATE, LAST_LOGIN_TIME, LATEST_MOD_TIME, 
            ADD_FRIEDN_TIME, BLACKLIST_TIME, DELETE_FRIEND_TIME, FRIEND_RELATION_TYPE) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 群组信息
SQL_CREATE_TABLE_WA_MFORENSICS_020300 = '''
    create table if not exists WA_MFORENSICS_020300(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        GROUP_NUM text,
        GROUP_NAME text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        GROUP_OWNER_NICKNAME text,
        GROUP_MEMBER_COUNT text,
        GROUP_MAX_MEMBER_COUT text,
        GROUP_NOTICE text,
        GROUP_DESCRIPTION text,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        GROUP_OWNER_INTERNAL_ID text,
        GROUP_OWNER text,
        GROUP_ADMIN_NICKNAME text,
        CREATE_TIME int,
        GROUPPHOTO text)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020300 = '''
    insert into WA_MFORENSICS_020300(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            GROUP_NUM, GROUP_NAME, FRIEND_ID, FRIEND_ACCOUNT, GROUP_OWNER_NICKNAME, GROUP_MEMBER_COUNT,
            GROUP_MAX_MEMBER_COUT, GROUP_NOTICE, GROUP_DESCRIPTION, DELETE_STATUS, DELETE_TIME, 
            GROUP_OWNER_INTERNAL_ID, GROUP_OWNER, GROUP_ADMIN_NICKNAME, CREATE_TIME, GROUPPHOTO) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 群组成员信息
SQL_CREATE_TABLE_WA_MFORENSICS_020400 = '''
    create table if not exists WA_MFORENSICS_020400(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        GROUP_NUM text,
        GROUP_NAME text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        FRIEND_NICKNAME text,
        FRIEND_REMARK text,
        AREA text,
        CITY_CODE text,
        FIXED_PHONE text,
        MSISDN text,
        EMAIL_ACCOUNT text,
        CERTIFICATE_TYPE text,
        CERTIFICATE_CODE text,
        SEXCODE int,
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
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        USER_PHOTO text,
        ACCOUNT_REG_DATE int,
        LAST_LOGIN_TIME int,
        LATEST_MOD_TIME int,
        LAST_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020400 = '''
    insert into WA_MFORENSICS_020400(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            GROUP_NUM, GROUP_NAME, FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, FRIEND_REMARK, AREA, 
            CITY_CODE, FIXED_PHONE, MSISDN, EMAIL_ACCOUNT, CERTIFICATE_TYPE,  CERTIFICATE_CODE, 
            SEXCODE, AGE, POSTAL_ADDRESS, POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, NAME, SIGN_NAME, 
            PERSONAL_DESC, REG_CITY, GRADUATESCHOOL, ZODIAC, CONSTALLATION, BIRTHDAY, DELETE_STATUS, 
            DELETE_TIME, USER_PHOTO, ACCOUNT_REG_DATE, LAST_LOGIN_TIME, LATEST_MOD_TIME, LAST_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 好友聊天记录信息
SQL_CREATE_TABLE_WA_MFORENSICS_020500 = '''
    create table if not exists WA_MFORENSICS_020500(
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
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        MAINFILE text,
        MEDIA_TYPE text,
        CITY_CODE text,
        COMPANY_ADDRESS text,
        LONGITUDE text,
        LATITUDE text,
        ABOVE_SEALEVEL text)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020500 = '''
    insert into WA_MFORENSICS_020500(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            REGIS_NICKNAME, FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, CONTENT, MAIL_SEND_TIME,
            LOCAL_ACTION, TALK_ID, DELETE_STATUS, DELETE_TIME, MAINFILE, MEDIA_TYPE, CITY_CODE, 
            COMPANY_ADDRESS, LONGITUDE, LATITUDE, ABOVE_SEALEVEL) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 群组聊天记录信息
SQL_CREATE_TABLE_WA_MFORENSICS_020600 = '''
    create table if not exists WA_MFORENSICS_020600(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        GROUP_NUM text,
        GROUP_NAME text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        FRIEND_NICKNAME text,
        CONTENT text,
        MAIL_SEND_TIME int,
        LOCAL_ACTION text,
        TALK_ID text,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        MAINFILE text,
        MEDIA_TYPE text,
        CITY_CODE text,
        COMPANY_ADDRESS text,
        LONGITUDE text,
        LATITUDE text,
        ABOVE_SEALEVEL text)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020600 = '''
    insert into WA_MFORENSICS_020600(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            GROUP_NUM, GROUP_NAME, FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, CONTENT, MAIL_SEND_TIME,
            LOCAL_ACTION, TALK_ID, DELETE_STATUS, DELETE_TIME, MAINFILE, MEDIA_TYPE, CITY_CODE, 
            COMPANY_ADDRESS, LONGITUDE, LATITUDE, ABOVE_SEALEVEL) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 动态信息
SQL_CREATE_TABLE_WA_MFORENSICS_020700 = '''
    create table if not exists WA_MFORENSICS_020700(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        FRIEND_ID text,
        FRIEND_ACCOUNT text,
        FRIEND_NICKNAME text,
        WEIBO_MESSAGE_TYPE text,
        MAIL_SEND_TIME int,
        WEIBO_MESSAGE text,
        WEIBO_REPLY_COUNTER int,
        WEIBO_RETWEET_COUNTER int,
        WEIBO_LIKE_COUNTER int,
        MBLOG_ID text,
        RELEVANT_MBLOG_ID text,
        IDROOT_MBLOG_ID text,
        GROUP_NUM text,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        MEDIA_TYPE text,
        CITY_CODE text,
        COMPANY_ADDRESS text,
        LONGITUDE text,
        LATITUDE text,
        ABOVE_SEALEVEL text,
        FANS_COUNTER int,
        VISITS int,
        DUAL_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020700 = '''
    insert into WA_MFORENSICS_020700(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            FRIEND_ID, FRIEND_ACCOUNT, FRIEND_NICKNAME, WEIBO_MESSAGE_TYPE, MAIL_SEND_TIME,
            WEIBO_MESSAGE, WEIBO_REPLY_COUNTER, WEIBO_RETWEET_COUNTER, WEIBO_LIKE_COUNTER, 
            MBLOG_ID, RELEVANT_MBLOG_ID, IDROOT_MBLOG_ID, GROUP_NUM, DELETE_STATUS, DELETE_TIME, 
            MEDIA_TYPE, CITY_CODE, COMPANY_ADDRESS, LONGITUDE, LATITUDE, ABOVE_SEALEVEL, 
            FANS_COUNTER, VISITS, DUAL_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?)'''

# 搜索记录信息
SQL_CREATE_TABLE_WA_MFORENSICS_020800 = '''
    create table if not exists WA_MFORENSICS_020800(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        CREATE_TIME int,
        KEYWORD text,
        DELETE_STATUS text default "0",
        DELETE_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_020800 = '''
    insert into WA_MFORENSICS_020800(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT, 
            CREATE_TIME, KEYWORD, DELETE_STATUS, DELETE_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?)'''

# 浏览类信息

# 保存的网站密码信息
SQL_CREATE_TABLE_WA_MFORENSICS_050100 = '''
    create table if not exists WA_MFORENSICS_050100(
        COLLECT_TARGET_ID text,
        BROWSE_TYPE text,
        URL text,
        ACCOUNT text,
        PASSWORD text,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        NETWORK_APP text,
        ACCOUNT_ID text,
        FRIEND_ACCOUNT text,
        LOGIN_COUNT text)'''

SQL_INSERT_TABLE_WA_MFORENSICS_050100 = '''
    insert into WA_MFORENSICS_050100(COLLECT_TARGET_ID, BROWSE_TYPE, URL, ACCOUNT, PASSWORD, 
            DELETE_STATUS, DELETE_TIME, NETWORK_APP, ACCOUNT_ID, FRIEND_ACCOUNT, LOGIN_COUNT) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 收藏夹信息
SQL_CREATE_TABLE_WA_MFORENSICS_050200 = '''
    create table if not exists WA_MFORENSICS_050200(
        COLLECT_TARGET_ID text,
        BROWSE_TYPE text,
        NAME text,
        URL text,
        CREATE_TIME int,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        NETWORK_APP text,
        ACCOUNT_ID text,
        ACCOUNT text,
        LATEST_MOD_TIME int,
        VISITS int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_050200 = '''
    insert into WA_MFORENSICS_050200(COLLECT_TARGET_ID, BROWSE_TYPE, NAME, URL, CREATE_TIME,
            DELETE_STATUS, DELETE_TIME, NETWORK_APP, ACCOUNT_ID, ACCOUNT, LATEST_MOD_TIME, VISITS) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 历史记录信息
SQL_CREATE_TABLE_WA_MFORENSICS_050300 = '''
    create table if not exists WA_MFORENSICS_050300(
        COLLECT_TARGET_ID text,
        BROWSE_TYPE text,
        WEB_TITLE text,
        URL text,
        VISIT_TIME int,
        VISITS int,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        NETWORK_APP text,
        ACCOUNT_ID text,
        ACCOUNT text,
        DUAL_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_050300 = '''
    insert into WA_MFORENSICS_050300(COLLECT_TARGET_ID, BROWSE_TYPE, WEB_TITLE, URL, VISIT_TIME, 
            VISITS, DELETE_STATUS, DELETE_TIME, NETWORK_APP, ACCOUNT_ID, ACCOUNT, DUAL_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# COOKIES信息
SQL_CREATE_TABLE_WA_MFORENSICS_050400 = '''
    create table if not exists WA_MFORENSICS_050400(
        COLLECT_TARGET_ID text,
        BROWSE_TYPE text,
        URL text,
        KEY_NAME text,
        KEY_VALUE text,
        CREATE_TIME int,
        EXPIRE_TIME int,
        VISIT_TIME int,
        VISITS int,
        DELETE_STATUS text default "0",
        DELETE_TIME int,
        NETWORK_APP text,
        ACCOUNT_ID text,
        ACCOUNT text,
        LATEST_MOD_TIME int,
        NAME text)'''

SQL_INSERT_TABLE_WA_MFORENSICS_050400 = '''
    insert into WA_MFORENSICS_050400(COLLECT_TARGET_ID, BROWSE_TYPE, URL, KEY_NAME, KEY_VALUE,
            CREATE_TIME, EXPIRE_TIME, VISIT_TIME, VISITS, DELETE_STATUS, DELETE_TIME, 
            NETWORK_APP, ACCOUNT_ID, ACCOUNT, LATEST_MOD_TIME, NAME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 账号信息
SQL_CREATE_TABLE_WA_MFORENSICS_050500 = '''
    create table if not exists WA_MFORENSICS_050500(
        COLLECT_TARGET_ID text,
        NETWORK_APP text,
        ACCOUNT_ID text,
        ACCOUNT text,
        REGIS_NICKNAME text,
        PASSWORD text,
        INSTALL_TIME int,
        AREA text,
        CITY_CODE text,
        FIXED_PHONE text,
        MSISDN text,
        EMAIL_ACCOUNT text,
        CERTIFICATE_TYPE text,
        CERTIFICATE_CODE text,
        SEXCODE int,
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
        BIRTHDAY int,
        HASH_TYPE text,
        USER_PHOTO text,
        ACCOUNT_REG_DATE int,
        LAST_LOGIN_TIME int,
        LATEST_MOD_TIME int,
        DELETE_STATUS text default "0",
        DELETE_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_050500 = '''
    insert into WA_MFORENSICS_050500(COLLECT_TARGET_ID, NETWORK_APP, ACCOUNT_ID, ACCOUNT, 
            REGIS_NICKNAME, PASSWORD, INSTALL_TIME, AREA, CITY_CODE, FIXED_PHONE, MSISDN, 
            EMAIL_ACCOUNT, CERTIFICATE_TYPE, CERTIFICATE_CODE, SEXCODE, AGE, POSTAL_ADDRESS, 
            POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, NAME, SIGN_NAME, PERSONAL_DESC, 
            REG_CITY, GRADUATESCHOOL, ZODIAC, CONSTALLATION, BIRTHDAY, HASH_TYPE, USER_PHOTO, 
            ACCOUNT_REG_DATE, LAST_LOGIN_TIME, LATEST_MOD_TIME, DELETE_STATUS, DELETE_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

# 搜索记录信息
SQL_CREATE_TABLE_WA_MFORENSICS_050600 = '''
    create table if not exists WA_MFORENSICS_050600(
        COLLECT_TARGET_ID text,
        CONTACT_ACCOUNT_TYPE text,
        ACCOUNT_ID text,
        ACCOUNT text,
        CREATE_TIME int,
        KEYWORD text,
        DELETE_STATUS text default "0",
        DELETE_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_050600 = '''
    insert into WA_MFORENSICS_050600(COLLECT_TARGET_ID, CONTACT_ACCOUNT_TYPE, ACCOUNT_ID, ACCOUNT,
            CREATE_TIME, KEYWORD, DELETE_STATUS, DELETE_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?)'''

# 历史表单信息
SQL_CREATE_TABLE_WA_MFORENSICS_050700 = '''
    create table if not exists WA_MFORENSICS_050700(
        COLLECT_TARGET_ID text,
        NETWORK_APP text,
        ACCOUNT_ID text,
        ACCOUNT text,
        URL text,
        KEY_NAME text,
        KEY_VALUE text,
        VISITS int,
        DELETE_STATUS text default "0",
        DELETE_TIME int)'''

SQL_INSERT_TABLE_WA_MFORENSICS_050700 = '''
    insert into WA_MFORENSICS_050700(COLLECT_TARGET_ID, NETWORK_APP, ACCOUNT_ID, ACCOUNT, URL,
            KEY_NAME, KEY_VALUE, VISITS, DELETE_STATUS, DELETE_TIME) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''


TypeRotationTable = {
    "account": SQL_CREATE_TABLE_WA_MFORENSICS_020100,
    "friends" : SQL_CREATE_TABLE_WA_MFORENSICS_020200,
    "group": SQL_CREATE_TABLE_WA_MFORENSICS_020300,
    "group-member": SQL_CREATE_TABLE_WA_MFORENSICS_020400,
    "friend_chat": SQL_CREATE_TABLE_WA_MFORENSICS_020500,
    "group_chat": SQL_CREATE_TABLE_WA_MFORENSICS_020600,
    "dynamic": SQL_CREATE_TABLE_WA_MFORENSICS_020700,
    "search": SQL_CREATE_TABLE_WA_MFORENSICS_020800
}

SEXCODE_UNKNOWN = 0
SEXCODE_MALE = 1
SEXCODE_FEMALE = 2
SEXCODE_OTHER = 9

MEDIA_TYPE_TEXT = '01'
MEDIA_TYPE_IMAGE = '02'
MEDIA_TYPE_VOICE = '03'
MEDIA_TYPE_VIDEO = '04'
MEDIA_TYPE_OTHER = '99'

DELETE_STATUS_NOT_DELETED = '0'
DELETE_STATUS_DELETED = '1'

LOCAL_ACTION_RECEIVER = '01'
LOCAL_ACTION_SENDER = '02'
LOCAL_ACTION_OTHER = '99'

# 即时通信
CONTACT_ACCOUNT_TYPE_IM_QQ = '1030001'
CONTACT_ACCOUNT_TYPE_IM_ICQ = '1030002'
CONTACT_ACCOUNT_TYPE_IM_MSN = '1030005'
CONTACT_ACCOUNT_TYPE_IM_BEETALK = '1030011'
CONTACT_ACCOUNT_TYPE_IM_SKYPE = '1030027'
CONTACT_ACCOUNT_TYPE_IM_FETION = '1030028'
CONTACT_ACCOUNT_TYPE_IM_MILIAO = '1030035'
CONTACT_ACCOUNT_TYPE_IM_WECHAT = '1030036'
CONTACT_ACCOUNT_TYPE_IM_BAIDUHI = '1030037'
CONTACT_ACCOUNT_TYPE_IM_WHATSAPP = '1030038'
CONTACT_ACCOUNT_TYPE_IM_LINE = '1030043'
CONTACT_ACCOUNT_TYPE_IM_MOMO = '1030044'
CONTACT_ACCOUNT_TYPE_IM_FACEBOOK = '1030045'
CONTACT_ACCOUNT_TYPE_IM_RENREN = '1030046'
CONTACT_ACCOUNT_TYPE_IM_YIXIN = '1030047'
CONTACT_ACCOUNT_TYPE_IM_LAIWANG = '1030048'
CONTACT_ACCOUNT_TYPE_IM_WANGXIN = '1030049'
CONTACT_ACCOUNT_TYPE_IM_YY = '1030050'
CONTACT_ACCOUNT_TYPE_IM_TALKBOX = '1030050'
CONTACT_ACCOUNT_TYPE_IM_VOXER = '1030052'
CONTACT_ACCOUNT_TYPE_IM_VIBER = '1030053'
CONTACT_ACCOUNT_TYPE_IM_YUJIAN = '1030056'
CONTACT_ACCOUNT_TYPE_IM_KAKAOTALK = '1030057'
CONTACT_ACCOUNT_TYPE_IM_OOVOO = '1030059'
CONTACT_ACCOUNT_TYPE_IM_NIMBUZZ = '1030060'
CONTACT_ACCOUNT_TYPE_IM_TELEGRAM = '1030063'
CONTACT_ACCOUNT_TYPE_IM_ZELLO = '1030080'
CONTACT_ACCOUNT_TYPE_IM_HELLOTALK = '1030083'
CONTACT_ACCOUNT_TYPE_IM_COCO = '1030084'
CONTACT_ACCOUNT_TYPE_IM_TANGO = '1030086'
CONTACT_ACCOUNT_TYPE_IM_YOUXIN = '1030087'
CONTACT_ACCOUNT_TYPE_IM_BILIN = '1030099'
CONTACT_ACCOUNT_TYPE_IM_KEECHAT = '1030102'
CONTACT_ACCOUNT_TYPE_IM_BBM = '1030104'
CONTACT_ACCOUNT_TYPE_IM_KIK = '1030110'
CONTACT_ACCOUNT_TYPE_IM_ZALO = '1030122'
CONTACT_ACCOUNT_TYPE_IM_PEEEM = '1030124'
CONTACT_ACCOUNT_TYPE_IM_PAL = '1030125'
CONTACT_ACCOUNT_TYPE_IM_RAILCALL = '1030126'
CONTACT_ACCOUNT_TYPE_IM_DIDI = '1030128'
CONTACT_ACCOUNT_TYPE_IM_HI = '1030128'
CONTACT_ACCOUNT_TYPE_IM_BLUED = '1030146'
CONTACT_ACCOUNT_TYPE_IM_DINGDING = '1030162'
CONTACT_ACCOUNT_TYPE_IM_KUAIYA = '1030169'
CONTACT_ACCOUNT_TYPE_IM_WEIHUA = '1030172'
CONTACT_ACCOUNT_TYPE_IM_CHATAPP = '1030189'
CONTACT_ACCOUNT_TYPE_IM_OTHER = '1039999'

class IM(object):
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
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020100
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020200
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020300
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020400
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020500
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020600
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020700
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_020800
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
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020100, column.get_values())

    def db_insert_table_friend(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020200, column.get_values())

    def db_insert_table_group(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020300, column.get_values())

    def db_insert_table_group_member(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020400, column.get_values())

    def db_insert_table_friend_message(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020500, column.get_values())

    def db_insert_table_group_message(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020600, column.get_values())

    def db_insert_table_feed(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020700, column.get_values())

    def db_insert_table_search(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_020800, column.get_values())


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
        self.install_time = None  # 安装日期
        self.area = None  # 国家代号
        self.city_code = None  # 行政区划
        self.fixed_phone = None  # 电话号码
        self.msisdn = None  # 手机号码
        self.email_account = None  # 邮箱地址
        self.certificate_type = None  # 对象证件类型
        self.certificate_code = None  # 对象证件号码
        self.sexcode = None  # 性别
        self.age = None  # 年龄
        self.postal_address = None  # 联系地址
        self.postal_code = None  # 邮政编码
        self.occupation_name = None  # 职业名称
        self.blood_type = None  # 血型
        self.name = None  # 真实名
        self.sign_name = None  # 个性签名
        self.personal_desc = None  # 个人说明
        self.reg_city = None  # 城市
        self.graduateschool = None  # 毕业院校
        self.zodiac = None  # 生肖
        self.constallation = None  # 星座
        self.birthday = None  # 出生年月
        self.hash_type = None  # 密码算法类型
        self.user_photo = None  # 头像
        self.account_reg_date = None  # 账号注册时间
        self.last_login_time = None  # 账号最后登录时间
        self.latest_mod_time = None  # 账号最后更新时间

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.regis_nickname, self.password, self.install_time, self.area, self.city_code,
                self.fixed_phone, self.msisdn, self.email_account, self.certificate_type,
                self.certificate_code, self.sexcode, self.age, self.postal_address, self.postal_code,
                self.occupation_name, self.blood_type, self.name, self.sign_name, self.personal_desc,
                self.reg_city, self.graduateschool, self.zodiac, self.constallation, self.birthday,
                self.delete_status, self.delete_time, self.hash_type, self.user_photo, self.account_reg_date,
                self.last_login_time, self.latest_mod_time)

class Friend(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Friend, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.friend_id = None  # 好友用户ID
        self.friend_account = None  # 好友账号
        self.friend_nickname = None  # 好友昵称
        self.friend_group = None  # 好友分组信息
        self.friend_remark = None  # 好友备注
        self.area = None  # 国家代号
        self.city_code = None  # 行政区划
        self.fixed_phone = None  # 电话号码
        self.msisdn = None  # 手机号码
        self.email_account = None  # 邮箱地址
        self.certificate_type = None  # 对象证件类型
        self.certificate_code = None  # 对象证件号码
        self.sexcode = None  # 性别
        self.age = None  # 年龄
        self.postal_address = None  # 联系地址
        self.postal_code = None  # 邮政编码
        self.occupation_name = None  # 职业名称
        self.blood_type = None  # 血型
        self.name = None  # 真实名
        self.sign_name = None  # 个性签名
        self.personal_desc = None  # 个人说明
        self.reg_city = None  # 城市
        self.graduateschool = None  # 毕业院校
        self.zodiac = None  # 生肖
        self.constallation = None  # 星座
        self.birthday = None  # 出生年月
        self.user_photo = None  # 头像
        self.account_reg_date = None  # 账号注册时间
        self.last_login_time = None  # 账号最后登录时间
        self.latest_mod_time = None  # 账号最后更新时间
        self.add_friedn_time = None  # 添加好友时间
        self.blacklist_time = None  # 拉黑时间
        self.delete_friend_time = None  # 删除好友时间
        self.friend_relation_type = None  # 好友关系类型

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.friend_id, self.friend_account, self.friend_nickname, self.friend_group,
                self.friend_remark, self.area, self.city_code, self.fixed_phone, self.msisdn,
                self.email_account, self.certificate_type, self.certificate_code, self.sexcode,
                self.age, self.postal_address, self.postal_code, self.occupation_name, self.blood_type,
                self.name, self.sign_name, self.personal_desc, self.reg_city, self.graduateschool,
                self.zodiac, self.constallation, self.birthday, self.delete_status, self.delete_time,
                self.user_photo, self.account_reg_date, self.last_login_time, self.latest_mod_time,
                self.add_friedn_time, self.blacklist_time, self.delete_friend_time, self.friend_relation_type)

class Group(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Group, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.group_num = None  # 群组ID
        self.group_name = None  # 群组名称
        self.friend_id = None  # 创建者ID
        self.friend_account = None  # 创建者账号
        self.group_owner_nickname = None  # 创建者昵称
        self.group_member_count = None  # 人数
        self.group_max_member_cout = None  # 最大成员数
        self.group_notice = None  # 群公告
        self.group_description = None  # 群简介
        self.group_owner_internal_id = None  # 群主ID
        self.group_owner = None  # 群主账号
        self.group_admin_nickname = None  # 群主昵称
        self.create_time = None  # 群创建时间
        self.groupphoto = None  # 群头像

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.group_num, self.group_name, self.friend_id, self.friend_account,
                self.group_owner_nickname, self.group_member_count, self.group_max_member_cout,
                self.group_notice, self.group_description, self.delete_status, self.delete_time,
                self.group_owner_internal_id, self.group_owner, self.group_admin_nickname,
                self.create_time, self.groupphoto)


class GroupMember(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(GroupMember, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.group_num = None  # 群组ID
        self.group_name = None  # 群组名称
        self.friend_id = None  # 好友用户ID
        self.friend_account = None  # 好友账号
        self.friend_nickname = None  # 好友昵称
        self.friend_remark = None  # 好友备注
        self.area = None  # 国家代号
        self.city_code = None  # 行政区划
        self.fixed_phone = None  # 电话号码
        self.msisdn = None  # 手机号码
        self.email_account = None  # 邮箱地址
        self.certificate_type = None  # 对象证件类型
        self.certificate_code = None  # 对象证件号码
        self.sexcode = None  # 性别
        self.age = None  # 年龄
        self.postal_address = None  # 联系地址
        self.postal_code = None  # 邮政编码
        self.occupation_name = None  # 职业名称
        self.blood_type = None  # 血型
        self.name = None  # 真实名
        self.sign_name = None  # 个性签名
        self.personal_desc = None  # 个人说明
        self.reg_city = None  # 城市
        self.graduateschool = None  # 毕业院校
        self.zodiac = None  # 生肖
        self.constallation = None  # 星座
        self.birthday = None  # 出生年月
        self.user_photo = None  # 头像
        self.account_reg_date = None  # 账号注册时间
        self.last_login_time = None  # 账号最后登录时间
        self.latest_mod_time = None  # 账号最后更新时间
        self.last_time = None  # 最后发言时间

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.group_num, self.group_name, self.friend_id, self.friend_account, self.friend_nickname,
                self.friend_remark, self.area, self.city_code, self.fixed_phone, self.msisdn,
                self.email_account, self.certificate_type, self.certificate_code, self.sexcode,
                self.age, self.postal_address, self.postal_code, self.occupation_name, self.blood_type,
                self.name, self.sign_name, self.personal_desc, self.reg_city, self.graduateschool,
                self.zodiac, self.constallation, self.birthday, self.delete_status, self.delete_time,
                self.user_photo, self.account_reg_date, self.last_login_time, self.latest_mod_time,
                self.last_time)


class FriendMessage(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(FriendMessage, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.regis_nickname = None  # 昵称
        self.friend_id = None  # 好友用户ID
        self.friend_account = None  # 好友账号
        self.friend_nickname = None  # 好友昵称
        self.content = None  # 即时信息内容
        self.mail_send_time = None  # 发送时间
        self.local_action = None  # 本地动作
        self.talk_id = None  # 聊天记录ID
        self.mainfile = None  # 文件的显示名称
        self.media_type = None  # 媒体类型
        self.city_code = None  # 地点名称
        self.company_address = None  # 详细地址
        self.longitude = None  # 经度
        self.latitude = None  # 纬度
        self.above_sealevel = None  # 海拔

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.regis_nickname, self.friend_id, self.friend_account, self.friend_nickname,
                self.content, self.mail_send_time, self.local_action, self.talk_id, self.delete_status,
                self.delete_time, self.mainfile, self.media_type, self.city_code, self.company_address,
                self.longitude, self.latitude, self.above_sealevel)


class GroupMessage(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(GroupMessage, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.group_num = None  # 群组ID
        self.group_name = None  # 群组名称
        self.friend_id = None  # 好友用户ID
        self.friend_account = None  # 好友账号
        self.friend_nickname = None  # 好友昵称
        self.content = None  # 即时信息内容
        self.mail_send_time = None  # 发送时间
        self.local_action = None  # 本地动作
        self.talk_id = None  # 聊天记录ID
        self.mainfile = None  # 文件的显示名称
        self.media_type = None  # 媒体类型
        self.city_code = None  # 地点名称
        self.company_address = None  # 详细地址
        self.longitude = None  # 经度
        self.latitude = None  # 纬度
        self.above_sealevel = None  # 海拔

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.group_num, self.group_name, self.friend_id, self.friend_account, self.friend_nickname,
                self.content, self.mail_send_time, self.local_action, self.talk_id, self.delete_status,
                self.delete_time, self.mainfile, self.media_type, self.city_code, self.company_address,
                self.longitude, self.latitude, self.above_sealevel)


class Feed(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Feed, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.friend_id = None  # 发布者id
        self.friend_account = None  # 发布者账号
        self.friend_nickname = None  # 发布者昵称
        self.weibo_message_type = None  # 消息类型
        self.mail_send_time = None  # 时间
        self.weibo_message = None  # 内容
        self.weibo_reply_counter = None  # 评论数
        self.weibo_retweet_counter = None  # 转发数
        self.weibo_like_counter = None  # 点赞数
        self.mblog_id = None  # 动态消息id
        self.relevant_mblog_id = None  # 相关动态消息id
        self.idroot_mblog_id = None  # 原始动态消息id
        self.group_num = None  # 群组id
        self.media_type = None  # 媒体类型
        self.city_code = None  # 地点名称
        self.company_address = None  # 详细地址
        self.longitude = None  # 经度
        self.latitude = None  # 纬度
        self.above_sealevel = None  # 海拔
        self.fans_counter = None  # 访问人数
        self.visits = None  # 阅读次数
        self.dual_time = None  # 阅读时长

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.friend_id, self.friend_account, self.friend_nickname, self.weibo_message_type,
                self.mail_send_time, self.weibo_message, self.weibo_reply_counter, self.weibo_retweet_counter,
                self.weibo_like_counter,self.mblog_id, self.relevant_mblog_id, self.idroot_mblog_id,
                self.group_num, self.delete_status, self.delete_time, self.media_type, self.city_code,
                self.company_address, self.longitude, self.latitude, self.above_sealevel, self.fans_counter,
                self.visits, self.dual_time)


class Search(Column):
    def __init__(self, collect_target_id, contact_account_type, account_id, account):
        super(Search, self).__init__(collect_target_id, contact_account_type, account_id, account)
        self.create_time = None  # 时间
        self.keyword = None  # 关键字

    def get_values(self):
        return (self.collect_target_id, self.contact_account_type, self.account_id, self.account,
                self.create_time, self.keyword, self.delete_status, self.delete_time)


class GenerateBcp(object):
    def __init__(self, bcp_path, cache_db, bcp_db, collect_target_id, contact_account_type):
        self.bcp_path = bcp_path
        self.cache_db = cache_db
        self.bcp_db = bcp_db
        self.collect_target_id = collect_target_id
        self.contact_account_type = contact_account_type
        self.cache_path = os.path.join(self.bcp_path, 'wechat')
        self.im = IM()

    def generate(self):
        self.im.db_create(self.bcp_db)
        self.db = sqlite3.connect(self.cache_db)
        self._generate_account()
        self._generate_friend()
        self._generate_group()
        self._generate_group_member()
        self._generate_message()
        self._generate_feed()
        self._generate_search()
        self.db.close()
        self.im.db_close()

    def _generate_account(self):
        if canceller.IsCancellationRequested:
            return
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
            if canceller.IsCancellationRequested:
                break
            account = Account(self.collect_target_id, self.contact_account_type, row[0], row[2])
            account.delete_status = self._convert_delete_status(row[16])
            account.regis_nickname = row[1]
            account.password = row[3]
            account.area = row[9]
            account.city_code = row[11]
            account.msisdn = row[5]
            account.email_account = row[6]
            account.sexcode = self._convert_sexcode(row[7])
            account.age = self._convert_sexcode(row[8])
            account.postal_address = row[12]
            account.sign_name = row[14]
            account.birthday = row[13]
            account.user_photo = row[4]
            self.im.db_insert_table_account(account)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_friend(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = '''select account_id, friend_id, nickname, remark, photo, type, telephone, email, gender, 
                        age, address, birthday, signature, source, deleted, repeated
                 from friend'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            friend = Friend(self.collect_target_id, self.contact_account_type, row[0], None)
            friend.delete_status = self._convert_delete_status(row[14])
            friend.friend_id = row[1]
            friend.friend_nickname = row[2]
            friend.friend_remark = row[3]
            friend.msisdn = row[6]
            friend.email_account = row[7]
            friend.sexcode = self._convert_sexcode(row[8])
            friend.age = row[9]
            friend.postal_address = row[10]
            friend.sign_name = row[12]
            friend.birthday = row[11]
            friend.user_photo = row[4]
            self.im.db_insert_table_friend(friend)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_group(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = '''select account_id, chatroom_id, name, photo, type, notice, description, creator_id, 
                        owner_id, member_count, max_member_count, create_time, source, deleted, repeated
                 from chatroom'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            group = Group(self.collect_target_id, self.contact_account_type, row[0], None)
            group.delete_status = self._convert_delete_status(row[14])
            group.group_num = row[1]
            group.group_name = row[2]
            group.friend_id = row[7]
            group.group_owner_nickname = None
            group.group_member_count = row[9]
            group.group_max_member_cout = row[10]
            group.group_notice = row[5]
            group.group_description = row[6]
            group.group_owner_internal_id = row[8]
            group.group_admin_nickname = None
            group.create_time = row[11]
            group.groupphoto = row[3]
            self.im.db_insert_table_group(group)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_group_member(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = '''select account_id, chatroom_id, member_id, display_name, photo, telephone, email, 
                        gender, age, address, birthday, signature, source, deleted, repeated
                 from chatroom_member'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            gm = GroupMember(self.collect_target_id, self.contact_account_type, row[0], None)
            gm.delete_status = self._convert_delete_status(row[13])
            gm.group_num = row[1]
            gm.group_name = None
            gm.friend_id = row[2]
            gm.friend_nickname = None
            gm.friend_remark = None
            gm.fixed_phone = row[5]
            gm.email_account = row[6]
            gm.sexcode = self._convert_sexcode(row[7])
            gm.age = row[8]
            gm.postal_address = row[9]
            gm.sign_name = row[11]
            gm.birthday = row[10]
            gm.user_photo = row[4]
            self.im.db_insert_table_group_member(gm)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_message(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = '''select account_id, talker_id, talker_name, sender_id, sender_name, is_sender, msg_id, type, 
                        content, media_path, send_time, extra_id, status, talker_type, source, deleted, repeated
                 from message'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            talker_type = row[13]
            if talker_type == model_im.CHAT_TYPE_GROUP:
                message = GroupMessage(self.collect_target_id, self.contact_account_type, row[0], None)
                message.delete_status = self._convert_delete_status(row[15])
                message.group_num = row[1]
                message.group_name = row[2]
                message.friend_id = row[3]
                message.friend_nickname = row[4]
                message.content = row[8]
                message.mail_send_time = row[10]
                message.local_action = self._convert_local_action(row[5])
                message.talk_id = row[6]
                message.media_type = self._convert_media_type(row[7])
                if row[7] == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                    location = self._get_location_from_id(row[11])
                    if location is not None:
                        message.company_address = location.get('address')
                        message.longitude = location.get('longitude')
                        message.latitude = location.get('latitude')
                        message.above_sealevel = location.get('elevation')
                self.im.db_insert_table_group_message(message)
            else:
                message = FriendMessage(self.collect_target_id, self.contact_account_type, row[0], None)
                message.delete_status = self._convert_delete_status(row[15])
                message.regis_nickname = None  # 昵称
                message.friend_id = row[1]
                message.friend_nickname = row[2]
                message.content = row[8]
                message.mail_send_time = row[10]
                message.local_action = self._convert_local_action(row[5])
                message.talk_id = row[6]
                message.media_type = self._convert_media_type(row[7])
                if row[7] == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                    location = self._get_location_from_id(row[11])
                    if location is not None:
                        message.company_address = location.get('address')
                        message.longitude = location.get('longitude')
                        message.latitude = location.get('latitude')
                        message.above_sealevel = location.get('elevation')
                self.im.db_insert_table_friend_message(message)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_feed(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = '''select account_id, sender_id, type, content, media_path, urls, preview_urls, 
                        attachment_title, attachment_link, attachment_desc, send_time, likes, 
                        comments, location, source, deleted, repeated
                 from feed '''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            like_count = None
            if row[11] is not None:
                try:
                    like_count = len(row[11].split(','))
                except Exception as e:
                    pass

            comment_count = None
            if row[12] is not None:
                try:
                    comment_count = len(row[12].split(','))
                except Exception as e:
                    pass

            feed = Feed(self.collect_target_id, self.contact_account_type, row[0], None)
            feed.delete_status = self._convert_delete_status(row[15])
            feed.friend_id = row[1]
            feed.friend_nickname = None
            feed.mail_send_time = row[10]
            feed.weibo_message = row[3]
            feed.weibo_reply_counter = comment_count
            feed.weibo_like_counter = like_count
            location = self._get_location_from_id(row[13])
            if location is not None:
                feed.company_address = location.get('address')
                feed.longitude = location.get('longitude')
                feed.latitude = location.get('latitude')
                feed.above_sealevel = location.get('elevation')
            self.im.db_insert_table_feed(feed)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _generate_search(self):
        if canceller.IsCancellationRequested:
            return
        cursor = self.db.cursor()
        sql = '''select account_id, key, create_time, source, deleted, repeated
                 from search'''
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if canceller.IsCancellationRequested:
                break
            search = Search(self.collect_target_id, self.contact_account_type, row[0], None)
            search.delete_status = self._convert_delete_status(row[4])
            search.create_time = row[2]
            search.keyword = row[1]
            self.im.db_insert_table_search(search)
            row = cursor.fetchone()
        self.im.db_commit()
        cursor.close()

    def _get_location_from_id(self, location_id):
        if location_id in [None, '']:
            return None

        location = None
        cursor = self.db.cursor()
        sql = '''select latitude, longitude, elevation, address, timestamp, source, deleted, repeated
                 from location where location_id='{0}' '''.format(location_id)
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        if row != None:
            location = {}
            location['latitude'] = row[0]
            location['longitude'] = row[1]
            location['elevation'] = row[2]
            location['address'] = row[3]

        cursor.close()
        return location

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
