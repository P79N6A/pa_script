# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite

import os
import time
import sqlite3
import traceback
import re
import shutil
import hashlib


DEBUG = True
DEBUG = False

def exc():
    if DEBUG:
        TraceService.Trace(TraceLevel.Error, "解析出错: 短信 {}".format(traceback.format_exc()))
    else:
        pass    

SMS_TYPE_ALL    = 0
SMS_TYPE_INBOX  = 1
SMS_TYPE_SENT   = 2
SMS_TYPE_DRAFT  = 3
SMS_TYPE_OUTBOX = 4
SMS_TYPE_FAILED = 5
SMS_TYPE_QUEUED = 6

SMS_TYPE_TO_FOLDER = (
    '',
    '收件箱',
    '正在发送',
    '草稿箱',
    '发件箱',
    '发送失败',
    '',
)

VERSION_VALUE_DB = 1
VERSION_KEY_DB  = 'db'
VERSION_KEY_APP = 'app'
        
SQL_CREATE_TABLE_SIM_CARDS = '''
    create table if not exists sim_cards(
        sim_id        INTEGER DEFAULT 0,
        number        TEXT,
        sync_enabled  INTEGER DEFAULT 1,
        source        TEXT,
        deleted       INT DEFAULT 0, 
        repeated      INT DEFAULT 0)                                
    '''

SQL_INSERT_TABLE_SIM_CARDS = '''
    insert into sim_cards(
        sim_id,
        number,
        sync_enabled,
        source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?)
    '''

''' bcp: 
    6.1.9　短信记录信息(WA_MFORENSICS_010700)

    字段说明（短信记录信息）
    0	手机取证采集目标编号	COLLECT_TARGET_ID
    1	本机号码	MSISDN
    2	对方号码	RELATIONSHIP_ACCOUNT
    3	联系人姓名	RELATIONSHIP_NAME
    4	本地动作	LOCAL_ACTION            标示本机是收方还是发方，01接收方、02发送方、99其他
    5	发送时间	MAIL_SEND_TIME
    6	短消息内容	CONTENT
    7	查看状态	MAIL_VIEW_STATUS        0未读，1已读，9其它
    8	存储位置	MAIL_SAVE_FOLDER        01收件箱、02发件箱、03草稿箱、04垃圾箱、99其他
    9	加密状态	PRIVACYCONFIG
    10	删除状态	DELETE_STATUS
    11	删除时间	DELETE_TIME             19700101000000基准
    12	拦截状态	INTERCEPT_STATE
table-sms TYPE
    SMS_TYPE_ALL    = 0
    SMS_TYPE_INBOX  = 1
    SMS_TYPE_SENT   = 2
    SMS_TYPE_DRAFT  = 3
    SMS_TYPE_OUTBOX = 4
    SMS_TYPE_FAILED = 5
    SMS_TYPE_QUEUED = 6    
'''

SQL_CREATE_TABLE_SMS = '''
    create table if not exists sms(
        _id                 TEXT, 
        sim_id              INT,
        sender_phonenumber  TEXT,
        sender_name         TEXT,
        read_status         INT DEFAULT 0,
        type                INT DEFAULT 0,
        suject              TEXT,
        body                TEXT,
        send_time           INT,
        delivered_date      INT,
        is_sender           INT,
        source              TEXT,
        deleted             INT DEFAULT 0, 
        repeated            INT DEFAULT 0,
        recv_phonenumber    TEXT,
        recv_name           TEXT,
        smsc                TEXT
        )
    '''
SQL_INSERT_TABLE_SMS = ''' 
    insert into sms(
        _id, 
        sim_id,
        sender_phonenumber,
        sender_name,
        read_status,
        type,
        suject,
        body,
        send_time,
        delivered_date,
        is_sender,
        source, 
        deleted, 
        repeated,
        recv_phonenumber,
        recv_name,
        smsc
        ) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
               ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert or REPLACE into version(key, version) values(?, ?)'''


class Model_SMS(object):
    def __init__(self):
        # self.db = None
        # self.cursor = None
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            # os.remove(db_path)
            try:
                os.remove(db_path)
            except:
                print('error with remove sms db_path:', db_path)
                exc()
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            self.db_trans = self.db.BeginTransaction()
        except:
            exc()
            
        self.db_create_table()
        self.db_commit()

    def db_commit(self):
        # if self.db is not None:
        #     self.db.commit()
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_close(self):
        # if self.cursor is not None:
        #     self.cursor.close()
        #     self.cursor = None
        # if self.db is not None:
        #     self.db.close()
        #     self.db = None
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None   
            
    def db_create_table(self):
        # if self.cursor is not None:
        if self.db_cmd is not None:
            # self.cursor.execute(SQL_CREATE_TABLE_SMS)
            # self.cursor.execute(SQL_CREATE_TABLE_SIM_CARDS)
            # self.cursor.execute(SQL_CREATE_TABLE_VERSION)
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SMS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SIM_CARDS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
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

    def db_insert_table_sim_cards(self, column):
        # if self.cursor is not None:
        #     self.cursor.execute(SQL_INSERT_TABLE_SIM_CARDS, column.get_values())
        self.db_insert_table(SQL_INSERT_TABLE_SIM_CARDS, column.get_values())


    def db_insert_table_sms(self, column):
        # if self.cursor is not None:
        #     try:
        #         self.cursor.execute(SQL_INSERT_TABLE_SMS, column.get_values())
        #     except Exception as e:
        #         pass
        self.db_insert_table(SQL_INSERT_TABLE_SMS, column.get_values())


    def db_insert_table_version(self, key, version):
        # if self.cursor is not None:
        #     self.cursor.execute(SQL_INSERT_TABLE_VERSION, (key, version))
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    '''
    版本检测分为两部分
    如果中间数据库结构改变，会修改db_version
    如果app增加了新的内容，需要修改app_version
    只有db_version和app_version都没有变化时，才不需要重新解析
    '''
    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key, version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            if row[0] == VERSION_KEY_DB and row[1] == VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == VERSION_KEY_APP and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)

class Column(object):
    def __init__(self):
        self.source   = ''
        self.deleted  = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            self.__dict__[name] = value

    def get_values(self):
        return (self.source, self.deleted, self.repeated)

class Sim_cards(Column):
    def __init__(self):
        super(Sim_cards, self).__init__()
        self.sim_id       = None
        self.number       = None  # 手机号
        self.sync_enabled = None  # 是否同步

    def get_values(self):
        return (
                self.sim_id,
                self.number,
                self.sync_enabled,
            ) + super(Sim_cards, self).get_values()

class SMS(Column):
    def __init__(self):
        super(SMS, self).__init__()
        self._id                = None  # 消息ID[INT]
        self.sim_id             = None  # SIM 卡 ID[INT]
        self.sender_phonenumber = None  # 发送者手机号码[TEXT]
        self.sender_name        = None  # 发送者姓名[TEXT]
        self.read_status        = None  # 读取状态[INT], 1 已读, 0 未读
        self.type               = None  # 消息类型[INT], SMS_TYPE
        self.suject             = None  # 主题, 一般是彩信 mms 才有[TEXT]
        self.body               = None  # 内容[TEXT]
        self.send_time          = None  # 发送时间[INT]
        self.delivered_date     = None  # 送达时间
        self.is_sender          = None  # 自己是否为发送方[INT]
        self.recv_phonenumber   = None  # 接受者手机号码[TEXT]
        self.recv_name          = None  # 接受者姓名[TEXT]        
        self.smsc               = None  # 短信服务中心号码[TEXT]        

    def get_values(self):
        return (
                self._id, 
                self.sim_id, 
                self.sender_phonenumber, 
                self.sender_name, 
                self.read_status,
                self.type, 
                self.suject,   
                self.body,  
                self.send_time, 
                self.delivered_date, 
                self.is_sender, 
                self.source, 
                self.deleted, 
                self.repeated,
                self.recv_phonenumber,
                self.recv_name,
                self.smsc
            )

class GenerateModel(object):
    def __init__(self, cache_db, cachepath=None):
        self.cache_db = cache_db
        self.cachepath = cachepath

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.cache_db)
        # self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_path))
        self.cursor = self.db.cursor()

        models.extend(self._get_sms_models())

        self.cursor.close()
        self.db.close()
        return models

    def _get_sms_models(self):
        models = []
        sql = ''' select * from sms '''

        ''' sms.Body
            sms.Folder
            sms.Delivered
            sms.Read
            sms.TimeStamp
            sms.SourceFile

        table - sms
            0    _id              TEXT, 
            1    sim_id              INT,
            2    sender_phonenumber  TEXT,
            3    sender_name         TEXT,
            4    read_status         INT DEFAULT 0,
            5    type                INT,
            6    suject              TEXT,
            7    body                TEXT,
            8    send_time           INT,
            9    delivered_date      INT,
            10    is_sender           INT,
            11    source              TEXT,
            12    deleted             INT DEFAULT 0, 
            13    repeated            INT DEFAULT 0,

            14    recv_phonenumber  TEXT,
            15    recv_name         TEXT,
            16    smsc               TEXT

            SMS_TYPE_ALL    = 0
            SMS_TYPE_INBOX  = 1
            SMS_TYPE_SENT   = 2
            SMS_TYPE_DRAFT  = 3
            SMS_TYPE_OUTBOX = 4
            SMS_TYPE_FAILED = 5
            SMS_TYPE_QUEUED = 6                        
        '''
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except:
            exc()
            return 
        while row is not None:
            if canceller.IsCancellationRequested:
                return            
            sms = Generic.SMS()
            if row[7] is not None:
                sms.Body.Value = row[7]
            if row[5] in range(7):
                sms.Folder.Value = SMS_TYPE_TO_FOLDER[row[5]]
            if row[9] is not None:
                sms.Delivered.Value = self._get_timestamp(row[9])
            if row[8] is not None:
                sms.TimeStamp.Value = self._get_timestamp(row[8])

            # 注意优先级  row[4] read_status, row[5]: type
            sms.Status.Value = MessageStatus.Read if row[4] == 1 else MessageStatus.Unread
            if row[5] in [2, 3, 4]:
                sms.Status.Value = self._convert_sms_type(row[5]) 

            # 发件人
            party = Generic.Party()
            # party.Role.Value = PartyRole.From if row[10] != 1 else PartyRole.To
            party.Role.Value = PartyRole.From
            if row[2] is not None:
                party.Identifier.Value = row[2] # sender_phonenumber
            if row[3] is not None:
                party.Name.Value = row[3]       # sender_name
            sms.Parties.Add(party)     
            # 收件人
            party = Generic.Party()
            # party.Role.Value = PartyRole.To if row[10] != 1 else PartyRole.From
            party.Role.Value = PartyRole.To
            if row[14] is not None:
                party.Identifier.Value = row[14]   # recv_phonenumber
            if row[15] is not None:                                     
                party.Name.Value = row[15]        # recv_name     
            sms.Parties.Add(party)               

            if row[16] is not None:                                     
                sms.SMSC.Value = row[16]  

            if row[11] is not None:
                sms.SourceFile.Value = self._get_source_file(row[11])
            if row[12] is not None:
                sms.Deleted = self._convert_deleted_status(row[12])

            models.append(sms)
            row = self.cursor.fetchone()
        return models        

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (long, float, str, Int64)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, (int, Int64)) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    def _get_source_file(self, source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return ''    
    
    @staticmethod
    def _convert_sms_type(sms_type):
        '''
        SMS_TYPE_ALL    = 0
        SMS_TYPE_INBOX  = 1
        SMS_TYPE_SENT   = 2
        SMS_TYPE_DRAFT  = 3
        SMS_TYPE_OUTBOX = 4
        SMS_TYPE_FAILED = 5
        SMS_TYPE_QUEUED = 6 
        '''
        if sms_type in [2,4]:  # 发件箱
            return MessageStatus.Sent
        elif sms_type == 3:    # 草稿箱
            return MessageStatus.Unsent
        # elif sms_type == 1:    # 未读
        #     return MessageStatus.Unread
        # return MessageStatus.Default
