# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('unity_c37r')
except:
    pass
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

from unity_c37r import mapping_file_with_copy


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


def exc():
    pass
    # traceback.print_exc()

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

SQL_CREATE_TABLE_SMS = '''
    create table if not exists sms(
        msg_id              TEXT, 
        sim_id              INT,
        sender_phonenumber  TEXT,
        sms_or_mms          TEXT,
        read                INT DEFAULT 0,
        type                INT,
        suject              TEXT,
        body                TEXT,
        send_time           INT,
        deliverd            INT,
        is_sender           INT,
        source              TEXT,
        deleted             INT DEFAULT 0, 
        repeated            INT DEFAULT 0)
    '''

SQL_INSERT_TABLE_SMS = ''' 
    insert into sms(
        msg_id, 
        sim_id,
        sender_phonenumber,
        sms_or_mms,
        read,
        type,
        suject,
        body,
        send_time,
        deliverd,
        is_sender,
        source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


class Model_SMS(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None
        

    def db_create(self, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_create_table()
        self.db_commit()

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None   
            
    def db_create_table(self):
        if self.db_cmd is not None:
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
        self.db_insert_table(SQL_INSERT_TABLE_SIM_CARDS, column.get_values())


    def db_insert_table_sms(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_SMS, column.get_values())


    def db_insert_table_version(self, key, version):
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
        sql = 'select key,version from version'
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
            if isinstance(value, str):
                # 过滤控制字符, 防止断言失败
                value = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', value)               
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
        self.msg_id             = None  # 消息ID[INT]
        self.sim_id             = None  # SIM 卡 ID[INT]
        self.sender_phonenumber = None  # 发送者ID[TEXT]
        self.sms_or_mms         = None  # 消息ID[TEXT]
        self.read               = None  # 消息ID[TEXT]
        self.type               = None  # 消息类型[INT], SMS_TYPE
        self.suject             = None  # 主题,        一般是彩信 mms 才有[TEXT]
        self.body               = None  # 内容[TEXT]
        self.send_time          = None  # 发送时间[INT]
        self.deliverd           = None  # 发送时间[INT]
        self.is_sender          = None  # 自己是否为发送发[INT]
        # self.media_path       = None  # 媒体文件地址[TEXT]

    def get_values(self):
        return (
                self.msg_id, 
                self.sim_id, 
                self.sender_phonenumber, 
                self.sms_or_mms, 
                self.read,
                self.type, 
                self.suject,   
                self.body,  
                self.send_time, 
                self.deliverd, 
                self.is_sender, 
            ) + super(SMS, self).get_values()

class GenerateModel(object):
    def __init__(self, cache_db, cachepath):
        self.cache_db = cache_db
        self.cachepath = cachepath

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.cache_db)
        self.cursor = self.db.cursor()

        # 跨库连表
        try:
            db_path_calls = 'CALLS\calls.db'
            BASE_DIR      = os.path.dirname(self.cachepath)
            raw_calls_path = os.path.join(BASE_DIR, db_path_calls)
            calls_path = os.path.join(self.cachepath, 'calls.db')
            mapping_file_with_copy(raw_calls_path, calls_path)
            self.db.execute("ATTACH DATABASE '{}' AS calls".format(calls_path))
            models.extend(self._get_sms_models())
        except:
            exc()
            models.extend(self._get_sms_models_no_join())

        self.cursor.close()
        self.db.close()
        return models

    def _get_sms_models(self):
        models = []
        sql = '''
            select 
                b.body, 
                b.type,
                b.deliverd,
                b.send_time,
                b.read,
                
                c.name,

                b.source,
                b.deleted
            from 
                sms            as b
            left join 
                calls.contacts as c
            on 
                b.sender_phonenumber = c.phone_number
        '''
        '''     sms.Body
                sms.Folder
                sms.Delivered
                sms.Read
                sms.TimeStamp
                sms.SourceFile
        
        table - sms
            msg_id            
            sim_id            
            sender_phonenumber
            sms_or_mms   
            read     
            type              
            suject            
            body              
            send_time       
            deliverd  
            is_sender         
            source            
            deleted           
            repeated               
            
            calls.db - contacts
            RecNo	FieldName	
            1	raw_contact_id	INTEGER
            2	mimetype_id	INTEGER
            3	mail	TEXT
            4	company	TEXT
            5	title	TEXT
            6	last_time_contact	INTEGER
            7	last_time_modify	INTEGER
            8	times_contacted	INTEGER
            9	phone_number	TEXT
            10	name	TEXT
            11	address	TEXT
            12	notes	TEXT
            13	telegram	TEXT
            14	head_pic	BLOB
            15	source	TEXT
            16	deleted	INTEGER
            17	repeated	INTEGER

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
            return []
        while row is not None:
            if canceller.IsCancellationRequested:
                return
            sms = Generic.SMS()
            if row[0] is not None:
                sms.Body.Value = row[0]
            if row[1] in range(7):
                sms.Folder.Value = SMS_TYPE_TO_FOLDER[row[1]]
            if row[2] is not None:
                sms.Delivered.Value = self._get_timestamp(row[2])
            if row[3] is not None:
                sms.TimeStamp.Value = self._get_timestamp(row[3])
            if row[4] == 0:
                sms.Status.Value = MessageStatus.Unread
            elif row[4] == 1:
                sms.Status.Value = MessageStatus.Read
            else:
                sms.Status.Value = self._convert_sms_type(row[1])
            #     sms.PhoneNumber.Value = row[4]
            if row[-2] is not None:
                sms.SourceFile.Value = self._get_source_file(row[-2])
            if row[-1] is not None:
                sms.Deleted = self._convert_deleted_status(row[-1])
            models.append(sms)
            row = self.cursor.fetchone()

        return models        


    def _get_sms_models_no_join(self):
        models = []
        sql = '''
            select 
                body, 
                type,
                deliverd,
                send_time,
                suject,
                read,

                source,
                deleted
            from 
                sms
        '''
        '''     sms.Body
                sms.Folder
                sms.Delivered
                sms.Read
                sms.TimeStamp
                sms.SourceFile
        
        table - sms
                msg_id            
                sim_id            
                sender_phonenumber
                sms_or_mms        
                type              
                suject            
                body              
                send_time       
                deliverd  
                is_sender         
                source            
                deleted           
                repeated               
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
            if row[0] is not None:
                sms.Body.Value = row[0]
            if row[1] in range(7):
                sms.Folder.Value = SMS_TYPE_TO_FOLDER[row[1]]
            if row[2] is not None:
                sms.Delivered.Value = self._get_timestamp(row[2])
            if row[3] is not None:
                sms.TimeStamp.Value = self._get_timestamp(row[3])
            if row[4] == 0:
                sms.Status.Value = MessageStatus.Unread
            elif row[4] == 1:
                sms.Status.Value = MessageStatus.Read
            else:
                sms.Status.Value = self._convert_sms_type(row[1])
            if row[-2] is not None:
                sms.SourceFile.Value = self._get_source_file(row[-2])
            if row[-1] is not None:
                sms.Deleted = self._convert_deleted_status(row[-1])

            models.append(sms)
            row = self.cursor.fetchone()
        return models        


    def _get_timestamp(self, timestamp):
        try:
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
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
        if sms_type in [2,4]:
            return MessageStatus.Sent
        elif sms_type == 3:
            return MessageStatus.Unsent
        elif sms_type == 1:
            return MessageStatus.Unread
        return MessageStatus.Default
