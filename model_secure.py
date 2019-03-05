# -*- coding: utf-8 -*-

from PA_runtime import *

import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import os
import sqlite3
import json
import time

import System.Data.SQLite as SQLite
import PA.InfraLib.ModelsV2.Base.Call as Call
import PA.InfraLib.ModelsV2.Secure as Secure
import PA.InfraLib.ModelsV2.Base.Contact as Contact
import PA.InfraLib.ModelsV2.CommonEnum.CallType as CallType
import PA.InfraLib.ModelsV2.Secure.CallBlockingType as CallBlockingType
import PA.InfraLib.ModelsV2.Secure.CallBlocking as CallBlocking
from ScriptUtils import CASE_NAME, exc, tp, DEBUG



VERSION_VALUE_DB = 1
VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

CALL_RECORD_TYPE_AD           = 1    # 广告推销
CALL_RECORD_TYPE_DEFRAUD      = 2    # 诈骗电话
CALL_RECORD_TYPE_EXPRESS      = 3    # 快递送餐
CALL_RECORD_TYPE_HARASS       = 4    # 故意骚扰
CALL_RECORD_TYPE_INSURANCE    = 5    # 保险理财
CALL_RECORD_TYPE_INTERMEDIARY = 6    # 房产中介
CALL_RECORD_TYPE_RECRIT       = 7    # 招聘猎头
CALL_RECORD_TYPE_RINGOUT      = 8    # 响一声
CALL_RECORD_TYPE_TEXI         = 9    # 出租车

CALLBLOCK_TYPE_CONVERTER = {
    1 : CallBlockingType.Advertisement,
    2 : CallBlockingType.Defraud,
    3 : CallBlockingType.Express,
    4 : CallBlockingType.Harass,
    5 : CallBlockingType.Insurance,
    6 : CallBlockingType.Intermediary,
    7 : CallBlockingType.Recruit,
    8 : CallBlockingType.Ringout,
    9 : CallBlockingType.Texi,
}

SQL_CREATE_TABLE_ACCOUNT = '''
    create table if not exists account(
        account_id      INTEGER,
        nickname        TEXT,
        username        TEXT,
        password        TEXT,
        photo           TEXT,
        telephone       TEXT,
        address         TEXT,
        source          TEXT,
        deleted         INTEGER DEFAULT 0,
        repeated        INTEGER DEFAULT 0
    )'''

SQL_INSERT_TABLE_ACCOUNT = '''
    insert into account(
        account_id, nickname ,username,
        password, photo, telephone, address,
        source, deleted, repeated)
    values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_CHARGE = '''
    create table if not exists charge(
        id              INTEGER,
        begin_time      INTEGER,
        end_time        INTEGER,
        begin_level     TEXT,
        end_level        TEXT,
        source          TEXT,
        deleted         INTEGER,
        repeated        INTEGER
    )'''

SQL_INSERT_TABLE_CHARGE = '''
    insert into charge(
        id, begin_time, end_time, begin_level,
        end_level, source, deleted, repeated)
    values(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_BLACKLIST = '''
    create table if not exists blacklist(
        id               INTEGER,
        name             TEXT,
        phone_number     TEXT,
        add_date         INTEGER,
        source           TEXT,
        deleted          INTEGER  DEFAULT 0,
        repeated         INTEGER DEFAULT 0
    )'''

SQL_INSERT_TABLE_BLACKLIST = '''
    insert into blacklist(
        id, name, phone_number, add_date,
        source, deleted, repeated)
    values(?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_BLOCKEDSMS = '''
    create table if not exists blocked_sms(
        id              INTEGER,
        content         TEXT,
        name            TEXT,
        phone_number    TEXT,
        block_time      INTEGER,
        source          TEXT,
        deleted         INTEGER DEFAULT 0,
        repeated        INTEGER DEFAULT 0
    )'''

SQL_INSERT_TABLE_BLOCKEDSMS = '''
    insert into blocked_sms(
        id, content, name, phone_number,
        block_time, source, deleted, repeated)
    values(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_CALLRECORD = '''
    create table if not exists callrecord(
        _id           INTEGER,
        phone_number  TEXT,
        date          INTEGER,
        call_type     INTEGER DEFAULT 0,
        source        TEXT,
        deleted       INT DEFAULT 0, 
        repeated      INT DEFAULT 0)                                
    '''

SQL_INSERT_TABLE_CALLRECORD = '''
    insert into callrecord(
        _id,           
        phone_number, 
        date,         
        call_type,         
        source,       
        deleted,      
        repeated
        ) 
        values(?, ?, ?, ?, ?, 
               ?, ?)
    '''

SQL_CREATE_TABLE_WIFI_SIGNAL = '''
    create table if not exists wifi_signal(
        id            INTEGER,
        ssid          TEXT,
        bssid         TEXT,
        first_time    INTEGER,
        last_time     INTEGER,
        source        TEXT,
        deleted       INT DEFAULT 0, 
        repeated      INT DEFAULT 0)                                
    '''

SQL_INSERT_TABLE_WIFI_SIGNAL = '''
    insert into wifi_signal(
        id,           
        ssid, 
        bssid,        
        first_time,         
        last_time,         
        source,       
        deleted,      
        repeated
        ) 
        values(?, ?, ?, ?, ?, 
               ?, ?, ?)
    '''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


class SM(object):
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
                return False

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        if self.db is not None:
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
            self.db_cmd.CommandText = SQL_CREATE_TABLE_ACCOUNT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CHARGE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_BLACKLIST
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_BLOCKEDSMS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CALLRECORD
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WIFI_SIGNAL
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

    def db_insert_table_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_ACCOUNT, column.get_values())

    def db_insert_table_charge(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_CHARGE, column.get_values())

    def db_insert_table_blacklist(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_BLACKLIST, column.get_values())

    def db_insert_table_blockedsms(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_BLOCKEDSMS, column.get_values())

    def db_insert_table_callrecord(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_CALLRECORD, column.get_values())
    
    def db_insert_table_wifi_signal(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WIFI_SIGNAL, column.get_values())

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
        except:
            exc()

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
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return self.source, self.deleted, self.repeated

class Account(Column):
    def __init__(self):
        super(Account, self).__init__()
        self.account_id = None  # 账户ID[TEXT]
        self.nickname = None  # 昵称[TEXT]
        self.username = None  # 用户名[TEXT]
        self.password = None  # 密码[TEXT]
        self.photo = None  # 头像[TEXT]
        self.telephone = None  # 电话[TEXT]
        self.address = None  # 地址[TEXT]

    def get_values(self):
        return (self.account_id, self.nickname, self.username, self.password, 
        self.photo, self.telephone, self.address) + super(Account, self).get_values()

class Charge(Column):
    def __init__(self):
        super(Charge, self).__init__()
        self.id          = None
        self.begin_time  = None
        self.end_time    = None
        self.begin_level = None
        self.end_level    = None

    def get_values(self):
        return (self.id, self.begin_time, self.end_time, self.begin_level,
                self.end_level) + super(Charge, self).get_values()

class Blacklist(Column):
    def __init__(self):
        super(Blacklist, self).__init__()
        self.id  = None
        self.name = None
        self.phone_number = None
        self.add_date = None

    def get_values(self):
        return (
            self.id, 
            self.name, 
            self.phone_number, 
            self.add_date
        ) + super(Blacklist, self).get_values()

class BlockedSms(Column):
    def __init__(self):
        super(BlockedSms, self).__init__()
        self.id = None
        self.content = None
        self.name = None
        self.phone_number = None
        self.block_time = None

    def get_values(self):
        return (
            self.id, 
            self.content, 
            self.name, 
            self.phone_number,
            self.block_time
        ) + super(BlockedSms, self).get_values()

class Callrecord(Column):
    ''' 陌生来电 '''
    def __init__(self):
        super(Callrecord, self).__init__()
        self._id          = None    # INTEGER
        self.phone_number = None    # TEXT
        self.date         = None    # INTEGER
        self.call_type    = 0       # INTEGER CALL_RECORD_TYPE

    def get_values(self):
        return (
            self._id,          
            self.phone_number, 
            self.date,         
            self.call_type,    
        ) + super(Callrecord, self).get_values()


class WifiSignal(Column):
    def __init__(self):
        super(WifiSignal, self).__init__()
        self.id         = None    # INTEGER,
        self.ssid       = None    # TEXT,
        self.bssid      = None    # TEXT,
        self.first_time = None    # INTEGER,
        self.last_time  = None    # INTEGER,

    def get_values(self):
        return (
            self.id,        
            self.ssid,      
            self.bssid,     
            self.first_time,
            self.last_time, 
            self.source,    
            self.deleted,   
            self.repeated,              
        ) + super(WifiSignal, self).get_values()


class GenerateModel(object):
    def __init__(self, cache_db, cachepath=None):
        self.cache_db = cache_db
        self.cachepath = cachepath
  
    def get_models(self):
        models = []

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_charge_models())
        models.extend(self._get_blacklist_models())
        models.extend(self._get_blockedsms_models())
        models.extend(self._get_callrecord_models())
        models.extend(self._get_wifi_signal_models())
        self.db.Close()
        return models

    def _get_charge_models(self):
        model = []
        sql = '''select distinct * from charge'''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                charge = Secure.ChargeLog()
                charge.BeginTime = self._get_timestamp(sr[1])
                charge.EndTime = self._get_timestamp(sr[2])
                begin_level = float(sr[3])
                end_level = float(sr[4])
                charge.BeginLevel = str(begin_level*100) + '%'
                charge.EndLevel = str(end_level*100) + '%'
                charge.SourceFile = self._get_source_file(str(sr[5]))
                charge.Deleted = self._convert_deleted_status(sr[6])
                model.append(charge)
            sr.Close()
            return model
        except Exception as e:
            print(e)
            exc()

    def _get_blacklist_models(self):
        model = []
        sql = '''select distinct * from blacklist'''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                blacklist = Secure.BlockedList()
                blacklist.CreateTime = self._get_timestamp(sr[3])
                blacklist.Name = self._db_reader_get_string_value(sr, 1)
                blacklist.PhoneNumber = self._db_reader_get_string_value(sr, 2)
                blacklist.SourceFile = self._get_source_file(str(sr[4]))
                blacklist.Deleted = self._convert_deleted_status(sr[5])
                model.append(blacklist)
            sr.Close()
            return model
        except Exception as e:
            exc()

    def _get_blockedsms_models(self):
        model = []
        sql = '''select distinct * from blocked_sms'''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                sms_block = Secure.SMSBlocking()
                sms_block.BlockTime = self._get_timestamp(sr[4])
                sms_block.Content = self._db_reader_get_string_value(sr, 1)
                sms_block.Name = self._db_reader_get_string_value(sr, 2)
                sms_block.PhoneNumber = self._db_reader_get_string_value(sr, 3)
                sms_block.SourceFile = self._get_source_file(str(sr[5]))
                sms_block.Deleted = self._convert_deleted_status(sr[6])
                model.append(sms_block)
            sr.Close()
            return model
        except Exception as e:
            print(e)
            exc()

    def _get_callrecord_models(self):
        if not self._db_has_table('callrecord'):
            return []
        models = []

        sql = '''select * from callrecord'''
        '''
        0    _id, 
        1    phone_number, 
        2    date, 
        3    call_type, 
        4    source, 
        5    deleted, 
        6    repeated        
        '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                deleted = 0
                try:
                    phone_number = self._db_reader_get_string_value(r, 1)
                    date = self._get_timestamp(self._db_reader_get_int_value(r, 2))
                    call_type = self._db_reader_get_int_value (r, 3)
                    source = self._db_reader_get_string_value(r, 4)
                    deleted = self._db_reader_get_int_value(r, 5, None)

                    c = CallBlocking()
                    if date:
                        c.BlockTime = date
                    if call_type in CALLBLOCK_TYPE_CONVERTER:                                      
                        c.Type = CALLBLOCK_TYPE_CONVERTER[call_type]
                    c.PhoneNumber = phone_number
                    if source:
                        c.SourceFile = source
                    if deleted:
                        c.Deleted = self._convert_deleted_status(deleted)                    
                    models.append(c)
                except:
                    exc()
            r.Close()
            return models                    
        except:
            exc()
            return models

    def _get_wifi_signal_models(self):
        if not self._db_has_table('wifi_signal'):
            return []
        models = []

        sql = '''select * from wifi_signal'''
        '''
        0       id,           
        1       ssid, 
        2       bssid,        
        3       first_time,         
        4       last_time,         
        5       source,       
        6       deleted,      
        7       repeated        
        '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                deleted = 0
                try:
                    id = self._db_reader_get_int_value(r, 0)        
                    ssid = self._db_reader_get_string_value(r, 1)  
                    bssid = self._db_reader_get_string_value(r, 2)     
                    first_time = self._get_timestamp(self._db_reader_get_int_value(r, 3))
                    last_time = self._get_timestamp(self._db_reader_get_int_value(r, 4) )
                    source = self._db_reader_get_string_value(r, 5)    
                    deleted = self._db_reader_get_int_value(r, 6)   

                    wireless = WirelessConnection()
                    wireless.SSId.Value = ssid
                    wireless.BSSId.Value = bssid
                    if last_time:                    
                        wireless.TimeStamp.Value = last_time
                    wireless.WirelessType.Value = WirelessType.Wifi
                    if source:
                        wireless.SourceFile = source
                    if deleted:
                        wireless.Deleted = self._convert_deleted_status(deleted)
                    models.append(wireless)
                except:
                    exc()
            r.Close()
            return models                    
        except:
            exc()
            return models

    def _db_has_table(self, table_name):
        try:
            sql = "select count(*) from sqlite_master where type='table' and name='{}' ".format(table_name)
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            r.Read()
            if r and self._db_reader_get_int_value(r, 0) >= 1:
                return True
            else:
                return False
        except:
            return False

    @staticmethod
    def _get_source_file(source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (Int64, long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, (Int64, long, int, str)) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        try:
            return reader.GetString(index) if not reader.IsDBNull(index) else default_value
        except:
            tp(index, reader[index], type(reader[index]))
            exc()

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        try:
            return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value
        except:
            tp(index, reader[index], type(reader[index]))
            exc()

    @staticmethod
    def _db_reader_get_float_value(reader, index, default_value=0):
        return reader.GetFloat(index) if not reader.IsDBNull(index) else default_value