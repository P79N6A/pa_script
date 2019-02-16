# -*- coding: utf-8 -*-
__author__ = "xiaoyuge"

from PA_runtime import *
import clr
try:
    clr.AddReference('System.Core')
    clr.AddReference('System.Xml.Linq')
    clr.AddReference('System.Data.SQLite')
except:
    pass
del clr

import PA.InfraLib.ModelsV2.CommonEnum.CallType as CallType
import PA.InfraLib.ModelsV2.Base.Contact as Contact
import PA.InfraLib.ModelsV2.Base.Call as Call

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import System
import System.Data.SQLite as SQLite
import sqlite3

#来电通话
INCOMING_TYPE = 1
#拨号通话
OUTGOING_TYPE = 2
#未接来电
MISSED_TYPE = 3
#语音邮箱
VOICEMAIL_TYPE = 4
#拒绝接听
REJECTED_TYPE = 5
#黑名单
BLOCKED_TYPE = 6
#其他设备接听
ANSWERED_EXTERNALLY_TYPE = 7

SQL_CREATE_TABLE_RECORDS = '''
    CREATE TABLE IF NOT EXISTS records(
        id INTEGER,
        phone_number TEXT,
        date INTEGER,
        duration INTEGER,
        type INTEGER,
        name TEXT,
        geocoded_location TEXT,
        ring_times INTEGER,
        mark_type TEXT,
        mark_content TEXT,
        country_code TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER,
        local_number TEXT
    )'''

SQL_INSERT_TABLE_RECORDS = '''
    INSERT INTO records(id, phone_number, date, duration, type, name, geocoded_location, ring_times, mark_type, mark_content, country_code, source, deleted, repeated, local_number)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_FIND_TABLE_RECORDS = '''
    SELECT * FROM RECORDS ORDER BY deleted ASC
    '''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

#中间数据库版本
VERSION_VALUE_DB = 1


class MC(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self,db_path):
        self.db_remove(db_path)
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

    def db_remove(self, db_path):
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except Exception as e:
            print("model_callrecord db_create() remove %s error:%s"%(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECORDS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        try:
            if self.db_cmd is not None:
                self.db_cmd.CommandText = sql
                self.db_cmd.Parameters.Clear()
                for value in values:
                    param = self.db_cmd.CreateParameter()
                    param.Value = value
                    self.db_cmd.Parameters.Add(param)
                self.db_cmd.ExecuteNonQuery()
        except Exception as e:
            print(e)

    def db_insert_table_call_records(self, Column):
        self.db_insert_table(SQL_INSERT_TABLE_RECORDS, Column.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

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
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            self.__dict__[name] = value
        else:
            self.__dict__[name] = None

    def get_values(self):
        return (self.source, self.deleted, self.repeated)


class Records(Column):
    def __init__(self):
        super(Records, self).__init__()
        self.id = None
        self.phone_number = None
        self.date = None
        self.duration = None
        self.type = None
        self.name = None
        self.geocoded_location = None
        self.ring_times = None
        self.mark_type = None
        self.mark_content = None
        self.country_code = None
        self.local_number = None

    def get_values(self):
        return (self.id, self.phone_number, self.date, self.duration, self.type, self.name, self.geocoded_location,
            self.ring_times, self.mark_type, self.mark_content, self.country_code, self.source, self.deleted, self.repeated, self.local_number)


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.db_cmd = None
        self.id = []

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_model_records())
        self.db_cmd.Dispose()
        self.db.Close()
        return models

    def _get_model_records(self):
        model = []
        sql = SQL_FIND_TABLE_RECORDS
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if sr[0] == 0:
                    continue
                if sr[2] not in self.id:
                    self.id.append(sr[2])
                else:
                    continue
                c = Call()
                if not IsDBNull(sr[10]):
                    c.CountryCode = sr[10]
                if not IsDBNull(sr[3]):
                    hours = sr[3]/3600
                    minutes = sr[3]-hours*3600
                    seconds = sr[3]-hours*3600-minutes*60
                    c.Duration = System.TimeSpan(hours, minutes, seconds)
                if not IsDBNull(sr[2]):
                    c.StartTime = TimeStamp.FromUnixTime(int(str(sr[2])[0:10:1]), False)
                if not IsDBNull(sr[4]):
                    c.Type = CallType.Incoming if sr[4] == INCOMING_TYPE else CallType.Outgoing if sr[4] == OUTGOING_TYPE else CallType.Missed if sr[4] == MISSED_TYPE else CallType.VoiceMail if sr[4] == VOICEMAIL_TYPE else CallType.Rejected if sr[4] == REJECTED_TYPE else CallType.Blocked if sr[4] == BLOCKED_TYPE else CallType.AnsweredExternally if sr[4] == ANSWERED_EXTERNALLY_TYPE else CallType.Unknown
                party = Contact()
                if not IsDBNull(sr[1]):
                    party.PhoneNumbers.Add(sr[1])
                if not IsDBNull(sr[5]):
                    party.FullName = sr[5]
                if not IsDBNull(sr[4]):
                    if sr[4] == 1 or sr[4] == 3:
                        c.FromSet.Add(party)
                    else:
                        c.ToSet.Add(party)
                party = Contact()
                if not IsDBNull(sr[14]):
                    party.FullName = sr[14]
                if not IsDBNull(sr[4]):
                    if sr[4] == 1 or sr[4] == 3:
                        c.ToSet.Add(party)
                    else:
                        c.FromSet.Add(party)
                if not IsDBNull(sr[11]):
                    c.SourceFile = self._get_source_file(str(sr[11]))
                if not IsDBNull(sr[12]):
                    c.Deleted = self._convert_deleted_status(sr[12])
                model.append(c)
            sr.Close()
            return model
        except Exception as e:
            print(e)

    @staticmethod
    def _get_source_file(source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, (Int64, long, int)) and len(str(timestamp)) == 10:
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