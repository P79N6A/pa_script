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

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import System
import sqlite3
import System.Data.SQLite as SQLite

SQL_CREATE_TABLE_CALENDAR = '''
    CREATE TABLE IF NOT EXISTS calendar(
        calendar_id INTEGER,
        title TEXT,
        latitude TEXT,
        longitude TEXT,
        description TEXT,
        dtstart INTEGER,
        remind INTEGER,
        dtend INTEGER,
        rrule TEXT,
        interval INTEGER,
        until INTEGER,
        calendar_displayName TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_CALENDAR = '''
    INSERT INTO calendar (calendar_id, title, latitude, longitude, description, dtstart, remind, dtend, 
        rrule, interval, until, calendar_displayName, source, deleted, repeated) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

#中间数据库版本
VERSION_VALUE_DB = 2

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
            print("model_calendar db_create() remove %s error:%s"%(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CALENDAR
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

    def db_insert_calendar(self, Column):
        self.db_insert_table(SQL_INSERT_TABLE_CALENDAR, Column.get_values())

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

class Calendar(object):
    def __init__(self):
        self.calendar_id = None
        self.title = None
        self.latitude = None
        self.longitude = None
        self.description = None
        self.dtstart = None
        self.remind = None
        self.dtend = None
        self.rrule = None
        self.interval = None
        self.until = None
        self.calendar_displayName = None
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if IsDBNull(value) or value is '':
            self.__dict__[name] = None
        else:
            if isinstance(value, str):
                value = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', value)
            self.__dict__[name] = value

    def get_values(self):
        return(self.calendar_id, self.title, self.latitude, self.longitude, self.description, self.dtstart, self.remind, 
        self.dtend, self.rrule, self.interval, self.until, self.calendar_displayName, self.source, self.deleted, self.repeated)


class Generate(object):
    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.cursor = None
        self.db_cmd = None

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_model_calendar())
        self.db_cmd.Dispose()
        self.db.Close()
        return models

    def _get_model_calendar(self):
        model = []
        sql = '''select * from calendar'''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                cal = Generic.CalendarEntry()
                if not IsDBNull(sr[11]):
                    cal.Category.Value = str(sr[11])
                if not IsDBNull(sr[1]):
                    cal.Subject.Value = sr[1]
                if not IsDBNull(sr[2]):
                    cal.Location.Value = str(sr[2])+','+str(sr[3])
                if not IsDBNull(sr[3]):
                    cal.Details.Value = sr[4]
                if not IsDBNull(sr[5]):
                    startDate = self._get_timestamp(int(sr[5]))
                    cal.StartDate.Value = startDate
                if not IsDBNull(sr[7]):
                    endDate = self._get_timestamp(int(sr[7]))
                    cal.EndDate.Value = endDate
                if not IsDBNull(sr[8]):
                    if sr[8].find('DAILY')>=0:
                        repeatRule = Generic.RepeatRule.Daily
                    elif sr[8].find('WEEKLY')>=0:
                        repeatRule = Generic.RepeatRule.Weekly
                    elif sr[8].find('MONTHLY')>=0:
                        repeatRule = Generic.RepeatRule.Monthly
                    elif sr[8].find('YEARLY')>=0:
                        repeatRule = Generic.RepeatRule.Yearly
                    else:
                        repeatRule = Generic.RepeatRule.None
                        cal.RepeatRule.Value = repeatRule
                if not IsDBNull(sr[9]):
                    if re.sub('.*=', '', sr[9]) is not '':
                        cal.RepeatInterval.Value = int(re.sub('.*=', '', sr[9]))
                else:
                    cal.RepeatInterval.Value = 1
                if not IsDBNull(sr[12]):
                    cal.SourceFile.Value = self._get_source_file(str(sr[12]))
                if not IsDBNull(sr[13]):
                    cal.Deleted = self._convert_deleted_status(sr[13])
                model.append(cal)
            sr.Close()
        except Exception as e:
            print(e)
        return model

    def _get_source_file(self, source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

    @staticmethod
    def _get_timestamp(timestamp):
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

