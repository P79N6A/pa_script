# -*- coding: utf-8 -*-
__author__ = "xiaoyuge"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import System
import sqlite3
from PA.InfraLib.Utils import ConvertHelper

SQL_CREATE_TABLE_RECORDS = '''
    CREATE TABLE IF NOT EXISTS records(
        record_id INTEGER,
        record_url TEXT,
        record_name TEXT,
        record_size INTEGER,
        record_create INTEGER,
        record_duration INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_RECORDS = '''
    INSERT INTO records(record_id, record_url, record_name, record_size, record_create,
        record_duration, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?)
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

class MS(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self,db_path):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()
        self.db_create_tables()

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def db_create_tables(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_RECORDS)
            self.cursor.execute(SQL_CREATE_TABLE_VERSION)

    def db_insert_table_records(self, Records):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_RECORDS, Records.get_values())

    def db_insert_table_version(self, key, version):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_VERSION, (key, version))

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


class Records(object):
    def __init__(self):
        self.record_id = None
        self.record_url = None
        self.record_name = None
        self.record_size = None
        self.record_create = None
        self.record_duration = None
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return (self.record_id, self.record_url, self.record_name, self.record_size,
            self.record_create, self.record_duration, self.source, self.deleted,
            self.repeated)


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.cursor = None

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        models.extend(self._get_model_records())
        self.db.close()
        self.db = None
        return models

    def _get_model_records(self):
        model = []
        self.cursor = self.db.cursor()
        self.cursor.execute('select distinct * from records')
        for row in self.cursor:
            canceller.ThrowIfCancellationRequested()
            r = Generic.Recording()
            if row[1] is not None:
                r.FileUri.Value = self._get_uri(row[1])
            if row[2] is not None:
                r.Title.Value = row[2]
            #if row[3] is not None:
            #    r.ByteLength.Value = row[3]
            if row[4] is not None:
                r.TimeStamp.Value = TimeStamp.FromUnixTime(int(str(row[4])[0:10:1]), False) if len(str(row[4])) > 10 else TimeStamp.FromUnixTime(row[4], False) if len(str(row[4])) == 10 else TimeStamp.FromUnixTime(0, False)
            if row[5] is not None:
                try:
                    hours = int(row[5])/3600
                    minutes = (int(row[5])-hours*3600)/60
                    seconds = int(row[5])-hours*3600-minutes*60
                    r.Duration.Value = System.TimeSpan(hours, minutes, seconds)
                except:
                    pass
            r.Type.Value = Generic.RecordingType.Audio
            model.append(r)
        self.cursor.close()
        self.cursor = None
        return model
    
    def _get_uri(self, path):
        if path.startswith('http') or len(path) == 0:
            return ConvertHelper.ToUri(path)
        else:
            return ConvertHelper.ToUri(path)