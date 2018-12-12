# -*- coding: utf-8 -*-
__author__ = "xiaoyuge"

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

import os
import System
import System.Data.SQLite as SQLite
import sqlite3
import re

SQL_CREATE_TABLE_MEDIA = '''
    CREATE TABLE IF NOT EXISTS media(
        id INTEGER PRIMARY KEY autoincrement,
        url TEXT,
        size INTEGER,
        parent TEXT,
        add_date INTEGER,
        modify_date INTEGER,
        mime_type TEXT,
        type TEXT,
        display_name TEXT,
        latitude TEXT,
        longitude TEXT,
        datetaken INTEGER,
        duration INTEGER,
        location TEXT,
        width INTEGER,
        height INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''


SQL_INSERT_TABLE_MEDIA = '''
    INSERT INTO media(id, url, size, parent, add_date, modify_date, mime_type, type, display_name, latitude,
    longitude, datetaken, duration, location, width, height, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

VERSION_VALUE_DB = 1
VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

class MM(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None

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
            print("model_media db_create() remove %s error:%s"%(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MEDIA
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

    def db_insert_table_media(self, Media):
        self.db_insert_table(SQL_INSERT_TABLE_MEDIA, Media.get_values())

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


class Media(object):
    def __init__(self):
        self.id = None #0
        self.url = None #1
        self.size = None #02
        self.parent = None #03
        self.add_date = None #04
        self.modify_date = None #05
        self.mime_type = None #06
        self.type = None #07
        self.display_name = None #08
        self.latitude = None #09
        self.longitude = None #10
        self.datetaken = None #11
        self.duration = None #12
        self.location = None #13
        self.width = None #14
        self.height = None #15
        self.source = None #16
        self.deleted = 0 #17
        self.repeated = None #18

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            if isinstance(value, str):
                self.__dict__[name] = re.compile(u"[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]").sub('', value)
            else:
                self.__dict__[name] = value
        else:
            self.__dict__[name] = None

    def get_values(self):
        return (self.id, self.url, self.size, self.parent, self.add_date, self.modify_date, self.mime_type, 
                self.type, self.display_name, self.latitude, self.longitude, self.datetaken, self.duration, 
                self.location, self.width, self.height, self.source, self.deleted, self.repeated)


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def get_models(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models = self._get_model_media()
        self.db.Close()
        self.db = None
        return models

    def _get_model_media(self):
        model = []
        try:
            self.db_cmd.CommandText = '''select distinct * from media'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    media = Common.Media()
                    coordinate = Locations.Coordinate()
                    if not IsDBNull(sr[9]) and sr[9] is not '':
                        coordinate.Latitude.Value = float(sr[9])
                    if not IsDBNull(sr[10]) and sr[10] is not '':
                        coordinate.Longitude.Value = float(sr[10])
                    media.Position.Value = coordinate
                    if not IsDBNull(sr[5]) and sr[5] is not '':
                        media.ModifyDate.Value = self._get_timestamp(sr[5])
                    if not IsDBNull(sr[0]) and sr[0] is not '':
                        media.ID.Value = str(sr[0])
                    if not IsDBNull(sr[1]) and sr[1] is not '':
                        media.Url.Value = sr[1]
                    if not IsDBNull(sr[11]) and sr[11] is not '':
                        media.DateTaken.Value = self._get_timestamp(sr[11])
                    if not IsDBNull(sr[12]) and sr[12] is not '':
                        hours = sr[12]/3600
                        minutes = sr[12]-hours*3600
                        seconds = sr[12]-hours*3600-minutes*60
                        media.Duration.Value = System.TimeSpan(hours, minutes, seconds)
                    if not IsDBNull(sr[2]) and sr[2] is not '':
                        media.Size.Value = sr[2]
                    if not IsDBNull(sr[6]) and sr[6] is not '':
                        media.MimeType.Value = sr[6]
                    if not IsDBNull(sr[7]) and sr[7] is not '':
                        media.Type.Value = MediaType.Image if sr[7] == 'image' else MediaType.Video if sr[7] == 'video' else MediaType.Other
                    if not IsDBNull(sr[8]) and sr[8] is not '':
                        media.DisplayName.Value = sr[8]
                    if not IsDBNull(sr[4]) and sr[4] is not '':
                        media.AddDate.Value = self._get_timestamp(sr[4])
                    if not IsDBNull(sr[3]) and sr[3] is not '':
                        media.Parent.Value = sr[3]
                    if not IsDBNull(sr[13]) and sr[13] is not '':
                        media.Location.Value = sr[13]
                    if not IsDBNull(sr[14]) and sr[14] is not '':
                        media.Width.Value = sr[14]
                    if not IsDBNull(sr[15]) and sr[15] is not '':
                        media.Height.Value = sr[15]
                    if not IsDBNull(sr[16]) and sr[16] is not '':
                        media.SourceFile.Value = self._get_source_file(str(sr[16]))
                    if not IsDBNull(sr[17]) and sr[17] is not '':
                        media.Deleted = self._convert_deleted_status(sr[17])
                    model.append(media)
                except:
                    traceback.print_exc()
            sr.Close()
        except:
            pass
        return model

    def _get_source_file(self, source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

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