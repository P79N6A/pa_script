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
import PA.InfraLib.ModelsV2.Base.MediaFile as MediaFile
import PA.InfraLib.ModelsV2.Base as Base

import os
import System
import System.Data.SQLite as SQLite
import sqlite3
import re
import traceback

SQL_CREATE_TABLE_MEDIA = '''
    CREATE TABLE IF NOT EXISTS media(
        id INTEGER,
        url TEXT,
        size INTEGER,
        add_date INTEGER,
        modify_date INTEGER,
        suffix TEXT,
        type TEXT,
        title TEXT,
        latitude DOUBLE,
        longitude DOUBLE,
        address TEXT,
        datetaken INTEGER,
        duration INTEGER,
        artist TEXT,
        album TEXT,
        width INTEGER,
        height INTEGER,
        description TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_CREATE_TABLE_THUMBNAILS = '''
    CREATE TABLE IF NOT EXISTS thumbnails(
        id INTEGER,
        url TEXT,
        media_id INTEGER,
        width INTEGER,
        height INTEGER,
        create_date INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_CREATE_TABLE_MEDIA_LOG = '''
    CREATE TABLE IF NOT EXISTS media_log(
        id INTEGER,
        media_id INTEGER,
        operation TEXT,
        filename TEXT,
        timeStamp INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_MEDIA = '''
    INSERT INTO media(id, url, size, add_date, modify_date, suffix, type, title, latitude, longitude, address,
        datetaken, duration, artist, album, width, height, description, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_INSERT_TABLE_THUMBNAILS = '''
    INSERT INTO thumbnails(id, url, media_id, width, height, create_date, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_INSERT_TABLE_MEDIA_LOG = '''
    INSERT INTO media_log(id, media_id, operation, filename, timeStamp, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

VERSION_VALUE_DB = 2

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
            self.db_cmd.CommandText = SQL_CREATE_TABLE_THUMBNAILS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MEDIA_LOG
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

    def db_insert_table_thumbnails(self, Thumbnails):
        self.db_insert_table(SQL_INSERT_TABLE_THUMBNAILS, Thumbnails.get_values())

    def db_insert_table_media_log(self, MediaLog):
        self.db_insert_table(SQL_INSERT_TABLE_MEDIA_LOG, MediaLog.get_values())

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
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

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
        self.source = None
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            if isinstance(value, str):
                self.__dict__[name] = re.compile(u"[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]").sub('', value)
            else:
                self.__dict__[name] = value
        else:
            self.__dict__[name] = None

    def get_values(self):
        return(self.source, self.deleted, self.repeated)

class Media(Column):
    def __init__(self):
        super(Media, self).__init__()
        self.id = None
        self.url = None
        self.size = None
        self.add_date = None
        self.modify_date = None
        self.suffix = None
        self.type = None
        self.title = None
        self.latitude = 0
        self.longitude = 0
        self.address = None
        self.datetaken = None
        self.duration = None
        self.artist = None
        self.album = None
        self.width = None
        self.height = None
        self.description = None

    def get_values(self):
        return (self.id, self.url, self.size, self.add_date, self.modify_date, self.suffix, self.type, self.title,
                self.latitude, self.longitude, self.address, self.datetaken, self.duration, self.artist, self.album, 
                self.width, self.height, self.description) + super(Media, self).get_values()


class Thumbnails(Column):
    def __init__(self):
        super(Thumbnails, self).__init__()
        self.id = None
        self.url = None
        self.media_id = None
        self.width = None
        self.height = None
        self.create_date = None
        
    def get_values(self):
        return (self.id, self.url, self.media_id, self.width, self.height, self.create_date) + super(Thumbnails, self).get_values()

class MediaLog(Column):
    def __init__(self):
        super(MediaLog, self).__init__()
        self.id = None
        self.media_id = None
        self.operation = None
        self.filename = None
        self.timeStamp = None

    def get_values(self):
        return (self.id, self.media_id, self.operation, self.filename, self.timeStamp) + super(MediaLog, self).get_values()


class Generate(object):

    def __init__(self, db_cache, node):
        self.db_cache = db_cache
        self.db = None
        self.db_cmd = None
        self.db_trans = None
        self.node = node

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_model_media())
        models.extend(self._get_model_thumbnail())
        models.extend(self._get_model_media_log())
        self.db.Close()
        self.db = None
        return models

    def _get_model_media(self):
        model = []
        try:
            self.db_cmd.CommandText = '''select distinct * from media order by deleted ASC'''
            sr = self.db_cmd.ExecuteReader()
            pk = []
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if sr[1] is None or sr[6] is None:
                        continue
                    if sr[0] is None or sr[0] == 0:
                        continue
                    media_type = sr[6]
                    media_id = sr[0]
                    deleted = sr[19]
                    #remove duplication
                    if media_id in pk:
                        continue
                    else:
                        pk.append(media_id)
                    #audio convert
                    if media_type == "audio":
                        audio = MediaFile.AudioFile()
                        audio.FileName = self._db_reader_get_string_value(sr, 7)
                        audio.Path = self._db_reader_get_string_value(sr, 1)
                        audio.Size = self._db_reader_get_int_value(sr, 2)
                        addTime = self._db_reader_get_int_value(sr, 3)
                        audio.AddTime = self._get_timestamp(addTime)
                        audio.Description = self._db_reader_get_string_value(sr, 17)
                        media_log = self._get_media_log(sr[0])
                        for log in media_log:
                            video.Logs.Add(log)
                        modifyTime = self._db_reader_get_int_value(sr, 4)
                        audio.ModifyTime = self._get_timestamp(modifyTime)
                        audio.Album = self._db_reader_get_string_value(sr, 14)
                        audio.Artist = self._db_reader_get_string_value(sr, 13)
                        audio.duration = self._db_reader_get_int_value(sr, 12)
                    #image convert
                    elif media_type == "image":
                        image = MediaFile.ImageFile()
                        image.FileName = self._db_reader_get_string_value(sr, 7)
                        image.Path = self._db_reader_get_string_value(sr, 1)
                        image.Size = self._db_reader_get_int_value(sr, 2)
                        addTime = self._db_reader_get_int_value(sr, 3)
                        image.AddTime = self._get_timestamp(addTime)
                        image.Description = self._db_reader_get_string_value(sr, 17)
                        location = Base.Location()
                        coordinate = Base.Coordinate()
                        coordinate.Latitude = float(sr[8])
                        coordinate.Longitude = float(sr[9])
                        location.Coordinate = coordinate
                        location.AddressName = self._db_reader_get_string_value(sr, 10)
                        image.Location = location
                        media_log = self._get_media_log(sr[0])
                        for log in media_log:
                            image.Logs.Add(log)
                        modifyTime = self._db_reader_get_int_value(sr, 4)
                        image.ModifyTime = self._get_timestamp(modifyTime)
                        image.Height = self._db_reader_get_int_value(sr, 16)
                        image.Width = self._db_reader_get_int_value(sr, 15)
                        takenDate = self._db_reader_get_int_value(sr, 11)
                        image.TakenDate = self._get_timestamp(takenDate)
                        image.Thumbnail = self._get_media_thumbnail(media_id)
                        image.SourceFile = self._get_source_file(str(sr[18]))
                        image.Deleted = self._convert_deleted_status(sr[19])
                        model.append(image)
                    #video convert
                    elif media_type == "video":
                        video = MediaFile.VideoFile()
                        video.FileName = self._db_reader_get_string_value(sr, 7)
                        video.Path = self._db_reader_get_string_value(sr, 1)
                        video.Size = self._db_reader_get_int_value(sr, 2)
                        addTime = self._db_reader_get_int_value(sr, 3)
                        video.AddTime = self._get_timestamp(addTime)
                        video.Description = self._db_reader_get_string_value(sr, 17)
                        media_log = self._get_media_log(sr[0])
                        for log in media_log:
                            video.Logs.Add(log)
                        modifyTime = self._db_reader_get_int_value(sr, 4)
                        video.ModifyTime = self._get_timestamp(modifyTime)
                        video.Duration = float(sr[12]/1000)
                        video.SourceFile = self._get_source_file(str(sr[18]))
                        video.Deleted = self._convert_deleted_status(sr[19])
                        model.append(video)
                except:
                    traceback.print_exc()
            sr.Close()
        except:
            pass
        return model

    def _get_model_thumbnail(self):
        model = []
        try:
            self.db_cmd.CommandText = '''select distinct * from thumbnails order by deleted ASC'''
            sr = self.db_cmd.ExecuteReader()
            pk = []
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if sr[0] is None or sr[0] == 0:
                        continue
                    id = sr[0]
                    if id in pk:
                        continue
                    else:
                        pk.append(id)
                    thumbnail = MediaFile.ThumbnailFile()
                    filePath = self._db_reader_get_string_value(sr, 1)
                    thumbnail.FileName = os.path.basename(filePath)
                    thumbnail.Path = filePath
                    thumbnail.Height = self._db_reader_get_int_value(sr, 4)
                    thumbnail.Width = self._db_reader_get_int_value(sr, 3)
                    addTime = self._db_reader_get_int_value(sr, 5)
                    thumbnail.AddTime = self._get_timestamp(addTime)
                    thumbnail.SourceFile = self._get_source_file(str(sr[6]))
                    thumbnail.Deleted = self._convert_deleted_status(sr[7])
                    model.append(thumbnail)
                except:
                    traceback.print_exc()
            sr.Close()
        except:
            traceback.print_exc()
        return model

    def _get_model_media_log(self):
        model = []
        try:
            self.db_cmd.CommandText = '''select distinct * from media_log order by timestamp asc'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    log = MediaFile.MediaLog()
                    log.FilePath = self._db_reader_get_string_value(sr, 3)
                    log.Operating = self._db_reader_get_string_value(sr, 2)
                    log.OperatingTime = self._db_reader_get_string_value(sr, 4)
                    model.append(log)
                except:
                    traceback.print_exc()
            sr.Close()
        except:
            traceback.print_exc()
        return model

    def _get_media_log(self, media_id):
        model = []
        try:
            db_cmd = SQLite.SQLiteCommand(self.db)
            db_cmd.CommandText = '''select distinct * from media_log where media_id = {} order by timestamp ASC'''.format(media_id)
            sr = db_cmd.ExecuteReader()
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    log = MediaFile.MediaLog()
                    log.FilePath = self._db_reader_get_string_value(sr, 3)
                    log.Operating = self._db_reader_get_string_value(sr, 2)
                    log.OperatingTime = self._db_reader_get_string_value(sr, 4)
                    model.append(log)
                except:
                    pass
        except:
            traceback.print_exc()
        return model

    def _get_media_thumbnail(self, media_id):
        try:
            db_cmd = SQLite.SQLiteCommand(self.db)
            db_cmd.CommandText = '''select distinct * from thumbnails where media_id = {}'''.format(media_id)
            sr = db_cmd.ExecuteReader()
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    thumbnail = MediaFile.ThumbnailFile()
                    filePath = self._db_reader_get_string_value(sr, 1)
                    thumbnail.FileName = os.path.basename(filePath)
                    thumbnail.Path = filePath
                    thumbnail.Height = self._db_reader_get_int_value(sr, 4)
                    thumbnail.Width = self._db_reader_get_int_value(sr, 3)
                    addTime = self._db_reader_get_int_value(sr, 5)
                    thumbnail.AddTime = self._get_timestamp(addTime)
                    thumbnail.SourceFile = self._get_source_file(str(sr[6]))
                    thumbnail.Deleted = self._convert_deleted_status(sr[7])
                    return thumbnail
                except:
                    traceback.print_exc()
        except:
            traceback.print_exc()

    def _get_source_file(self, source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

    def _get_timestamp(self, timestamp):
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
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_float_value(reader, index, default_value=0):
        return reader.GetFloat(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted