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

import os
import System
import sqlite3
import re

SQL_CREATE_TABLE_MEDIA = '''
    CREATE TABLE IF NOT EXISTS media(
        id INTEGER,
        url TEXT,
        size INTEGER,
        parent TEXT,
        add_date INTEGER,
        modify_date INTEGER,
        mime_type TEXT,
        title TEXT,
        display_name TEXT,
        latitude DOUBLE,
        longitude DOUBLE,
        datetaken INTEGER,
        bucket_display_name TEXT,
        year INTEGER,
        album_artist TEXT,
        duration INTEGER,
        artist TEXT,
        album TEXT,
        location TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_CREATE_TABLE_THUMBNAILS = '''
    CREATE TABLE IF NOT EXISTS thumbnails(
        id INTEGER,
        url TEXT,
        image_id INTEGER,
        width INTEGER,
        height INTEGER,
        location TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_MEDIA = '''
    INSERT INTO media(id, url, size, parent, add_date, modify_date, mime_type, title, display_name, latitude,
    longitude, datetaken, bucket_display_name, year, album_artist, duration, artist, album, location, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_INSERT_TABLE_THUMBNAILS = '''
    INSERT INTO thumbnails(id, url, image_id, width, height, location, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

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
            os.remove(db_path)
        except Exception as e:
            print("model_media db_create() remove %s error:%s"(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MEDIA
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_THUMBNAILS

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


class Media(object):
    def __init__(self):
        self.id = None
        self.url = None
        self.size = None
        self.parent = None
        self.add_date = None
        self.modify_date = None
        self.mime_type = None
        self.title = None
        self.display_name = None
        self.latitude = None
        self.longitude = None
        self.datetaken = None
        self.bucket_display_name = None
        self.year = None
        self.album_artist = None
        self.duration = None
        self.artist = None
        self.album = None
        self.location = None  #internal还是external
        self.source = ''
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
        return (self.id, self.url, self.size, self.parent, self.add_date, self.modify_date, self.mime_type, self.title,
            self.display_name, self.latitude, self.longitude, self.datetaken, self.bucket_display_name, self.year,
            self.album_artist, self.duration, self.artist, self.album, self.location, self.source, self.deleted, self.repeated)


class Thumbnails(object):
    def __init__(self):
        self.id = None
        self.url = None
        self.image_id = None
        self.width = None
        self.height = None
        self.location = None
        self.source = ''
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
        return (self.id, self.url, self.image_id, self.width, self.height, self.location, self.source, self.deleted, self.repeated)


class Generate(object):

    def __init__(self, db_cache, node):
        self.db_cache = db_cache
        self.db = None
        self.cursor = None
        self.node = node

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        models.extend(self._get_model_media())
        models.extend(self._get_model_thumbnail())
        self.db.close()
        self.db = None
        return models

    def _get_model_media(self):
        model = []
        try:
            cursor = self.db.cursor()
            cursor.execute('select distinct * from media')
            for row in cursor:
                media = Common.Media()
                if row[17] is not None and row[17] is not '':
                    media.Album.Value = row[17]
                coordinate = Locations.Coordinate()
                if row[9] is not None and row[9] is not '':
                    coordinate.Latitude.Value = row[9]
                if row[10] is not None and row[10] is not '':
                    coordinate.Longitude.Value = row[10]
                media.Position.Value = coordinate
                if row[5] is not None and row[5] is not '':
                    media.ModifyDate.Value = self._get_timestamp(row[5])
                if row[0] is not None and row[0] is not '':
                    media.ID.Value = str(row[0])
                if row[14] is not None and row[14] is not '':
                    media.AlbumArtist.Value = row[14]
                if row[12] is not None and row[12] is not '':
                    media.BucketDisplayName.Value = row[12]
                if row[1] is not None and row[1] is not '':
                    media.Url.Value = self._transToAbsolutePath(row[1])
                if row[11] is not None and row[11] is not '':
                    media.DateTaken.Value = self._get_timestamp(row[11])
                if row[15] is not None and row[15] is not '':
                    hours = row[15]/3600
                    minutes = row[15]-hours*3600
                    seconds = row[15]-hours*3600-minutes*60
                    media.Duration.Value = System.TimeSpan(hours, minutes, seconds)
                if row[7] is not None and row[7] is not '':
                    media.Title.Value = row[7]
                if row[2] is not None and row[2] is not '':
                    media.Size.Value = row[2]
                if row[6] is not None and row[6] is not '':
                    media.MimeType.Value = row[6]
                if row[8] is not None and row[8] is not '':
                    media.DisplayName.Value = row[8]
                if row[4] is not None and row[4] is not '':
                    media.AddDate.Value = self._get_timestamp(row[4])
                if row[3] is not None and row[3] is not '':
                    media.Parent.Value = row[3]
                if row[16] is not None and row[16] is not '':
                    media.Artist.Value = row[16]
                if row[18] is not None and row[18] is not '':
                    media.Location.Value = row[18]
                if row[19] is not None and row[19] is not '':
                    media.SourceFile.Value = self._get_source_file(str(row[19]))
                model.append(media)
        except:
                pass
        return model

    def _get_model_thumbnail(self):
        model = []
        try:
            cursor = self.db.cursor()
            cursor.execute('select distinct * from thumbnails')
            for row in cursor:
                thumbnail = Common.Thumbnail()
                if row[0] is not None and row[0] is not '':
                    thumbnail.MediaID.Value = str(row[0])
                if row[2] is not None and row[2] is not '':
                    thumbnail.ID.Value = str(row[2])
                if row[1] is not None and row[1] is not '':
                    thumbnail.Url.Value = self._transToAbsolutePath(row[1])
                if row[3] is not None and row[3] is not '':
                    thumbnail.Width.Value = row[3]
                if row[5] is not None and row[5] is not '':
                    thumbnail.Location.Value = row[5]
                if row[4] is not None and row[4] is not '':
                    thumbnail.Height.Value = row[4]
                if row[6] is not None and row[6] is not '':
                    thumbnail.SourceFile.Value = self._get_source_file(str(row[6]))
                model.append(thumbnail)
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

    def _transToAbsolutePath(self, dir):
        fs = self.node.FileSystem
        try:
            if re.match(r'^/storage/emulated/0', dir) is not None:
                subdir = dir.replace('/storage/emulated/0', '')
                fileNode = list(fs.Search(subdir + '$'))
                return fileNode[0].AbsolutePath
            elif  re.match(r'^/data/user/0', dir) is not None:
                subdir = dir.replace('/data/user/0', '')
                fileNode = list(fs.Search(subdir + '$'))
                return fileNode[0].AbsolutePath
            elif re.match(r'^/storage/0000-0000', dir) is not None:
                fileNode = fs.Search(subdir + '$')
                fileNode = list(fs.Search(subdir + '$'))
                return fileNode[0].AbsolutePath
            elif re.match(r'^/$', dir) is not None:
                return fs.AbsolutePath
            elif '*' not in dir:
                fileNode = fs.Search(dir+ '$')
                fileNode = list(fs.Search(subdir + '$'))
                return fileNode[0].AbsolutePath
            else:
                return ''
        except:
            pass