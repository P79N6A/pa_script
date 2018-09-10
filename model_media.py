# -*- coding: utf-8 -*-

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
import logging

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
        self.cursor = None

    def db_create(self,db_path):
        self.db_remove(db_path)
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

    def db_remove(self, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)

    def db_create_tables(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_MEDIA)
            self.cursor.execute(SQL_CREATE_TABLE_THUMBNAILS)

    def db_insert_table_media(self, Media):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_MEDIA, Media.get_values())

    def db_insert_table_thumbnails(self, Thumbnails):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_THUMBNAILS, Thumbnails.get_values())


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

    def get_values(self):
        return (self.id, self.url, self.image_id, self.width, self.height, self.location, self.source, self.deleted, self.repeated)


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.cursor = None

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        models.extend(self._get_model_media())
        self.db.close()
        self.db = None
        return models

    def _get_model_media(self):
        model = []
        
        return model