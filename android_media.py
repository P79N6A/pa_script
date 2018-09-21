# -*- coding: utf-8 -*-
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import threading
import traceback
SafeLoadAssembly('model_media')
from model_media import *


class MediaParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.nodes = [self.node, self.node.Parent.GetByPath('/external.db')]
        self.extractDeleted = False
        self.extractSource = extractSource
        self.db = None
        self.mm = MM()
        self.cache_path = ds.OpenCachePath("MEDIA")
        self.db_cache = self.cache_path + "\\media.db"
        self.mm.db_create(self.db_cache)

    def analyze_media(self):
        for i, node in enumerate(self.nodes):
            try:
                self.db = SQLiteParser.Database.FromNode(node, canceller)
                if self.db is None:
                    return 
                ts = SQLiteParser.TableSignature('files')
                media = Media()
                for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                    canceller.ThrowIfCancellationRequested()
                    media.id = rec['_id'].Value
                    media.url = rec['_data'].Value
                    media.size = rec['_size'].Value
                    media.parent = rec['parent'].Value
                    media.add_date = rec['date_added'].Value
                    media.modify_date = rec['date_modified'].Value
                    media.mime_type = rec['mime_type'].Value
                    media.title = rec['title'].Value
                    media.display_name = rec['_display_name'].Value
                    media.latitude = rec['latitude'].Value
                    media.longitude = rec['longitude'].Value
                    media.datetaken = rec['datetaken'].Value
                    media.bucket_display_name = rec['bucket_display_name'].Value
                    media.year = rec['year'].Value
                    media.album_artist = rec['album_artist'].Value
                    media.duration = rec['duration'].Value
                    media.artist = rec['artist'].Value
                    media.album = rec['album'].Value
                    media.location = 'internal' if i==0 else 'external'
                    media.source = node.AbsolutePath
                    self.mm.db_insert_table_media(media)
                self.mm.db_commit()
                media = Media()
                for rec in self.db.ReadTableDeletedRecords(ts, False):
                    canceller.ThrowIfCancellationRequested()
                    media.id = rec['_id'].Value
                    media.url = rec['_data'].Value
                    media.size = rec['_size'].Value
                    media.parent = rec['parent'].Value
                    media.add_date = rec['date_added'].Value
                    media.modify_date = rec['date_modified'].Value
                    media.mime_type = repr(rec['mime_type'].Value)
                    media.title = rec['title'].Value
                    media.display_name = rec['_display_name'].Value
                    media.latitude = rec['latitude'].Value
                    media.longitude = rec['longitude'].Value
                    media.datetaken = rec['datetaken'].Value
                    media.bucket_display_name = rec['bucket_display_name'].Value
                    media.year = rec['year'].Value
                    media.album_artist = rec['album_artist'].Value
                    media.duration = rec['duration'].Value
                    media.artist = rec['artist'].Value
                    media.album = rec['album'].Value
                    media.location = 'internal' if i==0 else 'external'
                    media.source = node.AbsolutePath
                    media.deleted = 1
                    self.mm.db_insert_table_media(media)
                self.mm.db_commit()
            except:
                traceback.print_exc()

    def analyze_thumbnails(self):
        for i, node in enumerate(self.nodes):
            try:
                self.db = SQLiteParser.Database.FromNode(node, canceller)
                if self.db is None:
                    return 
                ts = SQLiteParser.TableSignature('thumbnails')
                thumbnails = Thumbnails()
                for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                    canceller.ThrowIfCancellationRequested()
                    thumbnails.id = rec['_id'].Value
                    thumbnails.url = rec['_data'].Value
                    thumbnails.image_id = rec['image_id'].Value
                    thumbnails.width = rec['width'].Value
                    thumbnails.height = rec['height'].Value
                    thumbnails.location = 'internal' if i==0 else 'external'
                    thumbnails.source = node.AbsolutePath
                    self.mm.db_insert_table_thumbnails(thumbnails)
                self.mm.db_commit()
                thumbnails = Thumbnails()
                for rec in self.db.ReadTableDeletedRecords(ts, False):
                    canceller.ThrowIfCancellationRequested()
                    thumbnails.id = rec['_id'].Value
                    thumbnails.url = rec['_data'].Value
                    thumbnails.image_id = rec['image_id'].Value
                    thumbnails.width = rec['width'].Value
                    thumbnails.height = rec['height'].Value
                    thumbnails.location = 'internal' if i==0 else 'external'
                    thumbnails.source = node.AbsolutePath
                    thumbnails.deleted = 1
                    self.mm.db_insert_table_thumbnails(thumbnails)
                self.mm.db_commit()
            except:
                traceback.print_exc()

    def parse(self):
        self.analyze_thumbnails()
        self.analyze_media()
        self.mm.db_close()
        generate = Generate(self.db_cache, self.node)
        models = generate.get_models()
        return models

def analyze_android_media(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(MediaParse(node, extractDeleted, extractSource).parse())
    pr.Build('Media')
    return pr

def execute(node, extractDeleted):
    return analyze_android_soundrecord(node, extractDeleted, False)