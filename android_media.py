#coding=utf-8
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import logging 
SafeLoadAssembly('model_meida')
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
        media = Media()
        for i, node in enumerate(self.nodes):
            try:
                self.db = SQLiteParser.Database.FromNode(node)
                if self.db is None:
                    return 
                ts = SQLiteParser.TableSignature('files')
                for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                    media.id = rec['_id'].Value if '_id' in rec and not rec['_id'].IsDBNull else None
                    media.url = rec['_data'].Value if '_data' in rec and not rec['_data'].IsDBNull else None
                    media.size = rec['_size'].Value if '_size' in rec and not rec['_size'].IsDBNull else None
                    media.parent = rec['parent'].Value if 'parent' in rec and not rec['parent'].IsDBNull else None
                    media.add_date = rec['date_added'].Value if 'date_added' in rec and not rec['date_added'].IsDBNull else None
                    media.modify_date = rec['date_modified'].Value if 'date_modified' in rec and not rec['date_modified'].IsDBNull else None
                    media.mime_type = rec['mime_type'].Value if 'mime_type' in rec and not rec['mime_type'].IsDBNull else None
                    media.title = rec['title'].Value if 'title' in rec and not rec['title'].IsDBNull else None
                    media.display_name = rec['_display_name'].Value if '_display_name' in rec and not rec['_display_name'].IsDBNull else None
                    media.latitude = rec['latitude'].Value if 'latitude' in rec and not rec['latitude'].IsDBNull else None
                    media.longitude = rec['longitude'].Value if 'longitude' in rec and not rec['longitude'].IsDBNull else None
                    media.datetaken = rec['datetaken'].Value if 'datetaken' in rec and not rec['datetaken'].IsDBNull else None
                    media.bucket_display_name = rec['bucket_display_name'].Value if 'bucket_display_name' in rec and not rec['bucket_display_name'].IsDBNull else None
                    media.year = rec['year'].Value if 'year' in rec and not rec['year'].IsDBNull else None
                    media.album_artist = rec['album_artist'].Value if 'album_artist' in rec and not rec['album_artist'].IsDBNull else None
                    media.duration = rec['duration'].Value if 'duration' in rec and not rec['duration'].IsDBNull else None
                    media.artist = rec['artist'].Value if 'artist' in rec and not rec['artist'].IsDBNull else None
                    media.album = rec['album'].Value if 'album' in rec and not rec['album'].IsDBNull else None
                    media.location = 'internal' if i==0 else 'external'
                    media.source = 'android多媒体'
                    self.mm.db_insert_table_media(media)
                self.mm.db_commit()
            except Exception as e:
                logging.error(e)

    def analyze_thumbnails(self):
        thumbnails = Thumbnails()
        for i, node in enumerate(self.nodes):
            try:
                self.db = SQLiteParser.Database.FromNode(node)
                if self.db is None:
                    return 
                ts = SQLiteParser.TableSignature('thumbnails')
                for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                    thumbnails.id = rec['_id'].Value if '_id' in rec else None
                    thumbnails.url = rec['_data'].Value if '_data' in rec else None
                    thumbnails.image_id = rec['image_id'].Value if 'image_id' in rec else None
                    thumbnails.width = rec['width'].Value if 'width' in rec else None
                    thumbnails.height = rec['height'].Value if 'height' in rec else None
                    thumbnails.location = 'internal' if i==0 else 'external'
                    thumbnails.source = 'android多媒体'
                    self.mm.db_insert_table_thumbnails(thumbnails)
                self.mm.db_commit()
            except Exception as e:
                logging.error(e)

    def parse(self):
        self.analyze_media()
        self.analyze_thumbnails()
        self.mm.db_close()
        generate = Generate(self.db_cache)
        models = generate.get_models()
        return models

def analyze_android_media(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(MediaParse(node, extractDeleted, extractSource).parse())
    return pr

def execute(node, extractDeleted):
    return analyze_android_soundrecord(node, extractDeleted, False)