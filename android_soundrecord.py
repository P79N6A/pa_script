#coding=utf-8
__author__ = "xiaoyuge"

import os
import PA_runtime
import sqlite3
from PA_runtime import *
import clr
try:
    clr.AddReference('model_soundrecord')
except:
    pass
del clr
import hashlib
from model_soundrecord import *
import model_soundrecord

VERSION_APP_VALUE = 1


class SoundrecordParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.ms = MS()
        self.cache_path = ds.OpenCachePath("SOUNDRECORD")
        md5_db = hashlib.md5()
        db_name = 'soundrecord'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"

    def analyze_soundrecord_huawei(self):
        try:
            records = Records()
            self.db = SQLiteParser.Database.FromNode(self.node, canceller)

            if self.db is None:
                return 
            ts = SQLiteParser.TableSignature('normal_record_table')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                records.record_id = rec['_id'].Value if '_id' in rec else None
                records.record_name = rec['title'].Value if 'title' in rec else None
                records.record_create = rec['date_added'].Value if 'date_added' in rec else None
                if 'duration' in rec:
                    records.record_duration = int((rec['duration'].Value)/1000) if rec['duration'].Value is not None else None
                records.record_size = rec['file_size'].Value if 'file_size' in rec else None
                records.record_url = rec['_data'].Value if '_data' in rec else None
                media_nodes = self.node.FileSystem.Search(os.path.basename(records.record_url) + '$')
                if len(list(media_nodes)) != 0:
                    for node in media_nodes:
                        records.record_url = node.AbsolutePath
                        break
                records.source = self.node.AbsolutePath
                records.deleted = rec.IsDeleted
                self.ms.db_insert_table_records(records)
            self.ms.db_commit()
        except Exception as e:
            pass

    def analyze_soundrecord_xiaomi(self):
        records = Records()
        try:
            self.db = SQLiteParser.Database.FromNode(self.node, canceller)
            if self.db is None:
                return 
            ts = SQLiteParser.TableSignature('records')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                records.record_id = rec['_id'].Value if '_id' in rec else None
                records.record_name = rec['file_name'].Value if 'file_name' in rec else None
                records.record_create = rec['create_time'].Value if 'create_time' in rec else None
                records.record_duration = rec['duration'].Value if 'duration' in rec else None
                records.record_size = rec['file_size'].Value if 'file_size' in rec else None
                records.record_url = rec['file_path'].Value if 'file_path' in rec else None
                media_nodes = self.node.FileSystem.Search(os.path.basename(records.record_url) + '$')
                if len(list(media_nodes)) != 0:
                    for node in media_nodes:
                        records.record_url = node.AbsolutePath
                        break
                records.source = self.node.AbsolutePath
                records.deleted = rec.IsDeleted
                self.ms.db_insert_table_records(records)
            self.ms.db_commit()
        except Exception as e:
            pass

    def parse(self):
        if self.ms.need_parse(self.cachedb, VERSION_APP_VALUE):
            self.ms.db_create(self.cachedb)
            self.analyze_soundrecord_huawei()
            self.analyze_soundrecord_xiaomi()
            self.ms.db_insert_table_version(model_soundrecord.VERSION_KEY_DB, model_soundrecord.VERSION_VALUE_DB)
            self.ms.db_insert_table_version(model_soundrecord.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.ms.db_commit()
            self.ms.db_close()
        generate = Generate(self.cachedb)
        models = generate.get_models()
        return models

def analyze_android_soundrecord(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(SoundrecordParse(node, extractDeleted, extractSource).parse())
    pr.Build('录音')
    return pr

def execute(node, extractDeleted):
    return analyze_android_soundrecord(node, extractDeleted, False)