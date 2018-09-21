#coding=utf-8
import os
import PA_runtime
import sqlite3
from PA_runtime import *
#SafeLoadAssembly('model_soundrecord')
from model_soundrecord import *


class SoundrecordParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = False
        self.extractSource = extractSource
        self.db = None
        self.ms = MS()
        self.cache_path = ds.OpenCachePath("SOUNDRECORD")
        self.cachedb = self.cache_path + "\\soundrecord.db"
        self.ms.db_create(self.cachedb)

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
                records.source = self.node.AbsolutePath
                self.ms.db_insert_table_records(records)
            self.ms.db_commit()
            records = Records()
            for rec in self.db.ReadTableDeletedRecords(ts, False):
                canceller.ThrowIfCancellationRequested()
                records.record_id = rec['_id'].Value if '_id' in rec else None
                records.record_name = rec['title'].Value if 'title' in rec else None
                records.record_create = rec['date_added'].Value if 'date_added' in rec else None
                if 'duration' in rec:
                    records.record_duration = int((rec['duration'].Value)/1000) if rec['duration'].Value is not None else None
                records.record_size = rec['file_size'].Value if 'file_size' in rec else None
                records.record_url = rec['_data'].Value if '_data' in rec else None
                records.source = self.node.AbsolutePath
                records.deleted = 1
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
                records.source = self.node.AbsolutePath
                self.ms.db_insert_table_records(records)
            self.ms.db_commit()
        except Exception as e:
            pass

    def parse(self):
        self.analyze_soundrecord_huawei()
        self.analyze_soundrecord_xiaomi()
        self.ms.db_close()
        generate = Generate(self.cachedb)
        models = generate.get_models()
        return models

def analyze_android_soundrecord(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(SoundrecordParse(node, extractDeleted, extractSource).parse())
    pr.Build('SoundRecords')
    return pr

def execute(node, extractDeleted):
    return analyze_android_soundrecord(node, extractDeleted, False)