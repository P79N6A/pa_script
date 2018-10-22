#coding=utf-8
import os
import PA_runtime
import datetime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_callrecord')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import hashlib
import bcp_basic
from model_callrecord import MC, Records, Generate
import model_callrecord
import time
import shutil


class CallsParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = False
        self.extractSource = extractSource
        self.db = None
        self.db_cmd = None
        self.mc = MC()
        self.cache_path = ds.OpenCachePath("CallRecord")
        md5_db = hashlib.md5()
        db_name = 'record'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cache_path + '\\CallSourceDB'
        self.db_tempo = self.sourceDB + '\\db_tempo.db'
        self.mc.db_create(self.cachedb)

    def analyze_call_records(self):
        try:
            node = self.node.Parent.GetByPath('/calls.db')
            self.db = SQLiteParser.Database.FromNode(node, canceller)
            if self.db is None:
                node = self.node.Parent.GetByPath('/calllog.db')
                self.db = SQLiteParser.Database.FromNode(node, canceller)
                if self.db is None:
                    node = self.node.Parent.GetByPath('/contacts2.db')
                    self.db = SQLiteParser.Database.FromNode(node, canceller)
                    if self.db is None:
                        self.analyze_logic_calls()
                        return
            ts = SQLiteParser.TableSignature('calls')
            records = Records()
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                records.id = rec['_id'].Value if '_id' in rec else None
                records.phone_number = rec['number'].Value if 'number' in rec else None
                records.date = rec['date'].Value if 'date' in rec else None
                records.duration = rec['duration'].Value if 'duration' in rec else None
                records.type = rec['type'].Value if 'type' in rec else None
                records.name = rec['name'].Value if 'name' in rec else rec['number'].Value if 'number' in rec else None
                records.geocoded_location = rec['geocoded_location'].Value if 'geocoded_location' in rec else None
                records.ring_times = rec['ring_times'].Value if 'ring_times' in rec else None
                records.mark_type = rec['mark_type'].Value if 'mark_type' in rec else None
                records.country_code = rec['countryiso'].Value if 'countryiso' in rec else None
                records.mark_content = rec['mark_content'].Value if 'mark_content' in rec else None
                records.source = node.AbsolutePath
                self.mc.db_insert_table_call_records(records)
            self.mc.db_commit()
            records = Records()
            for rec in self.db.ReadTableDeletedRecords(ts, False):
                canceller.ThrowIfCancellationRequested()
                records.id = rec['_id'].Value if '_id' in rec else None
                records.phone_number = rec['number'].Value if 'number' in rec else None
                records.date = rec['date'].Value if 'date' in rec else None
                records.duration = rec['duration'].Value if 'duration' in rec else None
                records.type = rec['type'].Value if 'type' in rec else None
                records.name = rec['name'].Value if 'name' in rec else rec['number'].Value if 'number' in rec else None
                records.geocoded_location = rec['geocoded_location'].Value if 'geocoded_location' in rec else None
                records.ring_times = rec['ring_times'].Value if 'ring_times' in rec else None
                records.mark_type = rec['mark_type'].Value if 'mark_type' in rec else None
                records.country_code = rec['countryiso'].Value if 'countryiso' in rec else None
                records.mark_content = rec['mark_content'].Value if 'mark_content' in rec else None
                records.source = node.AbsolutePath
                records.deleted = 1
                self.mc.db_insert_table_call_records(records)
            self.mc.db_commit()
        except Exception as e:
            print(e)

    def analyze_logic_calls(self):
        try:
            node = self.node.Parent.GetByPath('/callinfo.db')
            self.db = SQLiteParser.Database.FromNode(node, canceller)
            if self.db is None:
                raise Exception('解析通话记录出错：无法读取通话记录数据库')
            try:
                ts = SQLiteParser.TableSignature('Calls')
            except:
                return
            records = Records()
            id = 0
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                id += 1
                canceller.ThrowIfCancellationRequested()
                records.id = id
                records.phone_number = rec['number'].Value if 'number' in rec else None
                records.date = self.transdate(rec['time'].Value) if 'time' in rec else None
                records.duration = self.transtime(rec['talkTime'].Value) if 'talkTime' in rec else None
                records.type = rec['callType'].Value if 'callType' in rec else None
                records.name = rec['name'].Value if 'name' in rec else rec['number'].Value if 'number' in rec else None
                records.source = node.AbsolutePath
                self.mc.db_insert_table_call_records(records)
            self.mc.db_commit()
        except:
            pass

    def transtime(self, calltime):
        minute = re.sub('分.*', '', calltime)
        second = re.findall('.*分(.*)秒',calltime)[0]
        duration = int(minute)*60 + int(second)
        return duration

    def transdate(self, calldate):
        return time.mktime(time.strptime(calldate,'%Y-%m-%d %H:%M:%S'))

    def parse(self):
        self._copytocache()
        self._closewal('contacts2.db')
        self.analyze_call_records()
        self.mc.db_close()
        try:
            if os.path.exists(self.sourceDB):
                shutil.rmtree(self.sourceDB)
        except:
            pass
        #bcp entry
        temp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_basic.BASIC_RECORD_INFORMATION, self.cachedb, temp_dir)
        generate = Generate(self.cachedb)
        models = generate.get_models()
        return models

    def _copytocache(self):
        sourceDir = self.node.Parent.PathWithMountPoint
        targetDir = self.sourceDB
        try:
            if os.path.exists(targetDir):
                shutil.rmtree(targetDir)
            shutil.copytree(sourceDir, targetDir)
        except Exception:
            traceback.print_exc()

    def _closewal(self, dbfile):
        try:
            sourceDB = self.node.Parent.PathWithMountPoint + '\\' + dbfile
            targetDB = self.sourceDB + '\\' + dbfile
            f = open(sourceDB, 'rb')
            context = list(f.read())
            f.close()
            if os.path.exists(targetDB):
                os.remove(targetDB)
            f = open(targetDB, 'ab+')
            for i,c in enumerate(context):
                if i == 18 or i == 19:
                    f.write('\x01')
                else:
                    f.write(c)
            f.close()
        except:
            pass

def analyze_android_calls(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(CallsParse(node, extractDeleted, extractSource).parse())
    pr.Build('通话记录')
    return pr

def execute(node, extractDeleted):
    return analyze_android_calls(node, extractDeleted, False)