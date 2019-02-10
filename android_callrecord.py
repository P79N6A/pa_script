#coding=utf-8
__author__ = 'xiaoyuge'

import os
import PA_runtime
import datetime
from PA_runtime import *
import clr
try:
    clr.AddReference('System.Xml.Linq')
    clr.AddReference('model_callrecord')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import hashlib
import bcp_basic
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from model_callrecord import MC, Records, Generate
import model_callrecord
import time
import shutil
import traceback

VERSION_APP_VALUE = 2


class CallsParse(MC):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.db_cmd = None
        self.cache_path = ds.OpenCachePath("CallRecord")
        md5_db = hashlib.md5()
        db_name = self.node.AbsolutePath
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cache_path + '\\CallSourceDB'

    def parse(self):
        if self.need_parse(self.cachedb, VERSION_APP_VALUE):
            self.db_create(self.cachedb)
            #通用备份案例
            if re.findall('callinfo.db', self.node.AbsolutePath):
                self.analyze_call_records_case4()
            #华为自带备份案例
            elif re.findall('calllog.db', self.node.AbsolutePath):
                self.analyze_call_records_case5()
            #oppo自带备份案例
            elif re.findall('callrecord_backup.xml', self.node.AbsolutePath):
                self.analyze_call_records_case6()
            #vivo自带备份案例
            #小米自带备份案例
            #华为全盘通话记录存储在calls.db中
            elif self.node.Parent.GetByPath('/calls.db'):
                self.analyze_call_records_case1()
            #vivo全盘通话记录存储在user_de/0/com.android.providers.contact/callllog.db中
            elif self.node.Parent.Parent.GetByPath('/databases/calllog.db'):
                self.analyze_call_records_case2()
            #通用全盘案例
            elif re.findall('contacts2.db', self.node.AbsolutePath):
                self.analyze_call_records_case3()
            self.db_insert_table_version(model_callrecord.VERSION_KEY_DB, model_callrecord.VERSION_VALUE_DB)
            self.db_insert_table_version(model_callrecord.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
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

    def analyze_call_records_case1(self):
        '''解析callls.db'''
        try:
            node = self.node.Parent.GetByPath('/calls.db')
            db = SQLiteParser.Database.FromNode(node, canceller)
            ts = SQLiteParser.TableSignature('calls')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                try:
                    self.give_values_to_model(node, rec)
                except:
                    traceback.print_exc()
            self.db_commit()
        except:
            traceback.print_exc()

    def analyze_call_records_case2(self):
        '''解析calllog.db'''
        try:
            node = self.node.Parent.GetByPath('/calllog.db')
            db = SQLiteParser.Database.FromNode(node, canceller)
            ts = SQLiteParser.TableSignature('calls')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                try:
                    if 'logtype' in rec:
                        if rec['logtype'].Value == 300:
                            continue
                    self.give_values_to_model(node, rec)
                except:
                    pass
            self.db_commit()
        except:
            traceback.print_exc()

    def analyze_call_records_case3(self):
        '''解析contacts2.db'''
        try:
            node = self.node.Parent.GetByPath('/contacts2.db')
            db = SQLiteParser.Database.FromNode(node, canceller)
            ts = SQLiteParser.TableSignature('calls')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                try:
                    self.give_values_to_model(node, rec)
                except:
                    pass
            self.db_commit()
        except:
            traceback.print_exc()

    def give_values_to_model(self, node, rec):
        try:
            records = Records()
            records.id = self._db_record_get_int_value(rec, '_id')
            records.phone_number = self._db_record_get_string_value(rec, 'number')
            records.date = self._db_record_get_int_value(rec, 'date')
            records.duration = self._db_record_get_int_value(rec, 'duration')
            records.type = self._db_record_get_int_value(rec, 'type')
            name = self._db_record_get_string_value(rec, 'name')
            records.name = name if name is not '' else records.phone_number
            records.geocoded_location = self._db_record_get_string_value(rec, 'geocoded_location')
            records.ring_times = self._db_record_get_int_value(rec, 'ring_times')
            records.mark_type = self._db_record_get_string_value(rec, 'mark_type')
            records.country_code = self._db_record_get_string_value(rec, 'countryiso')
            records.mark_content = self._db_record_get_string_value(rec, 'mark_content')
            records.source = node.AbsolutePath
            records.deleted = rec.IsDeleted
            self.db_insert_table_call_records(records)
        except:
            traceback.print_exc()

    def analyze_call_records_case4(self):
        try:
            node = self.node.Parent.GetByPath('/callinfo.db')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            try:
                ts = SQLiteParser.TableSignature('Calls')
            except:
                return
            records = Records()
            id = 0
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    id += 1
                    canceller.ThrowIfCancellationRequested()
                    records.id = id
                    records.phone_number = self._db_record_get_string_value(rec, 'number')
                    records.date = self.transdate(rec['time'].Value) if 'time' in rec else None
                    records.duration = self.transtime(rec['talkTime'].Value) if 'talkTime' in rec else None
                    records.type = self._db_record_get_int_value(rec, 'callType')
                    name = self._db_record_get_string_value(rec, 'name')
                    records.name = name if name is not '' else records.phone_number
                    records.source = node.AbsolutePath
                    self.db_insert_table_call_records(records)
                except:
                    pass
            self.db_commit()
        except:
            pass

    def transtime(self, calltime):
        minute = re.sub('分.*', '', calltime)
        second = re.findall('.*分(.*)秒',calltime)[0]
        duration = int(minute)*60 + int(second)
        return duration

    def transdate(self, calldate):
        return time.mktime(time.strptime(calldate,'%Y-%m-%d %H:%M:%S'))

    def analyze_call_records_case5(self):
        '''华为自带备份'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            ts = SQLiteParser.TableSignature('calls_tb')
            id = 0
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                try:
                    records = Records()
                    id += 1
                    records.id = id
                    records.phone_number = self._db_record_get_string_value(rec, 'number')
                    records.date = self._db_record_get_int_value(rec, 'date')
                    records.duration = self._db_record_get_int_value(rec, 'duration')
                    records.type = self._db_record_get_int_value(rec, 'type')
                    name = self._db_record_get_string_value(rec, 'name')
                    records.name = name if name is not '' else records.phone_number
                    records.geocoded_location = self._db_record_get_string_value(rec, 'geocoded_location')
                    records.ring_times = self._db_record_get_int_value(rec, 'ring_times')
                    records.mark_type = self._db_record_get_string_value(rec, 'mark_type')
                    records.country_code = self._db_record_get_string_value(rec, 'countryiso')
                    records.mark_content = self._db_record_get_string_value(rec, 'mark_content')
                    records.source = node.AbsolutePath
                    records.deleted = rec.IsDeleted
                    self.db_insert_table_call_records(records)
                except:
                    pass
            self.db_commit()
        except:
            traceback.print_exc()

    def analyze_call_records_case6(self):
        try:
            if self.node and self.node.Data:
                xml_data = XElement.Parse(self.node.read())
                xml_data = str(xml_data)
                print(xml_data)
                recs = re.findall('<CALL_RECORDS(.*?)/>', xml_data)
                for rec in recs:
                    try:
                        d = re.findall('"(.*?)"', rec)
                        records = Records()
                        records.id = int(d[0])
                        records.phone_number = str(d[1])
                        records.date = int(d[4]) if d[4] is not ' ' else 0
                        records.duration = int(d[2]) if d[2] is not ' ' else 0
                        records.type = int(d[3]) if d[3] is not ' ' else 0
                        name = str(d[5]) 
                        records.name = name if name is not ' ' else records.phone_number
                        records.ring_times = int(d[8]) if d[8] is not ' ' else 0
                        records.source = self.node.AbsolutePath
                        self.db_insert_table_call_records(records)
                    except:
                        traceback.print_exc()
                self.db_commit() 
        except:
            traceback.print_exc()

    @staticmethod
    def _db_record_get_value(record, column, default_value=None):
        if column not in record:
            return default_value
        if not record[column].IsDBNull:
            return record[column].Value
        return default_value

    @staticmethod
    def _db_record_get_string_value(record, column, default_value=''):
        if column not in record:
            return default_value
        if not record[column].IsDBNull:
            try:
                value = str(record[column].Value)
                #if record.Deleted != DeletedState.Intact:
                #    value = filter(lambda x: x in string.printable, value)
                return value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_int_value(record, column, default_value=0):
        if column not in record:
            return default_value
        if not record[column].IsDBNull:
            try:
                return int(record[column].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_blob_value(record, column, default_value=None):
        if column not in record:
            return default_value
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytes(value)
            except Exception as e:
                return default_value
        return default_value

def analyze_android_calls(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(CallsParse(node, extractDeleted, extractSource).parse())
    pr.Build('通话记录')
    return pr

def execute(node, extractDeleted):
    return analyze_android_calls(node, extractDeleted, False)