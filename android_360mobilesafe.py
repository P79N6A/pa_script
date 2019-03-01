# _*_ coding:utf-8 _*_
__author__ = "xiaoyuge"

from PA_runtime import *
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('model_secure')
except:
    pass
del clr

from PA.Common.Utilities.Types import TimeStampFormats

import System.Data.SQLite as SQLite
import model_secure

import re
import hashlib
import traceback
import json
import time

from System.Text import *

import base64
import hashlib

VERSION_APP_VALUE = 1

class QihooMobileSecureParser(model_secure.SM):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node
        self.extractDeleted = extract_deleted
        self.cachepath = ds.OpenCachePath("350安全卫士")
        md5_db = hashlib.md5()
        db_name = self.node.AbsolutePath
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\360MobileSafeSource'

    def parse(self):
        if self.need_parse(self.cachedb, VERSION_APP_VALUE):
            self.db_create(self.cachedb)
            self.analyze_data()
            self.db_insert_table_version(model_secure.VERSION_KEY_DB, model_secure.VERSION_VALUE_DB)
            self.db_insert_table_version(model_secure.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
        models = []
        models_secure = model_secure.GenerateModel(self.cachedb).get_models()
        models.extend(models_secure)
        return models

    def analyze_data(self):
        '''分析数据'''
        self.parse_account()
        self.parse_blacklist()
        self.parse_blockedsms()
        self.mark()
        self.parse_blockedrecord()

    def parse_account(self):
        try:
            account = model_secure.Account()
            account.account_id = 1
            account.nickname = '本地账户'
            account.username = '本地账户'
            self.db_insert_table_account(account)
            self.db_commit()
        except:
            pass

    def parse_blacklist(self):
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('blacklist')
            ts.Add("phone_number", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            data = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if rec.IsDeleted:
                        continue
                    id = self._db_record_get_int_value(rec, '_id')
                    contact_name = self._db_record_get_string_value(rec, 'contact_name')
                    phone_number = self._db_record_get_string_value(rec, 'phone_number')
                    date = 0
                    blacklist = model_secure.Blacklist()
                    blacklist.id = id
                    blacklist.name = contact_name
                    blacklist.phone_number = phone_number
                    blacklist.add_date = 0
                    blacklist.source = self.node.AbsolutePath
                    blacklist.deleted = rec.IsDeleted
                    self.db_insert_table_blacklist(blacklist)
                except:
                    traceback.print_exc()
            self.db_commit()
        except:
            traceback.print_exc()

    def parse_blockedsms(self):
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('blocksystem')
            ts.Add("block_msg", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            ts.Add("number", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            data = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if rec.IsDeleted:
                        continue
                    id = self._db_record_get_int_value(rec, '_id')
                    block_msg = self._db_record_get_string_value(rec, 'block_msg')
                    phone_number = self._db_record_get_string_value(rec, 'number')
                    block_date = self._db_record_get_int_value(rec, 'rec_date')
                    blockedsms = model_secure.BlockedSms()
                    blockedsms.id = id
                    blockedsms.content = block_msg
                    blockedsms.phone_number = phone_number
                    blockedsms.block_time = block_date
                    blockedsms.source = self.node.AbsolutePath
                    blockedsms.deleted = rec.IsDeleted
                    self.db_insert_table_blockedsms(blockedsms)
                except:
                    traceback.print_exc()
            self.db_commit()
        except:
            traceback.print_exc()

    def mark(self):
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('marker')
            self.mark2type = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    address = self._db_record_get_string_value(rec, 'address')
                    marker_type = self._db_record_get_string_value(rec, 'marker_type_name')
                    if re.findall('骚扰', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_HARASS
                    elif re.findall('广告', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_AD
                    elif re.findall('中介', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_INTERMEDIARY
                    elif re.findall('诈骗', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_DEFRAUD
                    elif re.findall('快递', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_EXPRESS
                    elif re.findall('出租车', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_TEXI
                    elif re.findall('响一声', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_RINGOUT
                    elif re.findall('保险', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_INSURANCE
                    elif re.findall('招聘', marker_type):
                        marker_type = model_secure.CALL_RECORD_TYPE_RECRIT
                    if address not in self.mark2type:
                        self.mark2type[address] = marker_type
                except:
                    traceback.print_exc()
            self.db_commit()
        except:
            traceback.print_exc()

    def parse_blockedrecord(self):
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('call_history')
            ts.Add("address", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            data = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if rec.IsDeleted:
                        continue
                    id = self._db_record_get_int_value(rec, '_id')
                    phone_number = self._db_record_get_string_value(rec, 'address')
                    block_date = self._db_record_get_int_value(rec, 'date')
                    call = model_secure.Callrecord()
                    call._id = id
                    call.phone_number = phone_number
                    call.date = block_date
                    call.call_type = self.mark2type[phone_number] if phone_number in self.mark2type else 0
                    call.source = self.node.AbsolutePath
                    call.deleted = rec.IsDeleted
                    if call.phone_number is not '':
                        self.db_insert_table_callrecord(call)
                except:
                    traceback.print_exc()
            self.db_commit()
        except:
            traceback.print_exc()

    @staticmethod
    def _db_record_get_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            return record[column].Value
        return default_value

    @staticmethod
    def _db_record_get_string_value(record, column, default_value=''):
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
        if not IsDBNull(record[column].Value):
            try:
                return int(record[column].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_blob_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytes(value)
            except Exception as e:
                return default_value
        return default_value

def analyze_android_360mobilesafe(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(QihooMobileSecureParser(node, extractDeleted, extractSource).parse())
    pr.Build('360安全卫士')
    return pr

def execute(node, extractDeleted):
    return analyze_android_360mobilesafe(node, extractDeleted, False)