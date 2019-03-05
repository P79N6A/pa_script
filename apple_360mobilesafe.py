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
        self.db = None
        self.cachepath = ds.OpenCachePath("360手机卫士")
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
        self.parse_battery()

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
            blacklist_node = self.node.Parent.Search('CallerIdentity.plist$')
            if len(list(blacklist_node)) == 0:
                return
            blacklist = blacklist_node[0].PathWithMountPoint
            f = open(blacklist, 'rb')
            content = bytes(f.read())
            if content is not None:
                tree = BPReader.GetTree(MemoryRange.FromBytes(Array[Byte](bytearray(content))))
                if tree is None:
                    return
                bplist_dic = self.bplist2dic(tree)
                if 'localSpamNumbers' in bplist_dic:
                    tree = BPReader.GetTree(MemoryRange.FromBytes(bplist_dic['localSpamNumbers']))
                    dic = self.bplist2dic(tree)
                    for item in dic.values():
                        number = item['number'] if 'number' in item else ''
                        displayNumber = item['displayNumber'] if 'displayNumber' in item else ''
                        blacklist = model_secure.Blacklist()
                        blacklist.phone_number = displayNumber if displayNumber is not '' else number
                        blacklist.add_date = 0
                        blacklist.source = blacklist_node[0].AbsolutePath
                        self.db_insert_table_blacklist(blacklist)
                    self.db_commit()
            f.close()
        except Exception as e:
            print(e)

    def bplist2dic(self, value):
        if str(type(value)) != "<type 'Dictionary[str, IKNode]'>" and str(type(value)) != "<type 'BPObjectTreeNode'>" and str(type(value)) != "<type 'List[IKNode]'>" and str(type(value)) != "<type 'BPRawTreeNode'>":
            return value
        d = {}
        for c, j in enumerate(value):
            key = 'key'+str(c) if j.Key is None else j.Key
            if key == 'attachments':
                pass
            v = '' if j.Value is None else j.Value
            v = self.bplist2dic(v)
            d[str(key)] = v
        if d == {}:
            if str(type(value)) == "<type 'List[IKNode]'>":
                if len(value) == 0:
                    return ''
                else:
                    return self.bplist2dic(value.Value)
            if str(type(value)) == "<type 'BPObjectTreeNode'>":
                return self.bplist2dic(value.Value)
            return str(value.Value)
        return d

    def parse_battery(self):
        try:
            battery_node = self.node.Parent.GetByPath('/Battery/charge.db')
            db = SQLiteParser.Database.FromNode(battery_node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('charge_log')
            ts.Add("begin_time", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            ts.Add("end_time", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            ts.Add("begin_level", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            ts.Add("end_level", SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull))
            for rec in db.ReadTableRecords(ts, False, True):
                try:
                    id = self._db_record_get_int_value(rec, 'id')
                    begin_time = self._db_record_get_string_value(rec, 'begin_time')
                    end_time = self._db_record_get_string_value(rec, 'end_time')
                    begin_level = self._db_record_get_value(rec, 'begin_level')
                    end_level = self._db_record_get_value(rec, 'end_level')
                    charge = model_secure.Charge()
                    charge.id = id
                    charge.begin_time = begin_time
                    charge.end_time = end_time
                    charge.begin_level = begin_level
                    charge.end_level = end_level
                    charge.source = battery_node.AbsolutePath
                    charge.deleted = rec.IsDeleted
                    self.db_insert_table_charge(charge)
                except:
                    traceback.print_exc()
            self.db_commit()
        except:
            traceback.print_exc()

    @staticmethod
    def _db_record_get_value(record, column, default_value=0):
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

def analyze_apple_360mobilesafe(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(QihooMobileSecureParser(node, extractDeleted, extractSource).parse())
    pr.Build('360手机卫士')
    return pr

def execute(node, extractDeleted):
    return analyze_apple_360mobilesafe(node, extractDeleted, False)