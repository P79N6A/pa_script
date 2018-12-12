#coding=utf-8
__author__ = "xiaoyuge"

import os
import PA_runtime
import datetime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_contact')
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import hashlib
from model_contact import MC, Contact, Generate
import model_contact
import bcp_basic
import System.Data.SQLite as SQLite
import sys
import re
import time
import shutil

SQL_TABLE_JOIN_CONTACT = '''
    select distinct e.*, f.mimetype from (select c.*, d.last_time_contacted, d.contact_last_updated_timestamp, d.times_contacted from(
        select a.raw_contact_id, a.mimetype_id, a.data1, a.data2, a.data3, a.data4, a.data15, a.deleted,
        b.contact_id from data as a left join raw_contacts as b on a.raw_contact_id = b._id) 
        as c left join contacts as d on c.contact_id == d._id) as e left join mimetypes as f on e.mimetype_id = f._id 
    '''

SQL_CREATE_TABLE_DATA = '''CREATE TABLE IF NOT EXISTS data(
    raw_contact_id INTEGER,
    mimetype_id INTEGER,
    data1 TEXT,
    data2 TEXT,
    data3 TEXT,
    data4 TEXT,
    data15 TEXT,
    deleted INTEGER
    )'''

SQL_INSERT_TABLE_DATA = '''
    INSERT OR IGNORE INTO data(raw_contact_id, mimetype_id, data1, data2, data3, data4, data15, deleted)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_RAW_CONTACTS = '''CREATE TABLE IF NOT EXISTS raw_contacts(
    _id INTEGER,
    contact_id INTEGER
    )'''

SQL_INSERT_TABLE_RAW_CONTACTS = '''
    INSERT OR IGNORE INTO raw_contacts(_id, contact_id)
    VALUES(?, ?)'''

SQL_CREATE_TABLE_CONTACTS = '''CREATE TABLE IF NOT EXISTS contacts(
    _id INTEGER,
    last_time_contacted INTEGER,
    contact_last_updated_timestamp INTEGER,
    times_contacted INTEGER
    )'''

SQL_INSERT_TABLE_CONTACTS = '''
    INSERT OR IGNORE INTO contacts(_id, last_time_contacted, contact_last_updated_timestamp, times_contacted)
    VALUES(?, ?, ?, ?)'''

SQL_CREATE_TABLE_MIMETYPES = '''CREATE TABLE IF NOT EXISTS mimetypes(
    _id INTEGER,
    mimetype TEXT
    )'''

SQL_INSERT_TABLE_MIMETYPES = '''
    INSERT OR IGNORE INTO mimetypes(_id, mimetype)
    VALUES(?, ?)'''

VERSION_APP_VALUE = 1


class CallsParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = False
        self.extractSource = extractSource
        self.db = None
        self.db_cmd = None
        self.mc = MC()
        self.cache_path = ds.OpenCachePath("Contact")
        md5_db = hashlib.md5()
        db_name = 'contact'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cache_path + '\\CallSourceDB'
        self.db_tempo = self.sourceDB + '\\db_tempo.db'
        self.db_deleted = self.sourceDB + '\\db_deleted.db'

    def analyze_call_contacts(self):
        try:
            contactsNode = self.db_tempo
            db = SQLite.SQLiteConnection('Data Source = {}'.format(contactsNode))
            db.Open()
            db_cmd = SQLite.SQLiteCommand(db)
        except:
            return
        try:
            sqls = [model_contact.SQL_FIND_TABLE_CONTACTS_INTACT, model_contact.SQL_FIND_TABLE_CONTACTS_DELETED]
            for sql in sqls:
                if db is None:
                    return
                db_cmd.CommandText = sql
                sr = db_cmd.ExecuteReader()
                while (sr.Read()):
                    contacts = Contact()
                    if canceller.IsCancellationRequested:
                        break
                    contacts.raw_contact_id = sr[0]
                    contacts.mail = sr[1]
                    contacts.company = sr[2]
                    contacts.title = sr[3]
                    contacts.last_time_contact = sr[4]
                    contacts.last_time_modify = sr[5]
                    contacts.times_contacted = sr[6]
                    contacts.phone_number = sr[7]
                    contacts.name = sr[8]
                    contacts.address = sr[9]
                    contacts.notes = sr[10]
                    contacts.telegram = sr[11]
                    contacts.head_pic = sr[12]
                    contacts.source = sr[13]
                    contacts.deleted = sr[14]
                    self.mc.db_insert_table_call_contacts(contacts)
                self.mc.db_commit()
                sr.Close()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def analyze_deleted_records(self):
        #创建恢复数据库
        if os.path.exists(self.db_deleted):
            os.remove(self.db_deleted)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_deleted))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_cmd.CommandText = SQL_CREATE_TABLE_DATA
        self.db_cmd.ExecuteNonQuery()
        self.db_cmd.CommandText = SQL_CREATE_TABLE_RAW_CONTACTS
        self.db_cmd.ExecuteNonQuery()
        self.db_cmd.CommandText = SQL_CREATE_TABLE_CONTACTS
        self.db_cmd.ExecuteNonQuery()
        self.db_cmd.CommandText = SQL_CREATE_TABLE_MIMETYPES
        self.db_cmd.ExecuteNonQuery()
        #向恢复数据库中插入数据
        node = self.node.Parent.GetByPath('/contacts2.db')
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            self.db_cmd.Dispose()
            self.db.Close()
            return
        ts = SQLiteParser.TableSignature('data')
        for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
            try:
                if rec['raw_contact_id'].Value is not None and 'raw_contact_id' in rec:
                    raw_contact_id = rec['raw_contact_id'].Value
                if rec['mimetype_id'].Value is not None and 'mimetype_id' in rec:
                    mimetype_id = rec['mimetype_id'].Value
                if rec['data1'].Value is not None and 'data1' in rec:
                    data1 = rec['data1'].Value
                if rec['data2'].Value is not None and 'data2' in rec:
                    data2 = rec['data2'].Value
                if rec['data3'].Value is not None and 'data3' in rec:
                    data3 = rec['data3'].Value
                if rec['data4'].Value is not None and 'data4' in rec:
                    data4 = rec['data4'].Value
                if rec['data5'].Value is not None and 'data5' in rec:
                    data15 = rec['data5'].Value
                deleted = 0
                params = (raw_contact_id, mimetype_id, data1, data2, data3, data4, data15, deleted)
                self.db_insert_table(SQL_INSERT_TABLE_DATA, params)
            except:
                traceback.print_exc()
        for rec in db.ReadTableDeletedRecords(ts, False):
            try:
                if rec['raw_contact_id'].Value is not None and 'raw_contact_id' in rec:
                    raw_contact_id = rec['raw_contact_id'].Value
                if rec['mimetype_id'].Value is not None and 'mimetype_id' in rec:
                    mimetype_id = rec['mimetype_id'].Value
                if rec['data1'].Value is not None and 'data1' in rec:
                    data1 = rec['data1'].Value
                if rec['data2'].Value is not None and 'data2' in rec:
                    data2 = rec['data2'].Value
                if rec['data3'].Value is not None and 'data3' in rec:
                    data3 = rec['data3'].Value
                if rec['data4'].Value is not None and 'data4' in rec:
                    data4 = rec['data4'].Value
                if rec['data5'].Value is not None and 'data5' in rec:
                    data15 = rec['data5'].Value
                deleted = 1
                params = (raw_contact_id, mimetype_id, data1, data2, data3, data4, data15, deleted)
                self.db_insert_table(SQL_INSERT_TABLE_DATA, params)
            except:
                traceback.print_exc()
        ts2 = SQLiteParser.TableSignature('raw_contacts')
        for rec in db.ReadTableRecords(ts2, self.extractDeleted, True):
            try:
                if rec['_id'].Value is not None and '_id' in rec:
                    _id = rec['_id'].Value
                if rec['contact_id'].Value is not None and 'contact_id' in rec:
                    contact_id = rec['contact_id'].Value
                params = (_id, contact_id)
                self.db_insert_table(SQL_INSERT_TABLE_RAW_CONTACTS, params)
            except:
                traceback.print_exc()
        for rec in db.ReadTableDeletedRecords(ts2, False):
            try:
                if rec['_id'].Value is not None and '_id' in rec:
                    _id = rec['_id'].Value
                if rec['contact_id'].Value is not None and 'contact_id' in rec:
                    contact_id = rec['contact_id'].Value
                params = (_id, contact_id)
                self.db_insert_table(SQL_INSERT_TABLE_RAW_CONTACTS, params)
            except:
                traceback.print_exc()
        ts3 = SQLiteParser.TableSignature('contacts')
        for rec in db.ReadTableRecords(ts3, self.extractDeleted, True):
            try:
                if rec['_id'].Value is not None and '_id' in rec:
                    _id = rec['_id'].Value
                if rec['last_time_contacted'].Value is not None and 'last_time_contacted' in rec:
                    last_time_contacted = rec['last_time_contacted'].Value
                if rec['contact_last_updated_timestamp'].Value is not None and 'contact_last_updated_timestamp' in rec:
                    contact_last_updated_timestamp = rec['contact_last_updated_timestamp'].Value
                if rec['times_contacted'].Value is not None and 'times_contacted' in rec:
                    times_contacted = rec['times_contacted'].Value
                params = (_id, last_time_contacted, contact_last_updated_timestamp, times_contacted)
                self.db_insert_table(SQL_INSERT_TABLE_CONTACTS, params)
            except:
                traceback.print_exc()
        for rec in db.ReadTableDeletedRecords(ts3, False):
            try:
                if rec['_id'].Value is not None and '_id' in rec:
                    _id = rec['_id'].Value
                if rec['last_time_contacted'].Value is not None and 'last_time_contacted' in rec:
                    last_time_contacted = rec['last_time_contacted'].Value
                if rec['contact_last_updated_timestamp'].Value is not None and 'contact_last_updated_timestamp' in rec:
                    contact_last_updated_timestamp = rec['contact_last_updated_timestamp'].Value
                if rec['times_contacted'].Value is not None and 'times_contacted' in rec:
                    times_contacted = rec['times_contacted'].Value
                params = (_id, last_time_contacted, contact_last_updated_timestamp, times_contacted)
                self.db_insert_table(SQL_INSERT_TABLE_CONTACTS, params)
            except:
                traceback.print_exc()
        ts4 = SQLiteParser.TableSignature('mimetypes')
        for rec in db.ReadTableRecords(ts4, self.extractDeleted, True):
            try:
                if rec['_id'].Value is not None and '_id' in rec:
                    _id = rec['_id'].Value
                if rec['mimetype'].Value is not None and 'mimetype' in rec:
                    mimetype = rec['mimetype'].Value
                params = (_id, mimetype)
                self.db_insert_table(SQL_INSERT_TABLE_MIMETYPES, params)
            except:
                traceback.print_exc()
        for rec in db.ReadTableDeletedRecords(ts4, False):
            try:
                if rec['_id'].Value is not None and '_id' in rec:
                    _id = rec['_id'].Value
                if rec['mimetype'].Value is not None and 'mimetype' in rec:
                    mimetype = rec['mimetype'].Value
            except:
                traceback.print_exc()
        self.db_cmd.Dispose()
        self.db.Close()

    def analyze_logic_contacts(self):
        try:
            node = self.node.Parent.GetByPath('/contacts.db')
            self.db = SQLiteParser.Database.FromNode(node, canceller)
            if self.db is None:
                raise Exception('解析联系人出错：无法读取联系人数据库')
            ts = SQLiteParser.TableSignature('AddressBook')
            id = 0
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                contacts = Contact()
                id += 1
                if canceller.IsCancellationRequested:
                    break
                contacts.id = id
                homeEmail = ''
                jobEmail = ''
                customEmail = ''
                otherEmail = ''
                if rec['homeEmails'].Value is not None:
                    homeEmail = rec['homeEmails'].Value
                if rec['jobEmails'].Value is not None:
                    jobEmail = rec['jobEmails'].Value
                if rec['customEmails'].Value is not None:
                    customEmail = rec['customEmails'].Value
                if rec['otherEmails'].Value is not None:
                    otherEmail = rec['otherEmails'].Value
                emails = homeEmail + jobEmail + customEmail + otherEmail
                contacts.mail = emails.replace('][', ',').replace('[', '').replace(']', '').replace('\n', '').replace('\"', '').replace(' ', '')
                contacts.company = rec['organization'].Value
                phonenumber = ''
                homenumber = ''
                jobnumber = ''
                othernumber = ''
                customnumber = ''
                if rec['phoneNumbers'].Value is not None:
                    phonenumber = rec['phoneNumbers'].Value
                if rec['homeNumbers'].Value is not None:
                    homenumber = rec['homeNumbers'].Value
                if rec['jobNumbers'].Value is not None:
                    jobnumber = rec['jobNumbers'].Value
                if rec['otherNumbers'].Value is not None:
                    othernumber = rec['otherNumbers'].Value
                if rec['customNumbers'].Value is not None:
                    customnumber = rec['customNumbers'].Value
                numbers = phonenumber + homenumber + jobnumber + othernumber + customnumber
                contacts.phone_number = numbers.replace('][', ',').replace('[', '').replace(']', '').replace('\n', '').replace('\"', '').replace(' ', '')
                contacts.name = rec['name'].Value
                contacts.address = rec['homeStreets'].Value
                contacts.notes = rec['remark'].Value
                contacts.head_pic = rec['photoPath'].Value
                contacts.source = node.AbsolutePath
                self.mc.db_insert_table_call_contacts(contacts)
            self.mc.db_commit()
            for rec in self.db.ReadTableDeletedRecords(ts, False):
                contacts = Contact()
                id += 1
                if canceller.IsCancellationRequested:
                    break
                contacts.id = id
                homeEmail = ''
                jobEmail = ''
                customEmail = ''
                otherEmail = ''
                if rec['homeEmails'].Value is not None:
                    homeEmail = rec['homeEmails'].Value
                if rec['jobEmails'].Value is not None:
                    jobEmail = rec['jobEmails'].Value
                if rec['customEmails'].Value is not None:
                    customEmail = rec['customEmails'].Value
                if rec['otherEmails'].Value is not None:
                    otherEmail = rec['otherEmails'].Value
                emails = homeEmail + jobEmail + customEmail + otherEmail
                contacts.mail = emails.replace('][', ',').replace('[', '').replace(']', '').replace('\n', '').replace('\"', '').replace(' ', '')
                contacts.company = rec['organization'].Value
                phonenumber = ''
                homenumber = ''
                jobnumber = ''
                othernumber = ''
                customnumber = ''
                if rec['phoneNumbers'].Value is not None:
                    phonenumber = rec['phoneNumbers'].Value
                if rec['homeNumbers'].Value is not None:
                    homenumber = rec['homeNumbers'].Value
                if rec['jobNumbers'].Value is not None:
                    jobnumber = rec['jobNumbers'].Value
                if rec['otherNumbers'].Value is not None:
                    othernumber = rec['otherNumbers'].Value
                if rec['customNumbers'].Value is not None:
                    customnumber = rec['customNumbers'].Value
                numbers = phonenumber + homenumber + jobnumber + othernumber + customnumber
                contacts.phone_number = numbers.replace('][', ',').replace('[', '').replace(']', '').replace('\n', '').replace('\"', '').replace(' ', '')
                contacts.name = rec['name'].Value
                contacts.address = rec['homeStreets'].Value
                contacts.notes = rec['remark'].Value
                contacts.head_pic = rec['photoPath'].Value
                contacts.source = node.AbsolutePath
                self.mc.db_insert_table_call_contacts(contacts)
            self.mc.db_commit()
        except Exception as e:
            print(e)

    def create_db_tempo(self):
        try:
            if os.path.exists(self.db_tempo):
                os.remove(self.db_tempo)
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_tempo))
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            self.db_cmd.CommandText = model_contact.SQL_CREATE_TABLE_CONTACTS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.Dispose()
            self.db.Close()
        except:
            pass

    def insert_contact_tempo(self):
        try:
            db_ext = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_tempo))
            db_ext.Open()
            self.db_cmd = SQLite.SQLiteCommand(db_ext)
        except:
            return
        try:
            contactsNode = self.sourceDB + '\\db_deleted.db'
            db = SQLite.SQLiteConnection('Data Source = {}'.format(contactsNode))
            db.Open()
            db_cmd = SQLite.SQLiteCommand(db)
            if db is None:
                return
            try:
                db_cmd.CommandText = SQL_TABLE_JOIN_CONTACT
                db_cmd.ExecuteNonQuery()
                sr = db_cmd.ExecuteReader()
            except:
                db_cmd.Dispose()
                db.Close()
                self.db_cmd.Dispose()
                db_ext.Close()
                self.analyze_logic_contacts()
                return
            if sr.Read() is False:
                self.db_cmd.Dispose()
                db_ext.Close()
                db_cmd.Dispose()
                sr.Close()
                db.Close()
                self.analyze_logic_contacts()
                return
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                raw_contact_id = sr[0]
                mimetype_id = sr[1]
                if not IsDBNull(sr[12]):
                    mail = sr[2] if self._regularMatch(sr[12]) == 1 else None
                if not IsDBNull(sr[12]):    
                    company = sr[2] if self._regularMatch(sr[12]) == 2 else None
                if not IsDBNull(sr[12]):
                    title = sr[5] if self._regularMatch(sr[12]) == 2 else None
                last_time_contact = sr[9]
                last_time_modify = sr[10]
                times_contacted = sr[11]
                if not IsDBNull(sr[12]):
                    phone_number = sr[5] if self._regularMatch(sr[12]) == 3 else sr[4] if (self._regularMatch(sr[12]) == 8 or self._regularMatch(sr[12]) == 9 or self._regularMatch(sr[12]) == 10) else None
                if not IsDBNull(sr[12]):
                    name = sr[2] if self._regularMatch(sr[12]) == 4 else None
                if not IsDBNull(sr[12]):                    
                    address = sr[2] if self._regularMatch(sr[12]) == 5 else None
                if not IsDBNull(sr[12]):    
                    notes = sr[2] if self._regularMatch(sr[12]) == 6 else sr[3] if (self._regularMatch(sr[12]) == 8 or self._regularMatch(sr[12]) == 10) else None
                if not IsDBNull(sr[12]):
                    telegram = sr[3] if self._regularMatch(sr[12]) == 9 else None
                if not IsDBNull(sr[12]):
                    head_pic = sr[6] if self._regularMatch(sr[12]) == 7 else None
                source = self.node.AbsolutePath
                deleted = sr[7]
                param = (raw_contact_id, mimetype_id, mail, company, title, last_time_contact, last_time_modify, times_contacted, phone_number, name, address, notes, telegram, head_pic, source, deleted, 0)
                self.db_insert_table(model_contact.SQL_INSERT_TABLE_CONTACTS, param)
            sr.Close()
            db_cmd.Dispose()
            db.Close()
            self.db_cmd.Dispose()
            db_ext.Close()
        except Exception as e:
            traceback.print_exc()

    def db_insert_table(self, sql, values):
        try:
            if self.db_cmd is not None:
                self.db_cmd.CommandText = sql
                self.db_cmd.Parameters.Clear()
                for value in values:
                    param = self.db_cmd.CreateParameter()
                    param.Value = value
                    self.db_cmd.Parameters.Add(param)
                self.db_cmd.ExecuteNonQuery()
        except Exception as e:
            print(e)

    def _regularMatch(self, str):
        flag = None
        if re.match('.+email.*', str) is not None:
            flag = 1
            return flag
        if re.match(r'.+/organization$', str) is not None:
            flag = 2
            return flag
        if re.match(r'.+/phone.*', str) is not None:
            flag = 3
            return flag
        if re.match(r'.+/name$', str) is not None:
            flag = 4
            return flag
        if re.match(r'.+/postal-address.*', str) is not None:
            flag = 5
            return flag
        if re.match(r'.+/note$', str) is not None:
            flag = 6
            return flag
        if re.match(r'.+/photo$', str) is not None:
            flag = 7
            return flag
        if re.match(r'.+/vnd.com.whatsapp.profile$', str) is not None:
            flag = 8
            return flag
        if re.match(r'.+/vnd.org.telegram.messenger.android.profile$', str) is not None:
            flag = 9
            return flag
        if re.match(r'.+com.facebook.messenger.chat$', str) is not None:
            flag = 10
            return flag
        return flag

    def parse(self):
        if self.mc.need_parse(self.cachedb, VERSION_APP_VALUE):
            self.mc.db_create(self.cachedb)
            self._copytocache()
            self._closewal('contacts2.db')
            self.analyze_deleted_records()
            self.create_db_tempo()
            self.insert_contact_tempo()
            self.analyze_call_contacts()
            self.mc.db_insert_table_version(model_contact.VERSION_KEY_DB, model_contact.VERSION_VALUE_DB)
            self.mc.db_insert_table_version(model_contact.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.mc.db_commit()
            self.mc.db_close()
            SQLite.SQLiteConnection.ClearAllPools()
            if os.path.exists(self.sourceDB):
                shutil.rmtree(self.sourceDB)
        #bcp entry
        temp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_basic.BASIC_CONTACT_INFORMATION, self.cachedb, temp_dir)
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
    pr.Build('联系人')
    return pr

def execute(node, extractDeleted):
    return analyze_android_calls(node, extractDeleted, False)