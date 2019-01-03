# -*- coding: utf-8 -*-
__author__ = "xiaoyuge"

from PA_runtime import *
import clr
try:
    clr.AddReference('System.Core')
    clr.AddReference('System.Xml.Linq')
    clr.AddReference('System.Data.SQLite')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import System
import System.Data.SQLite as SQLite
import sqlite3

SQL_CREATE_TABLE_CONTACTS = '''
    CREATE TABLE IF NOT EXISTS contacts(
        raw_contact_id INTEGER,
        mimetype_id INTEGER,
        mail TEXT,
        company TEXT,
        title TEXT,
        last_time_contact INTEGER,
        last_time_modify INTEGER,
        times_contacted INTEGER,
        phone_number TEXT,
        name TEXT,
        address TEXT,
        notes TEXT,
        telegram TEXT,
        head_pic BLOB,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_CONTACTS = '''
    INSERT INTO contacts(raw_contact_id, mimetype_id, mail, company, title, last_time_contact, last_time_modify, times_contacted, phone_number, name, address, notes, head_pic, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_FIND_TABLE_CONTACTS_INTACT = '''
    SELECT DISTINCT raw_contact_id, group_concat(mail), group_concat(company), group_concat(title), last_time_contact, last_time_modify,
    times_contacted, group_concat(phone_number), name, group_concat(address), group_concat(notes), telegram, 
    group_concat(head_pic), source, deleted, repeated FROM contacts where deleted = 0 GROUP BY raw_contact_id
    '''

SQL_FIND_TABLE_CONTACTS_DELETED = '''
    SELECT DISTINCT raw_contact_id, group_concat(mail), group_concat(company), group_concat(title), last_time_contact, last_time_modify,
    times_contacted, group_concat(phone_number), group_concat(name), group_concat(address), group_concat(notes), telegram, 
    group_concat(head_pic), source, deleted, repeated FROM contacts where deleted = 1 GROUP BY raw_contact_id
    '''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

#中间数据库版本
VERSION_VALUE_DB = 1


class MC(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self,db_path):
        self.db_remove(db_path)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()
        self.db_create_table()
        self.db_commit()

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()
            
    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_remove(self, db_path):
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except Exception as e:
            print("model_mail db_create() remove %s error:%s"%(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CONTACTS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()

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

    def db_insert_table_call_contacts(self, Column):
        self.db_insert_table(SQL_INSERT_TABLE_CONTACTS, Column.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key,version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            if row[0] == VERSION_KEY_DB and row[1] == VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == VERSION_KEY_APP and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)

class Column(object):
    def __init__(self):
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            self.__dict__[name] = value
        else:
            self.__dict__[name] = None

    def get_values(self):
        return (self.source, self.deleted, self.repeated)


class Contact(Column):
    def __init__(self):
        super(Contact, self).__init__()
        self.raw_contact_id = None
        self.mimetype_id = None
        self.mail = None
        self.company = None
        self.title = None
        self.last_time_contact = None
        self.last_time_modify = None
        self.times_contacted = None
        self.phone_number = None
        self.name = None
        self.address = None
        self.notes = None
        self.telegram = None
        self.head_pic = None

    def get_values(self):
        return (self.raw_contact_id, self.mimetype_id, self.mail, self.company, self.title, self.last_time_contact, self.last_time_modify, self.times_contacted,
            self.phone_number, self.name, self.address, self.notes, self.telegram, self.head_pic) + super(Contact, self).get_values()


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.db_cmd = None

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_model_contacts())
        self.db_cmd.Dispose()
        self.db.Close()
        return models

    def _get_model_contacts(self):
        model = []
        sql = '''select distinct * from contacts'''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                contact = Contacts.Contact()
                if not IsDBNull(sr[10]):
                    addresses = sr[10].split(',')
                    for a in range(len(addresses)):
                        addr = Contacts.StreetAddress()
                        addr.FullName.Value = addresses[a]
                        contact.Addresses.Add(addr)
                if not IsDBNull(sr[9]):
                    contact.Name.Value = sr[9]
                if not IsDBNull(sr[11]):
                    contact.Notes.Add(sr[11])
                if not IsDBNull(sr[5]):
                    contact.TimeContacted.Value = self._get_timestamp(sr[5])
                if not IsDBNull(sr[6]):
                    contact.TimeModified.Value = self._get_timestamp(sr[6])
                if not IsDBNull(sr[7]):
                    contact.TimesContacted.Value = sr[7]
                if not IsDBNull(sr[8]):
                    phone = sr[8].split(',')
                    for e in range(len(phone)):
                        entry = Contacts.ContactEntry()
                        entry.Value.Value = phone[e]
                        contact.Entries.Add(entry)
                if not IsDBNull(sr[13]):
                    contact.SourceFile.Value = self._get_source_file(str(sr[14]))
                if not IsDBNull(sr[14]):
                    contact.Deleted = self._convert_deleted_status(sr[15])
                model.append(contact)
            sr.Close()
            return model
        except Exception as e:
            print(e)

    @staticmethod
    def _get_source_file(source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = None
                return ts
        except:
            return None

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted