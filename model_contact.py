# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import System
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
    INSERT INTO contacts(raw_contact_id, mimetype_id, mail, company, title, last_time_contact, last_time_modify, times_contacted, phone_number, name, address, notes, telegram, head_pic, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_FIND_TABLE_CONTACTS = '''
    SELECT DISTINCT raw_contact_id, mimetype_id, group_concat(mail), group_concat(company), group_concat(title), last_time_contact, last_time_modify,
    times_contacted, group_concat(phone_number), group_concat(name), group_concat(address), group_concat(notes), group_concat(telegram), 
    group_concat(head_pic), source, deleted, repeated FROM contacts GROUP BY raw_contact_id
    '''



class MC(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self,db_path):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()
        self.db_create_tables()
        self.db_commit()

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def db_create_tables(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_CONTACTS)

    def db_insert_table_call_contacts(self, Contacts):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_CONTACTS, Contacts.get_values())


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
        self.cursor = None

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        models.extend(self._get_model_contacts())
        self.db.close()
        self.db = None
        return models

    def _get_model_contacts(self):
        model = []
        self.cursor = self.db.cursor()
        self.cursor.execute('select distinct * from contacts')
        for row in self.cursor:
            contact = Contacts.Contact()
            if row[10] is not None:
                addresses = row[10].split(',')
                for a in range(len(addresses)):
                    addr = Contacts.StreetAddress()
                    addr.FullName.Value = addresses[a]
                    contact.Addresses.Add(addr)
            if row[9] is not None:
                contact.Name.Value = row[9]
            if row[10] is not None:
                contact.Notes.Add(row[11])
            if row[5] is not None:
                contact.TimeContacted.Value = self._get_timestamp(row[5])
            if row[6] is not None:
                contact.TimeModified.Value = self._get_timestamp(row[6])
            if row[7] is not None:
                contact.TimesContacted.Value = row[7]
            if row[8] is not None:
                phone = row[8].split(',')
                for e in range(len(phone)):
                    entry = Contacts.ContactEntry()
                    entry.Value.Value = phone[e]
                    contact.Entries.Add(entry)
            if row[14] is not None:
                contact.SourceFile.Value = self._get_source_file(str(row[14]))
            if row[15] is not None:
                contact.Deleted = self._convert_deleted_status(row[15])
            model.append(contact)
        self.cursor.close()
        self.cursor = None
        return model

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