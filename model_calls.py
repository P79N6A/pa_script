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
import logging

SQL_CREATE_TABLE_RECORDS = '''
    CREATE TABLE IF NOT EXISTS records(
        id INTEGER,
        phone_number TEXT,
        date INTEGER,
        duration INTEGER,
        type INTEGER,
        name TEXT,
        geocoded_location TEXT,
        ring_times INTEGER,
        mark_type TEXT,
        mark_content TEXT,
        country_code TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_RECORDS = '''
    INSERT INTO records(id, phone_number, date, duration, type, name, geocoded_location, ring_times, mark_type, mark_content, country_code, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

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

SQL_FIND_TABLE_RECORDS = '''
    SELECT * FROM RECORDS
    '''

SQL_FIND_TABLE_CONTACTS = '''
    SELECT DISTINCT raw_contact_id, group_concat(mail), group_concat(company), group_concat(title), last_time_contact, last_time_modify,
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
            self.cursor.execute(SQL_CREATE_TABLE_RECORDS)
            self.cursor.execute(SQL_CREATE_TABLE_CONTACTS)

    def db_insert_table_call_records(self, Records):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_RECORDS, Records.get_values())

    def db_insert_table_call_contacts(self, Contacts):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_CONTACTS, Contacts.get_values())


class Column(object):
    def __init__(self):
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return (self.source, self.deleted, self.repeated)


class Records(Column):
    def __init__(self):
        super(Records, self).__init__()
        self.id = None
        self.phone_number = None
        self.date = None
        self.duration = None
        self.type = None
        self.name = None
        self.geocoded_location = None
        self.ring_times = None
        self.mark_type = None
        self.mark_content = None
        self.country_code = None

    def get_values(self):
        return (self.id, self.phone_number, self.date, self.duration, self.type, self.name, self.geocoded_location,
            self.ring_times, self.mark_type, self.mark_content, self.country_code) + super(Records, self).get_values()


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
        models.extend(self._get_model_records())
        models.extend(self._get_model_contacts())
        self.db.close()
        self.db = None
        return models

    def _get_model_records(self):
        model = []
        self.cursor = self.db.cursor()
        self.cursor.execute(SQL_FIND_TABLE_RECORDS)
        for row in self.cursor:
            c = Calls.Call()
            c.CountryCode.Value = row[10]
            hours = row[3]/3600
            minutes = row[3]-hours*3600
            seconds = row[3]-hours*3600-minutes*60
            c.Duration.Value = System.TimeSpan(hours, minutes, seconds)
            c.TimeStamp.Value = TimeStamp.FromUnixTime(int(str(row[2])[0:-3:1]), False)
            c.Type.Value = CallType.Incoming if row[4] == 1 else CallType.Outgoing if row[4] == 2 else CallType.Missed if row[4] == 3 else CallType.Unknown
            party = Generic.Party()
            party.Identifier.Value = row[1]
            party.Name.Value = row[5]
            party.Role.Value = PartyRole.From if row[4] == 1 or row[4] == 3 else PartyRole.To
            c.Parties.Add(party)
        self.cursor.close()
        self.cursor = None
        model.append(c)
        return model

    def _get_model_contacts(self):
        model = []
        self.cursor = self.db.cursor()
        self.cursor.execute(SQL_FIND_TABLE_CONTACTS)
        for row in self.cursor:
            contact = Contacts.Contact()
            if row[9] is not None:
                addresses = row[9].split(',')
                for a in range(len(addresses)):
                    addr = Contacts.StreetAddress()
                    addr.FullName.Value = addresses[a]
                    contact.Addresses.Add(addr)
            contact.Name.Value = row[8][0]
            contact.Notes.Add(row[10])
            if row[4] is not None:
                contact.TimeContacted.Value = TimeStamp.FromUnixTime(int(str(row[4])[0:-3:1]), False) if len(str(row[4])) > 10 else TimeStamp.FromUnixTime(row[4], False) if len(str(row[4])) == 10 else TimeStamp.FromUnixTime(0, False)
            if row[5] is not None:
                contact.TimeModified.Value = TimeStamp.FromUnixTime(int(str(row[5])[0:-3:1]), False) if len(str(row[5])) > 10 else TimeStamp.FromUnixTime(row[5], False) if len(str(row[5])) == 10 else TimeStamp.FromUnixTime(0, False)
            if row[6] is not None:
                contact.TimesContacted.Value = row[6]
            if row[7] is not None:
                phone = row[7].split(',')
                for e in range(len(phone)):
                    entry = Contacts.ContactEntry()
                    entry.Value.Value = phone[e]
                    contact.Entries.Add(entry)
        self.cursor.close()
        self.cursor = None
        model.append(contact)
        return model