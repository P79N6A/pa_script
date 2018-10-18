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

SQL_FIND_TABLE_RECORDS = '''
    SELECT * FROM RECORDS
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

    def db_insert_table_call_records(self, Records):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_RECORDS, Records.get_values())


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


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.cursor = None

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        models.extend(self._get_model_records())
        self.db.close()
        self.db = None
        return models

    def _get_model_records(self):
        model = []
        self.cursor = self.db.cursor()
        self.cursor.execute(SQL_FIND_TABLE_RECORDS)
        for row in self.cursor:
            c = Calls.Call()
            if row[10] is not None:
                c.CountryCode.Value = row[10]
            if row[3] is not None:
                hours = row[3]/3600
                minutes = row[3]-hours*3600
                seconds = row[3]-hours*3600-minutes*60
                c.Duration.Value = System.TimeSpan(hours, minutes, seconds)
            if row[2] is not None:
                c.TimeStamp.Value = TimeStamp.FromUnixTime(int(str(row[2])[0:10:1]), False)
            if row[4] is not None:
                c.Type.Value = CallType.Incoming if row[4] == 1 else CallType.Outgoing if row[4] == 2 else CallType.Missed if row[4] == 3 else CallType.Unknown
            party = Generic.Party()
            if row[1] is not None:
                party.Identifier.Value = row[1]
            if row[5] is not None:
                party.Name.Value = row[5]
            if row[4] is not None:
                party.Role.Value = PartyRole.From if row[4] == 1 or row[4] == 3 else PartyRole.To
            c.Parties.Add(party)
            if row[11] is not None:
                c.SourceFile.Value = self._get_source_file(str(row[11]))
            if row[12] is not None:
                c.Deleted = self._convert_deleted_status(row[12])
            model.append(c)
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