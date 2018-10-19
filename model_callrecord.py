# -*- coding: utf-8 -*-

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
            print("model_callrecord db_create() remove %s error:%s"%(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECORDS
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

    def db_insert_table_call_records(self, Column):
        self.db_insert_table(SQL_INSERT_TABLE_RECORDS, Column.get_values())


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
        self.db_cmd = None

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_model_records())
        self.db_cmd.Dispose()
        self.db.Close()
        return models

    def _get_model_records(self):
        model = []
        sql = SQL_FIND_TABLE_RECORDS
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                c = Calls.Call()
                if not IsDBNull(sr[10]):
                    c.CountryCode.Value = sr[10]
                if not IsDBNull(sr[3]):
                    hours = sr[3]/3600
                    minutes = sr[3]-hours*3600
                    seconds = sr[3]-hours*3600-minutes*60
                    c.Duration.Value = System.TimeSpan(hours, minutes, seconds)
                if not IsDBNull(sr[2]):
                    c.TimeStamp.Value = TimeStamp.FromUnixTime(int(str(sr[2])[0:10:1]), False)
                if not IsDBNull(sr[4]):
                    c.Type.Value = CallType.Incoming if sr[4] == 1 else CallType.Outgoing if sr[4] == 2 else CallType.Missed if sr[4] == 3 else CallType.Unknown
                party = Generic.Party()
                if not IsDBNull(sr[1]):
                    party.Identifier.Value = sr[1]
                if not IsDBNull(sr[5]):
                    party.Name.Value = sr[5]
                if not IsDBNull(sr[4]):
                    party.Role.Value = PartyRole.From if sr[4] == 1 or sr[4] == 3 else PartyRole.To
                c.Parties.Add(party)
                if not IsDBNull(sr[11]):
                    c.SourceFile.Value = self._get_source_file(str(sr[11]))
                if not IsDBNull(sr[12]):
                    c.Deleted = self._convert_deleted_status(sr[12])
                model.append(c)
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