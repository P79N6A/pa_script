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

SQL_CREATE_TABLE_CALENDAR = '''
    CREATE TABLE IF NOT EXISTS calendar(
        calendar_id INTEGER,
        title TEXT,
        latitude TEXT,
        longitude TEXT,
        description TEXT,
        dtstart INTEGER,
        remind INTEGER,
        dtend INTEGER,
        rrule TEXT,
        interval INTEGER,
        until INTEGER,
        source INTEGER,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_CALENDAR = '''
    INSERT INTO calendar (calendar_id, title, latitude, longitude, description, dtstart, remind, dtend, 
        rrule, interval, until, source, deleted, repeated) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''



class MC(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self, db_cache):
        if os.path.exists(db_cache):
            os.remove(db_cache)
        self.db = sqlite3.connect(db_cache)
        self.cursor = self.db.cursor()
        self.create_tables()

    def create_tables(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_CALENDAR)

    def db_insert_calendar(self, Calendar):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_CALENDAR, Calendar.get_values())

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

class Calendar(object):
    def __init__(self):
        self.calendar_id = None
        self.title = None
        self.latitude = None
        self.longitude = None
        self.description = None
        self.dtstart = None
        self.remind = None
        self.dtend = None
        self.rrule = None
        self.interval = None
        self.until = None
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return(self.calendar_id, self.title, self.latitude, self.longitude, self.description, self.dtstart, self.remind, 
        self.dtend, self.rrule, self.interval, self.until, self.source, self.deleted, self.repeated)


class Generate(object):
    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.cursor = None

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        models.extend(self._get_model_calendar())
        return models

    def _get_model_calendar(self):
        model = []
        self.cursor = self.db.cursor()
        self.cursor.execute('select * from calendar')
        for row in self.cursor:
            canceller.ThrowIfCancellationRequested()
            cal = Generic.CalendarEntry()
            if row[0] is not None:
                cal.Category.Value = str(row[0])
            if row[1] is not None:
                cal.Subject.Value = row[1]
            if row[2] is not None:
                cal.Location.Value = str(row[2])+','+str(row[3])
            if row[3] is not None:
                cal.Details.Value = row[4]
            if row[5] is not None:
                startDate = self._get_timestamp(int(row[5]))
                cal.StartDate.Value = startDate
            if row[7] is not None:
                endDate = self._get_timestamp(int(row[7]))
                cal.EndDate.Value = endDate
            if row[8] is not None:
                if row[8].find('DAILY')>=0:
                    repeatRule = Generic.RepeatRule.Daily
                elif row[8].find('WEEKLY')>=0:
                    repeatRule = Generic.RepeatRule.Weekly
                elif row[8].find('MONTHLY')>=0:
                    repeatRule = Generic.RepeatRule.Monthly
                elif row[8].find('YEARLY')>=0:
                    repeatRule = Generic.RepeatRule.Yearly
                else:
                    repeatRule = Generic.RepeatRule.None
                    cal.RepeatRule.Value = repeatRule
            if row[9] is not None:
                cal.RepeatInterval.Value = int(re.sub('.*=?', '', row[8]))
            else:
                cal.RepeatInterval.Value = 1
            if row[11] is not None:
                cal.SourceFile.Value = self._get_source_file(str(row[10]))
            cal.Deleted = self._convert_deleted_status(row[12])
            model.append(cal)
        return model

    def _get_source_file(self, source_file):
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
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)
        
    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

