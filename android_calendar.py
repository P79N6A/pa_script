#coding=utf-8
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import logging 
SafeLoadAssembly('model_calendar')
import shutil
from model_calendar import *

SQL_JOIN_TABLE_CALENDAR = '''select Events.calendar_id, Events._id, Events.title, Events.eventLocation, Events.description, Events.dtstart, 
    Reminders.minutes, Events.dtend, Events.rrule from Events left join Reminders on Events._id = Reminders.event_id'''

class CalendarParser(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.mc = MC()
        self.db_cache = ds.OpenCachePath("CALENDAR") + '\\calendar.db'
        self.sourceDB = ds.OpenCachePath("CALENDAR") + '\\CalendarSourceDB'
        self.mc.db_create(self.db_cache)

    def analyze_calendar(self):
        calendar = Calendar()
        try:
            db_source = self.sourceDB + '\\calendar.db'
            self.db = sqlite3.connect(db_source)
            if self.db is None:
                return
            cursor = self.db.cursor()
            cursor.execute(SQL_JOIN_TABLE_CALENDAR)
            for row in cursor:
                canceller.ThrowIfCancellationRequested()
                calendar.calendar_id = row[0]
                calendar.title = row[2]
                calendar.description = row[4]
                calendar.dtstart = row[5]
                calendar.remind = row[6]
                calendar.dtend = row[7]
                calendar.rrule = self._extractData(row[8],'FREQ')
                calendar.interval = self._extractData(row[8],'INTERVAL')
                calendar.until = self._extractData(row[8],'UNTIL')
                self.mc.db_insert_calendar(calendar)
            self.mc.db_commit()
            self.db.close()
        except Exception as e:
            logging.error(e)

    def decode_recover_calendar_table(self):
        self.db = SQLiteParser.Database.FromNode(self.node)
        if self.db is None:
            return
        ts = SQLiteParser.TableSignature('Events')
        try:
            calendar = Calendar()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                canceller.ThrowIfCancellationRequested()
                calendar.calendar_id = row['calendar_id'].Value if 'calendar_id' in row and not row['calendar_id'].IsDBNull else None
                calendar.title = repr(row['title'].Value) if 'title' in row and not row['title'].IsDBNull else None
                calendar.eventLocation = repr(row['eventLocation'].Value) if 'eventLocation' in row and not row['eventLocation'].IsDBNull else None
                calendar.description = repr(row['description'].Value) if 'description' in row and not row['description'].IsDBNull else None
                calendar.dtstart = row['dtstart'].Value if 'dtstart' in row and not row['dtstart'].IsDBNull else None
                calendar.dend = row['dend'].Value if 'dend' in row and not row['dend'].IsDBNull else None
                calendar.rrule = self._extractData(row['rrule'].Value,'FREQ') if 'rrule' in row and not row['rrule'].IsDBNull else None
                calendar.interval = self._extractData(row['rrule'].Value,'INTERVAL') if 'rrule' in row and not row['rrule'].IsDBNull else None
                calendar.until = self._extractData(row['rrule'].Value,'UNTIL') if 'rrule' in row and not row['rrule'].IsDBNull else None
                calendar.deleted = 1
                self.mc.db_insert_calendar(calendar)
            self.mc.db_commit()
        except Exception as e:
            logging.error(e)

    def _extractData(self,s,subs):
        if s is not None:
            lis = s.split(';')
            for i in lis:
                if i.find(subs)>=0:
                    return i
        return None

    def parse(self):
        self._copytocache()
        self.analyze_calendar()
        self.decode_recover_calendar_table()
        self.mc.db_close()
        generate = Generate(self.db_cache)
        models = generate.get_models()
        return models

    def _copytocache(self):
        sourceDir = self.node.Parent.PathWithMountPoint
        targetDir = self.sourceDB
        try:
            if not os.path.exists(targetDir):
                shutil.copytree(sourceDir, targetDir)
        except Exception as e:
            print(e)

def analyze_android_calendar(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(CalendarParser(node, extractDeleted, extractSource).parse())
    return pr

def execute(node, extractDeleted):
    return analyze_android_calendar(node, extractDeleted, False)