# -*- coding: utf-8 -*-
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import clr
try:
    clr.AddReference('model_calendar')
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import shutil
import hashlib
import bcp_basic
import model_calendar
from model_calendar import *
import System.Data.SQLite as SQLite

SQL_JOIN_TABLE_CALENDAR = '''select Events.calendar_id, Events._id, Events.title, Events.eventLocation, Events.description, Events.dtstart, 
    Reminders.minutes, Events.dtend, Events.rrule, Calendars.calendar_displayName from Events left join Reminders on Events._id = Reminders.event_id left join Calendars on Events.calendar_id = Calendars._id'''

VERSION_APP_VALUE = 1

class CalendarParser(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.mc = MC()
        md5_db = hashlib.md5()
        md5_db.update(self.node.AbsolutePath.encode(encoding = 'utf-8'))
        self.db_cache = ds.OpenCachePath("CALENDAR") + '\\' + md5_db.hexdigest().upper() +'.db'
        self.sourceDB = ds.OpenCachePath("CALENDAR") + '\\CalendarSourceDB'

    def analyze_calendar(self):
        try:
            db_source = self.sourceDB + '\\calendar.db'
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_source))
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db is None:
                return
            try:
                self.db_cmd.CommandText = SQL_JOIN_TABLE_CALENDAR
                sr = self.db_cmd.ExecuteReader()
            except Exception as e:
                self.db_cmd.Dispose()
                self.db.Close()
                self.analyze_logic_calendar()
                return
            while (sr.Read()):
                calendar = Calendar()
                if canceller.IsCancellationRequested:
                    break
                try:
                    calendar.calendar_id = sr[0]
                    calendar.title = sr[2]
                    calendar.description = sr[4]
                    calendar.dtstart = sr[5]
                    calendar.remind = sr[6]
                    calendar.dtend = sr[7]
                    if not IsDBNull(sr[8]):
                        calendar.rrule = self._extractData(sr[8],'FREQ')
                        calendar.interval = self._extractData(sr[8],'INTERVAL')
                        calendar.until = self._extractData(sr[8],'UNTIL')
                    calendar.source = self.node.AbsolutePath
                    calendar.calendar_displayName = sr[9]
                    self.mc.db_insert_calendar(calendar)
                except:
                    pass
            self.mc.db_commit()
            sr.Close()
            self.db.Close()
        except Exception as e:
            print(e)

    def decode_recover_calendar_table(self):
        try:
            self.db = SQLiteParser.Database.FromNode(self.node, canceller)
            if self.db is None:
                return
            ts = SQLiteParser.TableSignature('Events')
        except:
            return
        try:
            calendar = Calendar()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                try:
                    calendar.calendar_id = row['calendar_id'].Value if 'calendar_id' in row and not row['calendar_id'].IsDBNull else None
                    calendar.title = row['title'].Value if 'title' in row and not row['title'].IsDBNull else None
                    calendar.description = row['description'].Value if 'description' in row and not row['description'].IsDBNull else None 
                    calendar.dtstart = row['dtstart'].Value if 'dtstart' in row and not row['dtstart'].IsDBNull else None
                    calendar.dend = row['dend'].Value if 'dend' in row and not row['dend'].IsDBNull else None
                    calendar.rrule = self._extractData(row['rrule'].Value,'FREQ') if 'rrule' in row and not row['rrule'].IsDBNull else None
                    calendar.interval = self._extractData(row['rrule'].Value,'INTERVAL') if 'rrule' in row and not row['rrule'].IsDBNull else None
                    calendar.until = self._extractData(row['rrule'].Value,'UNTIL') if 'rrule' in row and not row['rrule'].IsDBNull else None
                    calendar.source = self.node.AbsolutePath
                    calendar.deleted = 1
                    self.mc.db_insert_calendar(calendar)
                except:
                    pass
            self.mc.db_commit()
        except Exception as e:
            pass

    def analyze_logic_calendar(self):
        try:
            db_source = self.node.PathWithMountPoint
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_source))
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db is None:
                return
            self.db_cmd.CommandText = '''select distinct * from Calendar'''
            sr = self.db_cmd.ExecuteReader()
            while (sr.Read()):
                calendar = Calendar()
                if canceller.IsCancellationRequested:
                    break
                try:
                    calendar.calendar_id = sr[0]
                    calendar.title = sr[1]
                    calendar.description = sr[3]
                    calendar.dtstart = sr[5]
                    calendar.dtend = sr[6]
                    calendar.rrule = self._extractData(sr[4],'FREQ')
                    calendar.interval = self._extractData(sr[4],'INTERVAL')
                    calendar.until = self._extractData(sr[4],'UNTIL')
                    calendar.source = self.node.AbsolutePath
                    self.mc.db_insert_calendar(calendar)
                except:
                    pass
            self.mc.db_commit()
            sr.Close()
            self.db.Close()
        except Exception as e:
            print(e)

    def _extractData(self,s,subs):
        if s is not None:
            lis = s.split(';')
            for i in lis:
                if i.find(subs)>=0:
                    return i
        return None

    def parse(self):
        if self.mc.need_parse(self.db_cache, VERSION_APP_VALUE):
            self.mc.db_create(self.db_cache)
            self._copytocache()
            self.analyze_calendar()
            self.decode_recover_calendar_table()
            self.mc.db_insert_table_version(model_calendar.VERSION_KEY_DB, model_calendar.VERSION_VALUE_DB)
            self.mc.db_insert_table_version(model_calendar.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.mc.db_commit()
            self.mc.db_close()
            SQLite.SQLiteConnection.ClearAllPools()
            try:
                if os.path.exists(self.sourceDB):
                    shutil.rmtree(self.sourceDB)
            except:
                pass
        #bcp entry
        temp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_basic.BASIC_CALENDAR_INFOMATION, self.db_cache, temp_dir)
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
    pr.Build('系统日历')
    return pr

def execute(node, extractDeleted):
    return analyze_android_calendar(node, extractDeleted, False)