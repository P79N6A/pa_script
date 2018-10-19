#coding=utf-8
import os
import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import System.Data.SQLite as SQLite
import hashlib
import bcp_basic

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

SQL_SEARCH_TABLE_CALENDAR = '''select a.ROWID, a.summary, b.latitude, b.longitude, a.description, a.start_date,
    a.end_date from CalendarItem as a left join Location as b on a.ROWID = b.ROWID'''

def _get_repeat_day(ce, rec_item, rec_recurrence, extractSource, repeat_day_reg = re.compile(r"D=\+?(?P<week>\d)(?P<day>SU|MO|TU|WE|TH|FR|SA)")):
    repeat_day = {
        "SU": RepeatDay.Sunday,
            0: RepeatDay.Sunday,
        "MO": RepeatDay.Monday,
            1: RepeatDay.Monday,
        "TU": RepeatDay.Tuesday,
            2: RepeatDay.Tuesday,
        "WE": RepeatDay.Wednesday,
            3: RepeatDay.Wednesday,
        "TH": RepeatDay.Thursday,
            4: RepeatDay.Thursday,
        "FR": RepeatDay.Friday,
            5: RepeatDay.Friday,
        "SA": RepeatDay.Saturday,
            6: RepeatDay.Saturday,
          "1" :RepeatDay.First,
          "2" :RepeatDay.Second,
          "3" :RepeatDay.Third,
          "4" :RepeatDay.Fourth
        }
    ce.RepeatDay.Value = RepeatDay.None

    if ("specifier" in rec_recurrence and not 
            IsDBNull(rec_recurrence["specifier"].Value) and 
                type(rec_recurrence["specifier"].Value) == str and 
                    repeat_day_reg.search(rec_recurrence["specifier"].Value)):
        specifier = repeat_day_reg.search(rec_recurrence["specifier"].Value) 
        if specifier.group("day") in repeat_day:
            ce.RepeatDay.Value = repeat_day[specifier.group("day")]
        if specifier.group("week") in repeat_day:
            ce.RepeatDay.Value = ce.RepeatDay.Value | repeat_day[specifier.group("week")]
        if extractSource:
            ce.RepeatDay.Source = MemoryRange(rec_recurrence["specifier"].Source)
        return
    elif ce.RepeatRule.Value == RepeatRule.Weekly:
        day = ce.StartDate.Value.Value.Date.DayOfWeek.value__ if type(ce.StartDate.Value) == TimeStamp and type(ce.StartDate.Value.Value) == DateTimeOffset else -1
        if day > -1 and day < 7:
            ce.RepeatDay.Value = repeat_day[day]
            return


def analyze_calender(node, extractDeleted, extractSource):
    #ios 5的reminders类似日历
    repeat_rule = {
        1: RepeatRule.Daily,
        2: RepeatRule.Weekly,
        3: RepeatRule.Monthly,
        4: RepeatRule.Yearly,
        }
    
    db = SQLiteParser.Database.FromNode(node)

    cachepath = ds.OpenCachePath("Calendar")
    md5_db = hashlib.md5()
    db_name = 'calendar'
    md5_db.update(db_name.encode(encoding = 'utf-8'))
    db_path = cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
    
    if os.path.exists(db_path):
        os.remove(db_path)
    db_cache = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
    db_cache.Open()
    db_cmd = SQLite.SQLiteCommand(db_cache)
    if db_cmd is not None:
        db_cmd.CommandText = SQL_CREATE_TABLE_CALENDAR
        db_cmd.ExecuteNonQuery()
    db_cmd.Dispose()

    calendars = {}
    for record in db['Calendar']:
        calendars[record['ROWID'].Value] = record['title']
    
    ts = SQLiteParser.TableSignature('Alarm')  
    if extractDeleted:  
        ts['type'] = ts['entity_type'] = SQLiteParser.Signatures.NumericSet(1)
        ts['owner_id'] = ts['entity_id'] = ts['trigger_interval'] = IntNotNull

    alarms = defaultdict(list)
    for record in db.ReadTableRecords(ts, False, False):
        if record.ContainsKey('entity_id'):
            key = record['entity_id'].Value
        elif record.ContainsKey('owner_id'):
            key = record['owner_id'].Value
        elif record.ContainsKey('calendaritem_owner_id'):
            key = record['calendaritem_owner_id'].Value
        if (record.ContainsKey('location_id') and record['location_id'].Value == 0) or not record.ContainsKey('location_id'):
            alarms[key].append(record['trigger_interval'])
        else :
            alarms[key].append(record['location_id'])

    ts = SQLiteParser.TableSignature('Recurrence')
    if extractDeleted:
        ts['owner_id'] = ts['entity_id'] = IntNotNull
        ts['end_date'] = SQLiteParser.Signatures.NumericSet(1, 4)

    recurrences = {}
    for record in db.ReadTableRecords(ts, extractDeleted):
        if record.ContainsKey('event_id'):
            key = record['event_id'].Value
        elif record.ContainsKey('owner_id'):
            key = record['owner_id'].Value
        recurrences[key] = record

    attendees = defaultdict(list)

    #Old Db ???
    
    results = []           
    if 'Event' in db.Tables:
        ts = SQLiteParser.TableSignature('Event')
        if extractDeleted:
            ts['start_date'] = ts['end_date'] = SQLiteParser.Signatures.NumericSet(4)
            ts['all_day'] = ts['availability'] = ts['hidden'] = ts['organizer_is_self'] = ts['privacy_level'] = SQLiteParser.Signatures.NumericSet(1)
            ts['last_modified'] = SQLiteParser.Signatures.NumericRange(4, 6)

        for record in db.ReadTableRecords (ts, extractDeleted, True):
            res = CalendarEntry()
            res.Deleted = record.Deleted
            if not IsDBNull (record['summary'].Value):
                res.Subject.Value = record['summary'].Value
                if extractSource:
                    res.Subject.Source = MemoryRange(record['summary'].Source)
            if not IsDBNull (record['location'].Value):
                res.Location.Value = record['location'].Value
                if extractSource:
                    res.Location.Source = MemoryRange(record['location'].Source)
            if not IsDBNull (record['description'].Value):
                res.Details.Value = record['description'].Value
                if extractSource:
                    res.Details.Source = MemoryRange(record['description'].Source)
            if not IsDBNull (record['calendar_id'].Value) and record['calendar_id'].Value in calendars:
                res.Category.Value = calendars[record['calendar_id'].Value].Value
                if extractSource:
                    res.Category.Source = MemoryRange(calendars[record['calendar_id'].Value].Source)
            if not IsDBNull (record['start_date'].Value):
                res.StartDate.Value = TimeStamp(epoch.AddSeconds(record['start_date'].Value), True)
                if extractSource:
                    res.StartDate.Source = MemoryRange(record['start_date'].Source)
                if record['ROWID'].Value in alarms:
                    for alarm in alarms[record['ROWID'].Value]:                    
                        res.Reminders.Add(TimeStamp(epoch.AddSeconds(record['start_date'].Value + alarm.Value), True), MemoryRange(alarm.Source) if extractSource else None)
            if not IsDBNull (record['end_date'].Value):
                res.EndDate.Value = TimeStamp(epoch.AddSeconds(record['end_date'].Value), True)
                if extractSource:
                    res.EndDate.Source = MemoryRange(record['end_date'].Source)

            if record['ROWID'].Value in recurrences:
                recurrence = recurrences[record['ROWID'].Value]
                if not IsDBNull (recurrence['frequency'].Value):
                    res.RepeatRule.Value = repeat_rule[recurrence['frequency'].Value]

                    _get_repeat_day(res, record, recurrence, extractSource)
                    if extractSource:
                        res.RepeatRule.Source = MemoryRange(recurrence['frequency'].Source)
                if not IsDBNull (recurrence['interval'].Value):
                    res.RepeatInterval.Value = recurrence['interval'].Value
                    if extractSource:
                        res.RepeatInterval.Source = MemoryRange(recurrence['interval'].Source)
                repeat_end = recurrence['end_date']
                if not IsDBNull(repeat_end.Value) and repeat_end.Value != 0:
                    res.RepeatUntil.Value = TimeStamp(epoch.AddSeconds(repeat_end.Value), True)
                    if extractSource:
                        res.RepeatUntil.Source = MemoryRange(repeat_end.Source)

            if record['ROWID'].Value in attendees:
                res.Attendees.AddRange(attendees[record['ROWID'].Value])                     
            if res not in results:
                results.append(res)

    elif 'CalendarItem' in db.Tables:
        locations = {}
        ts = SQLiteParser.TableSignature ('Location')
        if extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'title', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'radius', SQLiteParser.Tools.SignatureType.Float, SQLiteParser.Tools.SignatureType.Const0)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'latitude', SQLiteParser.Tools.SignatureType.Float)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'longitude', SQLiteParser.Tools.SignatureType.Float)
            
        for record in db.ReadTableRecords(ts, extractDeleted):
            locations[record['ROWID'].Value] = record['title']
        
        ts = SQLiteParser.TableSignature ('CalendarItem')
        if extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'summary', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'start_date', SQLiteParser.Tools.SignatureType.Int)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'start_tz', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'end_date', SQLiteParser.Tools.SignatureType.Int)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'all_day', SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'unique_identifier', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        for record in db.ReadTableRecords(ts, extractDeleted, True):
            try:
                start_date = int('1' + str(record['start_date'].Value)) if isinstance(record['start_date'].Value, int) else None
                end_date = int('1' + str(record['end_date'].Value)) if isinstance(record['end_date'].Value, int) else None
                calendar = (record['ROWID'].Value, record['summary'].Value, None, None, record['description'].Value, start_date, None, end_date, None, None, None, node.AbsolutePath, 0, 0)
                db_insert_table(db_cache, SQL_INSERT_TABLE_CALENDAR, calendar)
            except:
                pass
            res = CalendarEntry()
            res.Deleted = record.Deleted
            if not IsDBNull(record['summary'].Value):
                res.Subject.Value = record['summary'].Value 
                if extractSource:
                    res.Subject.Source = MemoryRange(record['summary'].Source)
            if not IsDBNull (record['location_id'].Value) and record['location_id'].Value in locations and record['location_id'].Value > 0 and not IsDBNull(locations[record['location_id'].Value].Value):
                res.Location.Value = locations[record['location_id'].Value].Value
                if extractSource:
                    res.Location.Source = MemoryRange(locations[record['location_id'].Value].Source)
            if not IsDBNull (record['description'].Value):
                res.Details.Value = record['description'].Value
                if extractSource:
                    res.Details.Source = MemoryRange(record['description'].Source)
            if not IsDBNull(record['calendar_id'].Value) and record['calendar_id'].Value in calendars and not IsDBNull(calendars[record['calendar_id'].Value].Value):
                res.Category.Value = calendars[record['calendar_id'].Value].Value
                if extractSource:
                    res.Category.Source = MemoryRange(calendars[record['calendar_id'].Value].Source)
            if not IsDBNull(record['start_date'].Value) and record['start_date'].Value !=0 :
                try:
                    time = TimeStamp(epoch.AddSeconds(record['start_date'].Value), True)
 
                    if time.IsValidForSmartphone() or record.Deleted == DeletedState.Intact:
                        res.StartDate.Value = time
                        if extractSource:
                            res.StartDate.Source = MemoryRange(record['start_date'].Source)
                except:
                    pass

                if record['ROWID'].Value in alarms:
                    for alarm in alarms[record['ROWID'].Value]:
                        if not IsDBNull(alarm.Value) and (record.Deleted == DeletedState.Intact or abs(alarm.Value) < 3153600000): # Dates prior to ~1/1/2101 and beyond 1/1/1901                                                                                            
                            res.Reminders.Add(TimeStamp(epoch.AddSeconds(record['start_date'].Value + alarm.Value), True), MemoryRange(alarm.Source) if extractSource else None)
            if not IsDBNull(record['end_date'].Value) and record['end_date'].Value !=0 and not IsDBNull(record['start_date'].Value) and (record['end_date'].Value > record['start_date'].Value or record.Deleted == DeletedState.Intact):
                try:
                    res.EndDate.Value = TimeStamp(epoch.AddSeconds(record['end_date'].Value), True)
                    if extractSource:
                        res.EndDate.Source = MemoryRange(record['end_date'].Source)
                except:
                    pass

            if record['ROWID'].Value in recurrences:
                recurrence = recurrences[record['ROWID'].Value]
                if not IsDBNull (recurrence['frequency'].Value) and recurrence['frequency'].Value in repeat_rule:
                    res.RepeatRule.Value = repeat_rule[recurrence['frequency'].Value]
                    #repeat_day
                    _get_repeat_day(res, record, recurrence, extractSource)
                    if extractSource:
                        res.RepeatRule.Source = MemoryRange(recurrence['frequency'].Source)
                if not IsDBNull (recurrence['interval'].Value):
                    res.RepeatInterval.Value = recurrence['interval'].Value
                    if extractSource:
                        res.RepeatInterval.Source = MemoryRange(recurrence['interval'].Source)
                repeat_end = recurrence['end_date']
                if not IsDBNull(repeat_end.Value) and repeat_end.Value != 0:
                    res.RepeatUntil.Value = TimeStamp(epoch.AddSeconds(repeat_end.Value), True)
                    if extractSource:
                        res.RepeatUntil.Source = MemoryRange(repeat_end.Source)

            if record['ROWID'].Value in attendees:
                res.Attendees.AddRange(attendees[record['ROWID'].Value])

            if 'availability' in record and not IsDBNull(record['availability'].Value):
                if record['availability'].Value == 1:
                    res.Class.Value = EventClass.Private
            if res not in results:        
                results.append(res)
    db_cmd.Dispose()
    db_cache.Close()
    #bcp entry
    temp_dir = ds.OpenCachePath('tmp')
    PA_runtime.save_cache_path(bcp_basic.BASIC_CALENDAR_INFOMATION, db_path, temp_dir)
    pr = ParserResults()
    pr.Models.AddRange(results)
    pr.Build('系统日历')
    return pr

def db_insert_table(db, sql, values):
    db_cmd = SQLite.SQLiteCommand(db)
    if db_cmd is not None:
        db_cmd.CommandText = sql
        db_cmd.Parameters.Clear()
        for value in values:
            param = db_cmd.CreateParameter()
            param.Value = value
            db_cmd.Parameters.Add(param)
        db_cmd.ExecuteNonQuery()