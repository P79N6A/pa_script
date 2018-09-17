#coding=utf-8
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import logging 
SafeLoadAssembly('model_calls')
from model_calls import MC, Records, Contact, Generate

SQL_TABLE_JOIN_CONTACT = '''
    select e.*, f.mimetype from (select c.*, d.last_time_contacted, d.contact_last_updated_timestamp, d.times_contacted from(
        select a.raw_contact_id, a.mimetype_id, a.data1, a.data2, a.data3, a.data4, a.data15, 
        b.contact_id from data as a left join raw_contacts as b on a.raw_contact_id = b._id) 
        as c left join contacts as d on c.contact_id == d._id) as e left join mimetypes as f on e.mimetype_id = f._id 
    '''


class CallsParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.mc = MC()
        self.cache_path = ds.OpenCachePath("CALLS")
        self.cachedb = self.cache_path + "\\calls.db"
        self.mc.db_create(self.cachedb)

    def analyze_call_records(self):
        records = Records()
        try:
            node = self.node.GetByPath('/calls.db')
            self.db = SQLiteParser.Database.FromNode(node)
            if self.db is None:
                node = self.node.GetByPath('/contacts2.db')
                self.db = SQLiteParser.Database.FromNode(node)
                if self.db is None:
                    raise Exception('解析通话记录出错：无法读取通话记录数据库')
            ts = SQLiteParser.TableSignature('calls')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                records.id = rec['_id'].Value if '_id' in rec else None
                records.phone_number = rec['number'].Value if 'number' in rec else None
                records.date = rec['date'].Value if 'date' in rec else None
                records.duration = rec['duration'].Value if 'duration' in rec else None
                records.type = rec['type'].Value if 'type' in rec else None
                if 'name' in rec:
                    records.name = rec['name'].Value if not rec['name'].IsDBNull else rec['number'].Value if 'number' in rec else None
                records.geocoded_location = rec['geocoded_location'].Value if 'geocoded_location' in rec and not rec['geocoded_location'].IsDBNull else None
                records.ring_times = rec['ring_times'].Value if 'ring_times' in rec else None
                records.mark_type = rec['mark_type'].Value if 'mark_type' in rec else None
                records.country_code = rec['countryiso'].Value if 'countryiso' in rec else None
                records.mark_content = rec['mark_content'].Value if 'mark_content' in rec else None
                records.source = "Android通话记录"
                self.mc.db_insert_table_call_records(records)
            self.mc.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_call_contacts(self):
        contacts = Contact()
        try:
            contactsNode = self.node.AbsolutePath
            self.db = sqlite3.connect(contactsNode)
            if self.db is None:
                raise Exception('解析联系人出错：无法读取联系人数据库')
            cursor = self.db.cursor()
            cursor.execute(SQL_TABLE_JOIN_CONTACT)
            for row in cursor:
                contacts.raw_contact_id = row[7]
                contacts.mimetype_id = row[1]
                contacts.mail = row[2] if self._regularMatch(row[11]) == 1 else None
                contacts.company = row[2] if self._regularMatch(row[11]) == 2 else None
                contacts.title = row[5] if self._regularMatch(row[11]) == 2 else None
                contacts.last_time_contact = row[8]
                contacts.last_time_modify = row[9]
                contacts.times_contacted = row[10]
                contacts.phone_number = row[5] if self._regularMatch(row[11]) == 3 else row[4] if (self._regularMatch(row[11]) == 8 or self._regularMatch(row[11]) == 9 or self._regularMatch(row[11]) == 10) else None
                contacts.name = row[2] if self._regularMatch(row[11]) == 4 else None
                contacts.address = row[2] if self._regularMatch(row[11]) == 5 else None
                contacts.notes = row[2] if self._regularMatch(row[11]) == 6 else raw[3] if (self._regularMatch(row[11]) == 8 or self._regularMatch(row[11]) == 10) else None
                contacts.telegram = row[3] if self._regularMatch(row[11]) == 9 else None
                contacts.head_pic = row[6] if self._regularMatch(row[11]) == 7 else None
                contacts.third_party = row[2] if self._regularMatch(row[11]) == 8 or self._regularMatch(row[11]) == 10 else None
                contacts.source = "Android Contacts"
                self.mc.db_insert_table_call_contacts(contacts)
            self.mc.db_commit()
        except Exception as e:
            logging.error(e)

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
        self.analyze_call_records()
        self.analyze_call_contacts()
        self.db.commit()
        self.db.close()
        generate = Generate(self.cachedb)
        models = generate.get_models()
        return models

def analyze_android_calls(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(CallsParse(node, extractDeleted, extractSource).parse())
    return pr