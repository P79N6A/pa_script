#coding=utf-8
import os
import PA_runtime
import datetime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_contact')
except:
    pass
del clr
import hashlib
from model_contact import MC, Contact, Generate
import model_contact
import sqlite3
import re
import time
import shutil

SQL_TABLE_JOIN_CONTACT = '''
    select e.*, f.mimetype from (select c.*, d.last_time_contacted, d.contact_last_updated_timestamp, d.times_contacted from(
        select a.raw_contact_id, a.mimetype_id, a.data1, a.data2, a.data3, a.data4, a.data15, 
        b.contact_id from data as a left join raw_contacts as b on a.raw_contact_id = b._id) 
        as c left join contacts as d on c.contact_id == d._id) as e left join mimetypes as f on e.mimetype_id = f._id 
    '''


class CallsParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = False
        self.extractSource = extractSource
        self.db = None
        self.mc = MC()
        self.cache_path = ds.OpenCachePath("Contact")
        md5_db = hashlib.md5()
        db_name = 'contact'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cache_path + '\\CallSourceDB'
        self.db_tempo = self.sourceDB + '\\db_tempo.db'

    def analyze_call_contacts(self):
        contacts = Contact()
        try:
            contactsNode = self.db_tempo
            try:
                self.db = sqlite3.connect(contactsNode)
            except:
                return
            if self.db is None:
                return
            cursor = self.db.cursor()
            cursor.execute(model_contact.SQL_FIND_TABLE_CONTACTS)
            for row in cursor:
                canceller.ThrowIfCancellationRequested()
                contacts.raw_contact_id = row[0]
                contacts.mimetype_id = row[1]
                contacts.mail = row[2]
                contacts.company = row[3]
                contacts.title = row[4]
                contacts.last_time_contact = row[5]
                contacts.last_time_modify = row[6]
                contacts.times_contacted = row[7]
                contacts.phone_number = row[8]
                contacts.name = row[9]
                contacts.address = row[10]
                contacts.notes = row[11]
                contacts.telegram = row[12]
                contacts.head_pic = row[13]
                contacts.source = row[14]
                self.mc.db_insert_table_call_contacts(contacts)
            self.mc.db_commit()
            self.db.commit()
            self.db.close()
        except Exception as e:
            pass

    def analyze_logic_contacts(self):
        try:
            node = self.node.Parent.GetByPath('/contacts.db')
            self.db = SQLiteParser.Database.FromNode(node, canceller)
            if self.db is None:
                raise Exception('解析联系人出错：无法读取联系人数据库')
            ts = SQLiteParser.TableSignature('AddressBook')
            id = 0
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                contacts = Contact()
                id += 1
                canceller.ThrowIfCancellationRequested()
                contacts.id = id
                homeEmail = ''
                jobEmail = ''
                customEmail = ''
                otherEmail = ''
                if rec['homeEmails'].Value is not None:
                    homeEmail = rec['homeEmails'].Value
                if rec['jobEmails'].Value is not None:
                    jobEmail = rec['jobEmails'].Value
                if rec['customEmails'].Value is not None:
                    customEmail = rec['customEmails'].Value
                if rec['otherEmails'].Value is not None:
                    otherEmail = rec['otherEmails'].Value
                emails = homeEmail + jobEmail + customEmail + otherEmail
                contacts.mail = emails.replace('][', ',').replace('[', '').replace(']', '').replace('\n', '').replace('\"', '').replace(' ', '')
                contacts.company = rec['organization'].Value
                phonenumber = ''
                homenumber = ''
                jobnumber = ''
                othernumber = ''
                customnumber = ''
                if rec['phoneNumbers'].Value is not None:
                    phonenumber = rec['phoneNumbers'].Value
                if rec['homeNumbers'].Value is not None:
                    homenumber = rec['homeNumbers'].Value
                if rec['jobNumbers'].Value is not None:
                    jobnumber = rec['jobNumbers'].Value
                if rec['otherNumbers'].Value is not None:
                    othernumber = rec['otherNumbers'].Value
                if rec['customNumbers'].Value is not None:
                    customnumber = rec['customNumbers'].Value
                numbers = phonenumber + homenumber + jobnumber + othernumber + customnumber
                contacts.phone_number = numbers.replace('][', ',').replace('[', '').replace(']', '').replace('\n', '').replace('\"', '').replace(' ', '')
                contacts.name = rec['name'].Value
                contacts.address = rec['homeStreets'].Value
                contacts.notes = rec['remark'].Value
                contacts.head_pic = rec['photoPath'].Value
                contacts.source = node.AbsolutePath
                self.mc.db_insert_table_call_contacts(contacts)
            self.mc.db_commit()
        except Exception as e:
            print(e)

    def create_db_tempo(self):
        try:
            if os.path.exists(self.db_tempo):
                os.remove(self.db_tempo)
            self.db = sqlite3.connect(self.db_tempo)
            cursor = self.db.cursor()
            cursor.execute(model_contact.SQL_CREATE_TABLE_CONTACTS)
            self.db.commit()
        except:
            pass

    def insert_contact_tempo(self):
        try:
            self.db = sqlite3.connect(self.db_tempo)
            self.cursor = self.db.cursor()
        except:
            return
        try:
            contactsNode = self.sourceDB + '\\contacts2.db'
            db = sqlite3.connect(contactsNode)
            if db is None:
                return
            cursor = db.cursor()
            try:
                cursor.execute(SQL_TABLE_JOIN_CONTACT)
            except:
                self.analyze_logic_contacts()
                return
            for row in cursor:
                if canceller.IsCancellationRequested:
                    break
                raw_contact_id = row[7]
                mimetype_id = row[1]
                if row[11] is not None:
                    mail = row[2] if self._regularMatch(row[11]) == 1 else None
                if row[11] is not None:    
                    company = row[2] if self._regularMatch(row[11]) == 2 else None
                if row[11] is not None:
                    title = row[5] if self._regularMatch(row[11]) == 2 else None
                last_time_contact = row[8]
                last_time_modify = row[9]
                times_contacted = row[10]
                if row[11] is not None:
                    phone_number = row[5] if self._regularMatch(row[11]) == 3 else row[4] if (self._regularMatch(row[11]) == 8 or self._regularMatch(row[11]) == 9 or self._regularMatch(row[11]) == 10) else None
                if row[11] is not None:
                    name = row[2] if self._regularMatch(row[11]) == 4 else None
                if row[11] is not None:                    
                    address = row[2] if self._regularMatch(row[11]) == 5 else None
                if row[11] is not None:    
                    notes = row[2] if self._regularMatch(row[11]) == 6 else raw[3] if (self._regularMatch(row[11]) == 8 or self._regularMatch(row[11]) == 10) else None
                if row[11] is not None:
                    telegram = row[3] if self._regularMatch(row[11]) == 9 else None
                if row[11] is not None:
                    head_pic = row[6] if self._regularMatch(row[11]) == 7 else None
                source = self.node.AbsolutePath
                param = (raw_contact_id, mimetype_id, mail, company, title, last_time_contact, last_time_modify, times_contacted, phone_number, name, address, notes, telegram, head_pic, source, 0, 0)
                self.cursor.execute(model_contact.SQL_INSERT_TABLE_CONTACTS, param)
            self.db.commit()
            self.cursor.close()
            self.db.close()
        except Exception as e:
            traceback.print_exc()

    def db_insert_table(self, sql, values):
        try:
            self.db_trans = self.db.BeginTransaction()
            if self.db_cmd is not None:
                self.db_cmd.CommandText = sql
                self.db_cmd.Parameters.Clear()
                for value in values:
                    param = self.db_cmd.CreateParameter()
                    param.Value = value
                    self.db_cmd.Parameters.Add(param)
                self.db_cmd.ExecuteNonQuery()
            self.db_trans.Commit()
        except Exception as e:
            print(e)

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
        self.mc.db_create(self.cachedb)
        self._copytocache()
        self._closewal('contacts2.db')
        self.create_db_tempo()
        self.insert_contact_tempo()
        self.analyze_call_contacts()
        self.mc.db_close()
        generate = Generate(self.cachedb)
        models = generate.get_models()
        return models

    def _copytocache(self):
        sourceDir = self.node.Parent.PathWithMountPoint
        targetDir = self.sourceDB
        try:
            if os.path.exists(targetDir):
                shutil.rmtree(targetDir)
            shutil.copytree(sourceDir, targetDir)
        except Exception:
            traceback.print_exc()

    def _closewal(self, dbfile):
        try:
            sourceDB = self.node.Parent.PathWithMountPoint + '\\' + dbfile
            targetDB = self.sourceDB + '\\' + dbfile
            f = open(sourceDB, 'rb')
            context = list(f.read())
            f.close()
            if os.path.exists(targetDB):
                os.remove(targetDB)
            f = open(targetDB, 'ab+')
            for i,c in enumerate(context):
                if i == 18 or i == 19:
                    f.write('\x01')
                else:
                    f.write(c)
            f.close()
        except:
            pass

def analyze_android_calls(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(CallsParse(node, extractDeleted, extractSource).parse())
    pr.Build('联系人')
    return pr

def execute(node, extractDeleted):
    return analyze_android_calls(node, extractDeleted, False)