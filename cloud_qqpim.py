#_*_ coding:utf-8 _*_
__author__ = 'xiaoyuge'

from PA_runtime import *
import clr
try:
    clr.AddReference('model_contact')
    clr.AddReference('model_callrecord')
    clr.AddReference('model_sms')
except:
    pass
del clr

import model_contact
import model_callrecord
import model_sms

import re
import json
import time
import hashlib

class QQPimParser(model_contact.MC, model_callrecord.MC, model_sms.ModelSMS):
    def __init__(self, node, extractDeleted, extractSource):
        #self.root = node
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.db_cmd = None
        self.cache_path = ds.OpenCachePath("qq同步助手")
        md5_db = hashlib.md5()
        db_name = "qq同步助手"
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cache_path + '\\QQPIMSourceDB'

    #def create_sub_dir_node(self, rpath):
    #    d_node = FileSystem.FromLocalDir(rpath)
    #    self.root.Children.Add(d_node)
    #    return d_node

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = model_contact.SQL_CREATE_TABLE_CONTACTS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_callrecord.SQL_CREATE_TABLE_RECORDS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_sms.SQL_CREATE_TABLE_SIM_CARDS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_sms.SQL_CREATE_TABLE_SMS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_contact.SQL_CREATE_TABLE_VERSION
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

    def parse(self):
        #self.node = self.create_sub_dir_node(r'F:\cases\data\qqpim')
        self.db_create(self.cachedb)
        pr = ParserResults()
        contact_nodes = self.node.Search('contact.*\.json')
        calllog_nodes = self.node.Search('calllog.*\.json')
        sms_nodes = self.node.Search('sms.*\.json')
        if len(list(contact_nodes)) != 0:
            pr1 = ParserResults()
            prog1 = progress.GetBackgroundProgress("云勘QQ同步助手-联系人", DescripCategories.Contacts)
            prog1.Start()
            for file_node in contact_nodes:
                file = file_node.PathWithMountPoint
                self.file_node = file_node
                self.parse_contact(file)
            contact_models = model_contact.Generate(self.cachedb).get_models()
            pr1.Models.AddRange(contact_models)
            pr1.Categories = DescripCategories.Contacts
            pr1.Build('云勘QQ同步助手')
            ds.Add(pr1)
            prog1.Finish(True)
        if len(list(calllog_nodes)) != 0:
            pr2 = ParserResults()
            prog2 = progress.GetBackgroundProgress("云勘QQ同步助手-通讯录", DescripCategories.Calls)
            prog2.Start()
            for file_node in calllog_nodes:
                file = file_node.PathWithMountPoint
                self.file_node = file_node
                self.parse_calllog(file)
            record_models = model_callrecord.Generate(self.cachedb).get_models()
            pr2.Models.AddRange(record_models)
            pr2.Categories = DescripCategories.Calls
            pr2.Build('云勘QQ同步助手')
            ds.Add(pr2)
            prog2.Finish(True)
        if len(list(sms_nodes)) != 0:
            pr3 = ParserResults()
            prog3 = progress.GetBackgroundProgress("云勘QQ同步助手-短信", DescripCategories.Messages)
            prog3.Start()
            for file_node in sms_nodes:
                file = file_node.PathWithMountPoint
                self.file_node = file_node
                self.parse_sms(file)
            sms_models = model_sms.GenerateSMSModel(self.cachedb).get_models()
            pr3.Models.AddRange(sms_models)
            pr3.Categories = DescripCategories.Messages
            pr3.Build('云勘QQ同步助手')
            ds.Add(pr3)
            prog3.Finish(True)
        return pr

    def parse_calllog(self, file):
        f = open(file, 'r')
        lines = f.readlines()
        f.close()
        data = ''.join(lines).strip().replace('\n', '')
        try:
            data = json.loads(data)
            info = data['info'] if 'info' in data else ''
            if info is not '':
                vcalllog = info['VCALLLOG'] if 'VCALLLOG' in info else []
                for calllog in vcalllog:
                    try:
                        tels = calllog['TEL'] if 'TEL' in calllog else []
                        numbers = []
                        for tel in tels:
                            if 'VALUE' in tel:
                                numbers.append(tel['VALUE'])
                        start_time = calllog['STARTTIME'] if 'STARTTIME' in calllog else []
                        stime = []
                        for start in start_time[::2]:
                            if 'VALUE' in start:
                                stime.append(start['VALUE'])
                        end_time = calllog['ENDTIME'] if 'ENDTIME' in calllog else []
                        etime = []
                        for end in end_time[::2]:
                            if 'VALUE' in end:
                                etime.append(end['VALUE'])
                        call_duration = calllog['DURATION'] if 'DURATION' in calllog else []
                        duration = []
                        for d in call_duration[::2]:
                            if 'VALUE' in d:
                                duration.append(d['VALUE'])
                        call_type = calllog['CALLTYPE'] if 'CALLTYPE' in calllog else []
                        calltype = []
                        for call in call_type[::2]:
                            if 'VALUE' in call:
                                ctype = 1 if call['VALUE'] == 'INCOMING' else 2 if call['VALUE'] == 'OUTGOING' else 3 if call['VALUE'] == 'MISS' else 0
                                calltype.append(ctype)
                        id = calllog['dataid'] if 'dataid' in calllog else 0
                        record = model_callrecord.Records()
                        record.id = id
                        record.phone_number = ','.join(numbers)
                        record.date = self.time2timestamp(stime[0]) if len(stime) != 0 else 0
                        record.duration = duration[0] if len(duration) != 0 else 0
                        record.type = calltype[0] if len(calltype) != 0 else 0
                        record.name = record.phone_number
                        record.source = self.file_node.AbsolutePath
                        self.db_insert_table_call_records(record)
                    except:
                        pass
                self.db_commit()
        except:
            return

    def parse_contact(self, file):
        f = open(file, 'r')
        lines = f.readlines()
        f.close()
        data = ''.join(lines).strip().replace('\n', '')
        try:
            data = json.loads(data)
            info = data['info'] if 'info' in data else ''
            if info is not '':
                vcards = info['vacrds'] if 'vacrds' in info else []
                for vcard in vcards:
                    try:
                        dataid = vcard['dataid'] if 'dataid' in vcard else 0
                        contact_info = vcard['vcard'] if 'vcard' in vcard else {}
                        if contact_info is {}:
                            return
                        n = contact_info['N'] if 'N' in contact_info else []
                        names = []
                        for i in n:
                            if 'VALUE' in i:
                                names.append(i['VALUE'])
                        n = contact_info['TEL'] if 'TEL' in contact_info else []
                        numbers = []
                        for i in n:
                            if 'VALUE' in i:
                                numbers.append(i['VALUE'])
                        n = contact_info['NOTE'] if 'NOTE' in contact_info else []
                        notes = []
                        for i in n:
                            if 'VALUE' in i:
                                notes.append(i['VALUE'])
                        n = contact_info['EMAIL'] if 'EMAIL' in contact_info else []
                        emails = []
                        for i in n:
                            if 'VALUE' in i:
                                emails.append(i['VALUE'])
                        n = contact_info['TITLE'] if 'TITLE' in contact_info else []
                        titles = []
                        for i in n:
                            if 'VALUE' in i:
                                titles.append(i['VALUE'])
                        n = contact_info['ORG'] if 'ORG' in contact_info else []
                        orgs = []
                        for i in n:
                            if 'VALUE' in i:
                                orgs.append(i['VALUE'])
                        contact = model_contact.Contact()
                        contact.raw_contact_id = dataid
                        contact.mail = ','.join(emails)
                        contact.company = ','.join(orgs)
                        contact.title = ','.join(titles)
                        contact.phone_number = ','.join(numbers)
                        contact.name = ','.join(names).replace(';', '')
                        contact.notes = ','.join(notes)
                        contact.source = self.file_node.AbsolutePath
                        self.db_insert_table_call_contacts(contact)
                    except:
                        pass
                self.db_commit()
        except:
            return

    def parse_sms(self, file):
        f = open(file, 'r')
        lines = f.readlines()
        f.close()
        data = ''.join(lines).strip().replace('\n', '')
        try:
            data = json.loads(data)
            info = data['info'] if 'info' in data else ''
            if info is not '':
                vcards = info['vcards'] if 'vcards' in info else []
                for vcard in vcards:
                    try:
                        information = vcard['INFORMATION'] if 'INFORMATION' in vcard else ''
                        sender = vcard['SENDER'] if 'SENDER' in vcard else ''
                        sender_name = vcard['SENDNAME'] if 'SENDNAME' in vcard else ''
                        send_date = vcard['SENDDATE'] if 'SENDDATE' in vcard else ''
                        id = vcard['dataid'] if 'dataid' in vcard else 0
                        sms = model_sms.SMS()
                        sms._id = id
                        sms.sender_phonenumber = sender
                        sms.sender_name = sender_name
                        sms.body = information
                        sms.send_time = self.time2timestamp(send_date)
                        sms.source = self.file_node.AbsolutePath
                        self.db_insert_table_sms(sms)
                    except:
                        pass
                self.db_commit()
        except:
            pass


    @staticmethod
    def time2timestamp(tss1):
        try:
            timeArray = time.strptime(tss1, "%Y-%m-%d %H:%M:%S")
            timestamp = int(str(time.mktime(timeArray))[0:10:1])
            return timestamp
        except:
            try:
                timeArray = time.strptime(tss1, "%Y/%m/%d %H:%M:%S")
                timestamp = int(str(time.mktime(timeArray))[0:10:1])
                return timestamp
            except:
                return 0

def analyze_cloud_qqpim(node, extractDeleted, extractSource):
    if node is not None:
        pr = QQPimParser(node, extractDeleted, extractSource).parse()
        return pr

def execute(node, extractDeleted):
    return analyze_cloud_qqpim(node, extractDeleted, False)