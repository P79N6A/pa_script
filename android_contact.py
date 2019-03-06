#coding=utf-8
__author__ = "xiaoyuge"

import os
import PA_runtime
import datetime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_contact')
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('bcp_basic')
except:
    pass
del clr

import model_contact
import bcp_basic

import System.Data.SQLite as SQLite
import hashlib
import os
import re
import binascii

import traceback


SQL_CREATE_TABLE_DATA = '''CREATE TABLE IF NOT EXISTS data(
    raw_contact_id INTEGER,
    mimetype_id INTEGER,
    data1 TEXT,
    data2 TEXT,
    data3 TEXT,
    data4 TEXT,
    data15 TEXT,
    deleted INTEGER
    )'''

SQL_INSERT_TABLE_DATA = '''
    INSERT INTO data(raw_contact_id, mimetype_id, data1, data2, data3, data4, data15, deleted)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_MIMETYPES = '''CREATE TABLE IF NOT EXISTS mimetypes(
    _id INTEGER,
    mimetype TEXT
    )'''

SQL_INSERT_TABLE_MIMETYPES = '''
    INSERT INTO mimetypes(_id, mimetype)
    VALUES(?, ?)'''

SQL_SEARCH_TABLE_DATA = '''
    select raw_contact_id, mimetype, 
    data1, data2, data3, data4, data15, deleted from data 
    left join mimetypes on data.mimetype_id = mimetypes._id order by raw_contact_id ASC'''

SQL_SEARCH_TABLE_DATA_HUAWEI_BACK = '''
    select raw_contact_id, mimetype, 
    data1, data2, data3, data4 from data_tb order by raw_contact_id ASC 
    '''

VERSION_APP_VALUE = 1

class ContactParser(model_contact.MC):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.cache_path = ds.OpenCachePath("联系人")
        md5_db = hashlib.md5()
        db_name = self.node.AbsolutePath
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"

        self.raw_contact = {}

        self.deleted_db = self.cache_path + "\\" + "deleted.db"
        self.oppo_dics = []
        self.oppo_dic = {}

    def parse(self):
        if self.need_parse(self.cachedb, VERSION_APP_VALUE):
            print(self.node.PathWithMountPoint)
            self.db_create(self.cachedb)
            #全盘案例 /com.android.provider.contacts/databases/contacts2.db/data
            if re.findall("contacts2.db", self.node.AbsolutePath):
                nodes = self.node.FileSystem.Search('/com.android.providers.contacts/databases/contacts2.db$')
                print(list(nodes))
                for node in nodes:
                    self.contact_node = node
                    self.create_deleted_db()
                    self.analyze_raw_contact_table()
                    self.analyze_contact_case1()
            #备份案例 /contacts/contacts.db/AddressBook
            elif re.findall("contacts.db", self.node.AbsolutePath):
                self.analyze_contact_logic_case1()
            #华为系统备份
            elif re.findall("contact.db", self.node.AbsolutePath):
                dbx = self.node.Parent.GetByPath("contact.dbx")
                if dbx:
                    self.node = dbx
                self.analyze_contact_huawei_bac()
            #oppo系统备份
            elif re.findall("contact.vcf", self.node.AbsolutePath):
                self.analyze_contact_oppo_bac()
            #vivo系统备份
            elif re.findall("contact$", self.node.AbsolutePath):
                self.analyze_contact_oppo_bac()
            self.db_insert_table_version(model_contact.VERSION_KEY_DB, model_contact.VERSION_VALUE_DB)
            self.db_insert_table_version(model_contact.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
        generate = model_contact.Generate(self.cachedb)
        models = generate.get_models()
        #bcp entry
        temp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_basic.BASIC_CONTACT_INFORMATION, self.cachedb, temp_dir)
        PA_runtime.save_cache_path(bcp_basic.BASIC_CONTACT_DETAILED_INFORMATION, self.cachedb, temp_dir)
        generate = model_contact.Generate(self.cachedb)
        models = generate.get_models()
        return models

    def create_deleted_db(self):
        '''恢复删除数据'''
        try:
            if os.path.exists(self.deleted_db):
                os.remove(self.deleted_db)
            self.rdb = SQLite.SQLiteConnection('Data Source = {}'.format(self.deleted_db))
            self.rdb.Open()
            self.rdb_cmd = SQLite.SQLiteCommand(self.rdb)
            self.rdb_cmd.CommandText = SQL_CREATE_TABLE_DATA
            self.rdb_cmd.ExecuteNonQuery()
            self.rdb_cmd.CommandText = SQL_CREATE_TABLE_MIMETYPES
            self.rdb_cmd.ExecuteNonQuery()
            #向恢复数据库中插入数据
            self.insert_deleted_db()
        except:
            traceback.print_exc()

    def insert_deleted_db(self):
        '''向恢复数据库中插入数据'''
        try:
            db = SQLiteParser.Database.FromNode(self.contact_node, canceller)
            ts = SQLiteParser.TableSignature('data')
            self.rdb_trans = self.rdb.BeginTransaction()
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    raw_contact_id = self._db_record_get_int_value(rec, 'raw_contact_id')
                    mimetype_id = self._db_record_get_int_value(rec, 'mimetype_id')
                    data1 = self._db_record_get_string_value(rec, 'data1')
                    data2 = self._db_record_get_string_value(rec, 'data2')
                    data3 = self._db_record_get_string_value(rec, 'data3')
                    data4 = self._db_record_get_string_value(rec, 'data4')
                    data15 = self._db_record_get_string_value(rec, 'data5')
                    deleted = rec.IsDeleted
                    params = (raw_contact_id, mimetype_id, data1, data2, data3, data4, data15, deleted)
                    self.db_insert_delete_table(SQL_INSERT_TABLE_DATA, params)
                except:
                    traceback.print_exc()
            ts = SQLiteParser.TableSignature('mimetypes')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if rec.IsDeleted == 1:
                        continue
                    if rec['_id'].Value is not None and '_id' in rec:
                        _id = rec['_id'].Value
                    if rec['mimetype'].Value is not None and 'mimetype' in rec:
                        mimetype = rec['mimetype'].Value
                    params = (_id, mimetype)
                    self.db_insert_delete_table(SQL_INSERT_TABLE_MIMETYPES, params)
                except:
                    traceback.print_exc()
            self.rdb_trans.Commit()
            self.rdb_cmd.Dispose()
            self.rdb.Close()
        except:
            traceback.print_exc()
        
    def db_insert_delete_table(self, sql, values):
        try:
            if self.rdb_cmd is not None:
                self.rdb_cmd.CommandText = sql
                self.rdb_cmd.Parameters.Clear()
                for value in values:
                    param = self.rdb_cmd.CreateParameter()
                    param.Value = value
                    self.rdb_cmd.Parameters.Add(param)
                self.rdb_cmd.ExecuteNonQuery()
        except Exception as e:
            print(e)

    def analyze_raw_contact_table(self):
        '''分析raw_contacts表获取联系次数与最近联系时间'''
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('raw_contacts')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                contacts = model_contact.Contact()
                if canceller.IsCancellationRequested:
                    break
                id = self._db_record_get_int_value(rec, 'contact_id')
                times_contacted = self._db_record_get_int_value(rec, 'times_contacted')
                last_time_contacted = self._db_record_get_int_value(rec, 'last_time_contacted')
                if id not in self.raw_contact.keys():
                    self.raw_contact[id] = [times_contacted, last_time_contacted]
        except Exception as e:
            print(e)
        except:
            traceback.print_exc()

    def analyze_contact_case1(self):
        '''从提取的删除数据库中获取数据，整理到中间数据库中'''
        try:
            db = SQLite.SQLiteConnection('Data Source = {}'.format(self.deleted_db))
            db.Open()
            db_cmd = SQLite.SQLiteCommand(db)
            if db is None:
                return
            db_cmd.CommandText = SQL_SEARCH_TABLE_DATA
            sr = db_cmd.ExecuteReader()
            data_dic = {}  #{id:data}
            data = {}  #{"name":"xxx", "email":"xxx", "phone":"xxx", address:"xxx", organization:"xxx", occupation:"xxx", note:"xxx"}
            #使用字典嵌套保存整理后的数据
            while (sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    id = self._db_reader_get_int_value(sr, 0)
                    mimetype = self._db_reader_get_string_value(sr, 1)
                    data1 = self._db_reader_get_string_value(sr, 2)
                    data2 = self._db_reader_get_string_value(sr, 3)
                    data3 = self._db_reader_get_string_value(sr, 4)
                    data4 = self._db_reader_get_string_value(sr, 5)
                    data5 = self._db_reader_get_string_value(sr, 6)
                    deleted = self._db_reader_get_int_value(sr, 7)
                    if id not in data_dic.keys():
                        data = self._regularMatch(mimetype, data1, data2, data3, data4, data5)
                        if not data:
                            continue
                        data = dict(data.items()+[("deleted", deleted)])
                        data_dic[id] = data
                    else:
                        d = self._regularMatch(mimetype, data1, data2, data3, data4, data5)
                        if not d:
                            continue
                        if "phone" in d and "phone" in data_dic[id]:
                            if not re.findall(d["phone"], data_dic[id]["phone"]):
                                data_dic[id]["phone"] = data_dic[id]["phone"] + "," + d["phone"]
                        elif "formatphone" in d and "formatphone" in data_dic[id]:
                            if not re.findall(d["formatphone"], data_dic[id]["formatphone"]):
                                data_dic[id]["formatphone"] = data_dic[id]["formatphone"] + "," + d["formatphone"]
                        elif "email" in d and "email" in data_dic[id]:
                            if not re.findall(d["email"], data_dic[id]["email"]):
                                data_dic[id]["email"] = data_dic[id]["email"] + "," + d["email"]
                        elif "address" in d and "address" in data_dic[id]:
                            if not re.findall(d["address"], data_dic[id]["address"]):
                                data_dic[id]["address"] = data_dic[id]["address"] + "," + d["address"]
                        else:
                            data_dic[id] = dict(data_dic[id].items()+d.items())
                except:
                    traceback.print_exc()
            sr.Close()
            db_cmd.Dispose()
            db.Close()
            #将嵌套字典中的数据保存至中间数据库
            for data in data_dic.items():
                try:
                    key = data[0]
                    value = data[1]
                    contacts = model_contact.Contact()
                    contacts.raw_contact_id = key
                    contacts.mail = self._verify_dict(value, "email")
                    contacts.company = self._verify_dict(value, "organization")
                    contacts.title = self._verify_dict(value, "occupation")
                    phone_number = self._verify_dict(value, "phone")
                    formatphone = self._verify_dict(value, "formatphone")
                    if formatphone is not None and formatphone is not '':
                        contacts.phone_number = formatphone
                    elif phone_number is not None:
                        contacts.phone_number = phone_number
                    else:
                        contacts.phone_number = ""
                    contacts.name = self._verify_dict(value, "name")
                    contacts.address = self._verify_dict(value, "address")
                    contacts.notes = self._verify_dict(value, "note")
                    raw_contact = self._verify_dict(self.raw_contact, key)
                    if raw_contact is not None:
                        contacts.times_contacted = raw_contact[0]
                        contacts.last_time_contact = raw_contact[1]
                    contacts.source = self.contact_node.AbsolutePath
                    contacts.deleted = self._verify_dict(value, "deleted")
                    self.db_insert_table_call_contacts(contacts)
                except:
                    pass
            self.db_commit()
        except:
            traceback.print_exc()

    def _regularMatch(self, mimetype, data1, data2, data3, data4, data5):
        '''
        通过mimetype匹配数据库中data字段内容类型
        eg:     
            mimetype为vnd.android.cursor.item/phone_v2，
            则数据库中data1字段储存号码(123-4567-8900)， 
            data4字段存储格式化号码(+86-123-4567-8900)
        最后匹配结果以字典格式返回
        '''
        try:
            data = {}
            #匹配姓名
            if re.findall("name", mimetype):
                if data1 is not '':
                    data["name"] = data1
            #匹配邮件
            elif re.findall("email", mimetype):
                if data1 is not '':
                    data["email"] = data1
            #匹配公司与职业
            elif re.findall("organization", mimetype):
                if data1 is not '':
                    data["organization"] = data1
                if data4 is not '':
                    data["occupation"] = data4
            #匹配备注
            elif re.findall("note", mimetype):
                if data1 is not None:
                    data["note"] = data1
            #匹配联系方式
            elif re.findall("phone", mimetype):
                if data1 is not None:
                    data["phone"] = data1
                if data4 is not None:
                    data["formatphone"] = data4
            #联系地址
            elif re.findall("address", mimetype):
                if data1 is not None:
                    data["address"] = data1
            #telegram
            elif re.findall("telegram", mimetype):
                if data1 is not None:
                    data["phone"] = data1
            return data
        except:
            return {}

    def analyze_contact_logic_case1(self):
        '''逻辑提取案例'''
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('AddressBook')
            id = 0
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                contacts = model_contact.Contact()
                id += 1
                if canceller.IsCancellationRequested:
                    break
                contacts.raw_contact_id = id
                #邮箱
                homeEmail = self._db_record_get_string_value(rec, "homeEmails")
                jobEmail = self._db_record_get_string_value(rec, "jobEmails")
                customEmail = self._db_record_get_string_value(rec, "customEmails")
                otherEmail = self._db_record_get_string_value(rec, "otherEmails")
                emails = homeEmail + jobEmail + customEmail + otherEmail
                contacts.mail = emails.replace('\n', '').replace('][', ',').replace('[', '').replace(']', '').replace('\"', '').replace(' ', '')
                contacts.company = self._db_record_get_string_value(rec, "organization")
                
                #电话号码
                phonenumber = self._db_record_get_string_value(rec, "phoneNumbers")
                homenumber = self._db_record_get_string_value(rec, "homeNumbers")
                jobnumber = self._db_record_get_string_value(rec, "jobNumbers")
                othernumber = self._db_record_get_string_value(rec, "otherNumbers")
                customnumber = self._db_record_get_string_value(rec, "customNumbers")
                numbers = phonenumber + homenumber + jobnumber + othernumber + customnumber
                pnumber = numbers.replace('\n', '').replace('][', ',').replace('[', '').replace(']', '').replace('\"', '').replace(' ', '')
                pnumber = pnumber.split(',')
                pnumber = list(set(pnumber))
                contacts.phone_number = ','.join(pnumber)
                contacts.name = self._db_record_get_string_value(rec, "name")
                contacts.address = self._db_record_get_string_value(rec, "homeStreets")
                contacts.notes = self._db_record_get_string_value(rec, "remark")
                #pic_url = self.node.Parent.Parent.AbsolutePath + '/' + self._db_record_get_string_value(rec, "photoPath")
                #contacts.head_pic = pic_url
                contacts.source = self.node.AbsolutePath
                contacts.deleted = rec.IsDeleted
                self.db_insert_table_call_contacts(contacts)
            self.db_commit()
        except Exception as e:
            print(e)

    def analyze_contact_huawei_bac(self):
        '''解析华为备份案例'''
        try:
            db = SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('data_tb')
            data_dic = {}  #{id:data}
            data = {}  #{"name":"xxx", "email":"xxx", "phone":"xxx", address:"xxx", organization:"xxx", occupation:"xxx", note:"xxx"}
            #使用字典嵌套保存整理后的数据
            #while (sr.Read()):
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    id = self._db_record_get_int_value(rec, "raw_contact_id")
                    mimetype = self._db_record_get_string_value(rec, "mimetype")
                    data1 = self._db_record_get_string_value(rec, "data1")
                    data2 = self._db_record_get_string_value(rec, "data2")
                    data3 = self._db_record_get_string_value(rec, "data3")
                    data4 = self._db_record_get_string_value(rec, "data4")
                    data5 = ""
                    if id not in data_dic.keys():
                        data = self._regularMatch(mimetype, data1, data2, data3, data4, data5)
                        if not data:
                            continue
                        data_dic[id] = data
                    else:
                        d = self._regularMatch(mimetype, data1, data2, data3, data4, data5)
                        if not d:
                            continue
                        if "phone" in d and "phone" in data_dic[id]:
                            if not re.findall(d["phone"], data_dic[id]["phone"]):
                                data_dic[id]["phone"] = data_dic[id]["phone"] + "," + d["phone"]
                        if "formatphone" in d and "formatphone" in data_dic[id]:
                            if not re.findall(d["formatphone"].replace("+", ""), data_dic[id]["formatphone"]):
                                data_dic[id]["formatphone"] = data_dic[id]["formatphone"] + "," + d["formatphone"]
                        elif "email" in d and "email" in data_dic[id]:
                            if not re.findall(d["email"], data_dic[id]["email"]):
                                data_dic[id]["email"] = data_dic[id]["email"] + "," + d["email"]
                        elif "address" in d and "address" in data_dic[id]:
                            if not re.findall(d["address"], data_dic[id]["address"]):
                                data_dic[id]["address"] = data_dic[id]["address"] + "," + d["address"]
                        else:
                            data_dic[id] = dict(data_dic[id].items()+d.items())
                except:
                    traceback.print_exc()
            #将嵌套字典中的数据保存至中间数据库
            for data in data_dic.items():
                try:
                    key = data[0]
                    value = data[1]
                    contacts = model_contact.Contact()
                    contacts.raw_contact_id = key
                    contacts.mail = self._verify_dict(value, "email")
                    contacts.company = self._verify_dict(value, "organization")
                    contacts.title = self._verify_dict(value, "occupation")
                    phone_number = self._verify_dict(value, "phone")
                    formatphone = self._verify_dict(value, "formatphone")
                    if formatphone is not None and formatphone is not '':
                        contacts.phone_number = formatphone
                    elif phone_number is not None:
                        contacts.phone_number = phone_number
                    else:
                        contacts.phone_number = ""
                    contacts.name = self._verify_dict(value, "name")
                    contacts.address = self._verify_dict(value, "address")
                    contacts.notes = self._verify_dict(value, "note")
                    contacts.source = self.node.AbsolutePath
                    self.db_insert_table_call_contacts(contacts)
                except:
                    pass
            self.db_commit()
        except:
            traceback.print_exc()

    def analyze_contact_oppo_bac(self):
        '''解析oppo系统自带案例'''
        try:
            vcf_dir = self.node.PathWithMountPoint
            f = open(vcf_dir, 'r')
            for line in f.readlines():
                if re.findall('BEGIN:VCARD', line):
                    self.oppo_dic = {}
                elif re.findall('END:VCARD', line):
                    self.oppo_dics.append(self.oppo_dic)
                #获取姓名
                elif re.findall("FN:", line) or re.findall("FN;", line):
                    self.vcf_helper("name", line)
                #获取电话号码
                elif re.findall("TEL:", line) or re.findall("TEL;", line):
                    self.vcf_helper("phone", line)
                #获取email
                elif re.findall("EMAIL:", line) or re.findall("EMAIL;", line):
                    self.vcf_helper("email", line)
                #获取地址
                elif re.findall("ADR:", line) or re.findall("ADR;", line):
                    self.vcf_helper("address", line)
                #获取备注
                elif re.findall("NOTE:", line) or re.findall("NOTE;", line):
                    self.vcf_helper("note", line)
                #获取公司
                elif re.findall("ORG:", line) or re.findall("ORG;", line):
                    self.vcf_helper("organization", line)
                #获取职务
                elif re.findall("TITLE:", line) or re.findall("TITLE;", line):
                    self.vcf_helper("occupation", line)

            for value in self.oppo_dics:
                try:
                    contacts = model_contact.Contact()
                    contacts.mail = self._verify_dict(value, "email")
                    contacts.company = self._verify_dict(value, "organization")
                    contacts.title = self._verify_dict(value, "occupation")
                    phone_number = self._verify_dict(value, "phone")
                    formatphone = self._verify_dict(value, "formatphone")
                    if formatphone is not None and formatphone is not '':
                        contacts.phone_number = formatphone
                    elif phone_number is not None:
                        contacts.phone_number = phone_number
                    else:
                        contacts.phone_number = ""
                    contacts.name = self._verify_dict(value, "name")
                    contacts.address = self._verify_dict(value, "address")
                    contacts.notes = self._verify_dict(value, "note")
                    contacts.source = self.node.AbsolutePath
                    self.db_insert_table_call_contacts(contacts)
                except:
                    traceback.print_exc()
            self.db_commit()
        except:
            traceback.print_exc()

    def vcf_helper(self, key, line):
        '''解析vcf名片工具方法'''
        if re.findall("ENCODING=QUOTED-PRINTABLE", line):
            hexStr = re.sub('.*:', '', line).replace("=", "").replace('\n', '').replace(";", "")
            try:
                strs = binascii.a2b_hex(hexStr.lower().strip()).decode("utf-8")
            except:
                strs = ''
        else:
            strs = re.sub('.*:', '', line).replace('\n', '').replace(";", "")
        if key in self.oppo_dic:
            self.oppo_dic[key] = self.oppo_dic[key] + ',' + strs
        else:
            self.oppo_dic[key] = strs

    @staticmethod
    def _verify_dict(dic, string, default_value = None):
        return dic[string] if string in dic else default_value

    @staticmethod
    def _db_record_get_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            return record[column].Value
        return default_value

    @staticmethod
    def _db_record_get_string_value(record, column, default_value=''):
        if not record[column].IsDBNull:
            try:
                value = str(record[column].Value)
                #if record.Deleted != DeletedState.Intact:
                #    value = filter(lambda x: x in string.printable, value)
                return value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_int_value(record, column, default_value=0):
        if not record[column].IsDBNull:
            try:
                return int(record[column].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_blob_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytes(value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_float_value(reader, index, default_value=0):
        return reader.GetFloat(index) if not reader.IsDBNull(index) else default_value

def analyze_android_contact(node, extractDeleted, extractSource):
    pr = ParserResults()
    try:
        if len(list(node.Search('/com.android.providers.contacts/databases/contacts2.db$'))) != 0:
            progress.Start()
            pr.Models.AddRange(ContactParser(node.Search('/com.android.providers.contacts/databases/contacts2.db$')[0], extractDeleted, extractSource).parse())
            pr.Build('联系人')
            return pr
        elif len(list(node.Search('/contacts/contacts.db$'))) != 0:
            progress.Start()
            pr.Models.AddRange(ContactParser(node.Search('/contacts/contacts.db$')[0], extractDeleted, extractSource).parse())
            pr.Build('联系人')
            return pr
        elif len(list(node.Search('contact.db$'))) != 0:
            progress.Start()
            pr.Models.AddRange(ContactParser(node.Search('contact.db$')[0], extractDeleted, extractSource).parse())
            pr.Build('联系人')
            return pr
        elif len(list(node.Search('contact.vcf$'))) != 0:
            progress.Start()
            pr.Models.AddRange(ContactParser(node.Search('contact.vcf$')[0], extractDeleted, extractSource).parse())
            pr.Build('联系人')
            return pr
        elif len(list(node.Search('contact$'))) != 0:
            progress.Start()
            pr.Models.AddRange(ContactParser(node.Search('contact$')[0], extractDeleted, extractSource).parse())
            pr.Build('联系人')
            return pr
        else:
            progress.Skip()
    except:
        progress.Skip()

def execute(node, extractDeleted):
    return analyze_android_contact(node, extractDeleted, False)