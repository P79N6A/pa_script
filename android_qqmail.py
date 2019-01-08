# -*- coding: utf-8 -*-
__author__ = "xiaoyuge"

import os
import PA_runtime
import datetime
import time
from PA_runtime import *

import clr
try:
    clr.AddReference('System.Core')
    clr.AddReference('System.Xml.Linq')
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('model_mail')
    clr.AddReference('bcp_mail')
except:
    pass
del clr

from System.Linq import Enumerable
from model_mail import MM, Account, Mail, Contact, Attachment, Generate
import model_mail
import bcp_mail

import traceback
import shutil
import hashlib
import System.Data.SQLite as SQLite

SQL_ASSOCIATE_TABLE_ACCOUNT = '''select id, email, name, pwd, deleted from AccountInfo'''

SQL_ASSOCIATE_TABLE_MAIL = '''select distinct a.id, a.accountId, a.fromAddr, a.fromAddrName, b.c1receiver, 
                                              a.utcSent, a.subject, a.abstract, c.content, a.isUnread, 
                                              d.name, a.size, a.deleted from QM_MAIL_INFO as a 
                              left join QM_MAIL_INFO_FTS_SEARCH_content as b on a.id = b.docid 
                              left join QM_MAIL_CONTENT as c on a.id = c.id 
                              left join QM_FOLDER as d on a.folderId = d.id'''

SQL_ASSOCIATE_TABLE_CONTACT = '''select distinct accountid, address, name, deleted from QM_CONTACT'''

SQL_ASSOCIATE_TABLE_ATTACH = '''select id, accountid, mailid, name, fileSizeByte, favtime, deleted from QM_MAIL_ATTACH'''

SQL_CREATE_TABLE_RECOVER_ACCOUNT = '''CREATE TABLE IF NOT EXISTS AccountInfo(id INTEGER, email TEXT, name TEXT, pwd TEXT, deleted INTEGER)'''

SQL_CREATE_TABLE_RECOVER_MAILINFO = '''
    CREATE TABLE IF NOT EXISTS QM_MAIL_INFO(
    id INTEGER,
    folderId INTEGER,
    accountId INTEGER,
    fromAddr TEXT,
    fromAddrName TEXT,
    utcSent INTEGER,
    subject TEXT,
    abstract TEXT,
    isUnread INTEGER,
    size INTEGER,
    deleted INTEGER
    )'''

SQL_CREATE_TABLE_RECOVER_TOS = '''
    CREATE TABLE IF NOT EXISTS QM_MAIL_INFO_FTS_SEARCH_content(
    docid INTEGER,
    c1receiver TEXT, 
    deleted INTEGER
    )'''

SQL_CREATE_TABLE_RECOVER_CONTENT = '''
    CREATE TABLE IF NOT EXISTS QM_MAIL_CONTENT(
    id INTEGER,
    content TEXT, 
    deleted INTEGER
    )'''

SQL_CREATE_TABLE_RECOVER_FOLDER = '''
    CREATE TABLE IF NOT EXISTS QM_FOLDER(
    id INTEGER,
    name TEXT, 
    deleted INTEGER
    )'''

SQL_CREATE_TABLE_RECOVER_CONTACT = '''
    CREATE TABLE IF NOT EXISTS QM_CONTACT(
    id INTEGER,
    accountid INTEGER,
    address TEXT,
    name TEXT, 
    deleted INTEGER
    )'''

SQL_CREATE_TABLE_RECOVER_ATTACH = '''
    CREATE TABLE IF NOT EXISTS QM_MAIL_ATTACH(
    id INTEGER,
    accountid INTEGER,
    mailid INTEGER,
    name TEXT,
    fileSizeByte INTEGER,
    favtime INTEGER, 
    deleted INTEGER
    )'''

VERSION_APP_VALUE = 2

class QQMailParser(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        fs = self.node.FileSystem
        nodes = list(fs.Search('/Download/QQMail$'))
        self.attachnode = nodes[0] if len(nodes) != 0 else None
        self.account_node = node.Parent.GetByPath('/AccountInfo')
        self.db = None
        self.mm = MM()
        self.cachepath = ds.OpenCachePath("QQMail")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = self.node.AbsolutePath
        rdb_name = self.node.PathWithMountPoint
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.recoverDB = self.cachepath + '\\' + md5_rdb.hexdigest().upper() + '.db'

    def parse(self):
        if self.mm.need_parse(self.cachedb, VERSION_APP_VALUE):
            self.mm.db_create(self.cachedb)
            self.extract_data()
            self.analyze_data()
            self.mm.db_insert_table_version(model_mail.VERSION_KEY_DB, model_mail.VERSION_VALUE_DB)
            self.mm.db_insert_table_version(model_mail.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.mm.db_commit()
            self.mm.db_close()
        temp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_mail.MAIL_TOOL_TYPE_QQMAIL, self.cachedb, temp_dir)
        generate = Generate(self.cachedb)
        models = generate.get_models()
        return models

    def extract_data(self):
        '''提取删除数据与正常数据'''
        self.create_rdb()
        self.rdb = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.rdb.Open()
        self.rdb_cmd = SQLite.SQLiteCommand(self.rdb)
        self.extract_account()
        self.extract_mailinfo()
        self.extract_contact()
        self.extract_content()
        self.extract_folder()
        self.extract_tos()
        self.extract_attach()
        self.rdb_cmd.Dispose()
        self.rdb.Close()

    def create_rdb(self):
        '''创建恢复数据库'''
        if os.path.exists(self.recoverDB):
            os.remove(self.recoverDB)
        db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        if db_cmd is not None:
            db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_ACCOUNT
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILINFO
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_TOS
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_CONTENT
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_FOLDER
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_CONTACT
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_ATTACH
            db_cmd.ExecuteNonQuery()
        db_cmd.Dispose()
        db.Close()


    def extract_account(self):
        '''提取账户数据'''
        try:
            db =SQLiteParser.Database.FromNode(self.account_node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('AccountInfo')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['id'].Value) and rec['id'].Value != 0:
                        param = (rec['id'].Value, rec['email'].Value, rec['name'].Value, rec['pwd'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into AccountInfo(id, email, name, pwd, deleted) values(?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def extract_mailinfo(self):
        '''提取邮件信息'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_INFO')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['id'].Value) and rec['id'].Value != 0:
                        param = (rec['id'].Value, rec['folderId'].Value, rec['accountId'].Value, rec['fromAddr'].Value, 
                                 rec['utcSent'].Value, rec['subject'].Value, rec['isUnread'].Value, rec['size'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into QM_MAIL_INFO(id, folderId, accountId, fromAddr, utcSent, 
                                                           subject, isUnread, size, deleted) values(?, ?, ?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def extract_tos(self):
        '''提取收件人'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_INFO_FTS_SEARCH_content')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['docid'].Value) and rec['docid'].Value != 0:
                        param = (rec['docid'].Value, rec['c1receiver'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into QM_MAIL_INFO_FTS_SEARCH_content(docid, c1receiver, deleted) 
                                                           values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def extract_content(self):
        '''提取邮件正文'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_CONTENT')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['id'].Value) and rec['id'].Value != 0:
                        param = (rec['id'].Value, rec['content'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into QM_MAIL_CONTENT(id, content, deleted) 
                                                           values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def extract_folder(self):
        '''提取邮件文件夹'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_FOLDER')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['id'].Value) and rec['id'].Value != 0:
                        param = (rec['id'].Value, rec['name'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into QM_FOLDER(id, name, deleted) 
                                                           values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def extract_contact(self):
        '''提取联系人数据'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_CONTACT')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['id'].Value) and rec['id'].Value != 0 and not IsDBNull(rec['accountid'].Value) and rec['accountid'].Value != 0:
                        param = (rec['id'].Value, rec['accountid'].Value, rec['address'].Value, rec['name'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into QM_CONTACT(id, accountid, address, name, deleted) 
                                                           values(?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def extract_attach(self):
        '''提取附件数据'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_ATTACH')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['id'].Value) and rec['id'].Value != 0 and not IsDBNull(rec['accountid'].Value) and rec['accountid'].Value != 0:
                        param = (rec['id'].Value, rec['accountid'].Value, rec['mailid'].Value, rec['name'].Value, rec['fileSizeByte'].Value, rec['favtime'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into QM_MAIL_ATTACH(id, accountid, mailid, name, fileSizeByte, favtime, deleted) 
                                                           values(?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def db_insert_to_deleted_table(self, sql, values):
        '''插入数据到恢复数据库'''
        try:
            self.rdb_trans = self.rdb.BeginTransaction()
            if self.rdb_cmd is not None:
                self.rdb_cmd.CommandText = sql
                self.rdb_cmd.Parameters.Clear()
                for value in values:
                    param = self.rdb_cmd.CreateParameter()
                    param.Value = value
                    self.rdb_cmd.Parameters.Add(param)
                self.rdb_cmd.ExecuteNonQuery()
            self.rdb_trans.Commit()
        except Exception as e:
            print(e)

    def analyze_data(self):
        '''分析数据'''
        self.analyze_account()
        self.analyze_mail()
        self.analyze_contact()
        self.analyze_attach()
    
    def analyze_account(self):
        '''保存账户数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_ACCOUNT
            sr = self.db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    account = Account()
                    if canceller.IsCancellationRequested:
                        break
                    account.account_id = self._db_reader_get_int_value(sr, 0)
                    account.account_user = self._db_reader_get_string_value(sr, 1)
                    account.account_alias = self._db_reader_get_string_value(sr, 2)
                    account.account_email = self._db_reader_get_string_value(sr, 1)
                    account.account_passwd = self._db_reader_get_string_value(sr, 3)
                    account.source = self.node.AbsolutePath
                    account.deleted = self._db_reader_get_int_value(sr, 4)
                    self.mm.db_insert_table_account(account)
                except:
                    traceback.print_exc()
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)


    def analyze_mail(self):
        '''保存邮件数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_MAIL
            sr = self.db_cmd.ExecuteReader()
            while sr.Read():
                try:
                    if canceller.IsCancellationRequested:
                        break
                    mail = Mail()
                    mail.mail_id = self._db_reader_get_int_value(sr, 0)
                    mail.owner_account_id = self._db_reader_get_int_value(sr, 1)
                    sendmail = self._db_reader_get_string_value(sr, 2)
                    sendname = self._db_reader_get_string_value(sr, 3)
                    mail.mail_from = sendmail + ' ' + sendname
                    mail.mail_to = self._db_reader_get_string_value(sr, 4)
                    mail.mail_sent_date = self._db_reader_get_int_value(sr, 5)
                    mail.mail_subject = self._db_reader_get_string_value(sr, 6)
                    mail.mail_abstract = self._db_reader_get_string_value(sr, 7)
                    mail.mail_content = self._db_reader_get_string_value(sr, 8)
                    mail.mail_read_status = 0 if self._db_reader_get_int_value(sr, 9) is 1 else 0
                    mail.mail_group = self._db_reader_get_string_value(sr, 10)
                    mail.mail_send_status = 0 if sr[10] is '草稿箱' else 1 if sr[10] is '已发送' else 2
                    mail.mail_size = self._db_reader_get_int_value(sr, 11)
                    mail.source = self.node.AbsolutePath
                    mail.deleted = self._db_reader_get_int_value(sr, 12)
                    self.mm.db_insert_table_mail(mail)
                except:
                    traceback.print_exc()
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_contact(self):
        '''保存联系人数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_CONTACT
            sr = self.db_cmd.ExecuteReader()
            id = 0
            while sr.Read():
                try:
                    if canceller.IsCancellationRequested:
                        break
                    contact = Contact()
                    id += 1
                    contact.contact_id = id
                    contact.owner_account_id = self._db_reader_get_int_value(sr, 0)
                    contact.contact_user = self._db_reader_get_string_value(sr, 1)
                    contact.contact_email = self._db_reader_get_string_value(sr, 1)
                    contact.contact_alias = self._db_reader_get_string_value(sr, 2)
                    contact.source = self.node.AbsolutePath
                    contact.deleted = self._db_reader_get_int_value(sr, 3)
                    self.mm.db_insert_table_contact(contact)
                except:
                    traceback.print_exc()
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_attach(self):
        '''保存附件数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_ATTACH
            sr = self.db_cmd.ExecuteReader()
            while sr.Read():
                try:
                    if canceller.IsCancellationRequested:
                        break
                    attachment = Attachment()
                    attachment.attachment_id = self._db_reader_get_int_value(sr, 0)
                    attachment.owner_account_id = self._db_reader_get_int_value(sr, 1)
                    attachment.mail_id = self._db_reader_get_int_value(sr, 2)
                    attachname = self._db_reader_get_string_value(sr, 3)
                    attachment.attachment_name = attachname
                    if attachname is not '':
                        attachNodes = self.attachnode.Search(attachname + '$')
                        for attachNode in attachNodes:
                            attachment.attachment_save_dir = attachNode.AbsolutePath
                            break
                    attachment.attachment_size = self._db_reader_get_int_value(sr, 4)
                    attachment.attachment_download_date = self._db_reader_get_int_value(sr, 5)
                    attachment.source = self.node.AbsolutePath
                    attachment.deleted = self._db_reader_get_int_value(sr, 6)
                    self.mm.db_insert_table_attachment(attachment)
                except:
                    traceback.print_exc()
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

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
    def _db_reader_get_blob_value(reader, index, default_value=None):
        if not reader.IsDBNull(index):
            try:
                return bytes(reader.GetValue(index))
            except Exception as e:
                return default_value
        else:
            return default_value

        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

def analyze_android_qqmail(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(QQMailParser(node, extractDeleted, extractSource).parse())
    pr.Build('QQ邮箱')
    return pr

def execute(node, extractDeleted):
    return analyze_android_qqmail(node, extractDeleted, False)