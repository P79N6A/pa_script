# -*- coding: utf-8 -*-
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

import shutil
import hashlib
import System.Data.SQLite as SQLite

SQL_ASSOCIATE_TABLE_ACCOUNT = '''select id, email, name, pwd from AccountInfo'''

SQL_ASSOCIATE_TABLE_MAIL = '''select distinct a.id, a.accountId, a.fromAddr, a.fromAddrName, b.c1receiver, a.utcSent, a.subject,
    a.abstract, c.content, a.isUnread, d.name, a.size from QM_MAIL_INFO as a left join QM_MAIL_INFO_FTS_SEARCH_content as b 
    on a.id = b.docid left join QM_MAIL_CONTENT as c on a.id = c.id left join QM_FOLDER as d on a.folderId = d.id'''

SQL_ASSOCIATE_TABLE_CONTACT = '''select id, accountid, address, name from QM_CONTACT'''

SQL_ASSOCIATE_TABLE_ATTACH = '''select id, accountid, mailid, displayname, fileSizeByte, favtime from QM_MAIL_ATTACH'''

SQL_CREATE_TABLE_RECOVER_ACCOUNT = '''CREATE TABLE IF NOT EXISTS AccountInfo(id INTEGER, email TEXT, name TEXT, pwd TEXT)'''

SQL_INSERT_TABLE_RECOVER_ACCOUNT = '''INSERT INTO AccountInfo(id, email, name, pwd) VALUES(?, ?, ?, ?)'''

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
    size INTEGER
    )'''

SQL_INSERT_TABLE_RECOVER_MAILINFO = '''INSERT INTO QM_MAIL_INFO(id, folderId, accountId, fromAddr, fromAddrName, utcSent, subject, abstract, isUnread, size)
    VALUES(? ,?, ?, ?, ?, ? ,?, ?, ?, ?)'''

SQL_CREATE_TABLE_RECOVER_TOS = '''
    CREATE TABLE IF NOT EXISTS QM_MAIL_INFO_FTS_SEARCH_content(
    docid INTEGER,
    c1receiver TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_TOS = '''INSERT INTO QM_MAIL_INFO_FTS_SEARCH_content(docid, c1receiver) VALUES(?, ?)'''

SQL_CREATE_TABLE_RECOVER_CONTENT = '''
    CREATE TABLE IF NOT EXISTS QM_MAIL_CONTENT(
    id INTEGER,
    content TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_CONTENT = '''INSERT INTO QM_MAIL_CONTENT(id, content) VALUES(?, ?)'''

SQL_CREATE_TABLE_RECOVER_FOLDER = '''
    CREATE TABLE IF NOT EXISTS QM_FOLDER(
    id INTEGER,
    name TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_FOLDER = '''INSERT INTO QM_FOLDER(id, name) VALUES(?, ?)'''

SQL_CREATE_TABLE_RECOVER_CONTACT = '''
    CREATE TABLE IF NOT EXISTS QM_CONTACT(
    id INTEGER,
    accountid INTEGER,
    address TEXT,
    name TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_CONTACT = '''INSERT INTO QM_CONTACT(id, accountid, address, name) VALUES(?, ?, ?, ?)'''

SQL_CREATE_TABLE_RECOVER_ATTACH = '''
    CREATE TABLE IF NOT EXISTS QM_MAIL_ATTACH(
    id INTEGER,
    accountid INTEGER,
    mailid INTEGER,
    displayname TEXT,
    fileSizeByte INTEGER,
    favtime INTEGER
    )'''

SQL_INSERT_TABLE_RECOVER_ATTACH = '''INSERT INTO QM_MAIL_ATTACH(id, accountid, mailid, displayname, fileSizeByte, favtime) VALUES(?, ?, ?, ?, ?, ?)'''

VERSION_APP_VALUE = 2

class QQMailParser(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.db = None
        self.mm = MM()
        self.cachepath = ds.OpenCachePath("QQMail")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = 'QQMail'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        rdb_name = 'QQMailRecover'
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\QQMailSourceDB'
        self.recoverDB = self.cachepath + '\\' + md5_rdb.hexdigest().upper() + '.db'
        self.attachDir = os.path.normpath(os.path.join(self.node.Parent.Parent.Parent.Parent.AbsolutePath, 'media/0/Download/QQMail'))
    
    def analyze_account(self, accountPath, deleteFlag):
        '''保存账户数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(accountPath))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_ACCOUNT
            sr = self.db_cmd.ExecuteReader()
            while (sr.Read()):
                account = Account()
                if canceller.IsCancellationRequested:
                    break
                account.account_id = sr[0]
                account.account_user = sr[1]
                account.account_alias = sr[2]
                account.account_email = sr[1]
                account.account_passwd = sr[3]
                account.source = self.node.AbsolutePath
                account.deleted = deleteFlag
                self.mm.db_insert_table_account(account)
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_mail(self, mailPath, deleteFlag):
        '''保存邮件数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(mailPath))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_MAIL
            sr = self.db_cmd.ExecuteReader()
            mail = Mail()
            while sr.Read():
                if canceller.IsCancellationRequested:
                    break
                mail.mail_id = sr[0]
                mail.owner_account_id = sr[1]
                sendmail = '' if IsDBNull(sr[2]) else sr[2]
                sendname = '' if IsDBNull(sr[3]) else sr[3]
                mail.mail_from = sendmail + ' ' + sendname
                if not IsDBNull(sr[4]) and sr[4] is not None:
                    mail.mail_to = sr[4]
                mail.mail_sent_date = sr[5]
                mail.mail_subject = sr[6]
                mail.mail_abstract = sr[7]
                mail.mail_content = sr[8]
                mail.mail_read_status = 0 if sr[9] is 1 else 0
                mail.mail_group = sr[10]
                mail.mail_send_status = 0 if sr[10] is '草稿箱' else 1 if sr[10] is '已发送' else 2
                mail.mail_size = sr[11]
                mail.source = self.node.AbsolutePath
                mail.deleted = deleteFlag
                self.mm.db_insert_table_mail(mail)
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_contact(self, mailPath, deleteFlag):
        '''保存联系人数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(mailPath))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_CONTACT
            sr = self.db_cmd.ExecuteReader()
            contact = Contact()
            while sr.Read():
                if canceller.IsCancellationRequested:
                    break
                contact.contact_id = sr[0]
                contact.owner_account_id = sr[1]
                contact.contact_user = sr[2]
                contact.contact_email = sr[2]
                contact.contact_alias = sr[3]
                contact.source = self.node.AbsolutePath
                contact.deleted = deleteFlag
                self.mm.db_insert_table_contact(contact)
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_attach(self, mailPath, deleteFlag):
        '''保存附件数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(mailPath))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_ATTACH
            sr = self.db_cmd.ExecuteReader()
            attachment = Attachment()
            while sr.Read():
                if canceller.IsCancellationRequested:
                    break
                attachment.attachment_id = sr[0]
                attachment.owner_account_id = sr[1]
                attachment.mail_id = sr[2]
                attachment.attachment_name = sr[3]
                if sr[3] is not None:
                    attachment.attachment_save_dir = self.attachDir
                attachment.attachment_size = sr[4]
                attachment.attachment_download_date = sr[5]
                attachment.source = self.node.AbsolutePath
                attachment.deleted = deleteFlag
                self.mm.db_insert_table_attachment(attachment)
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_accountinfo(self):
        '''恢复账户信息'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.Parent.GetByPath('/AccountInfo')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('AccountInfo')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['email'].Value,)
                param = param + (rec['name'].Value,)
                param = param + (rec['pwd'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_ACCOUNT, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailinfo(self):
        '''恢复邮件信息'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_INFO')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['folderId'].Value,)
                param = param + (rec['accountId'].Value,)
                param = param + (rec['fromAddr'].Value,)
                param = param + (rec['fromAddrName'].Value,)
                param = param + (rec['utcSent'].Value,)
                param = param + (rec['subject'].Value,)
                param = param + (rec['abstract'].Value,)
                param = param + (rec['isUnread'].Value,)
                param = param + (rec['size'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILINFO, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailtos(self):
        '''恢复收件人信息'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_INFO_FTS_SEARCH_content')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['docid'].Value,)
                param = param + (rec['c1receiver'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_TOS, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailcontent(self):
        '''恢复正文数据'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_CONTENT')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['content'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_CONTENT, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_folder(self):
        '''恢复邮件分类数据'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_FOLDER')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['name'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_FOLDER, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_contact(self):
        '''恢复联系人数据'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_CONTACT')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['accountid'].Value,)
                param = param + (rec['address'].Value,)
                param = param + (rec['name'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_CONTACT, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_attach(self):
        '''恢复附件数据'''
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('QM_MAIL_ATTACH')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['accountid'].Value,)
                param = param + (rec['mailid'].Value,)
                param = param + (rec['displayname'].Value,)
                param = param + (rec['fileSizeByte'].Value,)
                param = param + (rec['favtime'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_ATTACH, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table(self):
        '''读取删除数据保存到恢复数据库'''
        self.create_deleted_db()
        self.read_deleted_table_accountinfo()
        self.read_deleted_table_mailinfo()
        self.read_deleted_table_mailtos()
        self.read_deleted_table_mailcontent()
        self.read_deleted_table_folder()
        self.read_deleted_table_contact()
        self.read_deleted_table_attach()

    def create_deleted_db(self):
        '''创建恢复数据库'''
        if os.path.exists(self.recoverDB):
            os.remove(self.recoverDB)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_ACCOUNT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILINFO
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_TOS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_CONTENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_FOLDER
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_CONTACT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_ATTACH
            self.db_cmd.ExecuteNonQuery()
        self.db_cmd.Dispose()
        self.db.Close()

    def db_insert_table(self, sql, values):
        '''插入数据到恢复数据库'''
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

    def analyze_normal_data(self):
        '''读取普通数据到中间数据库'''
        accountPath = self.sourceDB + '\\AccountInfo'
        if accountPath is not None:
            self.analyze_account(accountPath, 0)
        mailPath = mailPath = self.sourceDB + '\\QMMailDB'
        if mailPath is not None:
            self.analyze_mail(mailPath, 0)
            self.analyze_contact(mailPath, 0)
            self.analyze_attach(mailPath, 0)

    def analyze_deleted_data(self):
        '''读取恢复数据库数据保存到中间数据库'''
        self.read_deleted_table()
        self.analyze_account(self.recoverDB, 1)
        self.analyze_mail(self.recoverDB, 1)
        self.analyze_contact(self.recoverDB, 1)
        self.analyze_attach(self.recoverDB, 1)

    def parse(self):
        if self.mm.need_parse(self.cachedb, VERSION_APP_VALUE):
            self.mm.db_create(self.cachedb)
            self._copytocache()
            self.analyze_normal_data()
            self.analyze_deleted_data()
            self.mm.db_insert_table_version(model_mail.VERSION_KEY_DB, model_mail.VERSION_VALUE_DB)
            self.mm.db_insert_table_version(model_mail.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.mm.db_commit()
            self.mm.db_close()
        temp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_mail.MAIL_TOOL_TYPE_QQMAIL, self.cachedb, temp_dir)
        generate = Generate(self.cachedb)
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

def analyze_android_qqmail(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(QQMailParser(node, extractDeleted, extractSource).parse())
    pr.Build('QQ邮箱')
    return pr

def execute(node, extractDeleted):
    return analyze_android_qqmail(node, extractDeleted, False)