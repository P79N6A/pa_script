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

SQL_ASSOCIATE_TABLE_MAIL = '''
    select distinct a.mailId, a.accountId, a.fromEmail, a.fromNick, a.tos, a.ccs, a.bcc, a.receivedUtc,
    a.subject, a.abstract, b.content, a.isRead, c.showName, a.isRecalled, a.ip, a.size from FM_MailInfo as a 
    left join FM_Mail_Content as b on a.mailId = b.mailId left join FM_Folder as c on a.folderId = c.id'''

SQL_ASSOCIATE_TABLE_ACCOUNT = '''select id, name, nickname from FM_Account_Setting'''

SQL_ASSOCIATE_TABLE_CONTACT = """
    select distinct a.contactid, a.accountid, b.email, a.nick, c.name, a.mark, a.mobile from FMContact as a 
    left join FMContactItem as b on a.contactid = b.contactid left join FMContactGroup as c on b.contactgroup = c.groupid"""

SQL_ASSOCIATE_TABLE_ATTACH = '''
    select attachId, accountId, mailId, name, downloadSize, downloadUtc 
    from FM_Mail_Attach'''

SQL_CREATE_TABLE_RECOVER_MAILINFO = '''
    CREATE TABLE IF NOT EXISTS FM_MailInfo(
    mailId INTEGER,
    accountId INTEGER,
    folderId INTEGER,
    fromEmail TEXT,
    fromNick TEXT,
    tos TEXT,
    ccs TEXT,
    bcc TEXT,
    receivedUtc INTEGER,
    subject TEXT,
    abstract TEXT,
    isRead INTEGER,
    isRecalled INTEGER,
    ip TEXT,
    size INTEGER
    )'''

SQL_INSERT_TABLE_RECOVER_MAILINFO = '''
    INSERT INTO FM_MailInfo(mailId, accountId, folderId, fromEmail, fromNick, tos, ccs, bcc, receivedUtc, subject, abstract, isRead, isRecalled, ip, size)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_RECOVER_MAILCONTENT = '''
    CREATE TABLE IF NOT EXISTS FM_Mail_Content(
    mailId INTEGER,
    content TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_MAILCONTENT = '''
    INSERT INTO FM_Mail_Content(mailId, content) 
    VALUES(?, ?)'''

SQL_CREATE_TABLE_RECOVER_MAILFOLDER = '''
    CREATE TABLE IF NOT EXISTS FM_Folder(
    id INTEGER,
    showName TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_MAILFOLDER = '''
    INSERT INTO FM_Folder(id, showName) 
    VALUES(?, ?)'''

SQL_CREATE_TABLE_RECOVER_MAILACCOUNT = '''
    CREATE TABLE IF NOT EXISTS FM_Account_Setting(
    id INTEGER,
    name TEXT,
    nickname TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_MAILACCOUNT = '''
    INSERT INTO FM_Account_Setting(id, name, nickname)
    VALUES(?, ?, ?)'''

SQL_CREATE_TABLE_RECOVER_MAILCONTACT = '''
    CREATE TABLE IF NOT EXISTS FMContact(
    contactid INTEGER,
    accountid INTEGER,
    nick TEXT,
    mark TEXT,
    mobile TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_MAILCONTACT = '''
    INSERT INTO FMContact(contactid, accountid, nick, mark, mobile)
    VALUES(?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_RECOVER_MAILCONTACTITEM = '''
    CREATE TABLE IF NOT EXISTS FMContactItem(
    contactid INTEGER,
    email TEXT,
    contactgroup INTEGER
    )'''

SQL_INSERT_TABLE_RECOVER_MAILCONTACTITEM = '''
    INSERT INTO FMContactItem(contactid, email, contactgroup)
    VALUES(?, ?, ?)'''

SQL_CREATE_TABLE_RECOVER_MAILCONTACTGROUP = '''
    CREATE TABLE IF NOT EXISTS FMContactGroup(
    groupid INTEGER,
    name TEXT
    )'''

SQL_INSERT_TABLE_RECOVER_MAILCONTACTGROUP = '''
    INSERT INTO FMContactGroup(groupid, name)
    VALUES(?, ?)'''

SQL_CREATE_TABLE_RECOVER_MAILATTACH = '''
    CREATE TABLE IF NOT EXISTS FM_Mail_Attach(
    attachId INTEGER,
    accountId INTEGER,
    mailId INTEGER,
    name TEXT,
    downloadSize INTEGER,
    downloadUtc INTEGER
    )'''

SQL_INSERT_TABLE_RECOVER_MAILATTACH = '''
    INSERT INTO FM_Mail_Attach(attachId, accountId, mailId, name, downloadSize, downloadUtc)
    VALUES(?, ?, ?, ?, ?, ?)'''

VERSION_APP_VALUE = 2



class MailParser(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.attachNode = None
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.db = None
        self.db_cmd = None
        self.mm = MM()
        self.cachepath = ds.OpenCachePath("QQMAIL")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = 'QQMail'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        rdb_name = 'QQMailRecover'
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest() + ".db"
        self.sourceDB = self.cachepath + '\\QQMailSourceDB'
        self.recoverDB = self.cachepath + '\\' + md5_rdb.hexdigest() + '.db'

    def analyze_account(self, mailPath, deleteFlag):
        '''保存账户数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(mailPath))
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
        self.db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(mailPath))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_MAIL
            sr = self.db_cmd.ExecuteReader()
            while sr.Read():
                mail = Mail()
                if canceller.IsCancellationRequested:
                    break
                mail.mail_id = sr[0]
                mail.owner_account_id = sr[1]
                sendmail = '' if IsDBNull(sr[2]) else sr[2]
                sendname = '' if IsDBNull(sr[3]) else sr[3]
                mail.mail_from = sendmail + ' ' + sendname
                if not IsDBNull(sr[4]) and sr[4] is not None:
                    mail.mail_to = self._deal(sr[4])
                if not IsDBNull(sr[5]) and sr[5] is not None:
                    mail.mail_cc = self._deal(sr[5])
                if not IsDBNull(sr[6]) and sr[6] is not None:
                    mail.mail_bcc = self._deal(sr[6])
                mail.mail_sent_date = sr[7]
                mail.mail_subject = sr[8]
                mail.mail_abstract = sr[9]
                mail.mail_content = sr[10]
                mail.mail_read_status = sr[11]
                mail.mail_group = sr[12]
                mail.mail_send_status = 0 if sr[12] is '草稿箱' else 1 if sr[12] is '已发送' else 2
                mail.mail_recall_status = sr[13]
                mail.mail_ip = sr[14]
                mail.mail_size = sr[15]
                mail.source = self.node.AbsolutePath
                mail.deleted = deleteFlag
                self.mm.db_insert_table_mail(mail)
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_contact(self, contactPath, deleteFlag):
        """保存联系人数据到中间数据库"""
        self.db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(contactPath))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_CONTACT
            sr = self.db_cmd.ExecuteReader()
            while sr.Read():
                contact = Contact()
                if canceller.IsCancellationRequested:
                    break
                contact.contact_id = sr[0]
                contact.owner_account_id = sr[1]
                contact.contact_user = sr[2]
                contact.contact_email = sr[2]
                contact.contact_alias = sr[3]
                contact.contact_group = sr[4]
                contact.contact_remark = sr[5]
                contact.contact_phone = sr[6]
                contact.source = self.node.AbsolutePath
                contact.deleted = deleteFlag
                self.mm.db_insert_table_contact(contact)
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_attachment(self, mailPath, deleteFlag):
        '''保存附件数据到中间数据库'''
        self.db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(mailPath))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if self.db is None:
                return
            self.db_cmd.CommandText = SQL_ASSOCIATE_TABLE_ATTACH
            sr = self.db_cmd.ExecuteReader()
            while sr.Read():
                attachment = Attachment()
                if canceller.IsCancellationRequested:
                    break
                attachment.attachment_id = sr[0]
                attachment.owner_account_id = sr[1]
                attachment.mail_id = sr[2]
                attachment.attachment_name = sr[3]
                if not IsDBNull(sr[0]) and not IsDBNull(sr[3]):
                    md5_attachname = hashlib.md5()
                    attach_name = str(sr[0]) + sr[3]
                    md5_attachname.update(attach_name.encode(encoding = 'utf-8'))
                    fs = self.node.FileSystem
                    local_name = md5_attachname.hexdigest()
                    fileNodes = fs.Search(local_name)
                    for node in fileNodes:
                        attachment.attachment_save_dir = node.AbsolutePath
                        break
                attachment.attachment_size = sr[4]
                attachment.attachment_download_date = sr[5]
                attachment.source = self.node.AbsolutePath
                self.mm.db_insert_table_attachment(attachment)
                attachment.deleted = deleteFlag
            self.mm.db_commit()
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def analyze_mail_search(self, mailPath, deleteFlag):
        """邮件搜索"""
        pass

    def analyze_normal_data(self):
        '''读取普通数据到中间数据库'''
        if self.node.GetByPath("/Documents/FMailDB.db") is not None:
            mailPath = self.node.GetByPath("/Documents/FMailDB.db").PathWithMountPoint
            self.analyze_account(mailPath, 0)
            self.analyze_mail(mailPath, 0)
            self.analyze_attachment(mailPath, 0)
        if self.node.GetByPath("/Documents/FMContact.db") is not None:
            contactPath = self.node.GetByPath("/Documents/FMContact.db").PathWithMountPoint
            self.analyze_contact(contactPath, 0)

    def analyze_deleted_data(self):
        '''读取恢复数据库数据保存到中间数据库'''
        self.read_deleted_table()
        self.analyze_account(self.recoverDB, 1)
        self.analyze_mail(self.recoverDB, 1)
        self.analyze_contact(self.recoverDB, 1)
        self.analyze_attachment(self.recoverDB, 1)

    def read_deleted_table(self):
        '''读取删除数据保存到恢复数据库'''
        self.create_deleted_db()
        # self.read_deleted_table_mailaccount()
        self.read_deleted_table_mailinfo()
        self.read_deleted_table_mailcontent()
        #self.read_deleted_table_mailfolder()
        self.read_deleted_table_mailcontact()
        self.read_deleted_table_mailcontactitem()
        self.read_deleted_table_mailcontactgroup()
        self.read_deleted_table_mailattach()

    def create_deleted_db(self):
        '''创建恢复数据库'''
        if os.path.exists(self.recoverDB):
            os.remove(self.recoverDB)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILACCOUNT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILATTACH
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILCONTACT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILCONTACTGROUP
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILCONTACTITEM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILCONTENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILFOLDER
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_RECOVER_MAILINFO
            self.db_cmd.ExecuteNonQuery()
        self.db_cmd.Dispose()
        self.db.Close()

    def read_deleted_table_mailaccount(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMailDB.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FM_Account_Setting')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['name'].Value,)
                param = param + (rec['nickname'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILACCOUNT, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailinfo(self):    
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMailDB.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FM_MailInfo')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['mailId'].Value,)
                param = param + (rec['accountId'].Value,)
                param = param + (rec['folderId'].Value,)
                param = param + (rec['fromEmail'].Value,)
                param = param + (rec['fromNick'].Value,)
                param = param + (rec['tos'].Value,)
                param = param + (rec['ccs'].Value,)
                param = param + (rec['bcc'].Value,)
                param = param + (rec['receivedUtc'].Value,)
                param = param + (rec['subject'].Value,)
                param = param + (rec['abstract'].Value,)
                param = param + (rec['isRead'].Value,)
                param = param + (rec['isRecalled'].Value,)
                param = param + (rec['ip'].Value,)
                param = param + (rec['size'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILINFO, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailcontent(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMailDB.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FM_Mail_Content')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['mailId'].Value,)
                param = param + (rec['content'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILCONTENT, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailfolder(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMailDB.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FM_Folder')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['id'].Value,)
                param = param + (rec['showName'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILFOLDER, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailcontact(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMContact.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FMContact')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['contactid'].Value,)
                param = param + (rec['accountid'].Value,)
                param = param + (rec['nick'].Value,)
                param = param + (rec['mark'].Value,)
                param = param + (rec['mobile'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILCONTACT, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailcontactitem(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMContact.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FMContactItem')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['contactid'].Value,)
                param = param + (rec['email'].Value,)
                param = param + (rec['contactgroup'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILCONTACTITEM, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailcontactgroup(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMContact.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FMContactGroup')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['groupid'].Value,)
                param = param + (rec['name'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILCONTACTGROUP, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

    def read_deleted_table_mailattach(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            node = self.node.GetByPath("/Documents/FMailDB.db")
            if node is None:
                return
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('FM_Mail_Attach')
            for rec in db.ReadTableDeletedRecords(ts, False):
                if canceller.IsCancellationRequested:
                    break
                param = ()
                param = param + (rec['attachId'].Value,)
                param = param + (rec['accountId'].Value,)
                param = param + (rec['mailId'].Value,)
                param = param + (rec['name'].Value,)
                param = param + (rec['downloadSize'].Value,)
                param = param + (rec['downloadUtc'].Value,)
                self.db_insert_table(SQL_INSERT_TABLE_RECOVER_MAILATTACH, param)
            self.db_cmd.Dispose()
            self.db.Close()
        except Exception as e:
            print(e)

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

    def _deal(self, s):
        '''处理iosQQMail收件人、抄送、密送等'''
        lst = s.replace('" <'," ").replace('>;"'," ").replace('>;',"").replace('"',"").split(' ')
        for i in range(len(lst)-1):
            if i%2 == 0:
                temp = lst[i]
                lst[i] = lst[i+1]
                lst[i+1] = temp
        s = " ".join(lst)
        return s

    def parse(self):
        if self.mm.need_parse(self.cachedb, VERSION_APP_VALUE):
            self.mm.db_create(self.cachedb)
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
    

def analyze_qqmail(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(MailParser(node, extractDeleted, extractSource).parse())
    pr.Build('QQ邮箱')
    return pr

def execute(node, extractDeleted):
    return analyze_qqmail(node, extractDeleted, False)