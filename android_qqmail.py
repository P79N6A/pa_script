# -*- coding: utf-8 -*-
import os
import PA_runtime
import datetime
import time
from PA_runtime import *
import logging
import sqlite3
import re
import shutil
SafeLoadAssembly('model_mails')
from model_mails import MM, Mails, Accounts, Contact, MailFolder, Attach, Generate

SQL_ATTACH_TABLE_ACCOUNT1 = """attach database '"""

SQL_ATTACH_TABLE_ACCOUNT2 = """' as 'A';"""

SQL_ASSOCIATE_TABLE_EMAILS = '''select a.id as mailId, a.accountId, a.subject, a.abstract, 
    a.fromAddr as fromEmail, a.utcReceived as receiveUtc, a.size, b.c1receiver as tos, a.isUnread, 
    c.email as account_email, c.name as alias, d.name as mail_folder, e.content, f.size as downloadSize, 
    f.displayname as attachName from QM_MAIL_INFO as a left join QM_MAIL_INFO_FTS_SEARCH_content as b on a.id = b.docid 
    left join A.AccountInfo as c on a.accountId = c.id left join QM_FOLDER as d on a.folderId = d.id left join QM_MAIL_CONTENT as e 
    on a.id = e.id left join QM_MAIL_ATTACH as f on a.id = f.mailid'''

SQL_ASSOCIATE_TABLE_ACCOUNT = '''select id as accountId, name as alias, email as accountEmail from AccountInfo'''

SQL_ASSOCIATE_TABLE_CONTACT = '''select a.name as contactName, a.address as contactEmail, a.name as contactNick, 
    b.email as accountEmail from QM_CONTACT as a left join A.AccountInfo as b on a.accountid = b.id'''

SQL_ASSOCIATE_TABLE_EMAIL_FOLDER = '''select a.name as folderName, b.name as accountNick, b.email as accountEmail from QM_FOLDER as a left join A.AccountInfo as b'''

SQL_ASSOCIATE_TABLE_ATTACH = '''select b.name as AccountNick, b.email as accountEmail, a.mailsubject as subject, a.favtime as downloadUtc, a.fileSizeByte as downloadSize, 
    a.mailsenderaddr as fromEmail, a.mailsenderaddr as fromNick, a.displayname as attachName from QM_MAIL_ATTACH as a left join A.AccountInfo as b'''

class QQMailParser(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.db = None
        self.mm = MM()
        self.cachepath = ds.OpenCachePath("QQMail")
        self.cachedb = self.cachepath + "\\QQMail.db"
        self.sourceDB = self.cachepath + '\\QQMailSourceDB'
        self.mm.db_create(self.cachedb)
        self.attachDir = os.path.normpath(os.path.join(self.node.Parent.Parent.Parent.Parent.AbsolutePath, 'media/0/Download/QQMail'))

    def analyze_mails(self):
        mailNode = self.sourceDB + '\\QMMailDB'
        if mailNode is None:
            return 
        self.db = sqlite3.connect(mailNode)
        SQL_ATTACH_TABLE_ACCOUNT = SQL_ATTACH_TABLE_ACCOUNT1 + self.sourceDB + '\\AccountInfo' + SQL_ATTACH_TABLE_ACCOUNT2
        if self.db is None:
            return
        mails = Mails()
        try:
            cursor = self.db.cursor()
            cursor.execute(SQL_ATTACH_TABLE_ACCOUNT)
            cursor.execute(SQL_ASSOCIATE_TABLE_EMAILS)
            for row in cursor:
                canceller.ThrowIfCancellationRequested()
                mails.mailId = row[0]
                mails.accountId = row[1]
                mails.subject = row[2]
                mails.abstract = row[3]
                mails.fromEmail = row[4]
                mails.receiveUtc = int(str(row[5])[0:-3:1])
                mails.size = row[6]
                mails.tos = row[7]
                mails.isRead = 1 if row[8] == 0 else 0
                mails.account_email = row[9]
                mails.alias = row[10]
                mails.mail_folder = row[11]
                mails.content = row[12]
                mails.downloadSize = row[13]
                mails.attachName = row[14]
                mails.attachDir = self.attachDir if row[14] is not None else None
                self.mm.db_insert_table_mails(mails)
            self.mm.db_commit()
            self.db.close()
        except Exception as e:
            logging.error(e)

    def decode_recover_mail_table(self):
        mailsNode = self.node
        self.db = SQLiteParser.Database.FromNode(mailsNode)
        if self.db is None:
            return
        ts1 = SQLiteParser.TableSignature('QM_MAIL_INFO')
        ts2 = SQLiteParser.TableSignature('QM_MAIL_INFO_FTS_SEARCH_content')
        ts3 = SQLiteParser.TableSignature('QM_FOLDER')
        ts4 = SQLiteParser.TableSignature('QM_MAIL_CONTENT')
        ts5 = SQLiteParser.TableSignature('QM_MAIL_ATTACH')
        try:
            mails = Mails()
            for row in self.db.ReadTableDeletedRecords(ts1, False):
                canceller.ThrowIfCancellationRequested()
                mails.mailId = row['id'].Value if 'id' in row and not row['id'].IsDBNull else None
                mails.folderId = row['folderId'].Value if 'folderId' in row and not row['folderId'].IsDBNull else None
                mails.accountId = row['accountId'].Value if 'accountId' in row and not row['accountId'].IsDBNull else None
                mails.subject = repr(row['subject'].Value) if 'subject' in row and not row['subject'].IsDBNull else None
                mails.abstract = repr(row['abstract'].Value) if 'abstract' in row and not row['abstract'].IsDBNull else None
                mails.fromEmail = repr(row['fromAddr'].Value) if 'fromAddr' in row and not row['fromAddr'].IsDBNull else None
                mails.receiveUtc = row['utcReceived'].Value if 'utcReceived' in row and not row['utcReceived'].IsDBNull else None
                mails.size = row['size'].Value if 'size' in row and not row['size'].IsDBNull else None
                mails.isRead = 1 if row['isUnread'].Value == 0 else 0
                mails.deleted = 1
                self.mm.db_insert_table_mails(mails)
            self.mm.db_commit()
            mails = Mails()
            for row in self.db.ReadTableDeletedRecords(ts2, False):
                canceller.ThrowIfCancellationRequested()
                mails.mailId = row['docid'].Value if 'docid' in row and not row['docid'].IsDBNull else None
                mails.tos = repr(row['clreceiver'].Value) if 'clreceiver' in row and not row['clreceiver'].IsDBNull else None
                mails.deleted = 1
                self.mm.db_insert_table_mails(mails)
            self.mm.db_commit()
            mails = Mails()
            for row in self.db.ReadTableDeletedRecords(ts3, False):
                canceller.ThrowIfCancellationRequested()
                mails.folderId = row['id'].Value if 'id' in row and not row['id'].IsDBNull else None
                mails.mail_folder = repr(row['name'].Value) if 'name' in row and not row['name'].IsDBNull else None
                mails.deleted = 1
                self.mm.db_insert_table_mails(mails)
            self.mm.db_commit()
            mails = Mails()
            for row in self.db.ReadTableDeletedRecords(ts4, False):
                canceller.ThrowIfCancellationRequested()
                mails.mailId = row['id'].Value if 'id' in row and not row['id'].IsDBNull else None
                mails.content = repr(row['content'].Value) if 'content' in row and not row['content'].IsDBNull else None
                mails.deleted = 1
                self.mm.db_insert_table_mails(mails)
            self.mm.db_commit()
            mails = Mails()
            for  row in self.db.ReadTableDeletedRecords(ts5, False):
                canceller.ThrowIfCancellationRequested()
                mails.mailId = row['mailid'].Value if 'mailid' in row and not row['mailid'].IsDBNull else None
                mails.downloadSize = row['size'].Value if 'size' in row and not row['size'].IsDBNull else None
                mails.attachName = repr(row['displayname'].Value) if 'displayname' in row and not row['displayname'].IsDBNull else None
                mails.attachDir = self.attachDir if mails.attachName is not None else None
                mails.deleted = 1
                self.mm.db_insert_table_mails(mails)
            self.mm.db_commit()
        except Exception as e:
            logging.error(e)
        accountNode = self.node.Parent.GetByPath('/AccountInfo')
        self.db = SQLiteParser.Database.FromNode(accountNode)
        if self.db is None:
            return
        ts = SQLiteParser.TableSignature('AccountInfo')
        try:
            mails = Mails()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                canceller.ThrowIfCancellationRequested()
                mails.accountId = row['id'].Value if 'id' in row and not row['id'].IsDBNull else None
                mails.account_email = repr(row['email'].Value) if 'email' in row and not row['email'].IsDBNull else None
                mails.alias = repr(row['name'].Value) if 'name' in row and not row['name'].IsDBNull else None
                mails.deleted = 1
                self.mm.db_insert_table_mails(mails)
            self.mm.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_accounts(self):
        mailNode = self.sourceDB + '\\AccountInfo'
        if mailNode is None:
            return 
        self.db = sqlite3.connect(mailNode)
        accounts = Accounts()
        try:
            cursor = self.db.cursor()
            cursor.execute(SQL_ASSOCIATE_TABLE_ACCOUNT)
            for row in cursor:
                canceller.ThrowIfCancellationRequested()
                accounts.accountId = row[0]
                accounts.alias = row[1]
                accounts.accountEmail = row[2]
                self.mm.db_insert_table_account(accounts)
            self.mm.db_commit()
            self.db.close()
        except Exception as e:
            logging.error(e)

    def analyze_contact(self):
        mailNode = self.sourceDB + '\\QMMailDB'
        if mailNode is None:
            return 
        self.db = sqlite3.connect(mailNode)
        contact = Contact()
        try:
            cursor  = self.db.cursor()
            cursor.execute(SQL_ATTACH_TABLE_ACCOUNT1 + self.sourceDB + '\\AccountInfo' + SQL_ATTACH_TABLE_ACCOUNT2)
            cursor.execute(SQL_ASSOCIATE_TABLE_CONTACT)
            for row in cursor:
                canceller.ThrowIfCancellationRequested()
                contact.contactName = row[0]
                contact.contactEmail = row[1]
                contact.contactNick = row[2]
                contact.accountEmail = row[3]
                self.mm.db_insert_table_contact(contact)
            self.mm.db_commit()
            self.db.close()
        except Exception as e:
            logging.error(e)

    def decode_recover_mail_contact(self):
        mailsNode = self.node
        self.db = SQLiteParser.Database.FromNode(mailsNode)
        if self.db is None:
            return
        ts = SQLiteParser.TableSignature('QM_CONTACT')
        try:
            contact = Contact()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                canceller.ThrowIfCancellationRequested()
                contact.accountId = row['accountid'].Value if 'accountid' in row and not row['accountid'].IsDBNull else None
                contact.contactName = repr(row['name'].Value) if 'name' in row and not row['name'].IsDBNull else None
                contact.contactEmail = repr(row['address'].Value) if 'address' in row and not row['address'].IsDBNull else None
                contact.contactNick = repr(row['name'].Value) if 'name' in row and not row['name'].IsDBNull else None
                contact.deleted = 1
                self.mm.db_insert_table_contact(contact)
            self.mm.db_commit()
        except Exception as e:
            logging.error(e)
        accountNode = self.node.Parent.GetByPath('/AccountInfo')
        self.db = SQLiteParser.Database.FromNode(accountNode)
        if self.db is None:
            return
        ts = SQLiteParser.TableSignature('AccountInfo')
        try:
            contact = Contact()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                canceller.ThrowIfCancellationRequested()
                contact.accountId = row['id'].Value if 'id' in row and not row['id'].IsDBNull else None
                contact.accountEmail = repr(row['email'].Value) if 'email' in row and not row['email'].IsDBNull else None
                contact.deleted = 1
                self.mm.db_insert_table_contact(contact)
        except Exception as e:
            logging.error(e)

    def analyze_email_folder(self):
        mailNode = self.sourceDB + '\\QMMailDB'
        if mailNode is None:
            return 
        self.db = sqlite3.connect(mailNode)
        mailFolder = MailFolder()
        try:
            cursor = self.db.cursor()
            cursor.execute(SQL_ATTACH_TABLE_ACCOUNT1 + self.sourceDB + '\\AccountInfo' + SQL_ATTACH_TABLE_ACCOUNT2)
            cursor.execute(SQL_ASSOCIATE_TABLE_EMAIL_FOLDER)
            for row in cursor:
                canceller.ThrowIfCancellationRequested()
                mailFolder.folderName = row[0]
                mailFolder.accountNick = row[1]
                mailFolder.accountEmail = row[2]
                self.mm.db_insert_table_mail_folder(mailFolder)
            self.mm.db_commit()
            self.db.close()
        except Exception as e:
            logging.error(e)

    def analyze_attach(self):
        mailNode = self.sourceDB + '\\QMMailDB'
        if mailNode is None:
            return 
        self.db = sqlite3.connect(mailNode)
        attach = Attach()
        try:
            cursor = self.db.cursor()
            cursor.execute(SQL_ATTACH_TABLE_ACCOUNT1 + self.sourceDB + '\\AccountInfo' + SQL_ATTACH_TABLE_ACCOUNT2)
            cursor.execute(SQL_ASSOCIATE_TABLE_ATTACH)
            for row in cursor:
                canceller.ThrowIfCancellationRequested()
                attach.accountNick = row[0]
                attach.accountEmail = row[1]
                attach.subject = row[2]
                attach.downloadUtc = row[3]
                attach.downloadSize = row[4]
                attach.fromEmail = row[5]
                attach.fromNick = row[6]
                attach.attachName = row[7]
                attach.attachDir = self.attachDir if row[7] is not None else None
                self.mm.db_insert_table_attach(attach)
            self.mm.db_commit()
            self.db.close()
        except Exception as e:
            logging.error(e)

    def parse(self):
        self._copytocache()
        self.analyze_mails()
        self.decode_recover_mail_table()
        self.analyze_accounts()
        self.analyze_contact()
        self.decode_recover_mail_contact()
        self.analyze_email_folder()
        self.analyze_attach()
        self.mm.db_close()
        generate = Generate(self.cachedb)
        models = generate.get_models()
        return models

    def _copytocache(self):
        sourceDir = self.node.Parent.PathWithMountPoint
        targetDir = self.sourceDB
        try:
            if not os.path.exists(targetDir):
                shutil.copytree(sourceDir, targetDir)
        except Exception:
            pass

def analyze_android_qqmail(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(QQMailParser(node, extractDeleted, extractSource).parse())
    pr.Build('QQMail')
    return pr

def execute(node, extractDeleted):
    return analyze_android_qqmail(node, extractDeleted, False)