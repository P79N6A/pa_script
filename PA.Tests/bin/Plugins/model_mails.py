# -*- coding: utf-8 -*-


from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import sqlite3
import logging

SQL_CREATE_TABLE_MAILS = '''
    create table if not exists mails(
        mailId INTEGER,
        accountId INTEGER,
        subject TEXT,
        abstract TEXT,
        fromEmail TEXT, 
        receiveUtc REAL,
        size INTEGER,
        tos TEXT,
        cc TEXT,
        bcc TEXT,
        ip TEXT,
        isForward INTEGER,
        isRead INTEGER,
        isRecalled INTEGER,
        sendStatus INTEGER,
        account_email TEXT,
        alias TEXT,
        mail_folder TEXT,
        content TEXT,
        downloadUtc TEXT,
        downloadSize TEXT,
        attachName TEXT,
        exchangeField TEXT,
        attachDir TEXT,
        attach_object TEXT,
        source TEXT,
        deleted INT,
        repeated INT
    )'''

SQL_INSERT_TABLE_MAILS = '''
    insert into mails(mailId, accountId, subject, abstract, fromEmail, receiveUtc, size, tos, cc, bcc, ip, isForward, isRead, isRecalled, sendStatus, account_email, alias,
    mail_folder, content, downloadUtc, downloadSize, attachName, exchangeField, attachDir, attach_object, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_INSERT_TABLE_MAILS_ANDROID = '''
    insert into mails(mailId, accountId, subject, abstract, fromEmail, receiveUtc, size, tos, isRead, account_email, alias, mail_folder,
    content, downloadSize, attachName, attachDir, source, deleted, repeated) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    '''

SQL_CREATE_TABLE_ACCOUNT = '''
    create table if not exists accounts(
        accountId INTEGER,
        alias TEXT,
        accountEmail TEXT,
        loginDate REAL,
        accountImage BLOB,
        accountSign BLOB,
        password TEXT,
        source TEXT,
        deleted INT,
        repeated INT
    )'''

SQL_INSERT_TABLE_ACCOUNT = '''
    insert into accounts(accountId, alias, accountEmail, loginDate, accountImage, accountSign, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_INSERT_TABLE_ACCOUNT_ANDROID = '''
    insert into accounts(accountId, alias, accountEmail, source, deleted, repeated) values(?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_CONTACT = '''
    create table if not exists contact(
        contactName TEXT,
        contactBirthday TEXT,
        contactDepartment TEXT,
        contactFamilyAddress TEXT,
        contactMark TEXT,
        contactMobile TEXT,
        contactTelephone TEXT,
        contactEmail TEXT,
        contactNick TEXT,
        groupName TEXT,
        alias TEXT,
        accountEmail TEXT,
        source TEXT,
        deleted INT,
        repeated INT
    )'''

SQL_INSERT_TABLE_CONTACT = '''
    insert into contact(contactName, contactBirthday, contactDepartment, contactFamilyAddress, contactMark,
    contactMobile, contactTelephone, contactEmail, contactNick, groupName, alias, accountEmail, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_INSERT_TABLE_CONTACT_ANDROID = '''
    insert into contact(contactName, contactEmail, contactNick, accountEmail, source, deleted, repeated)
    values(?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_MAIL_FOLDER = '''
    create table if not exists mail_folder(
        folderType TEXT,
        folderName TEXT,
        accountNick TEXT,
        accountEmail,TEXT,
        source TEXT,
        deleted INT,
        repeated INT
    )'''

SQL_INSERT_TABLE_MAIL_FOLDER = '''
    insert into mail_folder(folderType, folderName, accountNick, accountEmail, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?)'''

SQL_INSERT_TABLE_MAIL_FOLDER_ANDROID = '''
    insert into mail_folder(folderName, accountNick, accountEmail, source, deleted, repeated)
    values(?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_ATTACH = '''
    create table if not exists attach(
        accountNick TEXT,
        accountEmail TEXT,
        subject TEXT,
        downloadUtc TEXT,
        downloadSize TEXT,
        fromEmail TEXT,
        fromNick TEXT,
        mailUtc REAL,
        attachName TEXT,
        exchangeField TEXT,
        attachType TEXT,
        attachDir TEXT,
        emailFolder TEXT,
        source TEXT,
        deleted INT,
        repeated INT
    )'''

SQL_INSERT_TABLE_ATTACH = '''
    insert into attach(accountNick, accountEmail, subject, downloadUtc, downloadSize,
    fromEmail, fromNick, mailUtc, attachName, exchangeField, attachType, emailFolder, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_INSERT_TABLE_ATTACH_ANDROID = '''
    insert into attach(accountNick, accountEmail, subject, downloadUtc, downloadSize, 
    fromEmail, fromNick, attachName, emailFolder, source, deleted, repeated)
    values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_TODO = '''
    create table if not exists todo(
        content TEXT,
        createdTime INTEGER,
        reminderTime INTEGER,
        done INTEGER,
        isdeleted INTEGER,
        source TEXT,
        deleted INT, 
        repeated INT
    )'''

SQL_INSERT_TABLE_TODO = '''
    insert into todo(content, createdTime, reminderTime, done, deleted, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?)'''


SQL_CREATE_TABLE_SEARCH = ''''''

SQL_INSERT_TABLE_SEARCH = ''''''


class MM(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()
        self.db_create_table()

    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_create_table(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_MAILS)
            self.cursor.execute(SQL_CREATE_TABLE_ACCOUNT)
            self.cursor.execute(SQL_CREATE_TABLE_CONTACT)
            self.cursor.execute(SQL_CREATE_TABLE_MAIL_FOLDER)
            self.cursor.execute(SQL_CREATE_TABLE_ATTACH)
            self.cursor.execute(SQL_CREATE_TABLE_TODO)
            #self.cursor.execute(SQL_CREATE_TABLE_SEARCH)

    def db_insert_table_mails(self,Mails):
        if self.cursor is not None:
            # self.cursor.execute(SQL_INSERT_TABLE_MAILS, Mails.get_values())
            self.cursor.execute(SQL_INSERT_TABLE_MAILS_ANDROID, Mails.get_values())
    
    def db_insert_table_account(self,Accounts):
        if self.cursor is not None:
            # self.cursor.execute(SQL_INSERT_TABLE_ACCOUNT, Accounts.get_values())
            self.cursor.execute(SQL_INSERT_TABLE_ACCOUNT_ANDROID, Accounts.get_values())

    def db_insert_table_contact(self,Contact):
        if self.cursor is not None:
            # self.cursor.execute(SQL_INSERT_TABLE_CONTACT, Contact.get_values())
            self.cursor.execute(SQL_INSERT_TABLE_CONTACT_ANDROID, Contact.get_values())

    def db_insert_table_mail_folder(self,MailFolder):
        if self.cursor is not None:
            # self.cursor.execute(SQL_INSERT_TABLE_MAIL_FOLDER, MailFolder.get_values())
            self.cursor.execute(SQL_INSERT_TABLE_MAIL_FOLDER_ANDROID, MailFolder.get_values())

    def db_insert_table_attach(self,Attach):
        if self.cursor is not None:
            # self.cursor.execute(SQL_INSERT_TABLE_ATTACH, Attach.get_values())
            self.cursor.execute(SQL_INSERT_TABLE_ATTACH_ANDROID, Attach.get_values())

    def db_insert_table_search(self,Search):
        pass


class Column(object):
    def __init__(self):
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return (self.source, self.deleted, self.repeated)


class Mails(Column):
    def __init__(self):
        super(Mails, self).__init__()
        self.mailId = None
        self.accountId = None
        self.subject = None
        self.abstract = None
        self.fromEmail = None
        self.receiveUtc = None
        self.size = None
        self.tos = None
        self.cc = None
        self.bcc = None
        self.ip = None
        self.isForward = None
        self.isRead = None
        self.isRecalled = None
        self.sendStatus = None
        self.account_email = None
        self.alias = None
        self.mail_folder = None
        self.content = None
        self.downloadUtc = None
        self.downloadSize = None
        self.attachName = None
        self.exchangeField = None
        self.attachDir = None
        self.attach_object = None

    def get_values(self):
        return (self.mailId, self.accountId, self.subject, self.abstract, 
        self.fromEmail, self.receiveUtc,self.size,
        self.tos, self.cc, self.bcc, self.ip,self.isForward,
        self.isRead,self.isRecalled,self.sendStatus,self.account_email,self.alias,
        self.mail_folder, self.content, self.downloadUtc, 
        self.downloadSize, self.attachName, self.exchangeField, self.attachDir, self.attach_object) + super(Mails, self).get_values()

class Mails_Android(Column):
    def __init__(self):
        super(Mails_Android,self).__init__()
        self.mailId = None
        self.accountId = None
        self.subject = None
        self.abstract = None
        self.fromEmail = None
        self.receiveUtc = None
        self.size = None
        self.tos = None
        self.isRead = None
        self.account_email = None
        self.alias = None
        self.mail_folder = None
        self.content = None
        self.downloadSize = None
        self.attachName = None
        self.attachDir = None
    
    def get_values(self):
        return (self.mailId, self.accountId, self.subject, self.abstract, self.fromEmail, self.receiveUtc, self.size, self.tos, 
        self.isRead, self.account_email, self.alias, self.mail_folder, self.content, self.downloadSize, self.attachName, self.attachDir) + super(Mails_Android, self).get_values()
    


class Accounts(Column):
    def __init__(self):
        super(Accounts, self).__init__()
        self.accountId = None
        self.alias = None
        self.accountEmail = None
        self.loginDate = None
        self.accountImage = None
        self.accountSign = None

    def get_values(self):
        return (self.accountId, self.alias, self.accountEmail, self.loginDate,
        self.accountImage, self.accountSign) + super(Accounts,self).get_values()


class Accounts_Android(Column):
    def __init__(self):
        super(Accounts_Android, self).__init__()
        self.accountId = None
        self.alias = None
        self.accountEmail = None

    def get_values(self):
        return (self.accountId, self.alias, self.accountEmail) + super(Accounts_Android, self).get_values()



class Contact(Column):
    def __init__(self):
        super(Contact, self).__init__()
        self.contactName = None
        self.contactBirthday = None
        self.contactDepartment = None
        self.contactFamilyAddress = None
        self.contactMark = None
        self.contactMobile = None
        self.contactTelephone = None
        self.contactEmail = None
        self.contactNick = None
        self.groupName = None
        self.alias = None
        self.accountEmail = None

    def get_values(self):
        return(self.contactName, self.contactBirthday, self.contactDepartment,
        self.contactFamilyAddress, self.contactMark, self.contactMobile, self.contactTelephone,
        self.contactEmail,self.contactNick, self.groupName, self.alias, self.accountEmail) + super(Contact, self).get_values()


class Contact_Android(Column):
    def __init__(self):
        super(Contact_Android, self).__init__()
        self.contactName = None
        self.contactEmail = None
        self.contactNick = None
        self.accountEmail = None

    def get_values(self):
        return (self.contactName, self.contactEmail, self.contactNick, self.accountEmail) + super(Contact_Android, self).get_values()


class MailFolder(Column):
    def __init__(self):
        super(MailFolder, self).__init__()
        self.folderTtpe = None
        self.folderName = None
        self.accountNick = None
        self.accountEmail = None

    def get_values(self):
        return (self.folderTtpe, self.folderName, self.accountNick, self.accountEmail) + super(MailFolder, self).get_values()


class MailFolder_Android(Column):
    def __init__(self):
        super(MailFolder_Android, self).__init__()
        self.folderName = None
        self.accountNick = None
        self.accountEmail = None

    def get_values(self):
        return (self.folderName, self.accountNick, self.accountEmail) + super(MailFolder_Android, self).get_values()


class Attach(Column):
    def __init__(self):
        super(Attach, self).__init__()
        self.accountNick = None
        self.acocuntEmail = None
        self.subject = None
        self.downloadUtc = None
        self.downloadSize = None
        self.fromEmail = None
        self.fromNick = None
        self.mailUtc = None
        self.attachName = None
        self.exchangeField = None
        self.attachType = None
        self.emailFolder = None

    def get_values(self):
        return (self.accountNick, self.acocuntEmail, self.subject, self.downloadUtc,
        self.downloadSize, self.fromEmail, self.fromNick, self.mailUtc,
        self.attachName, self.exchangeField, self.attachType, self.emailFolder) + super(Attach, self).get_values()


class Attach_Android(Column):
    def __init__(self):
        super(Attach_Android, self).__init__()
        self.accountNick = None
        self.accountEmail = None
        self.subject = None
        self.downloadUtc = None
        self.downloadSize = None
        self.fromEmail = None
        self.fromNick = None
        self.attachName = None
        self.emailFolder = None

    def get_values(self):
        return (self.accountNick, self.accountEmail, self.subject, self.downloadUtc, self.downloadSize, self.fromEmail, self.fromNick, self.attachName, self.emailFolder) + super(Attach_Android, self).get_values()


class Search(Column):
    pass


class Generate(object):
    def __init__(self, db_cache):
        self.db_cache = db_cache
        
    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.db_cache)
        self.cursor = self.db.cursor()
        models.extend(self._get_mails_models())
        models.extend(self._get_accounts_models())
        models.extend(self._get_contact_models())
        models.extend(self._get_mail_folder_models())
        models.extend(self._get_attach_models())
        models.extend(self._get_search_models())
        self.cursor.close()
        self.db.close()
        return models

    def _get_mails_models(self):
        models = []
        sql = '''select a.*, accounts.accountEmail,accounts.loginDate from (
            select mailId,subject,abstract,fromEmail,receiveUtc,size,tos,cc,bcc,ip,isForward,
            isRead,isRecalled,sendStatus,account_email,alias,mail_folder,content,group_concat(downloadUtc) as downloadUtc,
            group_concat(downloadSize) as downloadSize,group_concat(attachName) as attachName,group_concat(attachDir) as attachDir,source,repeated,deleted,accountId
            from mails group by mailId) as a left join accounts on a.accountId = accounts.accountId'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            logging.error(e)
            print(e)
        while row is not None:
            mailMessage = Generic.MailMessage()
            mailMessage.Folder.Value = row[16]
            if row[16] == '已发送':
                mailMessage.Status.Value = MessageStatus.Sent
            elif row[16] == '草稿箱':
                mailMessage.Status.Value = MessageStatus.Unsent
            else:
                mailMessage.Status.Value = MessageStatus.Unread if row[11] == 0 else MessageStatus.Read
            mailMessage.Subject.Value = row[1]
            mailMessage.Body.Value = row[17]
            #mailMessage.TimeStamp.Value = TimeStamp.FromUnixTime(row[4], False)  #ios
            mailMessage.TimeStamp.Value = TimeStamp.FromUnixTime(int(row[4][0:-3:1]), False)  #android
            party = Generic.Party()
            party.Identifier.Value = row[3]
            if row[9] is not None:
                party.IPAddresses.Value.Add(str(row[9]))                
            #party.DatePlayed.Value = TimeStamp.FromUnixTime(row[4], False)  #ios
            party.DatePlayed.Value = TimeStamp.FromUnixTime(int(row[4][0:-3:1]), False)  #android
            mailMessage.From.Value = party
            tos = row[6].split(';')
            for t in range(len(tos)-1):
                party = Generic.Party()
                party.Identifier.Value = tos[t]
                #party.DatePlayed.Value = TimeStamp.FromUnixTime(row[4], False)  #ios
                party.DatePlayed.Value = TimeStamp.FromUnixTime(int(row[4][0:-3:1]), False)  #android
                mailMessage.To.Add(party)
            cc = row[7].split(';')
            for c in range(len(cc)-1):
                party = Generic.Party()
                party.Identifier.Value = cc[c]
                #party.DatePlayed.Value = TimeStamp.FromUnixTime(row[4], False)  #ios
                party.DatePlayed.Value = TimeStamp.FromUnixTime(int(row[4][0:-3:1]), False)  #android
                mailMessage.Cc.Add(party)
            bcc = row[8].split(';')
            for b in range(len(bcc)-1):
                party = Generic.Party()
                party.Identifier.Value = bcc[b]
                #party.DatePlayed.Value = TimeStamp.FromUnixTime(row[4], False)  #ios
                party.DatePlayed.Value = TimeStamp.FromUnixTime(int(row[4][0:-3:1]), False)  #android
                mailMessage.BCc.Add(party)
            if row[18] is not None:
                for a in range(len(row[18].split(','))):
                    attachment = Generic.Attachment()
                    attachment.Filename.Value = row[20][a]
                    attachment.URL.Value = row[21][a]
                    attachment.Uri.Value = row[21][a]
                    attachment.DownloadTime.Value = row[18][a]
                    attachment.Size.Value = row[19][a]
                    mailMessage.Attachments.Add(attachment)
            mailMessage.Abstract.Value = row[2]
            mailMessage.Size.Value = row[5]
            mailMessage.IsRecall.Value = row[12]
            user = Common.User()
            user.Name.Value = row[15]
            user.Username.Value = row[26]
            user.LastLoginTime.Value = TimeStamp.FromUnixTime(row[27], False)
            user.Email.Value = row[15]
            mailMessage.OwnerUser.Value = user
            models.append(mailMessage)
            row = self.cursor.fetchone()
        return models

    def _get_accounts_models(self):
        models = []
        return models

    def _get_contact_models(self):
        models = []
        sql = '''select * from contact'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)
        while row is not None:
            friend = Common.Friend()
            friend.OwnerUserID.Value = row[11]
            friend.FullName.Value = row[0]
            friend.CompanyName.Value = row[2]
            addr = Contacts.StreetAddress()
            addr.FullName.Value = row[3]
            friend.LivingAddresses.Add(addr)
            friend.Remarks.Value = row[4]
            friend.PhoneNumber.Value = row[5]
            friend.Email.Value = row[7]
            friend.Name.Value = row[8]
        return models

    def _get_mail_folder_models(self):
        models = []
        return models

    def _get_attach_models(self):
        models = []
        return models

    def _get_search_models(self):
        models = []
        return models

