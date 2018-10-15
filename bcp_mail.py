# -*- coding: utf-8 -*-

from PA_runtime import *
import os
import sqlite3
import clr
import shutil
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite


#电子邮箱类信息

#账号信息
SQL_CREATE_TABLE_WA_MFORENSICS_040100 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_040100(
        COLLECT_TARGET_ID TEXT,
        MAIL_TOOL_TYPE TEXT,
        ACCOUNT TEXT,
        REGIS_NICKNAME TEXT,
        PASSWORD TEXT,
        INSTALL_TIME INTEGER,
        AREA TEXT,
        CITY_CODE TEXT,
        FIXED_PHONE TEXT,
        MSISDN TEXT,
        EMAIL_ACCOUNT TEXT,
        CERTIFICATE_TYPE TEXT,
        CERTIFICATE_CODE TEXT,
        SEXCODE INTEGER,
        AGE INTEGER,
        POSTAL_ADDRESS TEXT,
        POSTAL_CODE TEXT,
        OCCUPATION_NAME TEXT,
        BLOOD_TYPE TEXT,
        NAME TEXT,
        SIGN_NAME TEXT,
        PERSONAL_DESC TEXT,
        REG_CITY TEXT,
        GRADUATESCHOOL TEXT,
        ZODIAC TEXT,
        CONSTALLATION TEXT,
        BIRTHDAY INTEGER,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INTEGER,
        USER_PHOTO TEXT,
        ACCOUNT_REG_DATE INTEGER,
        LAST_LOGIN_TIME INTEGER,
        LATEST_MOD_TIME INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_040100 = '''
    INSERT INTO WA_MFORENSICS_040100(COLLECT_TARGET_ID, MAIL_TOOL_TYPE, ACCOUNT,
    REGIS_NICKNAME, PASSWORD, INSTALL_TIME, AREA, CITY_CODE, FIXED_PHONE, MSISDN, EMAIL_ACCOUNT, CERTIFICATE_TYPE, CERTIFICATE_CODE,
    SEXCODE, AGE, POSTAL_ADDRESS, POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, NAME, SIGN_NAME, PERSONAL_DESC, REG_CITY, GRADUATESCHOOL,
    ZODIAC, CONSTALLATION, BIRTHDAY, DELETE_STATUS, DELETE_TIME, USER_PHOTO, ACCOUNT_REG_DATE, LAST_LOGIN_TIME, LATEST_MOD_TIME)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

#联系人信息
SQL_CREATE_TABLE_WA_MFORENSICS_040200 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_040200(
        COLLECT_TARGET_ID TEXT,
        MAIL_TOOL_TYPE TEXT,
        ACCOUNT TEXT,
        FRIEND_ACCOUNT TEXT,
        FRIEND_NICKNAME TEXT,
        FRIEND_GROUP TEXT,
        FRIEND_REMARK TEXT,
        AREA TEXT,
        CITY_CODE TEXT,
        FIXED_PHONE TEXT,
        MSISDN TEXT,
        EMAIL_ACCOUNT TEXT,
        CERTIFICATE_TYPE TEXT,
        CERTIFICATE_CODE TEXT,
        SEXCODE INTEGER,
        AGE INTEGER,
        POSTAL_ADDRESS TEXT,
        POSTAL_CODE TEXT,
        OCCUPATION_NAME TEXT,
        BLOOD_TYPE TEXT,
        NAME TEXT,
        SIGN_NAME TEXT,
        PERSONAL_DESC TEXT,
        REG_CITY TEXT,
        GRADUATESCHOOL TEXT,
        ZODIAC TEXT,
        CONSTALLATION TEXT,
        BIRTHDAY INTEGER,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INTEGER,
        USER_PHOTO TEXT,
        ACCOUNT_REG_DATE INTEGER,
        LAST_LOGIN_TIME INTEGER,
        LATEST_MOD_TIME INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_040200 = '''
    INSERT INTO WA_MFORENSICS_040200(COLLECT_TARGET_ID, MAIL_TOOL_TYPE, ACCOUNT, FRIEND_ACCOUNT,
    FRIEND_NICKNAME, FRIEND_GROUP, FRIEND_REMARK, AREA, CITY_CODE, FIXED_PHONE, MSISDN, EMAIL_ACCOUNT, CERTIFICATE_TYPE, CERTIFICATE_CODE,
    SEXCODE, AGE, POSTAL_ADDRESS, POSTAL_CODE, OCCUPATION_NAME, BLOOD_TYPE, NAME, SIGN_NAME, PERSONAL_DESC, REG_CITY, GRADUATESCHOOL,
    ZODIAC, CONSTALLATION, BIRTHDAY, DELETE_STATUS, DELETE_TIME, USER_PHOTO, ACCOUNT_REG_DATE, LAST_LOGIN_TIME, LATEST_MOD_TIME)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

#电子邮件信息
SQL_CREATE_TABLE_WA_MFORENSICS_040300 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_040300(
        COLLECT_TARGET_ID TEXT,
        MAIL_TOOL_TYPE TEXT,
        ACCOUNT TEXT,
        SEQUENCE_NAME INTEGER,
        [FROM] TEXT,
        [TO] TEXT,
        CC TEXT,
        BCC TEXT,
        MAIL_SEND_TIME INTEGER,
        MAIL_SUBJECT TEXT,
        MAINFILE TEXT,
        MAIL_VIEW_STATUS INTEGER,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INTEGER,
        MEDIA_TYPE TEXT,
        CITY_CODE TEXT,
        COMPANY_ADDRESS TEXT,
        LONGITUDE TEXT,
        LATITUDE TEXT,
        ABOVE_SEALEVEL TEXT,
        MAIL_SAVE_FOLDER TEXT,
        FRIEND_GROUP TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_040300 = '''
    INSERT INTO WA_MFORENSICS_040300(COLLECT_TARGET_ID, MAIL_TOOL_TYPE, ACCOUNT, SEQUENCE_NAME,
    [FROM], [TO], CC, BCC, MAIL_SEND_TIME, MAIL_SUBJECT, MAINFILE, MAIL_VIEW_STATUS, DELETE_STATUS, DELETE_TIME, MEDIA_TYPE, CITY_CODE,
    COMPANY_ADDRESS, LONGITUDE, LATITUDE, ABOVE_SEALEVEL, MAIL_SAVE_FOLDER, FRIEND_GROUP)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

#电子邮件附件信息
SQL_CREATE_TABLE_WA_MFORENSICS_040400 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_040400(
        COLLECT_TARGET_ID TEXT,
        MAIL_TOOL_TYPE TEXT,
        ACCOUNT TEXT,
        SEQUENCE_NAME INTEGER,
        MATERIALS_NAME TEXT,
        FILE_NAME TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INTEGER,
        FILE_SIZE INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_040400 = '''
    INSERT INTO WA_MFORENSICS_040400(COLLECT_TARGET_ID, MAIL_TOOL_TYPE, ACCOUNT, SEQUENCE_NAME,
    MATERIALS_NAME, FILE_NAME, DELETE_STATUS, DELETE_TIME, FILE_SIZE) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

DELETE_STATUS_NOT_DELETED = '0'
DELETE_STATUS_DELETED = '1'

MAIL_VIEW_UNREAD = 0
MAIL_VIEW_READ = 1
MAIL_VIEW_OTHER = 9

MAIL_SAVE_RECIEVE = "01"
MAIL_SAVE_POST = "02"
MAIL_SAVE_DRAFT = "03"
MAIL_SAVE_TRASH = "04"
MAIL_SAVE_OTHER = "99"

MAIL_TOOL_TYPE_FOXMAIL = "01001"
MAIL_TOOL_TYPE_OUTLOOK = "01002"
MAIL_TOOL_TYPE_PHONE = "01003"
MAIL_TOOL_TYPE_139 = "01004"
MAIL_TOOL_TYPE_189 = "01005"
MAIL_TOOL_TYPE_WO = "01006"
MAIL_TOOL_TYPE_QQMAIL = "01007"
MAIL_TOOL_TYPE_OTHER = "01999"

class MailBcp(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None
    
    def db_create(self, db_path):
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except Exception as e:
            print("bcp_mail db_create() remove %s error:%s"%(db_path, e))
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()
        self.db_create_table()
        self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_040100
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_040200
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_040300
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_040400
            self.db_cmd.ExecuteNonQuery()
    
    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_040100, column.get_values())

    def db_insert_table_contact(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_040200, column.get_values())

    def db_insert_table_mail(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_040300, column.get_values())

    def db_insert_table_attachment(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_040400, column.get_values())

    
class Account(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MAIL_TOOL_TYPE = None
        self.ACCOUNT = None
        self.REGIS_NICKNAME = None
        self.PASSWORD = None
        self.INSTALL_TIME = None
        self.AREA = None
        self.CITY_CODE = None
        self.FIXED_PHONE = None
        self.MSISDN = None
        self.EMAIL_ACCOUNT = None
        self.CERTIFICATE_TYPE = None
        self.CERTIFICATE_CODE = None
        self.SEXCODE = None
        self.AGE = None
        self.POSTAL_ADDRESS = None
        self.POSTAL_CODE = None
        self.OCCUPATION_NAME = None
        self.BLOOD_TYPE = None
        self.NAME = None
        self.SIGN_NAME = None
        self.PERSONAL_DESC = None
        self.REG_CITY = None
        self.GRADUATESCHOOL = None
        self.ZODIAC = None
        self.CONSTALLATION = None
        self.BIRTHDAY = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.USER_PHOTO = None
        self.ACCOUNT_REG_DATE = None
        self.LAST_LOGIN_TIME = None
        self.LATEST_MOD_TIME = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.MAIL_TOOL_TYPE, self.ACCOUNT, self.REGIS_NICKNAME, self.PASSWORD, self.INSTALL_TIME,
            self.AREA, self.CITY_CODE, self.FIXED_PHONE, self.MSISDN, self.EMAIL_ACCOUNT, self.CERTIFICATE_TYPE, self.CERTIFICATE_CODE,
            self.SEXCODE, self.AGE, self.POSTAL_ADDRESS, self.POSTAL_CODE, self.OCCUPATION_NAME, self.BLOOD_TYPE, self.NAME, self.SIGN_NAME,
            self.PERSONAL_DESC, self.REG_CITY, self.GRADUATESCHOOL, self.ZODIAC, self.CONSTALLATION, self.BIRTHDAY, self.DELETE_STATUS,
            self.DELETE_TIME, self.USER_PHOTO, self.ACCOUNT_REG_DATE, self.LAST_LOGIN_TIME, self.LATEST_MOD_TIME)


class Contact(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MAIL_TOOL_TYPE = None
        self.ACCOUNT = None
        self.FRIEND_ACCOUNT = None
        self.FRIEND_NICKNAME = None
        self.FRIEND_GROUP = None
        self.FRIEND_REMARK = None
        self.AREA = None
        self.CITY_CODE = None
        self.FIXED_PHONE = None
        self.MSISDN = None
        self.EMAIL_ACCOUNT = None
        self.CERTIFICATE_TYPE = None
        self.CERTIFICATE_CODE = None
        self.SEXCODE = None
        self.AGE = None
        self.POSTAL_ADDRESS = None
        self.POSTAL_CODE = None
        self.OCCUPATION_NAME = None
        self.BLOOD_TYPE = None
        self.NAME = None
        self.SIGN_NAME = None
        self.PERSONAL_DESC = None
        self.REG_CITY = None
        self.GRADUATESCHOOL = None
        self.ZODIAC = None
        self.CONSTALLATION = None
        self.BIRTHDAY = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.USER_PHOTO = None
        self.ACCOUNT_REG_DATE = None
        self.LAST_LOGIN_TIME = None
        self.LATEST_MOD_TIME = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.MAIL_TOOL_TYPE, self.ACCOUNT, self.FRIEND_ACCOUNT, self.FRIEND_NICKNAME, self.FRIEND_GROUP,
            self.FRIEND_REMARK, self.AREA, self.CITY_CODE, self.FIXED_PHONE, self.MSISDN, self.EMAIL_ACCOUNT, self.CERTIFICATE_TYPE,
            self.CERTIFICATE_CODE, self.SEXCODE, self.AGE, self.POSTAL_ADDRESS, self.POSTAL_CODE, self.OCCUPATION_NAME, self.BLOOD_TYPE,
            self.NAME, self.SIGN_NAME, self.PERSONAL_DESC, self.REG_CITY, self.GRADUATESCHOOL, self.ZODIAC, self.CONSTALLATION, self.BIRTHDAY,
            self.DELETE_STATUS, self.DELETE_TIME, self.USER_PHOTO, self.ACCOUNT_REG_DATE, self.LAST_LOGIN_TIME, self.LATEST_MOD_TIME)


class Mail(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MAIL_TOOL_TYPE = None
        self.ACCOUNT = None
        self.SEQUENCE_NAME = None
        self.FROM = None
        self.TO = None
        self.CC = None
        self.BCC = None
        self.MAIL_SEND_TIME = None
        self.MAIL_SUBJECT = None
        self.MAINFILE = None
        self.MAIL_VIEW_STATUS = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.MEDIA_TYPE = None
        self.CITY_CODE = None
        self.COMPANY_ADDRESS = None
        self.LONGITUDE = None
        self.LATITUDE = None
        self.ABOVE_SEALEVEL = None
        self.MAIL_SAVE_FOLDER = None
        self.FRIEND_GROUP = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.MAIL_TOOL_TYPE, self.ACCOUNT, self.SEQUENCE_NAME, self.FROM, self.TO,
            self.CC, self.BCC, self.MAIL_SEND_TIME, self.MAIL_SUBJECT, self.MAINFILE, self.MAIL_VIEW_STATUS, self.DELETE_STATUS,
            self.DELETE_TIME, self.MEDIA_TYPE, self.CITY_CODE, self.COMPANY_ADDRESS, self.LONGITUDE, self.LATITUDE, self.ABOVE_SEALEVEL,
            self.MAIL_SAVE_FOLDER, self.FRIEND_GROUP)


class Attachment(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MAIL_TOOL_TYPE = None
        self.ACCOUNT = None
        self.SEQUENCE_NAME = None
        self.MATERIALS_NAME = None
        self.FILE_NAME = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.FILE_SIZE = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.MAIL_TOOL_TYPE, self.ACCOUNT, self.SEQUENCE_NAME, self.MATERIALS_NAME,
            self.FILE_NAME, self.DELETE_STATUS, self.DELETE_TIME, self.FILE_SIZE)


class GenerateBcp(object):
    def __init__(self, bcp_path, cache_db, bcp_db, collect_target_id, mail_tool_type, mountDir):
        self.mountDir = mountDir
        self.bcp_path = bcp_path
        self.cache_db = cache_db
        self.collect_target_id = collect_target_id
        self.mail_tool_type = mail_tool_type
        self.cache_path = bcp_db
        self.mail = MailBcp()
        self.attachpath = self.bcp_path + '\\attachment'
        os.mkdir(self.attachpath)
        print('bcp_path:'+bcp_path+' cachedb:'+cache_db+' bcp_db'+bcp_db+' collect_target_id:'+collect_target_id+' mail_tool_type:'+mail_tool_type+' mountDir:'+mountDir)

    def generate(self):
        try:
            self.mail.db_create(self.cache_path)
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
            self.db.Open()
            self._generate_account()
            self._generate_contact()
            self._generate_mail()
            self._generate_attachment()
            self.db.Close()
            self.mail.db_close()
        except Exception as e:
            print(e)

    def _generate_account(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from account'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                account = Account()
                account.COLLECT_TARGET_ID = self.collect_target_id
                account.MAIL_TOOL_TYPE = self.mail_tool_type
                account.ACCOUNT = sr[1]
                account.REGIS_NICKNAME = sr[2]
                account.PASSWORD = sr[3]
                account.DELETE_STATUS = self._convert_delete_status(sr[18])
                account.LAST_LOGIN_TIME = sr[15]
                self.mail.db_insert_table_account(account)
            self.mail.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            print(e)

    def _generate_contact(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct b.account_user, a.contact_user,a.contact_alias, a.contact_group,
                                         a.contact_remark, a.contact_phone, a.contact_birthday, a.contact_profession, 
                                         a.contact_addr, a.contact_head_pic, a.contact_reg_date, a.contact_last_login,
                                         a.contact_last_modify, a.deleted from contact as a left join account as b on a.owner_account_id = b.account_id'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                contact = Contact()
                contact.COLLECT_TARGET_ID = self.collect_target_id
                contact.MAIL_TOOL_TYPE = self.mail_tool_type
                contact.ACCOUNT = sr[0]
                contact.FRIEND_ACCOUNT = sr[1]
                contact.FRIEND_NICKNAME = sr[2]
                contact.FRIEND_GROUP = sr[3]
                contact.FRIEND_REMARK = sr[4]
                contact.FIXED_PHONE = sr[5]
                contact.BIRTHDAY = sr[6]
                contact.OCCUPATION_NAME = sr[7]
                contact.POSTAL_ADDRESS = sr[8]
                contact.USER_PHOTO = sr[9]
                contact.ACCOUNT_REG_DATE = sr[10]
                contact.LAST_LOGIN_TIME = sr[11]
                contact.LATEST_MOD_TIME = sr[12]
                contact.DELETE_STATUS = sr[13]
                self.mail.db_insert_table_contact(contact)
            self.mail.db_commit()
        except Exception as e:
            print(e)

    def _generate_mail(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct b.account_user, a.mail_id, a.mail_from, a.mail_to, a.mail_cc,
                                         a.mail_bcc, a.mail_sent_date, a.mail_subject, a.mail_content, a.mail_read_status,
                                         a.deleted, a.mail_group, a.mail_group, a.deleted from mail as a left join account as b on a.owner_account_id = b.account_id'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                mail = Mail()
                mail.COLLECT_TARGET_ID = self.collect_target_id
                mail.MAIL_TOOL_TYPE = self.mail_tool_type
                mail.ACCOUNT = sr[0]
                mail.SEQUENCE_NAME = sr[1]
                mail.FROM = sr[2]
                mail.TO = sr[3]
                mail.CC = sr[4]
                mail.BCC = sr[5]
                mail.MAIL_SEND_TIME = self._get_timestamp(sr[6])
                mail.MAIL_SUBJECT = sr[7]
                mail.MAINFILE = sr[8]
                mail.MAIL_VIEW_STATUS = MAIL_VIEW_UNREAD if sr[9] == 0 else MAIL_VIEW_READ if sr[9] == 1 else MAIL_VIEW_OTHER
                mail.DELETE_STATUS = self._convert_delete_status(sr[10])
                if sr[11] is '收件箱':
                    mail.MAIL_SAVE_FOLDER = MAIL_SAVE_RECIEVE
                elif sr[11] is '发件箱':
                    mail.MAIL_SAVE_FOLDER = MAIL_SAVE_POST
                elif sr[11] is '草稿箱':
                    mail.MAIL_SAVE_FOLDER = MAIL_SAVE_DRAFT
                elif sr[11] is '垃圾箱':
                    mail.MAIL_SAVE_FOLDER = MAIL_SAVE_TRASH
                else:
                    mail.MAIL_SAVE_FOLDER = MAIL_SAVE_OTHER
                mail.FRIEND_GROUP = sr[12]
                mail.DELETE_STATUS = sr[13]
                self.mail.db_insert_table_mail(mail)
            self.mail.db_commit()
        except Exception as e:
            print(e)

    def _generate_attachment(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct b.account_user, a.mail_id, a.attachment_name, a.attachment_save_dir,
                                         a.attachment_size, a.deleted from attachment as a left join account as b on a.owner_account_id = b.account_id'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                attachment = Attachment()
                attachment.COLLECT_TARGET_ID = self.collect_target_id
                attachment.MAIL_TOOL_TYPE = self.mail_tool_type
                attachment.ACCOUNT = sr[0]
                attachment.SEQUENCE_NAME = sr[1]
                attachment.MATERIALS_NAME = sr[2]
                attachment.FILE_NAME = sr[3]
                attachment.FILE_SIZE = sr[4]
                attachment.DELETE_STATUS = sr[5]
                if not IsDBNull(sr[3]):
                    self._copy_attachment(self.mountDir, sr[3])
                self.mail.db_insert_table_attachment(attachment)
            self.mail.db_commit()
        except Exception as e:
            print(e)

    def _copy_attachment(self, mountDir, dir):
        x = mountDir + dir
        sourceDir = x.replace('\\','/')
        targetDir = self.attachpath
        try:
            if os.path.exists(targetDir):
                shutil.copy(sourceDir, targetDir)
        except:
            pass


    @staticmethod
    def _convert_delete_status(status):
        if status == 0:
            return DELETE_STATUS_NOT_DELETED
        else:
            return DELETE_STATUS_DELETED

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)

