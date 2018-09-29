# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions

import os
import System
import sqlite3
import System.Data.SQLite as SQLite
import traceback

'''
字段说明（账户）
0     account_id            账户id 
1     account_user          用户名（如果没有则为邮箱） 
2     account_alias         昵称
3     account_passwd        密码
4     account_area          账户所在地区
5     account_phone         用户电话
6     account_email         用户邮箱
7     account_gender        用户性别(1:MALE 0:FEMALE)
8     account_age           用户年龄
9     account_addr          用户常用地址
10    account_profession    用户职业
11    account_real_name     用户真实姓名
12    account_birthday      用户生日
13    account_head_pic      用户头像
14    account_reg_date      账户注册时间
15    account_last_login    最后登录日期
16    account_last_modify   最后修改日期
17    source                提取目录
18    deleted               是否删除
19    repeated              是否重复
'''
SQL_CREATE_TABLE_ACCOUNT = '''
    CREATE TABLE IF NOT EXISTS account(
        account_id INTEGER,
        account_user TEXT,
        account_alias TEXT,
        account_passwd TEXT,
        account_area TEXT,
        account_phone TEXT,
        account_email TEXT,
        account_gender INTEGER,
        account_age INTEGER,
        account_addr TEXT,
        account_profession TETX,
        account_real_name TEXT,
        account_birthday INTEGER,
        account_head_pic TEXT,
        account_reg_date INTEGER,
        account_last_login INTEGER,
        account_last_modify INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_ACCOUNT = '''
    INSERT INTO account(account_id, account_user, account_alias, account_passwd, account_area, account_phone, account_email,
        account_gender, account_age, account_addr, account_profession, account_real_name, account_birthday, account_head_pic,
        account_reg_date, account_last_login, account_last_modify, source, deleted, repeated) 
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（联系人）
0     contact_id            联系人id
1     owner_account_id      所属账户id
2     contact_user          联系人账户名（如果没有则为邮箱）
3     contact_alias         联系人昵称
4     contact_group         联系人所属分组
5     contact_remark        联系人备注
6     contact_area          联系人所在区域
7     contact_phone         联系人电话
8     contact_email         联系人邮箱
9     contact_gender        联系人性别
10    contact_age           联系人年龄
11    contact_addr          联系人常用地址
12    contact_profession    联系人职业
13    contact_real_name     联系人真实姓名
14    contact_birthday      联系人生日
15    contact_head_pic      联系人头像
16    contact_reg_date      联系人注册时间
17    contact_last_login    联系人最后登陆时间
18    contact_last_modify   联系人最后修改时间
19    source                提取源
20    deleted               是否删除
21    repeated              是否重复 
'''
SQL_CREATE_TABLE_CONTACT = '''
    CREATE TABLE IF NOT EXISTS contact(
        contact_id INTEGER,
        owner_account_id INTEGER,
        contact_user TEXT,
        contact_alias TEXT,
        contact_group TEXT,
        contact_remark TEXT,
        contact_area TEXT,
        contact_phone TEXT,
        contact_email TEXT,
        contact_gender INTEGER,
        contact_age INTEGER,
        contact_addr TEXT,
        contact_profession TEXT,
        contact_real_name TEXT,
        contact_birthday TEXT,
        contact_head_pic TEXT,
        contact_reg_date INTEGER,
        contact_last_login INTEGER,
        contact_last_modify INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_CONTACT = '''
    INSERT INTO contact(contact_id, owner_account_id, contact_user, contact_alias, contact_group,
        contact_remark, contact_area, contact_phone, contact_email, contact_gender, contact_age, contact_addr,
        contact_profession, contact_real_name, contact_birthday, contact_head_pic, contact_reg_date, contact_last_login,
        contact_last_modify, source, deleted, repeated)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（邮件）
0     mail_id               邮件id
1     owner_account_id      所属账户id
2     mail_from             发件人
3     mail_to               收件人
4     mail_cc               抄送
5     mail_bcc              密送
6     mail_sent_date        发送日期
7     mail_subject          主题
8     mail_abstract         摘要
9     mail_content          正文内容
10    mail_read_status      阅读状态（1：已读 0：未读）
11    mail_group            邮件分类
12    mail_save_dir         邮件保存路径
13    mail_send_status      邮件发送状态（1：已发送 0：未发送）
14    mail_recall_status    邮件撤回状态（1：撤回 0：非撤回）
15    mail_size             邮件大小
16    mail_ip               ip地址
17    source                提取源
18    deleted               是否删除
19    repeated              是否重复
'''
SQL_CREATE_TABLE_MAIL = '''
    CREATE TABLE IF NOT EXISTS mail(
        mail_id INTEGER,
        owner_account_id INTEGER,
        mail_from TEXT,
        mail_to TEXT,
        mail_cc TEXT,
        mail_bcc TEXT,
        mail_sent_date INTEGER,
        mail_subject TEXT,
        mail_abstract TEXT,
        mail_content TEXT,
        mail_read_status INTEGER,
        mail_group TEXT,
        mail_save_dir TEXT,
        mail_send_status INTEGER,
        mail_recall_status INTEGER,
        mail_ip TEXT,
        mail_size INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_MAIL = '''
    INSERT INTO mail(mail_id, owner_account_id, mail_from, mail_to, mail_cc, mail_bcc,
        mail_sent_date, mail_subject, mail_abstract, mail_content, mail_read_status, mail_group, mail_save_dir,
        mail_send_status, mail_recall_status, mail_ip, mail_size, source, deleted, repeated) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（附件）
0    attachment_id      附件id
1    owner_account_id   所属用户id
2    mail_id            所属邮件id
3    attachment_name    附件名
4    attachment_save_dir 保存路径
5    attachment_size    附件大小（字节）
6    attachment_download_date 附件下载日期
6    source             提取源
7    deleted            是否删除
8    repeated           是否重复
'''
SQL_CREATE_TABLE_ATTACHMENT = '''
    CREATE TABLE IF NOT EXISTS attachment(
        attachment_id INTEGER,
        owner_account_id INTEGER,
        mail_id INTEGER,
        attachment_name TEXT,
        attachment_save_dir TEXT,
        attachment_size INTEGER,
        attachment_download_date INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_ATTACHMENT = '''
    INSERT INTO attachment(attachment_id, owner_account_id, mail_id, attachment_name,
        attachment_save_dir, attachment_size, attachment_download_date, source, deleted, repeated) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（搜索）
0    search_id          搜索id
1    owner_account_id   所属账户id
2    search_date        搜索日期
3    search_key         搜索关键词
4    source             提取源
5    deleted            是否删除
6    repeated           是否重复
'''
SQL_CREATE_TABLE_SEARCH = '''
    CREATE TABLE IF NOT EXISTS search(
        search_id INTEGER,
        owner_account_id INTEGER,
        search_date INTEGER,
        search_key TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_SEARCH = '''
    INSERT INTO search(search_id, owner_account_id, search_date, search_key,
        source, deleted, repeated) VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

SEND_STATUS_UNSENT = 0
SEND_STATUS_SENT = 1
RECEIVE_MAIL = 2

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

#中间数据库版本
VERSION_VALUE_DB = 1


class MM(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None
    
    def db_create(self,db_path):
        self.db_remove(db_path)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()
        self.db_create_table()
        self.db_commit()

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_update(self, SQL, values=None):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL
            self.db_cmd.Parameters.Clear()            
            for v in values:
                param = self.db_cmd.CreateParameter()
                param.Value = v
                self.db_cmd.Parameters.Add(param)     
            try:
                self.db_cmd.ExecuteNonQuery()
            except:
                traceback.print_exc()
            
    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_remove(self, db_path):
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except Exception as e:
            print("model_mail db_create() remove %s error:%s"%(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_ACCOUNT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CONTACT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MAIL
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_ATTACHMENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SEARCH
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
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

    def db_insert_table_account(self, Account):
        self.db_insert_table(SQL_INSERT_TABLE_ACCOUNT, Account.get_values())

    def db_insert_table_contact(self, Contact):
        self.db_insert_table(SQL_INSERT_TABLE_CONTACT, Contact.get_values())

    def db_insert_table_mail(self, Mail):
        self.db_insert_table(SQL_INSERT_TABLE_MAIL, Mail.get_values())

    def db_insert_table_attachment(self, Attachment):
        self.db_insert_table(SQL_INSERT_TABLE_ATTACHMENT, Attachment.get_values())

    def db_insert_table_search(self, Search):
        self.db_insert_table(SQL_INSERT_TABLE_SEARCH, Search.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key,version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            pass

        while row is not None:
            if row[0] == VERSION_KEY_DB and row[1] == VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == VERSION_KEY_APP and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)


class Column(object):
    def __init__(self):
        self.source = None
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if IsDBNull(value) or value is '':
            self.__dict__[name] = None
        else:
            if isinstance(value, str):
                value = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', value)
            self.__dict__[name] = value


class Account(Column):
    def __init__(self):
        super(Account, self).__init__()
        self.account_id = None
        self.account_user = None
        self.account_alias = None
        self.account_passwd = None
        self.account_area = None
        self.account_phone = None
        self.account_email = None
        self.account_gender = None
        self.account_age = None
        self.account_addr = None
        self.account_profession = None
        self.account_real_name = None
        self.account_birthday = None
        self.account_head_pic = None
        self.account_reg_date = None
        self.account_last_login = None
        self.account_last_modify = None

    def get_values(self):
        return(self.account_id, self.account_user, self.account_alias, self.account_passwd,
        self.account_area, self.account_phone, self.account_email, self.account_gender,
        self.account_age, self.account_addr, self.account_profession, self.account_real_name,
        self.account_birthday, self.account_head_pic, self.account_reg_date, self.account_last_login,
        self.account_last_modify, self.source, self.deleted, self.repeated)


class Contact(Column):
    def __init__(self):
        super(Contact, self).__init__()
        self.contact_id = None
        self.owner_account_id = None
        self.contact_user = None
        self.contact_alias = None
        self.contact_group = None
        self.contact_remark = None
        self.contact_area = None
        self.contact_phone = None
        self.contact_email = None
        self.contact_gender = None
        self.contact_age = None
        self.contact_addr = None
        self.contact_profession = None
        self.contact_real_name = None
        self.contact_birthday = None
        self.contact_head_pic = None
        self.contact_reg_date = None
        self.contact_last_login = None
        self.contact_last_modify = None

    def get_values(self):
        return(self.contact_id, self.owner_account_id, self.contact_user, self.contact_alias, self.contact_group,
        self.contact_remark, self.contact_area, self.contact_phone, self.contact_email, self.contact_gender,
        self.contact_age, self.contact_addr, self.contact_profession, self.contact_real_name, self.contact_birthday,
        self.contact_head_pic, self.contact_reg_date, self.contact_last_login, self.contact_last_modify,
        self.source, self.deleted, self.repeated)


class Mail(Column):
    def __init__(self):
        super(Mail, self).__init__()
        self.mail_id = None
        self.owner_account_id = None
        self.mail_from = None
        self.mail_to = None
        self.mail_cc = None
        self.mail_bcc = None
        self.mail_sent_date = None
        self.mail_subject = None
        self.mail_abstract = None
        self.mail_content = None
        self.mail_read_status = None
        self.mail_group = None
        self.mail_save_dir = None
        self.mail_send_status = None
        self.mail_recall_status = None
        self.mail_ip = None
        self.mail_size = None

    def get_values(self):
        return(self.mail_id, self.owner_account_id, self.mail_from, self.mail_to, self.mail_cc, self.mail_bcc, self.mail_sent_date,
        self.mail_subject, self.mail_abstract, self.mail_content, self.mail_read_status, self.mail_group, self.mail_save_dir,
        self.mail_send_status, self.mail_recall_status, self.mail_ip, self.mail_size, self.source, self.deleted, self.repeated)


class Attachment(Column):
    def __init__(self):
        super(Attachment, self).__init__()
        self.attachment_id = None
        self.owner_account_id = None
        self.mail_id = None
        self.attachment_name = None
        self.attachment_save_dir = None
        self.attachment_size = None
        self.attachment_download_date = None

    def get_values(self):
        return(self.attachment_id, self.owner_account_id, self.mail_id, self.attachment_name, self.attachment_save_dir, self.attachment_size,
        self.attachment_download_date, self.source, self.deleted, self.repeated)


class Search(Column):
    def __init__(self):
        super(Search, self).__init__()
        self.search_id = None
        self.owner_account_id = None
        self.search_date = None
        self.search_key = None

    def get_values(self):
        return(self.search_id, self.owner_account_id, self.search_date, self.search_key,
            self.source, self.deleted, self.repeated)


class Generate(object):
    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_account_model())
        models.extend(self._get_contact_model())
        models.extend(self._get_mail_model())
        self.db_cmd.Dispose()
        self.db.Close()
        return models

    def _get_account_model(self):
        model = []
        sql = '''select distinct * from account'''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                user = Common.User()
                if not IsDBNull(sr[2]):
                    user.Name.Value = sr[2]  #账户昵称
                if not IsDBNull(sr[6]):
                    user.Email.Value = sr[6]  #账户邮箱
                if not IsDBNull(sr[1]):
                    user.Username.Value = sr[1]  #账户用户名
                if not IsDBNull(sr[15]):
                    user.LastLoginTime.Value = self._get_timestamp(sr[15])  #最近登录时间
                if not IsDBNull(sr[0]):
                    user.ID.Value = str(sr[0])  #账户id
                if not IsDBNull(sr[17]):
                    user.SourceFile.Value = self._get_source_file(sr[17])  #提取源
                if not IsDBNull(sr[18]):
                    user.Deleted = self._convert_deleted_status(sr[18])  #删除状态
                model.append(user)
            sr.Close()
            return model
        except Exception:
            traceback.print_exc()

    def _get_contact_model(self):
        model = []
        sql = '''select distinct * from contact'''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                friend = Common.Friend()
                if not IsDBNull(sr[1]):
                    friend.OwnerUserID.Value = str(sr[1])  #所属账户id
                if not IsDBNull(sr[2]):
                    friend.FullName.Value = sr[2]  #联系人名
                if not IsDBNull(sr[12]) is not None:
                    friend.CompanyName.Value = sr[12]  #联系人单位
                addr = Contacts.StreetAddress()  #联系人居住地
                if not IsDBNull(sr[11]):
                    addr.FullName.Value = sr[11]
                friend.LivingAddresses.Add(addr)
                if not IsDBNull(sr[5]):
                    friend.Remarks.Value = sr[5]  #联系人备注
                if not IsDBNull(sr[7]):
                    friend.PhoneNumber.Value = sr[7]  #联系人电话
                if not IsDBNull(sr[8]):
                    friend.Email.Value = sr[8]  #联系人邮箱
                if not IsDBNull(sr[19]):
                    friend.SourceFile.Value = self._get_source_file(sr[19])  #提取源
                if not IsDBNull(sr[20]):
                    friend.Deleted = self._convert_deleted_status(sr[20])  #删除状态
                model.append(friend)
            sr.Close()
            return model
        except Exception:
            traceback.print_exc()

    def _get_mail_model(self):
        model = []
        sql = '''select a.mail_group, a.mail_send_status, a.mail_subject, a.mail_content, a.mail_sent_date, 
            a.mail_from, a.mail_ip, a.mail_to, a.mail_cc, a.mail_bcc, b.attachment_name, b.attachment_save_dir, b.attachment_download_date, b.attachment_size,
            a.mail_abstract, a.mail_size, a.mail_recall_status, c.account_alias, c.account_user, c.account_last_login, c.account_email, c.account_id, a.mail_read_status, a.source, a.deleted
            from mail as a left join attachment as b on a.mail_id = b.mail_id left join account as c on a.owner_account_id = c.account_id
            '''
        try:
            self.db_cmd.CommandText = sql
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                email = Generic.Email()
                if not IsDBNull(sr[0]):
                    email.Folder.Value = sr[0]  #邮件分类
                if not IsDBNull(sr[1]):
                    if sr[1] is SEND_STATUS_SENT:#发送状态（已发送（发件箱）、未发送（草稿箱）、已读、未读）
                        email.Status.Value = MessageStatus.Sent
                    elif sr[1] is SEND_STATUS_UNSENT:
                        email.Status.Value = MessageStatus.Unsent
                    else:
                        if not IsDBNull(sr[22]):
                            email.Status.Value = MessageStatus.Unread if sr[22] == 0 else MessageStatus.Read
                if not IsDBNull(sr[2]):
                    email.Subject.Value = sr[2]  #标题
                if not IsDBNull(sr[3]):
                    email.Body.Value = sr[3]  #正文
                if not IsDBNull(sr[4]):
                    email.TimeStamp.Value = self._get_timestamp(sr[4])  #发件时间
                party = Generic.Party()  #发件人
                if not IsDBNull(sr[5]):
                    party.Identifier.Value = sr[5]  #发件人邮箱
                if not IsDBNull(sr[6]):
                    party.IPAddresses.Add(sr[6])  #发件人ip
                if not IsDBNull(sr[4]):
                    party.DatePlayed.Value = self._get_timestamp(sr[4])  #发送时间
                email.From.Value = party
                if not IsDBNull(sr[7]):
                    tos = sr[7].split(' ')
                    for t in range(len(tos)-1):
                        if t%2 == 0:
                            party = Party()  #收件人，有多个值
                            party.Identifier.Value = tos[t]  #收件人邮箱
                            party.Name.Value = tos[t+1]  #收件人名
                            party.DatePlayed.Value = self._get_timestamp(sr[4])  #收件时间
                            email.To.Add(party)
                if not IsDBNull(sr[8]):
                    cc = sr[8].split(' ')
                    for c in range(len(cc)-1):
                        if c%2 == 0:
                            party = Party()  #抄送者，有多个值
                            party.Identifier.Value = sr[8]  #抄送邮箱
                            party.Name.Value = sr[8]  #抄送者名
                            party.DatePlayed.Value = self._get_timestamp(sr[4])  #抄送时间
                            email.Cc.Add(party)
                if not IsDBNull(sr[9]):
                    bcc = sr[9].split(' ')
                    for b in range(len(bcc)-1):
                        if b%2 == 0:
                            party = Party()  #密送者，有多个值
                            party.Identifier.Value = sr[9]  #密送邮箱
                            party.Name.Value = sr[9]  #密送者名
                            party.DatePlayed.Value = self._get_timestamp(sr[4])  #密送时间
                            email.Bcc.Add(party)
                attachment = Generic.Attachment()  #附件
                if not IsDBNull(sr[10]):
                    attachment.Filename.Value = sr[10]  #附件名
                if not IsDBNull(sr[11]):
                    attachment.URL.Value = sr[11]  #附件保存路径
                if not IsDBNull(sr[12]):
                    attachment.DownloadTime.Value = self._get_timestamp(sr[12])  #附件下载时间
                if not IsDBNull(sr[13]):
                    attachment.Size.Value = sr[13]  #附件大小
                email.Attachments.Add(attachment)
                if not IsDBNull(sr[14]):
                    email.Abstract.Value = sr[14]  #摘要
                if not IsDBNull(sr[15]):
                    email.Size.Value = sr[15]  #邮件大小
                if not IsDBNull(sr[16]):
                    email.IsRecall.Value = sr[16]  #撤回状态
                user = Common.User()
                if not IsDBNull(sr[17]):
                    user.Name.Value = sr[17]  #账户昵称
                if not IsDBNull(sr[18]):
                    user.Username.Value = sr[18]  #账户
                if not IsDBNull(sr[19]):
                    user.LastLoginTime.Value = self._get_timestamp(sr[19])  #最后登录时间
                if not IsDBNull(sr[20]):
                    user.Email.Value = sr[20]  #账户邮箱
                if not IsDBNull(sr[21]):
                    user.ID.Value = str(sr[21])  #账户id
                    email.OwnerUserID.Value = str(sr[21])  #所属账户id
                email.OwnerUser.Value = user
                if not IsDBNull(sr[18]):
                    email.Account.Value = sr[18]  #所属账户
                if not IsDBNull(sr[23]):
                    email.SourceFile.Value = self._get_source_file(sr[23])  #提取源
                if not IsDBNull(sr[24]):
                    email.Deleted = self._convert_deleted_status(sr[24])  #删除状态
                model.append(email)
            sr.Close()
            return model
        except Exception:
            traceback.print_exc()

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    @staticmethod
    def _get_source_file(source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (long, float, str, Int64)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, (int, Int64)) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)