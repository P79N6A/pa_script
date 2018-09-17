# coding=utf-8
import os
import traceback

import PA_runtime
from PA_runtime import * 
import clr
try:
    clr.AddReference('model_mails')
except:
    pass
del clr
from model_mails import *


# 邮件内容类型
CONTENT_TYPE_HTML = 1  # HTML 格式
CONTENT_TYPE_TEXT = 2  # 纯文本
# 邮件类型
MAIL_OUTBOX   = '3'    # 已发送
MAIL_DRAFTBOX = '2'    # 草稿箱

SOURCE_APP = '网易邮箱'

def execute(node, extract_deleted):
    """ 
    """
    return analyze_neteasemail(node, extract_deleted, extract_source=False)

def analyze_neteasemail(node, extract_deleted, extract_source):
    """
        ios 网易邮箱大师 (Documents/imail.db)
    """
    pr = ParserResults()
    res = NeteaseMailParser(node, extract_deleted, extract_source).parse()
    pr.Models.AddRange(res)
    return pr

def exc():
    """ handle exception """
    # traceback.print_exc()
    pass

class NeteaseMailParser(object):
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node 
        self.extract_deleted = extract_deleted
        self.extract_source  = extract_source

        self.mm = MM()        
        self.cachepath = ds.OpenCachePath("NeteaseMasterMail")
        self.cachedb   = self.cachepath + "\\NeteaseMasterMail.db"
        self.mm.db_create(self.cachedb) 
        self.mm.db_create_table()
        
        self.accounts = {}
        self.mail_folder = {}

    def parse(self):
        self.parse_email(node=self.root.GetByPath("Documents/imail.db"))
        self.parse_contacts(node=self.root.GetByPath("Documents/contacts.db"))
        self.parse_todo(node=self.root.GetByPath("Documents/todo.db"))
        self.mm.db_close()
        generate = Generate(self.cachedb)
        return generate.get_models()

        
    def parse_email(self, node):
        """ 
            邮件内容 
        """
        imail_db = SQLiteParser.Database.FromNode(node)
        if imail_db is None:
            return
        self.source_imail_db = node.AbsolutePath

        self.parse_email_account(imail_db)
        self.parse_email_mailbox(imail_db)
        self.parse_email_attachment(imail_db)
        self.parse_email_abstract(node, imail_db)
        self.parse_email_password(imail_db)

    def parse_email_abstract(self, node, imail_db):
        """ imail - mailAttachment 邮件主要内容 """

        mailsNode = self.root.GetByPath("Documents/imail.db")
        if mailsNode is None:
            return
        mailsPath = mailsNode.PathWithMountPoint
        self.db = sqlite3.connect(mailsPath)

        SELECT_ATTACH_SQL = '''
            select attachName, attachDir, downloadSize, downloadUtc from attach where mailId=?
        '''
        SELECT_mailAbstract_JOIN_mailContent = '''
            select 
                a.localId        as mailId, 
                b.value          as content,
                a.mailBoxId      as mailBoxId, 
                a.subject        as subject, 
                a.summary        as abstract, 
                a.accountRawId   as accountId, 
                a.mailFrom       as fromEmail, 
                a.mailTos        as tos, 
                a.ccs            as cc,
                a.bccs           as bcc, 
                a.size           as size,
                a.forwarded      as isForward,
                a.unread         as isRead,
                a.deleted        as deleted,
                a.hasAttachments as hasAttachments,
                a.sentDate       as receiveUtc
            from 
                mailAbstract     as a
            left join 
                (select 
                    mailId, 
                    value 
                from  mailContent 
                where type=1)    as b
            on 
                a.localId = b.mailId
        '''
        cursor = self.db.cursor()
        cursor.execute(SELECT_mailAbstract_JOIN_mailContent)

        for row in cursor:
            mail = Mails()
            mail.mailId      = row[0]
            if mail.mailId == 21:
                pass
            mail.content     = row[1]
            mail.mail_folder = self.mail_folder.get(row[2], None) # row[2] => mailBoxId
            mail.subject     = row[3]
            mail.abstract    = row[4]
            mail.accountId   = row[5]
            mail.fromEmail   = self._convert_email_format(row[6])
            mail.tos         = self._convert_email_format(row[7])
            mail.cc          = self._convert_email_format(row[8])
            mail.bcc         = self._convert_email_format(row[9])
            mail.size        = row[10]
            mail.isForward   = row[11]
            mail.isRead      = row[12] ^ 1
            if row[2] == MAIL_OUTBOX:     # 发件箱
                mail.sendStatus = 1
            elif row[2] == MAIL_DRAFTBOX: # 草稿箱
                mail.sendStatus = 0
            mail.receiveUtc   = row[15]

            mail.deleted = row[13]
            mail.source  = self.source_imail_db
            if row[14] == 1: # hasAttachments
                self.mm.cursor.execute(SELECT_ATTACH_SQL, (mail.mailId,))
                rows = self.mm.cursor.fetchall() # list, None 以 '' 代替
                if rows:
                    mail.attachName   = ','.join(map(lambda x: x[0] if x[0] is not None else '', rows))
                    mail.attachDir    = ','.join(map(lambda x: x[1] if x[1] is not None else '', rows))
                    mail.downloadSize = ','.join(map(lambda x: x[2] if x[2] is not None else '', rows))
                    mail.downloadUtc  = ','.join(map(lambda x: x[3] if x[3] is not None else '0', rows))
            mail.account_email = self.accounts[mail.accountId]['email']
            mail.alias         = self.accounts[mail.accountId]['alias']
            try:
                self.mm.db_insert_table_mails(mail)
            except:
                exc()        
        try:                
            self.mm.db_commit()
        except:
            exc()

    def parse_email_account(self, imail_db):
        accounts = {}
        for rec in self.my_read_table(db=imail_db, table_name='account'):
            if IsDBNull(rec['email'].Value) or len(rec['email'].Value.strip()) < 5 or (type(rec['type'].Value) != int):
                continue
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, rec['email'].Value)
            if match_obj is None:
                continue

            account = Accounts()
            account.accountId    = rec['accountId'].Value
            account.accountEmail = rec['email'].Value
            account.alias        = rec['email'].Value
            account.deleted      = rec['deleted'].Value
            account.source       = self.source_imail_db
            if rec['type'].Value == 1 and rec['deleted'].Value == 0:
                accounts[account.accountId] = {
                    'email': account.accountEmail,
                    'alias': account.alias,
                }
            try:
                self.mm.db_insert_table_account(account)
            except:
                exc()
        self.accounts = accounts
        self.mm.db_commit()        


    def parse_email_mailbox(self, imail_db):
        """ imail - mailBox """
        for rec in self.my_read_table(db=imail_db, table_name='mailBox'):
            folder = MailFolder()
            self.mail_folder[rec['mailBoxId'].Value] = rec['name'].Value
            folder.folderTtpe   = rec['mailBoxId'].Value
            folder.folderName   = rec['name'].Value
            folder.accountEmail = self.accounts[rec['accountRawId'].Value]['email']
            folder.source       =  self.source_imail_db
            try:
                self.mm.db_insert_table_mail_folder(folder)
            except:
                exc()
        self.mm.db_commit()
        
    def parse_email_attachment(self, imail_db):
        """ imail - mailAttachment """
        for rec in self.my_read_table(db=imail_db, table_name='mailAttachment'):
            if IsDBNull(rec['name'].Value) or IsDBNull(rec['mailId'].Value):
                continue
            attach = Attach()
            attach.attachName   = rec['name'].Value
            attach.attachDir    = rec['localPath'].Value
            attach.attachType   = rec['contentType'].Value
            attach.downloadUtc  = rec['createdate'].Value
            attach.downloadSize = rec['size'].Value
            attach.mailId       = rec['mailId'].Value
            attach.source       = self.source_imail_db
            try:
                self.mm.db_insert_table_attach(attach)
            except:
                exc()
        try:
            self.mm.db_commit()
        except:
            exc()
            
    def parse_contacts(self, node):
        """ 
            联系人 
        """
        try:
            todo_db = SQLiteParser.Database.FromNode(node)
            if todo_db is None:
                return
            for rec in self.my_read_table(db=todo_db, table_name='recentcontact'):
                contact = Contact()
                contact.contactName  = rec['name'].Value
                contact.accountId    = rec['aid'].Value
                contact.contactEmail = rec['email'].Value
                contact.source       = node.AbsolutePath
                try:
                    self.mm.db_insert_table_contact(contact)
                except:
                    exc()
            self.mm.db_commit()
        except:
            exc()

    def parse_email_password(self, imail_db):
        """ 
            mail - password 
        """
        SQL_UPDATE_ACCOUNTS_password = '''
               update accounts set password=? where accountEmail=?
        ''' 
        password_accountEmail = []
        for accountId in self.accounts:
            table_name = 'accountConfig_{}'.format(accountId)
            for rec in self.my_read_table(db=imail_db, table_name=table_name):
                if rec['DBkey'].Value == 'wmsrexc()':
                    password_accountEmail.append((rec['DBvalue'].Value, self.accounts[accountId]['email']))
                    break
        try:
            self.mm.cursor.executemany(SQL_UPDATE_ACCOUNTS_password, password_accountEmail)
            self.mm.db_commit()
        except:
            exc()

    def parse_todo(self, node):
        """ 
            待办事项 
        """
        try:
            todo_db = SQLiteParser.Database.FromNode(node)
            if todo_db is None:
                return
            for rec in self.my_read_table(db=todo_db, table_name='todoList'):
                t = Todo()
                t.content      = rec['content'].Value
                t.createdTime  = rec['createdTime'].Value
                t.reminderTime = rec['reminderTime'].Value
                t.isdone       = rec['done'].Value
                t.deleted      = rec['deleted'].Value
                t.source       = node.AbsolutePath

                try:
                    self.mm.db_insert_table_todo(t)
                except:
                    exc()
            self.mm.db_commit()
        except:
            exc()

    def my_read_table(self, table_name, db_path=None, db=None):
        """ 
            读取手机数据库, 参数 db_path, db 二选一
        :type table_name: str
        :type db_path: str
        :type db: SQLiteParser.Database.FromNode(node)
        :rtype: db.ReadTableRecords()
        """
        if db is None:
            try:
                node = self.root.GetByPath(db_path)
                db = SQLiteParser.Database.FromNode(node)
                if db is None:
                    return 
            except:
                exc()
        tb = SQLiteParser.TableSignature(table_name)  
        return db.ReadTableRecords(tb, self.extract_deleted, True)

    @staticmethod
    def _convert_email_format(name_email):
        """
            转换邮件格式
        :type name_email: str
        :rtype: str
        from: [{"name":"pangu_x01","email":"pangu_x01@163.com"}]
        to:   pangu_x01@163.com pangu_x01
        """
        if not name_email:
            return None
        res = None
        try:
            res = ''
            name_email = eval(name_email)
            if isinstance(name_email, list):
                for i in name_email:
                    name   = i['name']
                    email  = i['email']
                    res   += ' ' + email.strip() + ' ' + name.strip()
            elif isinstance(name_email, dict):
                name   = name_email['name']
                email  = name_email['email']
                res   += ' ' + email.strip() + ' ' + name.strip()
        except:
            exc()
        return res.lstrip()
