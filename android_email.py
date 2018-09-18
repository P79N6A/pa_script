# coding=utf-8
import os
import traceback
import re

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_mails')
except:
    pass
del clr
from model_mails import *

MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT  = 1
MESSAGE_STATUS_SENT    = 2
MESSAGE_STATUS_UNREAD  = 3
MESSAGE_STATUS_READ    = 4

def execute(node, extract_deleted):
    """ main """
    return analyze_email(node, extract_deleted, extract_source=False)

def exc():
    pass
    # traceback.print_exc()

def analyze_email(node, extract_deleted, extract_source):
    """ android 邮件 华为 """
    pr = ParserResults()
    res = EmailParser(node, extract_deleted, extract_source).parse()
    pr.Build('系统邮箱')
    pr.Models.AddRange(res)
    return pr


class EmailParser(object):
    def __init__(self, node, extract_deleted, extract_source):

        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mm = MM()
        self.cachepath = ds.OpenCachePath("AndroidEmail")
        self.cachedb = self.cachepath + "\\AndroidEmail.db"
        self.mm.db_create(self.cachedb)
        self.mm.db_create_table()

        self.accounts = {}
        self.mail_folder = {}

    def parse(self):
        """ android 邮件        xiaomi
        
            (\data\com.android.email\databases\EmailProvider.db)
            或者(\data\com.google.android.gm\databases)
                1. Account          # 本人用户
                2. Attachment       # 附件
                3. Credential
                4. HostAuth
                5. Mailbox
                6. Message          # main
                7. Message_Deletes
                8. Message_Updates
                9. MessageMove
                10. MessageStateChange
                11. Policy
                12. QuickResponse                  
            (\data\com.android.email\databases\EmailProviderBody.db) 
                - Body
            (\data\com.android.email\databases\Contact.db)      华为没有     
                - contact_table 
                - nick_table
            (\data\com.android.email\databases\EmailProvider.db)
        """        
        try:
             self.parse_email_body(node=self.root.GetByPath("/EmailProviderBody.db"))
             self.parse_email(node=self.root.GetByPath("/EmailProvider.db"))
             self.parse_contact(node=self.root.GetByPath("/EmailProviderBody.db"))
        except:
            exc()
        finally:
            self.mm.db_close()

        models = Generate(self.cachedb).get_models()
        return models

    def parse_email(self, node):
        """ (/EmailProvider.db)
            1. Account          # 本人用户
            2. Attachment       # 附件
            # 3. Credential
            # 4. HostAuth
            5. Mailbox
            6. Message          # main
            7. Message_Deletes
            8. Message_Updates
            9. MessageMove
            10. MessageStateChange
            11. Policy
            12. QuickResponse  
        """
        self.db_email_provider = SQLiteParser.Database.FromNode(node)
        self.source_emailprovider = node.AbsolutePath
        
        if self.db_email_provider is None:
            return 
        self.parse_email_tb_account()
        self.parse_email_tb_attachment()
        self.parse_email_tb_mailbox()
        self.parse_email_tb_message()
        
    def parse_email_tb_account(self):
        """ EmailProvider - Account """
        for rec in self.my_read_table(self.db_email_provider, 'Account'):
            if canceller.IsCancellationRequested:
                return
            if IsDBNull(rec['emailAddress'].Value) or not self._is_email_format(rec['emailAddress'].Value):
                continue
            account = Accounts()
            account.accountId    = rec['_id'].Value
            account.alias        = rec['displayName'].Value
            account.accountEmail = rec['emailAddress'].Value
            account.source       = self.source_emailprovider
            self.accounts[account.accountId] = {
                'email': account.accountEmail,
                'name' : account.alias,
            }
            try:
                self.mm.db_insert_table_account(account)
            except:
                exc()
        self.mm.db_commit()

    def parse_email_tb_attachment(self):
        """ EmailProvider - Attachment
                fileName         TEXT,
                mimeType         TEXT,
                size             INTEGER,
                contentId        TEXT,
                contentUri       TEXT,
                messageKey       INTEGER,
                location         TEXT,
                encoding         TEXT,
                content          TEXT,
                flags            INTEGER,
                content_bytes    BLOB,
                accountKey       INTEGER,
                uiState          INTEGER,
                uiDestination    INTEGER,
                uiDownloadedSize INTEGER,
                cachedFile       TEXT        
        """
        for rec in self.my_read_table(self.db_email_provider, 'Attachment'):
            if canceller.IsCancellationRequested:
                return
            if IsDBNull(rec['fileName'].Value) or IsDBNull(rec['size'].Value):
                continue
            attach = Attach()
            attach.attachName   = rec['fileName'].Value
            attach.attachDir    = rec['contentUri'].Value  # or location?
            attach.attachType   = rec['mimeType'].Value
            attach.downloadUtc  = rec['previewTime'].Value
            attach.downloadSize = rec['size'].Value
            attach.mailId       = rec['messageKey'].Value
            attach.deleted      = rec['isDeleted'].Value
            attach.source       = self.source_emailprovider
            try:
                self.mm.db_insert_table_attach(attach)
            except:
                exc()
        self.mm.db_commit()

    def parse_email_tb_mailbox(self):
        """ EmailProvider - Mailbox """
        REPLACE_NAME = {
            'INBOX': '收件箱',
            'Outbox': '发件箱',
        }
        for rec in self.my_read_table(self.db_email_provider, 'Mailbox', extract_deleted=False):
            folder = MailFolder()
            if IsDBNull(rec['displayName'].Value) or IsDBNull(rec['accountKey'].Value):
                continue
            folderName = rec['displayName'].Value.replace('\0','')
            folder.folderName   = folderName if folderName not in REPLACE_NAME else REPLACE_NAME[folderName]
            self.mail_folder[rec['_id'].Value] = folder.folderName
            try:
                folder.accountEmail = self.accounts[rec['accountKey'].Value]['email']
                va = rec['accountKey'].Value
            except:
                exc()
            folder.accountNick  = self.accounts[rec['accountKey'].Value]['name']
            folder.source       = self.source_emailprovider
        
            try: 
                self.mm.db_insert_table_mail_folder(folder)
            except:
                exc()
        self.mm.db_commit()

    def parse_email_tb_message(self):
        """ EmailProvider - Message """
        
        SELECT_ATTACH_SQL = '''
            select attachName, attachDir, downloadSize from attach where mailId=?
        '''
        for rec in self.my_read_table(self.db_email_provider, 'Message'):
            if canceller.IsCancellationRequested:
                return
            mail = Mails()
            if IsDBNull(rec['subject'].Value) or IsDBNull(rec['accountKey'].Value):
                continue
            mail.mailId      = rec['_id'].Value
            mail.mail_folder = self.mail_folder.get(rec['mailboxKey'].Value, None)
            mail.subject     = rec['subject'].Value
            mail.abstract    = rec['snippet'].Value
            mail.accountId   = rec['accountKey'].Value
            mail.fromEmail   = rec['fromList'].Value        # TODO 类型 提取 邮箱地址
            mail.tos         = rec['toList'].Value
            mail.cc          = rec['ccList'].Value
            mail.bcc         = rec['bccList'].Value
            # mail.isForward   = rec['isForwarded'].Value
            mail.isRead      = rec['flagRead'].Value
            mail.receiveUtc  = rec['timeStamp'].Value
            try:
                mail.size    = rec['messageSize'].Value    # 此字段华为没有
            except:
                pass
            # mail.content     = rec['htmlContent'].Value
            mail.source      = self.source_emailprovider

            if self.mail_folder.get(rec['mailboxKey'].Value, None) == '已发送':
                mail.sendStatus = 1
            elif self.mail_folder.get(rec['mailboxKey'].Value, None) == '草稿箱':
                mail.sendStatus = 0

            # 附件
            if rec['flagAttachment'].Value == 1:
                self.mm.cursor.execute(SELECT_ATTACH_SQL, (mail.mailId,))
                rows = self.mm.cursor.fetchall() # list, None 值以 '' 代替
                mail.attachName   = ','.join(map(lambda x: x[0] if x[0] is not None else '', rows))
                mail.attachDir    = ','.join(map(lambda x: x[1] if x[1] is not None else '', rows))
                mail.downloadSize = ','.join(map(lambda x: x[2] if x[2] is not None else '', rows))

            try:
                self.mm.db_insert_table_mails(mail)
            except:
                exc()
        self.mm.db_commit()

    def parse_email_body(self, node):
        """
        /EmailProviderBody.db
        """
        self.db_email_provider_body = SQLiteParser.Database.FromNode(node)

        SQL_UPDATE_EMAIL_CONTENT = '''
            update mails set content=? where mailId=?
        '''
        def handle_html(h):
            # return h
            # return h if h.count('"') % 2 == 0 else h+'"'
            return h.replace('\0','')      

        try:
            for rec in self.my_read_table(self.db_email_provider_body, 'Body'):
                if canceller.IsCancellationRequested:
                    return
                if IsDBNull(rec['htmlContent'].Value) or IsDBNull(rec['messageKey'].Value):
                    continue
                mailId = rec['messageKey'].Value
                mailContent = rec['htmlContent'].Value
                try:
                    self.mm.cursor.execute(SQL_UPDATE_EMAIL_CONTENT, (mailContent, mailId))
                except:
                    exc()
            self.mm.db_commit()
        except:
            exc()

    def parse_contact(self, node):
        """
        华为没有此 db
            /Contact.db
        """
        try:        
            self.db_contact = SQLiteParser.Database.FromNode(node)
            self.source_contact = node.AbsolutePath

            SQL_UPDATE_EMAIL_CONTENT = '''
                update mails set content=? where mailId=?
            '''
            for rec in self.my_read_table(self.db_contact, 'contact_table'):
                if IsDBNull(rec['name'].Value) or IsDBNull(rec['email'].Value):
                    continue
                contact = Contact()
                contact.contactName  = rec['name'].Value
                contact.contactEmail = rec['email'].Value
                contact.source       = node.AbsolutePath
                try:
                    self.mm.db_insert_table_contact(contact)
                except:
                    exc()
            self.mm.db_commit()
        except:
            pass
            # exc()
            
    def my_read_table(self, db, table_name, extract_deleted=None):
        """ 
        读取手机数据库, 多数据库模式
        :type db: SQLiteParser.Database.FromNode(node)tr
        :type table_name: str 
        :rtype: db.ReadTableRecords()
        """
        if extract_deleted == None:
            extract_deleted = self.extract_deleted
        if db is None:
            return
        tb = SQLiteParser.TableSignature(table_name)
        return db.ReadTableRecords(tb, extract_deleted, True)

    @staticmethod
    def _timestamp_long2int(_long):
        """ long 13 位 => int 10 位 """
        if type(_long)==long:
            return int(_long * 0.001)
        return _long

    @staticmethod
    def _convert_email(eml):
        """
        pangu_x01 <pangu_x01@163.com>, pangu_x02 <pangu_x02@163.com>
        pangu_x01@163.com pangu_x01 pangu_x02@163.com pangu_x02
        """
        if not eml:
            return None
        try:
            res = ''
            for i in eml.split(', '):
                name  = i.split(' ')[0]
                email = i.split(' ')[1][1:-1]
                res += ' ' + email + ' ' + name
            return res.lstrip()
        except:
            return None

    @staticmethod
    def _is_email_format(rec):
        """ 匹配邮箱地址 """
        try:
            if IsDBNull(rec) or len(rec.strip()) < 5:
                return False
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, rec)
            if match_obj is None:
                return False      
            return True      
        except:
            return False

        