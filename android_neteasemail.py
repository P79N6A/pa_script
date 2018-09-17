# coding=utf-8
import os
import traceback
import re

import PA_runtime
from PA_runtime import *
SafeLoadAssembly('model_mails')
from model_mails import *

# 邮件内容类型
CONTENT_TYPE_HTML = 1  # HTML 格式
CONTENT_TYPE_TEXT = 2  # 纯文本
# 邮件类型
MAIL_OUTBOX   = '3'      # 已发送
MAIL_DRAFTBOX = '2'      # 草稿箱


def exc():
    """ handle exception """
    # traceback.print_exc()
    pass        

def execute(node, extract_deleted):
    """ main """
    return analyze_neteasemail(node, extract_deleted, extract_source=False)

def analyze_neteasemail(node, extract_deleted, extract_source):
    """
        android 网易邮箱大师 (databases/mmail)
    """
    pr = ParserResults()
    res = NeteaseMailParser(node, extract_deleted, extract_source).parse()
    pr.Models.AddRange(res)

    return pr


class NeteaseMailParser(object):
    """
        网易邮箱大师    
    """

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mm = MM()
        self.cachepath = ds.OpenCachePath("NeteaseMasterMail")
        self.cachedb = self.cachepath + "\\NeteaseMasterMail.db"
        self.mm.db_create(self.cachedb)
        self.mm.db_create_table()

        self.accounts    = {}
        self.mail_folder = {}
        self.attach      = {}

    def parse(self):

        node = self.root.GetByPath("mmail")
        imail_db = SQLiteParser.Database.FromNode(node)
        if imail_db is None:
            return

        self.source_mmail_db = node.AbsolutePath
        self.db = imail_db
        self.parse_main()
        self.mm.db_close()

        models = Generate(self.cachedb).get_models()
        return models

    def parse_main(self):
        # 预处理
        self.parse_account()
        self.parse_mailbox()

        # 表名:   table_accountId
        for account_id in self.accounts:
            self.cur_account_id = account_id
            self.parse_email_attachment()
            self.parse_email()
            self.parse_contacts()

    def parse_account(self):
        """ account_id account_email """

        for rec in self.my_read_table(table_name='AccountCore'):
            if IsDBNull(rec['mailAddress'].Value) or len(rec['mailAddress'].Value.strip()) < 5 or not (type(rec['protocolType'].Value) == int):
                continue
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, rec['mailAddress'].Value)
            if match_obj is None:
                continue
            account = Accounts()
            # accountID 类型 INT
            account.accountId    = rec['id'].Value 
            account.accountEmail = rec['mailAddress'].Value 
            account.deleted      = rec['isDeleted'].Value 
            account.source       = self.source_mmail_db
            if rec['protocolType'].Value == 1 and rec['isDeleted'].Value == 0:
                self.accounts[account.accountId] = {
                    'email': account.accountEmail,
                }
            try:
                self.mm.db_insert_table_account(account)
            except:
                exc()
        self.mm.db_commit()            

    def parse_mailbox(self):
        """ 邮件类型预处理 mailbox """
        for rec in self.my_read_table(table_name='Mailbox'):
            if IsDBNull(rec['serverId'].Value): # str
                continue
            self.mail_folder[rec['serverId'].Value] = rec['displayName'].Value
            folder = MailFolder()
            folder.folderTtpe   = rec['serverId'].Value
            folder.folderName   = rec['displayName'].Value
            try:
                folder.accountEmail = self.accounts.get(rec['accountId'].Value, {}).get('email', None)
            except:
                exc()
            folder.source       = self.source_mmail_db                
            try:
                self.mm.db_insert_table_mail_folder(folder)
            except:
                exc()
        self.mm.db_commit()

    def parse_email_attachment(self):
        """ 
            mail - attachment 
        """
        table_name = 'Part_' + str(self.cur_account_id)
        for rec in self.my_read_table(table_name=table_name):
            if IsDBNull(rec['name'].Value) or IsDBNull(rec['messageId'].Value):
                continue
            attach = Attach()
            attach.accountEmail = self.accounts[self.cur_account_id]['email']
            attach.attachName   = rec['filename'].Value
            attach.attachDir    = rec['localPath'].Value 
            attach.attachType   = rec['contentType'].Value
            attach.downloadUtc  = rec['downloadTime'].Value
            attach.downloadSize = rec['size'].Value
            attach.mailId       = rec['messageId'].Value
            attach.source       = self.source_mmail_db
            try:
                self.mm.db_insert_table_attach(attach)
            except:
                exc()
        try:
            self.mm.db_commit()
        except:
            exc()

    def parse_email(self):
        """" """
        SELECT_ATTACH_SQL = '''
            select attachName, attachDir, downloadSize, downloadUtc from attach where mailId=?
        '''
        table_name = 'Mail_' + str(self.cur_account_id)
        exist_mailId = []
        for rec in self.my_read_table(table_name):
            mail = Mails()
            if IsDBNull(rec['subject'].Value) or rec['localId'].Value in exist_mailId:
                continue
            exist_mailId.append(rec['localId'].Value)
            mail.mailId      = rec['localId'].Value 
            mail.mail_folder = self.mail_folder.get(rec['mailboxKey'].Value, None)
            mail.subject     = rec['subject'].Value
            mail.abstract    = rec['summary'].Value 
            mail.accountId   = self.cur_account_id
            mail.fromEmail   = self._convert_email_format(rec['mailFrom'].Value) 
            mail.tos         = self._convert_email_format(rec['mailTo'].Value) 
            mail.cc          = self._convert_email_format(rec['mailCC'].Value) 
            mail.bcc         = rec['mailBCC'].Value 
            mail.isForward   = rec['isForwarded'].Value 
            mail.isRead      = rec['isRead'].Value 
            mail.isDeleted   = rec['isDeleted'].Value 
            mail.receiveUtc  = rec['recvDate'].Value
            mail.size        = rec['mailSize'].Value 
            mail.content     = rec['htmlContent'].Value 
            mail.source      = self.source_mmail_db
            if rec['mailboxKey'].Value == MAIL_OUTBOX:      # 已发送
                mail.sendStatus = 1
            elif rec['mailboxKey'].Value == MAIL_DRAFTBOX:  # 草稿箱
                mail.sendStatus = 0
            # 附件
            if rec['hasAttach'].Value == 1:
                self.mm.cursor.execute(SELECT_ATTACH_SQL, (mail.mailId,))
                rows = self.mm.cursor.fetchall() # list, None 值以 '' 代替
                mail.attachName   = ','.join(map(lambda x: x[0] if x[0] is not None else '', rows))
                mail.attachDir    = ','.join(map(lambda x: x[1] if x[1] is not None else '', rows))
                mail.downloadSize = ','.join(map(lambda x: x[2] if x[2] is not None else '', rows))
                mail.downloadUtc  = ','.join(map(lambda x: x[3] if x[3] is not None else '0', rows))
            try:
                self.mm.db_insert_table_mails(mail)
            except:
                exc()
        self.mm.db_commit()

    def parse_contacts(self):
        """ 
            联系人 
        """
        table_name = 'Contact_' + str(self.cur_account_id)
        for rec in self.my_read_table(table_name):
            contact = Contact()
            if IsDBNull(rec['name'].Value) or IsDBNull(rec['email'].Value):
                continue
            contact.contactName  = self._parse_contacts(rec['name'].Value)
            contact.contactEmail = self._parse_contacts(rec['email'].Value)
            contact.alias        = contact.contactName
            contact.deleted      = rec['isDeleted'].Value
            contact.source       = self.source_mmail_db

            try:
                self.mm.db_insert_table_contact(contact)
            except:
                exc()
        self.mm.db_commit()
  

    def my_read_table(self, table_name):
        """ 
            读取手机数据库
        :type table_name: str
        :rtype: db.ReadTableRecords()
        """
        if self.db is None:
            return
        tb = SQLiteParser.TableSignature(table_name)
        return self.db.ReadTableRecords(tb, self.extract_deleted, True)

    @staticmethod
    def _convert_email_format(name_email):
        """
            转换邮件格式
        :type name_email: str
        :rtype: str
        from: [{"name":"pangu_x01", "mailAddress":"pangu_x01@163.com"}]
        to:   pangu_x01@163.com pangu_x01
        """
        if not name_email:
            return None        
        try:
            res = ''
            try:
                name_email = eval(name_email.replace('false', 'False'))
            except:
                return None
            if isinstance(name_email, list):
                for i in name_email:
                    name  = i['name']
                    email = i['mailAddress']
                    res += ' ' + email.strip() + ' ' + name.strip()
            elif isinstance(name_email, dict):
                name = name_email['name']
                email = name_email['mailAddress']
                res += ' ' + email.strip() + ' ' + name.strip()
            return res.lstrip()
        except:
            exc()
            return None

    @staticmethod
    def _parse_contacts(name_email):
        """ 联系人表中 格式转换 """
        if not name_email or IsDBNull(name_email):
            return 
        try:
            res = None
            ne = eval(name_email)
            if isinstance(ne, list):
                res = ne[0]['value']
            elif isinstance(ne, dict):
                res = ne['value']
            return res          
        except:
            exc()
            return None

