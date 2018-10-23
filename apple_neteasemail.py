# coding=utf-8
import traceback
import hashlib
import re

import clr
try:
    clr.AddReference('model_mail')
    clr.AddReference('bcp_mail')
except:
    pass
del clr
from model_mail import *
import bcp_mail


DEBUG = True
DEBUG = False


# 邮件内容类型
CONTENT_TYPE_HTML = 1  # HTML 格式
CONTENT_TYPE_TEXT = 2  # 纯文本
# 邮件类型
MAIL_OUTBOX   = '3'    # 已发送
MAIL_DRAFTBOX = '2'    # 草稿箱

VERSION_APP_VALUE = 1


def execute(node, extract_deleted):
    return analyze_neteasemail(node, extract_deleted, extract_source=False)

def analyze_neteasemail(node, extract_deleted, extract_source):
    """
        ios 网易邮箱大师 (Documents/imail.db)
    """
    pr = ParserResults()
    res = NeteaseMailParser(node, extract_deleted, extract_source).parse()
    pr.Models.AddRange(res)
    pr.Build('网易邮箱大师')
    return pr

class NeteaseMailParser(object):

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node 
        self.extract_deleted = extract_deleted
        self.extract_source  = extract_source
        self.mm = MM()        
        self.cachepath = ds.OpenCachePath("NeteaseMasterMail")

        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()
        self.cache_db = self.cachepath + '\\{}.db'.format(hash_str)

        self.accounts = {}
        self.mail_folder = {}
        self.neg_primary_key = 1

    def parse(self):
        ''' account
            contact
            mail
            attachment
            search
            vsersion
        ''' 
        if DEBUG or self.mm.need_parse(self.cache_db, VERSION_APP_VALUE):
            if not self._read_db('Documents/imail.db'):
                return
            self.mm.db_create(self.cache_db) 
            
            self.pre_parse_mail_box("Documents/imail.db", 'mailBox')
            self.parse_account("Documents/imail.db", 'account')
            self.parse_contact("Documents/contacts.db", 'localcontact')
            self.parse_mail("Documents/imail.db", 'mailAbstract', 'mailContent')
            self.parse_attachment("Documents/imail.db", 'mailAttachment')

            if not canceller.IsCancellationRequested:
                self.mm.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.mm.db_insert_table_version(VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mm.db_commit()
                
            self.mm.db_close()

        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_mail.MAIL_TOOL_TYPE_OTHER, self.cache_db, tmp_dir)

        generate = Generate(self.cache_db)
        return generate.get_models()

    def pre_parse_mail_box(self, db_path, table_name):
        ''' imail.db - mailBox

            RecNo	FieldName	SQLType	Size	 
            1	mailBoxId	        TEXT
            2	accountRawId	    INTEGER
            3	accountRawType	    INTEGER
            4	name	            TEXT
            5	type	            INTEGER
            6	flag	            INTEGER
            7	selectable	            INTEGER
            8	uidValid	            INTEGER
            9	uidFractured        INTEGER
            10	remindType	            INTEGER
            11	syncKey	            TEXT
            12	parentId	            TEXT
            13	folderType	            INTEGER
            14	encrypted	            INTEGER
        '''       
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            self.mail_folder[rec['mailBoxId'].Value] = rec['name'].Value

    def parse_account(self, db_path, table_name):
        ''' imail.db - account

            RecNo  FieldName  SQLType	
            1	accountId	    INTEGER
            2	email	    TEXT
            3	type	    INTEGER
            4	deleted	    INTEGER
        '''       
        if not self._read_db(db_path):
            return 
        accounts = {}
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'accountId', 'email') or not self._is_email_format(rec['email'].Value):
                continue
            account = Account()
            account.account_id    = rec['accountId'].Value
            account.account_user  = rec['email'].Value
            account.account_email = rec['email'].Value
            account.account_alias = rec['email'].Value
            account.deleted       = 1 if rec.IsDeleted else rec['deleted'].Value
            account.source        = self.cur_db_source
            if rec['type'].Value == 1 and rec['deleted'].Value == 0:
                accounts[account.account_id] = {
                    'email': account.account_email,
                    'alias': account.account_alias,
                }
            try:
                self.mm.db_insert_table_account(account)
            except:
                exc()
        self.accounts = accounts
        self.mm.db_commit()  
    
    def parse_contact(self, db_path, table_name):
        ''' contacts.db - localcontact
            RecNo	FieldName	SQLType	 
            1	rowid	        INTEGER
            2	aid	            INTEGER
            3	name	        TEXT
            4	initial	            TEXT
            5	pinyin	            TEXT
            6	email	            TEXT
            7	date	            INTEGER
        '''       
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'aid', 'email') or not self._is_email_format(rec['email'].Value):
                continue
            contact = Contact()
            contact.contact_id       = rec['rowid'].Value
            contact.owner_account_id = rec['aid'].Value
            contact.contact_user     = rec['name'].Value
            contact.contact_email    = rec['email'].Value
            contact.contact_alias    = rec['name'].Value
            contact.contact_reg_date = rec['date'].Value
            contact.source           = self.cur_db_source
            contact.deleted          = 1 if rec.IsDeleted else 0
            try:
                self.mm.db_insert_table_contact(contact)
            except:
                exc()
        self.mm.db_commit()  
    
    def parse_mail(self, db_path, mailAbstract, mailContent):
        ''' imail.db - mailAbstract 

            RecNo	FieldName	SQLType	Size	
            1	localId	            INTEGER
            2	serverId	            TEXT
            3	accountRawId	INTEGER			            False		
            4	accountRawType	            INTEGER
            5	mailBoxId	            TEXT
            6	subject	            TEXT
            7	summary	            TEXT
            8	sentDate	            INTEGER
            9	recvDate	            INTEGER
            10	deleDate	            INTEGER
            11	mailFrom	            TEXT
            12	mailTos	            TEXT
            13	ccs	            TEXT
            14	bccs	            TEXT
            15	replyto	            TEXT
            16	size	            INTEGER
            17	hasAttachments	            INTEGER
            18	stared	            INTEGER
            19	unread	            INTEGER
            20	deleted	            INTEGER
            21	replied	            INTEGER
            22	forwarded	            INTEGER
            23	sender	            TEXT
            24	messageId	            TEXT
            25	conversationId	            TEXT
            26	referenceIds	            TEXT
            27	inReplyTo	            TEXT
            28	wmsvrReferenceInfo	            TEXT
            29	merchid	            TEXT
            30	surfaceUrl	            TEXT
            31	hasHugeAttachments	            INTEGER
            32	messageClass	            TEXT
            33	mailHash	            TEXT
            34	unsub	            TEXT
            35	needClean	            INTERGER
            36	xHeaders	            TEXT
            37	isSentLocalMail	            INTERGER
            38	scheduled	            INTEGER
        '''       
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(mailAbstract):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'subject', 'mailFrom', 'mailTos'):
                continue
            mail = Mail()
            mail.mail_id          = self._convert_primary_key(rec['localId'].Value)
            mail.owner_account_id = rec['accountRawId'].Value
            mail_from             = self._convert_email_format(rec['mailFrom'].Value)
            mail_to               = self._convert_email_format(rec['mailTos'].Value)
            try:
                if False in [self._is_email_format(i.split(' ')[0]) for i in (mail_from, mail_to)]:
                    continue
            except:
                exc()
            mail.mail_from, mail.mail_to = mail_from, mail_to
            mail.mail_cc          = self._convert_email_format(rec['ccs'].Value)
            mail.mail_bcc         = self._convert_email_format(rec['bccs'].Value)
            mail.mail_sent_date   = rec['sentDate'].Value
            mail.mail_subject     = rec['subject'].Value
            mail.mail_abstract    = rec['summary'].Value
            # mail_content
            mail.mail_read_status = rec['unread'].Value ^ 1
            mail.mail_group       = self.mail_folder.get(rec['mailBoxId'].Value, None)
            # mail.mail_save_dir
            if rec['mailBoxId'].Value == MAIL_OUTBOX:     # '3' 表示已发送
                mail.mail_send_status = 1
            elif rec['mailBoxId'].Value == MAIL_DRAFTBOX: # '2' 表示草稿箱
                mail.mail_send_status = 0
            # mail.mail_recall_status  = rec['accountRawId'].Value
            mail.deleted          = 1 if rec.IsDeleted else rec['deleted'].Value 
            mail.source  = self.cur_db_source
            try:
                self.mm.db_insert_table_mail(mail)
            except:
                exc()
        self.mm.db_commit()

        SQL_UPDATE_EMAIL_CONTENT = '''
            update mail set mail_content=? where mail_id=?
        '''
        try:
            for rec in self._read_table(mailContent):
                if rec['type'].Value == CONTENT_TYPE_TEXT: # 只提取 HTML 格式, 跳过纯文本类型
                    continue
                elif rec['type'].Value == CONTENT_TYPE_HTML:
                    mailId = rec['mailId'].Value
                    mailContent = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', rec['value'].Value) 
                    a = (mailContent, mailId)
                    try:
                        self.mm.db_update(SQL_UPDATE_EMAIL_CONTENT, a)
                    except:
                        exc()
            self.mm.db_commit()
        except:
            exc()        

    def parse_attachment(self, db_path, table_name):
        ''' imail.db - mailAttachment
        
                RecNo	FieldName	SQLType	Size
                1	attachmentId	            INTEGER
                2	mailId	            INTEGER	
                3	rawType	            INTEGER
                4	contentType	            TEXT
                5	external	            INTEGER
                6	partId	            TEXT
                7	transferEncoding	            TEXT
                8	contentId	            TEXT
                9	name	            TEXT
                10	dispositionName	            TEXT
                11	size	            INTEGER
                12	localPath	            TEXT
                13	wmsvrType	            INTEGER
                14	wmsvrOwnedInfo	            TEXT
                15	wmsvrReferenceInfo	            TEXT
                16	hugeAttachmentInfo	TEXT
                17	oid	                TEXT
                18	method	            INTEGER
                19	fileReference	            TEXT
                20	contentLocation	            TEXT
                21	createdate	            INTEGER
        '''       
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'name', 'mailId'):
                continue
            attach = Attachment()
            attach.attachment_id            = rec['attachmentId'].Value
            # attach.owner_account_id         = rec['mailId'].Value
            attach.mail_id                  = rec['mailId'].Value
            attach.attachment_name          = rec['name'].Value
            if not self._is_empty(rec, 'localPath') and isinstance(rec['localPath'].Value, str): 
                attach.attachment_save_dir  = self.root.AbsolutePath + '/' + rec['localPath'].Value
            attach.attachment_size          = rec['size'].Value
            attach.attachment_download_date = rec['createdate'].Value
            attach.source                   = self.cur_db_source
            attach.deleted                  = 1 if rec.IsDeleted else 0
            try:
                self.mm.db_insert_table_attachment(attach)
            except:
                exc()
        try:
            self.mm.db_commit()
        except:
            exc()

    def _read_db(self, db_path):
        """ 读取手机数据库

        :type db_path: str
        :rtype: bool                              
        """
        node = self.root.GetByPath(db_path)
        self.cur_db = SQLiteParser.Database.FromNode(node, canceller)
        if self.cur_db is None:
            return False
        self.cur_db_source = node.AbsolutePath
        return True

    def _read_table(self, table_name):
        """ 读取手机数据库 - 表

        :type table_name: str
        :rtype: db.ReadTableRecords()                                       
        """
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            return self.cur_db.ReadTableRecords(tb, self.extract_deleted, True)
        except:
            exc()        
            return []

    @staticmethod
    def _convert_email_format(name_email):
        """ 转换邮件格式

        :type name_email: str
        :rtype: str
        from: [{"name":"pangu_x01","email":"pangu_x01@163.com"}]
        to:   pangu_x01@163.com pangu_x01
        """
        if not name_email:
            return ''
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
            return res.lstrip()
        except:
            return ''

    @staticmethod
    def _is_email_format(*args):
        """ 匹配邮箱地址 

        :type mail_address: str
        :rtype: bool        
        """
        try:
            for mail_address in args:
                if IsDBNull(mail_address) or len(mail_address.strip()) < 5:
                    return False
                if mail_address[0] == '<' and mail_address[-1] == '>':
                    mail_address = mail_address[1:-1]
                reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
                match_obj = re.match(reg_str, mail_address)
                if match_obj is None:
                    return False      
            return True      
        except:
            exc()
            return False

    def _convert_primary_key(self, mailId):
        if mailId == 0:
            self.neg_primary_key -= 1
            return self.neg_primary_key
        return mailId


    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        for i in args:
            if IsDBNull(rec[i].Value) or rec[i].Value in ('', ' ', None, [], {}):
                return True
        return False

def exc():
    if DEBUG:
        traceback.print_exc()
    else:
        pass       