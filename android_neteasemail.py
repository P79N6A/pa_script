# coding=utf-8
__author__ = 'YangLiyuan'

import traceback
import re
import hashlib

import clr
try:
    clr.AddReference('model_mail')
    clr.AddReference('bcp_mail')
except:
    pass
del clr
from model_mail import *
import bcp_mail


# 邮件内容类型
CONTENT_TYPE_HTML = 1  # HTML 格式
CONTENT_TYPE_TEXT = 2  # 纯文本
# 邮件类型
MAIL_OUTBOX   = '3'      # 已发送
MAIL_DRAFTBOX = '2'      # 草稿箱

VERSION_APP_VALUE = 1

DEBUG = True
DEBUG = False

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
    pr.Build('网易邮箱大师')
    return pr


class NeteaseMailParser(object):
    """
        网易邮箱大师    
    """
    def __init__(self, node, extract_deleted, extract_source):
        '''pattern: com.netease.mail/databases/mmail$ '''
        self.root = node.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mm = MM()
        self.cachepath = ds.OpenCachePath("NeteaseMasterMail")
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()
        self.cache_db = self.cachepath + '\\{}.db'.format(hash_str)

        self.accounts    = {}
        self.mail_folder = {}
        self.attach      = {}
        self.neg_primary_key = 1

    def parse(self):

        if DEBUG or self.mm.need_parse(self.cache_db, VERSION_APP_VALUE):
            if not self._read_db('mmail'):
                return
            self.mm.db_create(self.cache_db) 
            
            self.parse_main()

            if not canceller.IsCancellationRequested:
                self.mm.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.mm.db_insert_table_version(VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mm.db_commit()
            self.mm.db_close()

        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_mail.MAIL_TOOL_TYPE_OTHER, self.cache_db, tmp_dir)
 
        models = Generate(self.cache_db).get_models()
        return models

    def parse_main(self):

        self.pre_parse_mail_box('Mailbox')
        self.parse_account('AccountCore')

        # 表名: table_accountId
        for account_id in self.accounts:
            self.cur_account_id = account_id
            self.parse_contact('Contact_')
            self.parse_mail('Mail_')
            self.parse_attachment('Part_')


    def pre_parse_mail_box(self, table_name):
        """ mmail - Mailbox 
            RecNo	    FieldName	    SQLType	
            1	id	UNSIGNED BIG            INT
            2	accountId	UNSIGNED BIG INT
            3	displayName	            TEXT
            4	serverId	            TEXT
            5	serverSyncToken	        TEXT
            6	type	                INTEGER
            7	canSelected	            INTEGER
            8	hasEncrypt	            INTEGER
            9	syncFlag	            INTEGER
            10	delimiter	            TEXT
            11	parentBoxKey	        TEXT
            12	subBoxKeys	            TEXT
            13	position	            INTEGER
        """
        for rec in self._read_table(table_name):
            if self._is_empty(rec, 'serverId', 'displayName'):
                continue
            self.mail_folder[rec['serverId'].Value] = rec['displayName'].Value

    def parse_account(self, table_name):
        """ mmail      AccountCore 

            RecNo	FieldName	SQLType	
            1	id	UNSIGNED BIG        INT
            2	mailAddress	        TEXT
            3	localSignature	        TEXT
            4	accountType	        INTEGER
            5	protocolType	        INTEGER
            6	isDeleted	        INTEGER
        """
        for rec in self._read_table('AccountCore', extract_deleted=False):
            if canceller.IsCancellationRequested:
                return            
            if self._is_empty(rec, 'id') or not self._is_email_format(rec, 'mailAddress'):
                continue
            account = Account()
            account.account_id    = rec['id'].Value 
            account.account_user = rec['mailAddress'].Value 
            account.account_email = rec['mailAddress'].Value 
            account.account_alias = rec['mailAddress'].Value 
            account.deleted = 1 if rec.IsDeleted else rec['isDeleted'].Value               
            account.source       = self.cur_db_source
            if rec['protocolType'].Value == 1 and rec['isDeleted'].Value == 0:
                self.accounts[account.account_id] = {
                    'email': account.account_email,
                }
            try:
                self.mm.db_insert_table_account(account)
            except:
                exc()
        self.mm.db_commit()     


    def parse_contact(self, table_name):
        """ mmail - Contact_<account_id> 

            RecNo	FieldName	SQLTypd
            1	id	UNSIGNED BIG      d INT
            2	etag	        TEXT
            3	serverHref	        TEXT
            4	name	        TEXT
            5	isDeleted	        INTEGER
            6	title	        TEXT
            7	org	            TEXT
            8	birthday	        TEXT
            9	NameComponent	    TEXT
            10	ADRComponent	    TEXT
            11	tels	        TEXT
            12	isGroup	        INTEGER
            13	members	        TEXT
            14	cid	            TEXT
            15	changeState	        INTEGER
            16	uid	            TEXT
            17	rev	            TEXT
            18	version	        TEXT
            19	email	        TEXT
            20	extraInfo	        TEXT
            21	displayTag	        TEXT
            22	note	        TEXT
            23	isFromPhone	        INTEGER
            24	groups	        TEXT
            25	isMarked	        INTEGER
        """
        table_name = table_name + str(self.cur_account_id)
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'id', 'email', 'name'):
                continue
            contact = Contact()
            contact.contact_id       = rec['id'].Value
            contact.owner_account_id = self.cur_account_id
            contact.contact_user     = self._parse_contacts(rec['email'].Value)
            contact.contact_email    = self._parse_contacts(rec['email'].Value)
            contact.contact_alias    = self._parse_contacts(rec['name'].Value)
            contact.deleted = 1 if rec.IsDeleted else rec['isDeleted'].Value               
            contact.source           = self.cur_db_source
            try:
                self.mm.db_insert_table_contact(contact)
            except:
                exc()
        self.mm.db_commit()

    def parse_mail(self, table_name):
        ''' mmail - Mail_<account_id>

            RecNo	FieldName	SQLType	Size
            1	localId	UNSIGNED BIG            INT
            2	remoteId	            TEXT
            3	mailboxKey	            TEXT
            4	messageId	            TEXT
            5	referenceIds	            TEXT
            6	conversationId	            TEXT
            7	inReplyTo	            TEXT
            8	sendDate	UNSIGNED BIG    INT
            9	recvDate	UNSIGNED BIG    INT
            10	mailSize	UNSIGNED BIG    INT
            11	xHeader	            TEXT
            12	mailFrom	            TEXT
            13	mailTo	            TEXT
            14	mailBCC	            TEXT
            15	mailCC	            TEXT
            16	replyTo	            TEXT
            17	subject	            TEXT
            18	summary	            TEXT
            19	textContent	            TEXT
            20	htmlContent	            TEXT
            21	contentCharset	            TEXT
            22	priority	            INTEGER
            23	isRead	            INTEGER
            24	isReplied	            INTEGER
            25	isForwarded	            INTEGER
            26	isDeleted	            INTEGER
            27	isLocalOnly	            INTEGER
            28	isMarked	            INTEGER
            29	isShowImage	            INTEGER
            30	contentDownloadState	            INTEGER
            31	notificationTo	            TEXT
            32	needRequestReceipt	            INTEGER
            33	mailhash	            TEXT
            34	hasAttach	            INTEGER
        '''
        table_name = 'Mail_' + str(self.cur_account_id)
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return            
            if self._is_empty(rec, 'subject', 'mailboxKey') or not self._convert_email_format(rec['mailFrom'].Value):
                continue
            mail = Mail()
            mail.mail_id          = self.convert_primary_key(rec['localId'].Value)
            mail.mail_group       = self.mail_folder.get(rec['mailboxKey'].Value, None)
            mail.mail_subject     = rec['subject'].Value
            mail.mail_abstract    = rec['summary'].Value
            mail.owner_account_id = self.cur_account_id
            mail.mail_from        = self._convert_email_format(rec['mailFrom'].Value)
            mail.mail_to          = self._convert_email_format(rec['mailTo'].Value)
            mail.mail_cc          = self._convert_email_format(rec['mailCC'].Value)
            mail.mail_bcc         = rec['mailBCC'].Value
            mail.mail_read_status = rec['isRead'].Value
            mail.mail_sent_date   = rec['sendDate'].Value
            mail.mail_size        = rec['mailSize'].Value
            mail.mail_content     = rec['htmlContent'].Value
            mail.deleted = 1 if rec.IsDeleted else rec['isDeleted'].Value               
            mail.source           = self.cur_db_source
            if rec['mailboxKey'].Value == MAIL_OUTBOX:      # 已发送
                mail.sendStatus = 1
            elif rec['mailboxKey'].Value == MAIL_DRAFTBOX:  # 草稿箱
                mail.sendStatus = 0
            try:
                self.mm.db_insert_table_mail(mail)
            except:
                exc()
        self.mm.db_commit()

    def parse_attachment(self, table_name):
        """ mmail - Part_<account_id> 

        RecNo	FieldName	
        1	id	UNSIGNED BIG            INT
        2	messageId	UNSIGNED BIG            INT
        3	localPath	            TEXT
        4	name	            TEXT
        5	filename	            TEXT
        6	serverId	            TEXT
        7	type	            INTEGER
        8	contentType	            TEXT
        9	size	UNSIGNED BIG            INT
        10	previewURL	            TEXT
        11	contentId	            TEXT
        12	flag	            INTEGER
        13	isDownloaded	            INTEGER
        14	downloadTime	IUNSIGNED BIG           INT
        15	charset	            TEXT
        16	encoding	            TEXT
        """
        table_name = 'Part_' + str(self.cur_account_id)
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return            
            if self._is_empty(rec, 'filename', 'messageId'):
                continue
            attach = Attachment()
            attach.attachment_id            = rec['id'].Value
            attach.owner_account_id         = self.cur_account_id
            attach.mail_id                  = rec['messageId'].Value
            attach.attachment_name          = rec['filename'].Value
            attach.attachment_save_dir      = rec['localPath'].Value
            attach.attachment_download_date = rec['downloadTime'].Value
            attach.attachment_size          = rec['size'].Value
            attach.mail_id                  = rec['messageId'].Value
            attach.source                   = self.cur_db_source
            attach.deleted = 1 if rec.IsDeleted else 0             
            try:
                self.mm.db_insert_table_attachment(attach)
            except:
                exc()
        try:
            self.mm.db_commit()
        except:
            exc()

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

    def _read_db(self, db_path):
        """ 
            读取手机数据库
        :type db_path: str
        :rtype: bool                              
        """
        node = self.root.GetByPath(db_path)
        self.cur_db = SQLiteParser.Database.FromNode(node, canceller)
        if self.cur_db is None:
            return False
        self.cur_db_source = node.AbsolutePath
        return True

    def _read_table(self, table_name, extract_deleted=None):
        """ 
            读取手机数据库 - 表
        :type table_name: str
        :rtype: db.ReadTableRecords()                                       
        """
        if extract_deleted is None:
            extract_deleted = self.extract_deleted
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            return self.cur_db.ReadTableRecords(tb, extract_deleted, True)
        except:
            exc()          
            return []


    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull, 空数据 
        
        :type rec:   rec
        :type *args: str
        :rtype:      bool
        '''
        for i in args:
            if IsDBNull(rec[i].Value) or rec[i].Value in ('', ' ', None, [], {}):
                return True
        return False

    @staticmethod
    def _is_email_format(rec, key):
        """ 匹配邮箱地址 
        :type rec: type: <rec>
        :type key: str
        :rtype:    bool        
        """
        try:
            if IsDBNull(rec[key].Value) or len(rec[key].Value.strip()) < 5:
                return False
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, rec[key].Value)
            if match_obj is None:
                return False      
            return True      
        except:
            exc()
            return False

    def convert_primary_key(self, mailId):
        if mailId == 0:
            self.neg_primary_key -= 1
            return self.neg_primary_key
        return mailId

def exc():
    if DEBUG:
        traceback.print_exc()
    else:
        pass            