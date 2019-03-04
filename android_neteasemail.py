# coding=utf-8
__author__ = 'YangLiyuan'

from PA_runtime import *
import clr
try:
    clr.AddReference('model_mail')
    clr.AddReference('bcp_mail')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import traceback
import re
import hashlib

import bcp_mail
import model_mail
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, parse_decorator, BaseAndroidParser, base_analyze


# 邮件内容类型
CONTENT_TYPE_HTML = 1    # HTML 格式
CONTENT_TYPE_TEXT = 2    # 纯文本
# 邮件类型
MAIL_OUTBOX   = '3'      # 已发送
MAIL_DRAFTBOX = '2'      # 草稿箱

VERSION_APP_VALUE = 2

        
@parse_decorator
def analyze_neteasemail(node, extract_deleted, extract_source):
    return base_analyze(NeteaseMailParser, 
                        node, 
                        bcp_mail.MAIL_TOOL_TYPE_OTHER, 
                        VERSION_APP_VALUE,
                        build_name='网易邮箱大师',
                        db_name='NeteaseMasterMail')


class NeteaseMailParser(BaseAndroidParser):
    # Documents/imail.db
    def __init__(self, node, db_name):
        '''pattern: com.netease.mail/databases/mmail$ '''
        super(NeteaseMailParser, self).__init__(node, db_name)
        self.root = node.Parent
        self.csm = model_mail.MM()
        self.Generate = model_mail.Generate

        self.accounts    = {}
        self.mail_folder = {}
        self.attach      = {}
        self.neg_primary_key = 1

    def parse_main(self):
        if not self._read_db('mmail'):
            return
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
        for rec in self._read_table('AccountCore', read_delete=False):
            if canceller.IsCancellationRequested:
                return            
            if self._is_empty(rec, 'id') or not self._is_email_format(rec, 'mailAddress'):
                continue
            account = model_mail.Account()
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
                self.csm.db_insert_table_account(account)
            except:
                exc()
        self.csm.db_commit()     

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
            if self._is_empty(rec, 'id', 'email', 'name') or self._is_duplicate(rec, 'id'):
                continue
            contact = model_mail.Contact()
            contact.contact_id       = rec['id'].Value
            contact.owner_account_id = self.cur_account_id
            contact.contact_user     = self._parse_contacts(rec['email'].Value)
            contact.contact_email    = self._parse_contacts(rec['email'].Value)
            contact.contact_alias    = self._parse_contacts(rec['name'].Value)
            contact.deleted = 1 if rec.IsDeleted else rec['isDeleted'].Value               
            contact.source           = self.cur_db_source
            try:
                self.csm.db_insert_table_contact(contact)
            except:
                exc()
        self.csm.db_commit()

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
            if rec['localId'].Value == 0:
                if self._is_duplicate(rec, 'sendDate'):
                    continue
            else:
                self._pk_list.append(rec['sendDate'].Value)

            mail = model_mail.Mail()
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
                self.csm.db_insert_table_mail(mail)
            except:
                exc()
        self.csm.db_commit()

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
            if self._is_empty(rec, 'filename', 'messageId') or self._is_duplicate(rec, 'id'):
                continue
            attach = model_mail.Attachment()
            attach.attachment_id            = rec['id'].Value
            attach.owner_account_id         = self.cur_account_id
            attach.mail_id                  = rec['messageId'].Value
            attach.attachment_name          = rec['filename'].Value
            attach.attachment_save_dir      = self._convert_nodepath(rec['localPath'].Value)
            attach.attachment_download_date = rec['downloadTime'].Value
            attach.attachment_size          = rec['size'].Value
            attach.mail_id                  = rec['messageId'].Value
            attach.source                   = self.cur_db_source
            attach.deleted = 1 if rec.IsDeleted else 0             
            try:
                self.csm.db_insert_table_attachment(attach)
            except:
                exc()
        try:
            self.csm.db_commit()
        except:
            exc()

    def _convert_nodepath(self, raw_path):
        try:
            if not raw_path:
                return
            if self.rename_file_path: 
                # replace: '/storage/emulated', '/data/media'
                raw_path = raw_path.replace(self.rename_file_path[0], self.rename_file_path[1])

            fs = self.root.FileSystem
            for prefix in ['', '/data', ]:
                file_node = fs.GetByPath(prefix + raw_path)
                if file_node and file_node.Type==NodeType.File:
                    return file_node.AbsolutePath
                invalid_path = re.search(r'[\\:*?"<>|\r\n]+', raw_path)
                if invalid_path:
                    return 
                nodes = list(fs.Search(raw_path))
                if nodes and nodes[0].Type == NodeType.File:
                    return nodes[0].AbsolutePath
        except:
            exc()    
            return raw_path

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

    def convert_primary_key(self, mailId):
        if mailId == 0:
            self.neg_primary_key -= 1
            return self.neg_primary_key
        return mailId
 