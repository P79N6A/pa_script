# coding=utf-8
__author__ = 'YangLiyuan'

import os
import traceback
import re
import hashlib


import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_mail')
    clr.AddReference('bcp_mail')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr
import bcp_mail
import model_mail
from model_mail import MailUtils
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, BaseAndroidParser, parse_decorator, base_analyze


VERSION_APP_VALUE = 2

MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT  = 1
MESSAGE_STATUS_SENT    = 2
MESSAGE_STATUS_UNREAD  = 3
MESSAGE_STATUS_READ    = 4


@parse_decorator
def analyze_email(node, read_delete, extract_source):
    return base_analyze(AndroidEmailParser, 
                        node, 
                        bcp_mail.MAIL_TOOL_TYPE_PHONE, 
                        VERSION_APP_VALUE,
                        bulid_name='系统邮箱',
                        db_name='AndroidEmail')


class AndroidEmailParser(BaseAndroidParser):
    def __init__(self, node, db_name):
        super(AndroidEmailParser, self).__init__(node, db_name)
        self.root = node.Parent
        self.csm = model_mail.MM()
        self.Generate = model_mail.Generate
        self.VERSION_VALUE_DB = model_mail.VERSION_VALUE_DB

        self.accounts    = {}
        self.mail_folder = {}
        self.neg_primary_key = 1

    def parse_main(self):
        """ android 邮件   
        
            (\data\com.android.email\databases\EmailProvider.db)
            或者(\data\com.google.android.gm\databases)
                1. Account          # 本人用户
                2. Attachment       # 附件
                3. Credential
                4. HostAuth
                5. Mailbox          #
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
        self.pre_parse_mail_box('EmailProvider.db', 'Mailbox')     
        self.parse_account('EmailProvider.db', 'Account')     
        self.parse_mail('EmailProvider.db', 'Message')     
        self.parse_attachment('EmailProvider.db', 'Attachment')     
        self.parse_contact('Contact.db', 'contact_table') # 华为没有此数据库
   
    def pre_parse_mail_box(self, db_path, table_name):
        """ EmailProvider - Mailbox 
            RecNo	FieldName	SQLType	
            1	_id	            integer
            2	displayName	            text
            3	serverId	            text
            4	parentServerId	            text
            5	parentKey	            integer
            6	accountKey	            integer
            7	type	            integer
            8	delimiter	            integer
            9	syncKey	            text
            10	syncLookback	            integer
            11	syncInterval	            integer
            12	syncTime	            integer
            13	unreadCount	            integer
            14	flagVisible	            integer
            15	flags	            integer
            16	visibleLimit	            integer
            17	syncStatus	            text
            18	messageCount	            integer
            19	lastTouchedTime	            integer
            20	uiSyncStatus	            integer
            21	uiLastSyncResult	            integer
            22	lastNotifiedMessageKey	            integer
            23	lastNotifiedMessageCount	            integer
            24	totalCount	            integer
            25	hierarchicalName	            text
            26	lastFullSyncTime	            integer
            27	visibility	            integer
            28	sortOrder	            integer
        """
        REPLACE_NAME = {
            'INBOX': '收件箱',
            'Outbox': '发件箱',
        }
        if not self._read_db(db_path):
            return        
        for rec in self._read_table(table_name):
            if self._is_empty(rec, 'displayName', 'accountKey') or self._is_duplicate(rec, '_id'):
                continue
            account_id = rec['accountKey'].Value
            folderName = rec['displayName'].Value.replace('\0','')
            folderName = folderName if folderName not in REPLACE_NAME else REPLACE_NAME[folderName]
            self.mail_folder[rec['_id'].Value] = {'name': folderName, 
                                                  'account_id': account_id}

    def parse_account(self, db_path, table_name):
        """ EmailProvider - Account 

            RecNo	FieldName	SQLType	
            1	_id	        integer
            2	displayName	        text
            3	emailAddress	    text
            4	syncKey	        text
            5	syncLookback	        integer
            6	syncInterval	        text
            7	hostAuthKeyRecv	        integer
            8	hostAuthKeySend	        integer
            9	flags	        integer
            10	isDefault	        integer
            11	compatibilityUuid	text
            12	senderName	        text
            13	ringtoneUri	        text
            14	protocolVersion	        text
            15	newMessageCount	        integer
            16	securityFlags	        integer
            17	securitySyncKey	        text
            18	signature	        text
            19	policyKey	        integer
            20	maxAttachmentSize	        integer
            21	pingDuration	        integer
            22	calendarLookback	        integer
            23	peakSyncInterval	        text
            24	peakSyncStartTimeHour	    integer
            25	peakSyncStartTimeMinute	    integer
            26	peakSyncEndTimeHour	        integer
            27	peakSyncEndTimeMinute	        integer
            28	peakSyncDays	        text
            29	peakSyncEnabled	        integer
            30	isPeakDuration	        integer
            31	downloadLimit	        integer
            32	syncLimit	        integer
        """

        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name, read_delete=False):
            if canceller.IsCancellationRequested:
                return
            if not self._is_email_format(rec, 'emailAddress'):
                continue
            if self._is_duplicate(rec, '_id'):
                continue                    
            account = model_mail.Account()
            account.account_id    = rec['_id'].Value
            account.account_alias = rec['displayName'].Value
            account.account_email = rec['emailAddress'].Value
            account.account_user  = rec['emailAddress'].Value
            account.source        = self.cur_db_source
            account.deleted       = 1 if rec.IsDeleted else 0          
            self.accounts[account.account_id] = {
                'email': account.account_email,
                'name' : account.account_alias,
            }
            try:
                self.csm.db_insert_table_account(account)
            except:
                exc()
        self.csm.db_commit()

    def parse_mail(self, db_path, table_name):
        """ EmailProvider - Message 
        
            RecNo	FieldName	SQLType 
            1	_id	                integer
            2	syncServerId	            text
            3	syncServerTimeStamp	            integer
            4	displayName	            text
            5	timeStamp	            integer
            6	subject	            text
            7	flagRead	            integer
            8	flagLoaded	            integer
            9	flagFavorite	            integer
            10	flagAttachment	            integer
            11	flags	            integer
            12	clientId	            integer
            13	messageId	            text
            14	mailboxKey	            integer
            15	accountKey	            integer
            16	fromList	            text
            17	toList	            text
            18	ccList	            text
            19	bccList	            text
            20	replyToList	            text
            21	meetingInfo	            text
            22	snippet	            text
            23	protocolSearchInfo	            text
            24	threadTopic	            text
            25	syncData	            text
            26	flagSeen	            integer
            27	snippetCharNum	            integer
            28	subtitleDisplayName	        text
            29	displayNameSortKey	        text
            30	subtitleSubject	            text
            31	subjectSortKey	            text
            32	mainMailboxKey	            integer
            33	attachmentSize	            integer
            34	flagAllAttachmentInline	    integer
            35	referencesIds	            text
            36	aggregationId	            text
            37	emailPriority	            integer
        """
        if not self._read_db(db_path):
            return     
        try:
            for rec in self._read_table(table_name):
                if canceller.IsCancellationRequested:
                    return
                if self._is_empty(rec, 'displayName', 'subject', 'accountKey'):
                    continue
                if self._is_duplicate(rec, 'timeStamp'):
                    continue                    
                mail = model_mail.Mail()
                mail.mail_id          = self.convert_primary_key(rec['_id'].Value)
                mail.owner_account_id = rec['accountKey'].Value
                mail.mail_group       = self.mail_folder.get(rec['mailboxKey'].Value, None)
                mail.mail_subject     = rec['subject'].Value
                mail.mail_abstract    = rec['snippet'].Value
                mail.mail_from        = self._convert_email(rec['fromList'].Value)
                mail.mail_to          = self._convert_email(rec['toList'].Value)
                mail.mail_cc          = self._convert_email(rec['ccList'].Value)
                mail.mail_bcc         = self._convert_email(rec['bccList'].Value)
                mail.mail_read_status = rec['flagRead'].Value
                mail.mail_sent_date   = rec['timeStamp'].Value
                try: 
                    mail.mail_size = rec['messageSize'].Value    # 此字段华为没有
                except:
                    pass
                if self.mail_folder.get(rec['mailboxKey'].Value, {}).get('name', None) == '已发送':
                    mail.mail_send_status = 1
                elif self.mail_folder.get(rec['mailboxKey'].Value, {}).get('name', None) == '草稿箱':
                    mail.mail_send_status = 0
                mail.source  = self.cur_db_source
                mail.deleted = 1 if rec.IsDeleted else 0               
                self.csm.db_insert_table_mail(mail)

            self.csm.db_commit()

            self.parse_email_body('EmailProviderBody.db', 'Body')            
        except:
            exc()

    def parse_email_body(self, db_path, table_name):
        """
        /EmailProviderBody.db

        RecNo	FieldName	SQLType	Size 
        1	_id	            integer
        2	messageKey	            integer
        3	htmlContent	            text
        4	textContent	            text
        5	htmlReply	            text
        6	textReply	            text
        7	sourceMessageKey	            text
        8	introText	            text
        9	quotedTextStartPos	            integer
        10	searchContent	            text
        """
        SQL_UPDATE_EMAIL_CONTENT = '''
            update mail set mail_content=? where mail_id=?
        '''
        if not self._read_db(db_path):
            return        
        try:
            for rec in self._read_table(table_name):
                if not self._read_db(db_path):
                    return
                if self._is_empty(rec, 'htmlContent', 'messageKey'):
                    continue           
                if self._is_duplicate(rec, '_id'):
                    continue                     
                mailId = rec['messageKey'].Value
                mailContent = rec['htmlContent'].Value
                try:
                    self.csm.db_update(SQL_UPDATE_EMAIL_CONTENT, (mailContent, mailId))
                except:
                    exc()
            self.csm.db_commit()
        except:
            exc()

    def parse_attachment(self, db_path, table_name):
        """ EmailProvider - Attachment (xiaomi)
            RecNo	        FieldName	
            1	_id	            integer
            2	fileName	            text
            3	mimeType	            text
            4	size	            integer
            5	contentId	            text
            6	contentUri	            text
            7	messageKey	            integer
            8	location	            text
            9	encoding	            text
            10	content	            text
            11	flags	            integer
            12	content_bytes	            blob
            13	accountKey	            integer
            14	uiState	            integer
            15	uiDestination	            integer
            16	uiDownloadedSize	        integer
            17	cachedFile	            text
           ##############    以下字段华为没有    ############
            18	previewTime	        integer		False	
            19	snapshotPath	            text
            20	isDeleted	        integer		False	
            21	downloadFailureReason	 nteger
            22	sourceAttId	            integer
        """
        if not self._read_db(db_path):
            return            
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'fileName', 'size') or self._is_duplicate(rec, '_id'):
                continue
            attach = model_mail.Attachment()
            attach.attachment_id       = rec['_id'].Value
            attach.mail_id             = rec['messageKey'].Value
            attach.owner_account_id    = rec['accountKey'].Value
            attach.attachment_name     = rec['fileName'].Value
            attach.attachment_save_dir = rec['contentUri'].Value  # or location?
            attach.attachment_size     = rec['size'].Value
            attach.deleted = 1 if rec.IsDeleted else 0               
            try:
                attach.attachment_download_date = rec['previewTime'].Value
                attach.deleted                  = rec['isDeleted'].Value
            except:
                pass
            attach.source                   = self.cur_db_source
            try:
                self.csm.db_insert_table_attachment(attach)
            except:
                exc()
        self.csm.db_commit()

    def parse_contact(self, db_path, table_name):
        """
        华为没有此 db
            /Contact.db

            RecNo	FieldName	SQLType	Size
            1	_id	        integer
            2	name	        text
            3	email	        text
            4	blacklist	        integer
            5	writelist	        integer
            6	weight	            REAL	 
            7	nickname	        text
            8	pinyin	        text
            9	fristpinyin	        text
            10	isenable	        integer
            11	myemail	        text
            12	lasttime	        text
            13	timeStamp	        integer
            14	selected	        text
            15	color	        integer
            16	unread2top	        integer
            17	pop	            integer
            18	dirty	        integer
            19	lastTimestamp	integer
        """
        try:        
            if not self._read_db(db_path):
                return
            for rec in self._read_table(table_name):
                if self._is_empty(rec, 'name', 'email') or self._is_duplicate(rec, '_id'):
                    continue
                contact = model_mail.Contact()
                contact.contact_id          = rec['_id'].Value
                contact.owner_account_id    = self._get_account_id(rec['myemail'].Value)
                contact.contact_alias       = rec['name'].Value
                contact.contact_email       = rec['email'].Value
                contact.contact_last_modify = rec['timeStamp'].Value
                contact.source              = self.cur_db_source
                contact.deleted             = 1 if rec.IsDeleted else 0               
                try:
                    self.csm.db_insert_table_contact(contact)
                except:
                    exc()
            self.csm.db_commit()
        except:
            pass
            # exc()

    @staticmethod
    def _convert_email(eml):
        """
        :eml: (str) pangu_x01 <pangu_x01@163.com>, pangu_x02 <pangu_x02@163.com>
        :rtype: pangu_x01@163.com pangu_x01 pangu_x02@163.com pangu_x02
        """
        if not eml:
            return None
        try:
            res = ''
            _name = ''
            _email = ''           
            for i in eml.split(', '):
                _name_address = i.split(' ')
                if len(_name_address) == 2:
                    _name  = MailUtils.encoded_mailaddress_name(_name_address[0])
                    _email = _name_address[1][1:-1]
                elif '@' in _name_address[0]:
                    _email = _name_address[0]
                else:
                    tp(_name_address)
                    _name = _name_address[0]
                res += ' ' + _email + ' ' + _name
            return res.lstrip()
        except:
            exc()
            tp(eml)
            return None

    def _get_account_id(self, my_email):
        ''' return corresponding account_id by email
        
        :type my_email:   str
        :rtype:      int
        '''
        for i in self.accounts:
            if self.accounts[i]['email'] == my_email:
                return i
        return None

    def convert_primary_key(self, mailId):
        if mailId == 0:
            self.neg_primary_key -= 1
            return self.neg_primary_key
        return mailId

