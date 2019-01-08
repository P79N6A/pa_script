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
except:
    pass
del clr
from model_mail import *
import bcp_mail


VERSION_APP_VALUE = 1

MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT  = 1
MESSAGE_STATUS_SENT    = 2
MESSAGE_STATUS_UNREAD  = 3
MESSAGE_STATUS_READ    = 4

DEBUG = True
DEBUG = False

CASE_NAME = ds.ProjectState.ProjectDir.Name

def exc(e=''):
    ''' Exception output '''
    try:
        if DEBUG:
            py_name = os.path.basename(__file__)
            msg = 'DEBUG {} case:<{}> :'.format(py_name, CASE_NAME)
            TraceService.Trace(TraceLevel.Warning, (msg+'{}{}').format(traceback.format_exc(), e))
    except:
        pass   
        
def test_p(*e):
    ''' Highlight print in test environments vs console '''
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, "{}".format(e))
    else:
        pass

def analyze_email(node, extract_deleted, extract_source):
    """ android 邮件 华为 """
    res = []
    pr = ParserResults()
    try:
        res = EmailParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug, 
                           'android_email.py 解析新案例 "{}" 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('系统邮箱')
    return pr


class EmailParser(object):
    def __init__(self, node, extract_deleted, extract_source):

        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mm = MM()
        self.cachepath = ds.OpenCachePath("AndroidEmail")
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()[8:-8]

        self.cache_db = self.cachepath + '\\a_email_{}.db'.format(hash_str)

        self.accounts    = {}
        self.mail_folder = {}
        self.neg_primary_key = 1

    def parse(self):
     
        if DEBUG or self.mm.need_parse(self.cache_db, VERSION_APP_VALUE):
            if not self._read_db('EmailProvider.db'):
                return []
            self.mm.db_create(self.cache_db) 
            
            self.parse_main()

            if not canceller.IsCancellationRequested:
                self.mm.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.mm.db_insert_table_version(VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mm.db_commit()
            self.mm.db_close()

        tmp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_mail.MAIL_TOOL_TYPE_PHONE, self.cache_db, tmp_dir)
        models = Generate(self.cache_db).get_models()
        return models

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
        for rec in self._read_table(table_name, extract_deleted=False):
            if canceller.IsCancellationRequested:
                return
            if not self._is_email_format(rec, 'emailAddress'):
                continue
            if self._is_duplicate(rec, '_id'):
                continue                    
            account = Account()
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
                self.mm.db_insert_table_account(account)
            except:
                exc()
        self.mm.db_commit()

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
                mail = Mail()
                mail.mail_id          = self.convert_primary_key(rec['_id'].Value)
                mail.owner_account_id = rec['accountKey'].Value
                mail.mail_group       = self.mail_folder.get(rec['mailboxKey'].Value, None)
                mail.mail_subject     = rec['subject'].Value
                mail.mail_abstract    = rec['snippet'].Value
                mail.mail_from        = rec['fromList'].Value
                mail.mail_to          = rec['toList'].Value
                mail.mail_cc          = rec['ccList'].Value
                mail.mail_bcc         = rec['bccList'].Value
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
                self.mm.db_insert_table_mail(mail)

            self.mm.db_commit()

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
                    self.mm.db_update(SQL_UPDATE_EMAIL_CONTENT, (mailContent, mailId))
                except:
                    exc()
            self.mm.db_commit()
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
            attach = Attachment()
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
                self.mm.db_insert_table_attachment(attach)
            except:
                exc()
        self.mm.db_commit()

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
                contact = Contact()
                contact.contact_id          = rec['_id'].Value
                contact.owner_account_id    = self._get_account_id(rec['myemail'].Value)
                contact.contact_alias       = rec['name'].Value
                contact.contact_email       = rec['email'].Value
                contact.contact_last_modify = rec['timeStamp'].Value
                contact.source              = self.cur_db_source
                contact.deleted             = 1 if rec.IsDeleted else 0               
                try:
                    self.mm.db_insert_table_contact(contact)
                except:
                    exc()
            self.mm.db_commit()
        except:
            pass
            # exc()

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

    def _read_table(self, table_name, extract_deleted=None):
        ''' 读取手机数据库 - 表

        :type table_name: str
        :rtype: db.ReadTableRecords()                                       
        '''
        if extract_deleted == None:
            extract_deleted = self.extract_deleted
        self._PK_LIST = []
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            return self.cur_db.ReadTableRecords(tb, extract_deleted, True)
        except:
            exc()
            return []

    def _is_duplicate(self, rec, pk_name):
        ''' filter duplicate record
        
        Args:
            rec (record): 
            pk_name (str): 
        Returns:
            bool: rec[pk_name].Value in self._PK_LIST
        '''
        try:
            pk_value = rec[pk_name].Value
            if IsDBNull(pk_value) or pk_value in self._PK_LIST:
                return True
            self._PK_LIST.append(pk_value)
            return False
        except:
            exc()
            return True            

    def _get_account_id(self, my_email):
        ''' return corresponding account_id by email
        
        :type my_email:   str
        :rtype:      int
        '''
        for i in self.accounts:
            if self.accounts[i]['email'] == my_email:
                return i
        return None

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        try:
            for i in args:
                value = rec[i].Value
                if IsDBNull(value) or value in ('', ' ', None, [], {}):
                    return True
                if isinstance(value, str) and re.search(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', str(value)):
                    return True
            return False
        except:
            exc()
            return True   

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

