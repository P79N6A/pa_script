# coding=utf-8
__author__ = 'YangLiyuan'

import hashlib
import zlib
import struct

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('System.Xml.Linq')
    clr.AddReference('model_mail')
    clr.AddReference('model_browser')
    clr.AddReference('bcp_mail')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import bcp_mail
import model_mail 
import model_browser 
from System.Xml.Linq import XElement
from ScriptUtils import CASE_NAME, exc, tp, DEBUG, base_analyze, parse_decorator, BaseParser, ProtobufDecoder, BaseAndroidParser


VERSION_APP_VALUE = 1

MESSAGE_STATUS_DEFAULT = -1
MESSAGE_STATUS_UNSENT  = 0
MESSAGE_STATUS_SENT    = 1
MESSAGE_STATUS_UNREAD  = 0
MESSAGE_STATUS_READ    = 1
MESSAGE_STATUS_INBOX   = 3   # 属于收件但状态未知

MAIL_ADDRESS_FLAG = '08 01 12'

MAIL_INBOX    = '收件箱'
MAIL_OUTBOX   = '已发送'
MAIL_DRAFT    = '草稿箱'
MAIL_DELTED   = '已删除'

GMAIL_LABEL_TYPE = {
    '^all'                : '所有邮件',
    # '^a'                  : '^a',
    '^r'                  : MAIL_DRAFT,
    '^i'                  : MAIL_INBOX,
    '^iim'                : MAIL_INBOX,
    '^u'                  : MAIL_INBOX,  # 未读
    # '^o'                  : '^o',
    '^f'                  : MAIL_OUTBOX,
    '^k'                  : MAIL_DELTED,
    '^t'                  : '已加星标',
    '^s'                  : '垃圾邮件',
    '^io_im'              : '重要邮件',
    # '^io_unim'            : '不重要邮件',
    '^sq_ig_i_personal'   : '主要',
    '^smartlabel_personal': '主要',
    '^sq_ig_i_social'     : '社交',
    '^smartlabel_social'  : '社交',
    '^sq_ig_i_promo'      : '推广',
}

'''
# item_visibility: 
    view_type 
        2   收件
        4   草稿
        9   发件
        12  删除
    messages_in_view_bitmap
        0   已读
        2   未读
'''

@parse_decorator
def analyze_gmail(node, extract_deleted, extract_source):
    if 'es_recycle_content' in node.AbsolutePath:
        return ParserResults()
    return base_analyze(AndroidGmailParser, 
                        node, 
                        bcp_mail.MAIL_TOOL_TYPE_OTHER, 
                        VERSION_APP_VALUE,
                        'Gmail',
                        'Gmail_A')

class AndroidGmailParser(BaseAndroidParser):
    ''' \com.google.android.gm\databases '''

    def __init__(self, node, db_name):
        super(AndroidGmailParser, self).__init__(node, db_name)
        self.VERSION_KEY_DB = model_mail.VERSION_KEY_DB
        self.VERSION_VALUE_DB = model_mail.VERSION_VALUE_DB
        self.VERSION_KEY_APP = model_mail.VERSION_KEY_APP
        self.root = node.Parent.Parent
        self.Generate = model_mail.Generate
        self.csm = model_mail.MM()
        self.accounts = {}

        self.browser_cache_db = self.cache_db.replace('.db', '_browser.db')
        self.model_browser = model_browser.MB()
        self.model_browser.db_create(self.browser_cache_db)

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):
        models = super(AndroidGmailParser, self).parse(BCP_TYPE, VERSION_APP_VALUE)
        browser_models = model_browser.Generate(self.browser_cache_db).get_models()
        res = models.extend(browser_models)
        return res

    def parse_main(self):
        """ com.google.android.gm/databases

                bigTopDataDB.816998789-

                816998789

                com.google.android.gm/shared_prefs/pangux01@gmail.com.xml
        """
        accounts = self.parse_account('shared_prefs')     
        for account in accounts:
            self.cur_account_id = account.account_id
            self.cur_account_email = account.account_email

            if self._read_db('databases/bigTopDataDB.'+str(account.account_id)):
                tp('databases/bigTopDataDB.'+str(account.account_id))
                self.pre_parse_custom_mail_box('clusters')
                MAIL_ITEMS = self._parse_mail_items('items')
                MAIL_INFO = self._parse_mail_item_visibility('item_visibility')
                self._parse_mail_content('item_messages', MAIL_ITEMS, MAIL_INFO)
                self.parse_attachment('item_message_attachments') 
                # self.parse_contact('Contact.db', 'contact_table')
        self.model_browser.db_close()
        
    def pre_parse_custom_mail_box(self, table_name):
        ''' 解析自定义标签

            FieldName	            SQLType      	
            row_id	                INTEGER
            server_perm_id	        TEXT
            cluster_config_proto	BLOB
            grouped_in_inbox	    INTEGER
            write_sequence_id	    INTEGER
            eviction_eligibility	INTEGER
            server_version	        INTEGER
            type	                INTEGER
        '''
        global GMAIL_LABEL_TYPE
        CUSTOM_MAIL_BOX = {}
        for rec in self._read_table(table_name, read_delete=False):
            if rec['type'].Value != 2:
                continue    
            custom_label_data = ProtobufDecoder(rec['cluster_config_proto'].Value)
            custom_label_name = custom_label_data.find_p_after('12')
            CUSTOM_MAIL_BOX[rec['server_perm_id'].Value] = custom_label_name
        GMAIL_LABEL_TYPE.update(CUSTOM_MAIL_BOX)

    def _parse_mail_items(self, table_name):
        ''' items 解析 发件人姓名, 邮箱地址, 主题, 分类
            
            FieldName	                    SQLType	                         	
            row_id	                            INTEGER
            server_perm_id	                    TEXT
            item_summary_proto	                BLOB
            recurrence_id	                    TEXT    
            hidden	                            INTEGER
            write_sequence_id	                INTEGER
            server_version	                    INTEGER
            parent_server_perm_id	            TEXT
            legacy_storage_id	                INTEGER
            legacy_first_message_storage_id	    INTEGER
        ''' 
        MAIL_ITEMS = {}
        ITEMS_SUBJECT = '12'
        for rec in self._read_table(table_name):
            # 428 cookie, 464 图片, 466 三个附件
            if (self._is_empty(rec, 'item_summary_proto') or 
                self._is_duplicate(rec, 'row_id')):
                continue 
            item = ProtobufDecoder(rec['item_summary_proto'].Value)

            # mail_id     
            mail_id = rec['row_id'].Value

            # mail_subject
            item.idx = item.find('0a') + 1
            _mail_id = item.find_p_after('0a')
            mail_subject = item.find_p_after(ITEMS_SUBJECT)

            # mail_from
            mail_from = None
            _mail_from = item.find_p_after(MAIL_ADDRESS_FLAG)
            if _mail_from and self._is_email_format(email_str=_mail_from):
                mail_from = _mail_from
                if item.ord_read_char() == 0x1a:
                    mail_from += ' ' + item.find_p_after('1a')

            # mail_label
            item.idx = item.find('22')
            _next = item.read()
            mail_labels = mail_group = ''
            while _next=='22'.decode('hex'):
                label = item.find_p_after('22')
                mail_label = GMAIL_LABEL_TYPE.get(label, '')
                if mail_label:
                    if mail_label in [MAIL_INBOX, MAIL_OUTBOX, MAIL_DRAFT, MAIL_DELTED]:
                        mail_group = mail_label
                    elif mail_label not in mail_labels:
                        mail_labels = mail_labels + ',' + mail_label
                _next = item.read()

            #tp(res_labels)
            MAIL_ITEMS[mail_id] = {
                'mail_subject': mail_subject,
                'mail_from': mail_from,
                'mail_labels': mail_labels[1:],
                'mail_group': mail_group,
            }
        return MAIL_ITEMS

    def _parse_mail_content(self, table_name, MAIL_ITEMS, MAIL_INFO):
        """ 'bigTopDataDB.' + ACCOUNT_ID - item_messages
        
            FieldName	                        SQLType            	
            row_id	                                INTEGER
            server_perm_id	                        TEXT
            items_row_id	                        INTEGER			               		
            message_proto	                        BLOB
            is_missing_details	                    INTEGER
            write_sequence_id	                    INTEGER
            message_details_external_storage_id	    TEXT
            zipped_message_proto	                BLOB
            is_invalidated	                        INTEGER
            legacy_storage_id	                    INTEGER
        """
        MAIL_ADDRESS_FLAG = '08 01 12'
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'zipped_message_proto', 'items_row_id') or 
                    self._is_duplicate(rec, 'row_id')):
                    continue 
                mail = model_mail.Mail()              
                mail.mail_id          = rec['row_id'].Value
                #if mail.mail_id not in [76]:
                #   continue
                mail.owner_account_id = self.cur_account_id
                mail.mail_sent_date   = MAIL_INFO.get(rec['items_row_id'].Value, {}).get('mail_sent_date')
                mail.mail_labels      = MAIL_ITEMS.get(rec['items_row_id'].Value, {}).get('mail_labels')
                mail.mail_group       = MAIL_ITEMS.get(rec['items_row_id'].Value, {}).get('mail_group')
                if mail.mail_group in [MAIL_INBOX]:
                    _status = MAIL_INFO.get(rec['items_row_id'].Value, {}).get('mail_read_status')
                    mail.mail_read_status = _status if _status else MESSAGE_STATUS_UNREAD
                elif mail.mail_group in [MAIL_OUTBOX, MAIL_DRAFT]:
                    mail.mail_send_status = MAIL_INFO.get(rec['items_row_id'].Value, {}).get('mail_send_status')

                # 补充, 有些邮件没有收件箱标签
                _read_status = MAIL_INFO.get(rec['items_row_id'].Value, {}).get('mail_read_status')
                if _read_status in [MESSAGE_STATUS_INBOX, MESSAGE_STATUS_READ, MESSAGE_STATUS_UNREAD]:
                    mail.mail_group = MAIL_INBOX

                mail.mail_subject = MAIL_ITEMS.get(rec['items_row_id'].Value, {}).get('mail_subject')
                if not mail.mail_subject:
                    continue
                mail.source = self.cur_db_source
                mail.deleted = 1 if rec.IsDeleted else 0
                # mail.mail_bcc = rec['bccList'].Value

                blob = rec['zipped_message_proto'].Value
                try:
                    bytes_data = zlib.decompress(blob[1:])
                except:
                    continue
                hex_str = ProtobufDecoder(bytes_data)
                
                # mail_to
                mail_to_all = hex_str.find_p_after('0a')
                if MAIL_ADDRESS_FLAG in self._2_hexstr(mail_to_all):
                    mail.mail_to = self._get_mail_address_name(mail_to_all)
                    # mail_cc
                    if hex_str.ord_read_char() == 0x12:
                        mail.mail_cc = hex_str.find_p_after(MAIL_ADDRESS_FLAG)
                # mail_from
                mail_from1 = MAIL_ITEMS.get(rec['items_row_id'].Value, {}).get('mail_from')
                if hex_str.ord_read_char() == 0x22:
                    mail_from_all = hex_str.find_p_after('22')
                    mail.mail_from = self._get_mail_address_name(mail_from_all)
                else:
                    mail.mail_from = mail_from1
                if not mail.mail_to and not mail.mail_from:
                    continue

                # status 补充
                # mail_send_status
                if mail.mail_from and self.cur_account_email in mail.mail_from:
                    mail.mail_send_status = 1

                # mail_subject
                if hex_str.ord_read_char() == 0x2a:
                    hex_str.idx += 1
                    abstract = hex_str.get_parscal()
                    # tp(abstract)
                    # mail.mail_subject = abstract

                # mail_content
                CONTENT_ENDS = '18 00'
                if hex_str.ord_read_char() == 0x32:
                    hex_str.idx += 1
                    beg = hex_str.find('08 00 1a') - hex_str.idx
                    if self._2_hexstr(hex_str.data[beg: beg+3]) == '08 00 1a':
                        hex_str.idx += beg * 2 + 3
                        mail_content = hex_str.read_before(CONTENT_ENDS).decode('utf8', 'ignore')
                        end_idx = mail_content.rfind('0a'.decode('hex'))
                        mail.mail_content = mail_content[: end_idx]
                        self._browser_record_from_gmail(mail)
                self.csm.db_insert_table_mail(mail)
            except:
                exc()
        self.csm.db_commit()
        self.model_browser.db_commit()

    def _browser_record_from_gmail(self, _mail):
        _mail_content = _mail.mail_content
        if not _mail_content:
            return 
        _date = _mail.mail_sent_date
        _urls = []
        URL_PATTERN = r'(http|ftp|https)://([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6}|[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?'
        # IP_PATTERN = r'(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])'

        def _save_to_br(_url_name, _date):
            browser_record = model_browser.Browserecord()
            browser_record.url      = _url_name
            browser_record.datetime = _date
            browser_record.source   = _mail.source
            browser_record.deleted  = _mail.deleted
            self.model_browser.db_insert_table_browserecords(browser_record)

        for _u in re.finditer(URL_PATTERN, _mail_content):
            _save_to_br(_u.group(), _date)
        # for _ip in re.finditer(IP_PATTERN, _mail_content):
        #     _save_to_br(_ip.group(), _date)

    def _get_mail_address_name(self, mail_address_name_str):
        '''parse email address and name
        
        Args:
            mail_address_name_str (str): "\b\u0001\u0012\u0012pangux01@gmail.com\u001a\t张一一"
        
        Returns:
            str: pangux01@gmail.com 张一一
        '''
        _address, _name = '', ''
        try:
            _mail_from_all = ProtobufDecoder(mail_address_name_str.encode('utf8', 'ignore'))
            _address = _mail_from_all.find_p_after(MAIL_ADDRESS_FLAG)
            if not self._is_email_format(email_str=_address):
                return ''
            _name = ''
            if _mail_from_all.read() and ord(_mail_from_all.read()) == 0x1a:
                _name = _mail_from_all.find_p_after('1a')
                return _address + ' ' + _name    
            return _address
        except:
            exc()
            return ''

    def _parse_mail_item_visibility(self, table_name):
        """ 'bigTopDataDB.' + ACCOUNT_ID  - item_visibility

            MAIL_INFO[rec['items_row_id'].Value] = {
                'mail_sent_date'  : mail_sent_date,
                'mail_read_status': mail_read_status,
                'mail_send_status': mail_send_status,
            }        
            
            FieldName	                    SQLType              	
            row_id	                            INTEGER
            items_row_id	                    INTEGER               		
            clusters_row_id	                    INTEGER               		
            view_type	                        INTEGER
            rank	                            TEXT
            cluster_rank	                    TEXT
            write_sequence_id	                INTEGER
            do_not_show_before_timestamp_ms	    INTEGER
            do_not_show_after_timestamp_ms	    INTEGER
            nested_cluster_row_id	            INTEGER
            display_timestamp_ms	            INTEGER
            messages_in_view_bitmap	            INTEGER
        """
        MAIL_INFO = {}
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'row_id', 'items_row_id', 'display_timestamp_ms') or
                    len(str(rec['display_timestamp_ms'].Value)) not in [13, 16]):
                    continue                     
                mail_sent_date = rec['display_timestamp_ms'].Value
                #if rec['items_row_id'].Value != 20:
                #    continue
                if MAIL_INFO.has_key(rec['items_row_id'].Value):
                    item_info = MAIL_INFO[rec['items_row_id'].Value]
                    if rec['view_type'].Value == 2:  # 收件
                        item_info['mail_send_status'] = None
                        item_info['mail_read_status'] = MESSAGE_STATUS_INBOX
                        continue
                    elif item_info.get('mail_send_status', None) is not None:
                        continue
                # messages_in_view_bitmap 固定 0 或 2
                # view_type 不确定
                # 未读
                _item_info = {}
                if rec['messages_in_view_bitmap'].Value == 2:
                    _item_info['mail_read_status'] = MESSAGE_STATUS_UNREAD
                # 已读 或 已发送/未发送
                elif rec['messages_in_view_bitmap'].Value == 0:    
                    if rec['view_type'].Value == 9:
                        _item_info['mail_send_status'] = MESSAGE_STATUS_SENT
                    elif rec['view_type'].Value == 4:
                        _item_info['mail_send_status'] = MESSAGE_STATUS_UNSENT
                    else:
                        _item_info['mail_read_status'] = MESSAGE_STATUS_READ
                        
                _item_info['mail_sent_date'] = mail_sent_date

                MAIL_INFO[rec['items_row_id'].Value] = _item_info
            except:
                exc()    
        return MAIL_INFO

    def parse_account(self, xml_path):
        ''' associate email and id 
        
        Args:
            xml_path (str): 
        Returns:
            accounts (dict):
        '''
        ''' com.google.android.gm/shared_prefs/pangux01@gmail.com.xml
                                              /Account-pangux01@gmail.com.xml
            <set name="notificationShownIds_gig:816998789:^sq_ig_i_personal" />
        '''        
        self.cur_xml_source = None
        accounts = []
        account_id = auto_id = 0
        for file in self.root.GetByPath(xml_path).Children:
            if not file.Name.startswith('Account-'):
                continue
            email_str = file.Name.replace('Account-', '').replace('.xml', '')
            # find email&id relate xml
            for prefix in ['', 'com.google.android.apps.gmail.notifications_']:
                account_xml_path = xml_path + '/' + prefix + email_str + '.xml'
                xml_data = self._read_xml(account_xml_path)
                if xml_data:
                    break
            if self._is_email_format(email_str=email_str) and xml_data:
                m = xml_data.Elements('set')
                for i in m:
                    element_attr = i.FirstAttribute.Value
                    if element_attr.startswith('notificationShownIds_gig'):
                        try:
                            extract_pattern = r'notificationShownIds_gig:(.+?):\^sq_ig_i_personal'
                            account_id = int(re.search(extract_pattern, element_attr).group(1))
                            break
                        except:
                            account_id = auto_id
                            auto_id += 1

            account = model_mail.Account()
            account.account_id    = account_id
            account.account_alias = email_str
            account.account_email = email_str
            account.account_user  = email_str
            account.source        = self.cur_xml_source
            self.csm.db_insert_table_account(account)
            accounts.append(account)   

        self.csm.db_commit()
        return accounts

    def parse_attachment(self, table_name):
        """ 'bigTopDataDB.' + ACCOUNT_ID - item_message_attachments

            FieldName	        SQLType            	
            row_id	                INTEGER
            item_messages_row_id	INTEGER			               		
            is_synced	            INTEGER
            attachment_url	        TEXT
            attachment_cache_key	TEXT
            attachment_file_name	TEXT
            attachment_hash	        TEXT
        """       
        for rec in self._read_table(table_name):
            if (self._is_empty(rec, 'attachment_file_name', 'row_id') 
                or self._is_duplicate(rec, 'row_id')):
                continue
            attachment = model_mail.Attachment()
            attachment.attachment_id    = rec['row_id'].Value
            attachment.owner_account_id = self.cur_account_id
            attachment.mail_id          = rec['item_messages_row_id'].Value
            attachment.attachment_name  = rec['attachment_file_name'].Value
            attachment.source           = self.cur_db_source
            attachment.deleted          = 1 if rec.IsDeleted else 0
            # attachment.attachment_save_dir
            # attachment.attachment_size       
            # attachment.attachment_download_date
            try:
                self.csm.db_insert_table_attachment(attachment)
            except:
                exc()
        try:
            self.csm.db_commit()
        except:
            exc()

    def _2_hexstr(self, s):
        ''' convert hex list to string

        Args:
            string (str): '\n\x16\x08\x01'
        Returns:
            res (str): '0a 16 08 01'
        '''
        try:
            res = ''
            if s:
                hex_str = [ord(x) for x in s]
                res = ' '.join(map(lambda x: hex(x).replace('0x', '') if x>15 else hex(x).replace('0x', '0'), hex_str))
            return res
        except:
            exc()
            return  ''

    def _read_xml(self, xml_path):
        ''' _read_xml, set self.cur_xml_source

        Args: 
            xml_path (str): self.root.GetByPath(xm_path)
        Returns:
            xml_data (XElement)
        '''
        try:
            xml_node = self.root.GetByPath(xml_path)
            if xml_node and xml_node.Data:
                xml_data = XElement.Parse(xml_node.read())
                self.cur_xml_source = xml_node.AbsolutePath
                return xml_data
            else:
                return False
        except:
            exc()
            return False