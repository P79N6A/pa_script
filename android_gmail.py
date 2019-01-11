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
    clr.AddReference('bcp_mail')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import bcp_mail
import model_mail 
from System.Xml.Linq import XElement
from ScriptUtils import CASE_NAME, exc, tp, DEBUG, base_analyze, parse_decorator, BaseParser, ProtobufDecoder


VERSION_APP_VALUE = 1

MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT  = 1
MESSAGE_STATUS_SENT    = 2
MESSAGE_STATUS_UNREAD  = 3
MESSAGE_STATUS_READ    = 4


GMAIL_LABEL_TYPE = {
    '^all'              : '所有邮件',
    '^r'                : '草稿',
    '^f'                : '已发邮件',
    '^k'                : '已删除邮件',
    '^t'                : '已加星标',
    '^s'                : '垃圾邮件',
    '^io_im'            : '重要邮件',
    '^sq_ig_i_personal' : '主要',
    '^sq_ig_i_social'   : '社交',
    '^smartlabel_social': '社交',
    '^sq_ig_i_promo'    : '推广',

}

@parse_decorator
def analyze_gmail(node, extract_deleted, extract_source):
    return base_analyze(GmailParser, 
                        node, 
                        extract_deleted, 
                        extract_source, 
                        bcp_mail.MAIL_TOOL_TYPE_OTHER, 
                        VERSION_APP_VALUE,
                        'Gmail',
                        'Gmail_A')


class GmailParser(BaseParser):
    ''' \com.google.android.gm\databases '''

    def __init__(self, node, extract_deleted, extract_source, db_name):
        super(GmailParser, self).__init__(node, extract_deleted, extract_source, db_name)
        self.VERSION_KEY_DB = model_mail.VERSION_KEY_DB
        self.VERSION_VALUE_DB = model_mail.VERSION_VALUE_DB
        self.VERSION_KEY_APP = model_mail.VERSION_KEY_APP
        self.root = node.Parent.Parent
        self.Generate = model_mail.Generate
        self.csm = model_mail.MM()
        self.accounts    = {}
        self.mail_folder = {}

    def parse_main(self):
        """ com.google.android.gm/databases

                bigTopDataDB.816998789

                816998789

                com.google.android.gm/shared_prefs/pangux01@gmail.com.xml
        """           
        accounts = self.parse_account('shared_prefs')     
        for account in accounts:
            self.cur_account_id = account.account_id

            # if self._read_db('databases/bigTopDataDB.'+str(account.account_id)):
            if self._read_db('databases/bigTopDataDB.1446591167'):
                tp('databases/bigTopDataDB.'+str(account.account_id))
                self.pre_parse_custom_mail_box('clusters')
                tp(GMAIL_LABEL_TYPE)
                MAIL_ITEMS = self._parse_mail_items('items')
                MAIL_TS = self._parse_mail_ts('item_visibility')
                self._parse_mail_content('item_messages', MAIL_ITEMS, MAIL_TS)
                # self.parse_attachment('EmailProvider.db', 'Attachment')     
                # self.parse_contact('Contact.db', 'contact_table')

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
        ''' 发件人姓名邮箱地址, 主题, 分类
            
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
        # MAIL_FROM = ['08 01 12', '8a 01 02'] 
        MAIL_FROM = '08 01 12' 
        for rec in self._read_table(table_name):
            # 428 cookie, 464 图片, 466 三个附件
            #if rec['row_id'].Value not in [466]:
            #    continue
            if (self._is_empty(rec, 'item_summary_proto') or 
                self._is_duplicate(rec, 'row_id')):
                continue 
            item = ProtobufDecoder(rec['item_summary_proto'].Value)

            # mail_id     
            mail_id = rec['legacy_storage_id'].Value

            # mail_subject
            item.idx = item.find('0a') + 1
            _mail_id = item.find_p_after('0a')
            mail_subject = item.find_p_after(ITEMS_SUBJECT)

            # mail_from
            mail_from = None
            _mail_from = item.find_p_after(MAIL_FROM)
            if _mail_from and self._is_email_format(email_str=_mail_from):
                mail_from = _mail_from
            # mail_group
            # raw_labels = item.read_before('5a').decode('utf8', 'ignore')
            raw_labels = []
            item.idx = item.find('22')
            _next = item.read()
            mail_groups = ''
            while _next=='22'.decode('hex'):
                #raw_labels.append(item.find_p_after('22'))
                label = item.find_p_after('22')
                mail_group = GMAIL_LABEL_TYPE.get(label, '')
                if mail_group:
                    mail_groups = mail_groups + ',' + mail_group
                _next = item.read()
            # _labels = re.sub(r'\x22.', ',', raw_labels)
            #tp(res_labels)
            #res_labels = ','.join(raw_labels)
            MAIL_ITEMS[mail_id] = {
                'mail_subject': mail_subject,
                'mail_from': mail_from,
                'mail_group': mail_groups[1:],
            }
        return MAIL_ITEMS

    def _parse_mail_content(self, table_name, MAIL_ITEMS, MAIL_TS):
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
        ZIP_MAIL_FROM = ['08 01 12', '8a 01 02']  # seperate by 1a

        MAIL_FROM = '08 01 12' 

        for rec in self._read_table(table_name):
            # 428 cookie, 464 图片, 466 三个附件
            #if rec['row_id'].Value not in [466]:
            #    continue
            try:
                if (self._is_empty(rec, 'zipped_message_proto') or 
                    self._is_duplicate(rec, 'row_id')):
                    continue      
                mail = model_mail.Mail()              
                mail.mail_id            = rec['legacy_storage_id'].Value
                mail.owner_account_id   = self.cur_account_id
                mail.mail_sent_date     = MAIL_TS.get(mail.mail_id, None)
                mail.mail_from          = MAIL_ITEMS.get(mail.mail_id, {}).get('mail_from')
                mail.mail_subject       = MAIL_ITEMS.get(mail.mail_id, {}).get('mail_subject')
                mail.mail_group         = MAIL_ITEMS.get(mail.mail_id, {}).get('mail_group')
                mail.source             = self.cur_db_source
                mail.deleted            = 1 if rec.IsDeleted else 0
                # mail.mail_abstract    = rec['snippet'].Value
                # mail.mail_to          = rec['toList'].Value
                # mail.mail_cc          = rec['ccList'].Value
                # mail.mail_bcc         = rec['bccList'].Value
                # mail.mail_read_status = rec['flagRead'].Value
                blob = rec['zipped_message_proto'].Value
                try:
                    bytes_data = zlib.decompress(blob[1:])
                except:
                    continue
                hex_str = ProtobufDecoder(bytes_data)
                # mail_to
                mail_to_all = hex_str.find_p_after('0a')
                # tp(mail_to_all)
                MAIL_TO = '08 01 12'
                if MAIL_TO in self._2_hexstr(mail_to_all):
                    mail.mail_to = mail_to_all

                # mail_from
                #if not mail.mail_from and ord(hex_str.read()) == 0x22:
                if ord(hex_str.read()) == 0x22:
                    mail_from = hex_str.find_p_after('22')
                    mail_from = mail_from[1:].replace('011218'.decode('hex'), '')
                    mail_from = mail_from.replace('1a'.decode('hex'), ' ')
                    mail.mail_from = mail_from

                # mail_subject
                if hex_str.read() and ord(hex_str.read()) == 0x2a:
                    hex_str.idx += 1
                    abstract = hex_str.get_parscal()
                    tp(abstract)
                    # mail.mail_subject = abstract

                # mail_content
                CONTENT_ENDS = '18 00'
                if ord(hex_str.read()) == 0x32:
                    hex_str.idx += 1
                    if self._2_hexstr(hex_str.data[5:8]) == '08 00 1a':
                        hex_str.idx += 13
                        mail_content = hex_str.read_before(CONTENT_ENDS).decode('utf8', 'ignore')
                        end_idx = mail_content.rfind('0a'.decode('hex'))
                        mail.mail_content = mail_content[: end_idx]
                # tp(hex_str.read_move(idx))
                # tp(hex_str.read_move(3))
                # tp(hex_str.read_move(idx))
                tp(self._2_hexstr(bytes_data[:4]))
                self.csm.db_insert_table_mail(mail)
            except:
                exc()
        self.csm.db_commit()

    def _parse_mail_ts(self, table_name):
        """
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
        MAIL_TS = {}
        for rec in self._read_table(table_name):
            try:
                if (self._is_duplicate(rec, 'items_row_id') or
                    self._is_empty(rec, 'row_id', 'items_row_id', 'display_timestamp_ms') or
                    len(str(rec['display_timestamp_ms'].Value)) not in [13, 16]):
                    continue                     
                mail_id = rec['items_row_id'].Value
                mail_sent_date = rec['display_timestamp_ms'].Value
                MAIL_TS[mail_id] = mail_sent_date
            except:
                exc()    
        return MAIL_TS

    def _2_hexstr(self, s):
        ''' convert hex list to string

        Args:
            string (str): '\n\x16\x08\x01'
        Returns:
            res (str): '0a 16 08 01'
        '''
        try:
            if s:
                hex_str = [ord(x) for x in s]
                res = ' '.join(map(lambda x: hex(x).replace('0x', '') if x>15 else hex(x).replace('0x', '0'), hex_str))
            return res
        except:
            exc()
            return  ''

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
            
            u'\n\x1b\x08\x01\x12\x17taojianping01@gmail.com
            *0\xe6\x88\x91\xe4\xbb\xac\xe6\xb3\xa8\xe6\x84\x8f\xe5\x88\xb0\xe6\x82\xa8\xe7\x9a\x84 Dropbox \xe6\x9c\x89\xe6\x96\xb0\xe7\x9a\x84\xe7\x99\xbb\xe5\xbd\x95\xe3\x80\x822\xbc1\x12\xda0\x08\x00\x1a\xd00\x12\xcd0<u></u>\n\n\n\n\n <div style="padding:0;width:100%!important;margin:0" marginheight="0" marginwidth="0"><center><table cellpadding="8" cellspacing="0" style="padding:0;width:100%!important;background:#ffffff;margin:0;background-color:#ffffff" border="0"><tr><td valign="top">\n<table cellpadding="0" cellspacing="0" style="border-radius:4px;border:1px #dceaf5 solid" border="0" align="center">\n<tr><td colspan="3" height="6"></td></tr>\n<tr style="line-height:0px"><td width="100%" style="font-size:0px" align="center" height="1"><img src="https://ci6.googleusercontent.com/proxy/z0bWF43BJl3HebfvfCsWXqgFHPK3KUsIvSpS6DlrrQ0P_-CGsFTiJkEHtPlwXI73p2bZK9hm6uVCY7HJVg9mMLgvvZZOk8O4pgpwtmN1fJzGzBj4UhBwWqmDfu2KMaI=s0-d-e1-ft#https://cfl.dropboxstatic.com/static/images/emails/logo_glyph_34_m1%402x.png" style="max-height:73px;width:40px" alt="" width="40px"></td></tr> <tr><td><table cellpadding="0" cellspacing="0" style="line-height:25px" border="0" align="center">\n<tr><td colspan="3" height="30"></td></tr>\n<tr>\n<td width="36"></td>\n<td width="454" align="left" style="color:#444444;border-collapse:collapse;font-size:11pt;font-family:proxima_nova,&#39;Open Sans&#39;,&#39;Lucida Grande&#39;,&#39;Segoe UI&#39;,Arial,Verdana,&#39;Lucida Sans Unicode&#39;,Tahoma,&#39;Sans Serif&#39;;max-width:454px" valign="top">
            
            \xe5\xb0\x8a\xe6\x95\xac\xe7\x9a\x84Tao Jianping\xef\xbc\x9a\xe6\x82\xa8\xe5\xa5\xbd\xef\xbc\x81<br><br>\xe4\xb8\x80\xe5\x8f\xb0\xe6\x96\xb0Android \xe8\xae\xbe\xe5\xa4\x87\xe5\x88\x9a\xe5\x88\x9a\xe7\x99\xbb\xe5\xbd\x95\xe4\xba\x86\xe6\x82\xa8\xe7\x9a\x84 Dropbox \xe5\xb8\x90\xe6\x88\xb7\xe3\x80\x82\xe4\xb8\xba\xe7\xa1\xae\xe4\xbf\x9d\xe6\x82\xa8\xe7\x9a\x84\xe5\xb8\x90\xe6\x88\xb7\xe5\xae\x89\xe5\x85\xa8\xef\xbc\x8c\xe8\xaf\xb7\xe5\x91\x8a\xe8\xaf\x89\xe6\x88\x91\xe4\xbb\xac\xe8\xbf\x99\xe6\x98\xaf\xe4\xb8\x8d\xe6\x98\xaf\xe6\x82\xa8\xe6\x9c\xac\xe4\xba\xba\xe7\x9a\x84\xe6\x93\x8d\xe4\xbd\x9c\xe3\x80\x82<br><br><table style="border-radius:4px;width:100%!important;background:#e8f2fa">\n<tr>\n<td height="16px"></td>\n<td height="16px"></td>\n<td height="16px"></td>\n</tr>\n<tr>\n<td width="20px"></td>\n<td>\n<span style="color:#444;text-align:center"> <b> \xe7\xa1\xae\xe5\xae\x9e\xe6\x98\xaf\xe6\x82\xa8\xe5\x9c\xa8\xe7\x99\xbb\xe5\xbd\x95\xe5\x90\x97\xef\xbc\x9f</b> </span><table cellpadding="0" border="0" style="color:#444;font-size:14px" cellspacing="0" align="center">\n<tr>\n<td height="10px"></td>\n<td height="10px"></td>\n</tr>\n<tr valign="top">\n<td width="90px">\xe5\x9c\xb0\xe7\x82\xb9\xef\xbc\x9a</td>\n<td><b>London, England, United Kingdom\xe9\x99\x84\xe8\xbf\x91</b></td>\n</tr>\n<tr valign="top">\n<td width="90px">\xe6\x97\xb6\xe9\x97\xb4\xef\xbc\x9a</td>\n<td><b>Dec 10, 2018 at 8:07 am (GMT)</b></td>\n</tr>\n<tr valign="top">\n<td width="90px">\xe4\xba\x8b\xe4\xbb\xb6\xef\xbc\x9a</td>\n<td><b>Dropbox for Android</b></td>\n</tr>\n<tr>\n<td height="16px"></td>\n<td height="16px"></td>\n</tr>\n</table>\n<table cellpadding="0" border="0" align="center" cellspacing="0" style="text-align:center"><tr>\n<td width="124px"><a style="border-radius:3px;font-size:14px;border-right:1px #b1b1b1 solid;border-bottom:1px #aaaaaa solid;padding:7px 7px 7px 7px;border-top:1px #bfbfbf solid;max-width:97px;font-family:proxima_nova,&#39;Open Sans&#39;,&#39;lucida grande&#39;,&#39;Segoe UI&#39;,arial,verdana,&#39;lucida sans unicode&#39;,tahoma,sans-serif;color:#777777;text-align:center;background-image:-webkit-gradient(linear,0% 0%,0% 100%,from(rgb(251,251,251)),to(rgb(228,228,228)));text-decoration:none;width:97px;border-left:1px #b1b1b1 solid;margin:0;display:block;background-color:#f3f3f3" href="https://www.dropbox.com/l/AADKNulSDijVrqvUUsXA_7k3C1ELwQOJmWc" target="_blank" rel="noreferrer" data-saferedirecturl="https://www.google.com/url?q=https://www.dropbox.com/l/AADKNulSDijVrqvUUsXA_7k3C1ELwQOJmWc&amp;source=gmail&amp;ust=1544515666849000&amp;usg=AFQjCNGAXftVP6ykyxZiwQT99iGKmkg0UQ">\xe6\x98\xaf</a></td>\n<td></td>\n<td width="124px" height="0px"><a style="border-radius:3px;font-size:14px;border-right:1px #b1b1b1 solid;border-bottom:1px #aaaaaa solid;padding:7px 7px 7px 7px;border-top:1px #bfbfbf solid;max-width:97px;font-family:proxima_nova,&#39;Open Sans&#39;,&#39;lucida grande&#39;,&#39;Segoe UI&#39;,arial,verdana,&#39;lucida sans unicode&#39;,tahoma,sans-serif;color:#777777;text-align:center;background-image:-webkit-gradient(linear,0% 0%,0% 100%,from(rgb(251,251,251)),to(rgb(228,228,228)));text-decoration:none;width:97px;border-left:1px #b1b1b1 solid;margin:0;display:block;background-color:#f3f3f3" href="https://www.dropbox.com/l/AAC42--mb2U2aBS6rOz-K3UEkcRjd9b5miU" target="_blank" rel="noreferrer" data-saferedirecturl="https://www.google.com/url?q=https://www.dropbox.com/l/AAC42--mb2U2aBS6rOz-K3UEkcRjd9b5miU&amp;source=gmail&amp;ust=1544515666849000&amp;usg=AFQjCNF6zLQ6YkOhxM7WmuOiKDqhkTceAg">\xe5\x90\xa6</a></td>\n</tr></table>\n<table cellpadding="0" border="0" align="left" cellspacing="0" style="text-align:left"><tr align="left">\n<td width="97px" height="0px"><br></td>\n<td width="0px" height="0px"><br></td>\n</tr></table>\n<br><a href="https://www.dropbox.com/l/AAA1lamTFkr3rJdaJtbe16snNm4WttJh3jY" target="_blank" rel="noreferrer" data-saferedirecturl="https://www.google.com/url?q=https://www.dropbox.com/l/AAA1lamTFkr3rJdaJtbe16snNm4WttJh3jY&amp;source=gmail&amp;ust=1544515666849000&amp;usg=AFQjCNEkZvxqmlrlmpXMfbWx16WQ2zdx1w">\xe6\x88\x91\xe4\xb8\x8d\xe7\xa1\xae\xe5\xae\x9a</a><br>\n</td>\n<td width="20px"></td>\n</tr>\n<tr>\n<td height="20px"></td>\n<td height="20px"></td>\n<td height="20px"></td>\n</tr>\n</table>\n<br>\xe8\xaf\xa6\xe7\xbb\x86\xe4\xba\x86\xe8\xa7\xa3\xe5\xa6\x82\xe4\xbd\x95<a href="https://www.dropbox.com/l/AAB_dnXlF9BC3SgYky68umRVBHhLY8QQepc/help/1973" target="_blank" rel="noreferrer" data-saferedirecturl="https://www.google.com/url?q=https://www.dropbox.com/l/AAB_dnXlF9BC3SgYky68umRVBHhLY8QQepc/help/1973&amp;source=gmail&amp;ust=1544515666849000&amp;usg=AFQjCNEt_1YJJ144oTiMljpk0zfTacZXtg">\xe4\xbf\x9d\xe6\x8a\xa4\xe6\x82\xa8\xe7\x9a\x84\xe5\xb8\x90\xe6\x88\xb7</a>\xe3\x80\x82<br><br>\xe6\xad\xa4\xe8\x87\xb4<br>- Dropbox \xe5\x9b\xa2\xe9\x98\x9f<br>\n</td>\n<td width="36"></td>\n</tr>\n<tr><td colspan="3" height="36"></td></tr>\n</table></td></tr>\n</table>\n<table cellpadding="0" cellspacing="0" align="center" border="0">\n<tr><td height="10"></td></tr>\n<tr><td style="padding:0;border-collapse:collapse"><table cellpadding="0" cellspacing="0" align="center" border="0"><tr style="color:#a8b9c6;font-size:11px;font-family:proxima_nova,&#39;Open Sans&#39;,&#39;Lucida Grande&#39;,&#39;Segoe UI&#39;,Arial,Verdana,&#39;Lucida Sans Unicode&#39;,Tahoma,&#39;Sans Serif&#39;">\n<td width="400" align="left"></td>\n<td width="128" align="right">\xc2\xa9 2018 Dropbox</td>\n</tr></table></td></tr>\n</table>\n</td></tr></table></center></div>\n<img width="1" src="https://ci3.googleusercontent.com/proxy/BlzMkzt-UYW-nk-Boj0thJ_MqErv1rVLY8tCxvYlaurzyB1a-i_xxGUJogAypdUzCG3aHAUSXTzLTJGW8KuYPRkQ7IFSSFDOTf1nKgfZeyI=s0-d-e1-ft#https://www.dropbox.com/l/AAClJiduLqk9J2PBYHC0HT6wQPIHHZO95ig" height="1"> \xf7\xc2\xe6\x1b\x18\x00*?:=.msg3523256427469773141 a{color:#007ee6;text-decoration:none}2\x16msg35232564274697731418\x01@\x00:\xa2\x02\xe5\xb0\x8a\xe6\x95\xac\xe7\x9a\x84Tao Jianping\xef\xbc\x9a\xe6\x82\xa8\xe5\xa5\xbd\xef\xbc\x81 \xe4\xb8\x80\xe5\x8f\xb0\xe6\x96\xb0Android \xe8\xae\xbe\xe5\xa4\x87\xe5\x88\x9a\xe5\x88\x9a\xe7\x99\xbb\xe5\xbd\x95\xe4\xba\x86\xe6\x82\xa8\xe7\x9a\x84 Dropbox \xe5\xb8\x90\xe6\x88\xb7\xe3\x80\x82\xe4\xb8\xba\xe7\xa1\xae\xe4\xbf\x9d\xe6\x82\xa8\xe7\x9a\x84\xe5\xb8\x90\xe6\x88\xb7\xe5\xae\x89\xe5\x85\xa8\xef\xbc\x8c\xe8\xaf\xb7\xe5\x91\x8a\xe8\xaf\x89\xe6\x88\x91\xe4\xbb\xac\xe8\xbf\x99\xe6\x98\xaf\xe4\xb8\x8d\xe6\x98\xaf\xe6\x82\xa8\xe6\x9c\xac\xe4\xba\xba\xe7\x9a\x84\xe6\x93\x8d\xe4\xbd\x9c\xe3\x80\x82 \xe7\xa1\xae\xe5\xae\x9e\xe6\x98\xaf\xe6\x82\xa8\xe5\x9c\xa8\xe7\x99\xbb\xe5\xbd\x95\xe5\x90\x97\xef\xbc\x9f \xe5\x9c\xb0\xe7\x82\xb9\xef\xbc\x9a London, England, United Kingdom\xe9\x99\x84\xe8\xbf\x91 \xe6\x97\xb6\xe9\x97\xb4\xef\xbc\x9a Dec 10, 2018 at 8:07 am (GMT)BV<0101016797282cc3-5c1d11fc-f367-4c48-b53b-9267eb0419cd-000000@us-west-2.amazonses.com>ZM\x10\x01 \xff\xff\xff\xff\xff\xff\xff\xff\xff\x01B\x11email.dropbox.comJ\x0bdropbox.com\x08\x00\x1a\x00X\x00\x8a\x01\x14no-reply@dropbox.com\x90\x01\x01b\x1b\x08\x01\x12\x17taojianping01@gmail.com\x88\x01\xea\xdd\xa0\xb9\xf9,\xa2\x01\x02(\x02\xe8\x01\x86\x9f\x8b\xf5\xae\xd0\xdc\xbc\x16'
        '''        
        accounts = []
        auto_id = 0
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

    def _read_xml(self, xml_path):
        ''' _read_xml, set self.cur_xml_source

        Args: 
            xml_path (str): self.root.GetByPath(xm_path)
        Returns:
            xml_data (XElement)
        '''
        try:
            xml_node = self.root.GetByPath(xml_path)
            if not xml_node:
                return False
            xml_data = XElement.Parse(xml_node.read())

            self.cur_xml_source = xml_node.AbsolutePath
            return xml_data
        except:
            exc()
            return False

