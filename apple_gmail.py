# coding=utf-8
__author__ = 'YangLiyuan'

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_mail')
    clr.AddReference('bcp_mail')
    clr.AddReference('ScriptUtils')
    clr.AddReference('android_gmail')
except:
    pass
del clr

import bcp_mail
import model_mail 
from ScriptUtils import CASE_NAME, exc, tp, DEBUG, base_analyze, parse_decorator, BaseParser, ProtobufDecoder
from android_gmail import GMAIL_LABEL_TYPE, AndroidGmailParser

VERSION_APP_VALUE = 1


@parse_decorator
def analyze_gmail(node, extract_deleted, extract_source):
    return base_analyze(GmailParser, 
                        node, 
                        bcp_mail.MAIL_TOOL_TYPE_OTHER, 
                        VERSION_APP_VALUE,
                        'Gmail',
                        'Gmail_i')


class GmailParser(AndroidGmailParser):
    ''' /Library/Preferences/com\.google\.Gmail\.plist$

        Library/Application Support/data/pangux03@gmail.com 

        与 android_gmail 的不同:
            account 
    '''
    def __init__(self, node, db_name):
        super(GmailParser, self).__init__(node, db_name)
        self.root = node.Parent.Parent.Parent

    def parse_main(self):
        accounts = self.parse_account('Library/Application Support/data')     
        for account in accounts:
            self.cur_account_id = account.account_id
            self.cur_account_email = account.account_email
            if self._read_db('Library/Application Support/data/' + self.cur_account_email + '/sqlitedb'):

                self.pre_parse_custom_mail_box('clusters')
                MAIL_ITEMS = self._parse_mail_items('items')
                MAIL_INFO = self._parse_mail_item_visibility('item_visibility')
                self._parse_mail_content('item_messages', MAIL_ITEMS, MAIL_INFO)
                self.parse_attachment('item_message_attachments') 
                # self.parse_contact('Contact.db', 'contact_table')

    def parse_account(self, folder_path):
        ''' 'Library/Application Support/data'
        '''        
        accounts = []
        account_id = 1

        _folder = self.root.GetByPath(folder_path)
        if not _folder:
            return []
        for account_folder in _folder.Children:
            if not self._is_email_format(email_str=account_folder.Name):
                continue
            account = model_mail.Account()
            account.account_id    = account_id
            account.account_alias = account_folder.Name
            account.account_email = account_folder.Name
            account.account_user  = account_folder.Name
            # account.source        = self.cur_xml_source
            self.csm.db_insert_table_account(account)
            accounts.append(account)   
            account_id += 1

        self.csm.db_commit()

        return accounts
