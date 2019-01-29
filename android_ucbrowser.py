# coding=utf-8
__author__ = 'YangLiyuan'

import re
import time
import hashlib

import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

from PA_runtime import *
import model_browser
from model_browser import tp, exc, print_run_time, CASE_NAME
from ScriptUtils import BaseAndroidParser
import bcp_browser


# app数据库版本
VERSION_APP_VALUE = 2


def analyze_ucbrowser(node, extract_deleted, extract_source):
    ''' com.UCMobile/databases/WXStorage$ '''
    tp('android_ucbrowser.py is running ...')
    res = []

    pr = ParserResults()
    try:
        res = AndroidUCParser(node, db_name='UC_A').parse(bcp_browser.NETWORK_APP_UC, VERSION_APP_VALUE)        
    except:
        TraceService.Trace(TraceLevel.Debug,
                           'analyze_ucbrowser 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('UC浏览器')
    tp('android_ucbrowser.py is finished !')
    return pr


class AndroidUCParser(model_browser.BaseBrowserParser, BaseAndroidParser):
    def __init__(self, node, db_name):
        super(AndroidUCParser, self).__init__(node, db_name)        
        self.root = node.Parent

    def parse_main(self):
        ''' self.root: /com.UCMobile/ '''
        account_dict = self.parse_Account('databases')
        if not account_dict:
            account_dict = {
                'default_user': ''
            }
        for account_id in account_dict:
            self.cur_account_name = account_id
            self.parse_Bookmark('databases/' + account_id + '.db', 'bookmark')
            self.parse_Browserecord_SearchHistory('databases/history', 'history')
            self.parse_DownloadFile('databases/RecentFile.db', 'recent_file')
        self.parse_Cookie(['app_webview/Cookies', 'app_core_ucmobile/Cookies'], 'cookies')

    def parse_Account(self, node_path):
        ''' node_path: /databases
            头像位置:
                com.UCMobile\UCMobile\userdata\account\1883626966
            Returns:
                account_dict: {
                    'account_id': 'account_photo',
                }
        '''
        account_id_nodes = self.root.GetByPath(node_path)
        if not account_id_nodes:
            return
        account_dict = {}
        photo_path = ''
        for account_id_node in account_id_nodes.Children:
            raw_account_id = account_id_node.Name
            if raw_account_id and raw_account_id.replace('.db', '').isdigit():
                account_id = raw_account_id.replace('.db', '')
                # account_photo
                photo_node = self.root.GetByPath('UCMobile/userdata/account/'+account_id)
                if photo_node:
                    photo_path = photo_node.AbsolutePath
                    #tp('photo_path', photo_path)
                account_dict[account_id] = photo_path if photo_path else None
        
        if account_dict:
            for account_id, photo_path in account_dict.items():
                try:
                    account = model_browser.Account()
                    account.id = account_id
                    # account.name
                    # account.logindate
                    # account.source
                    # account.deleted
                    self.csm.db_insert_table_accounts(account)
                except:
                    exc()
            self.csm.db_commit()
        return account_dict

    def parse_Bookmark(self, db_path, table_name):
        ''' 'databases/' + account_id + '.db', 'bookmark'

            FieldName	        SQLType	         	
            luid	            INTEGER
            guid	            TEXT
            parent_id	        INTEGER
            title	            TEXT
            url	                TEXT
            path	            TEXT
            order_index	        INTEGER
            property	        INTEGER
            folder	            INTEGER
            last_modify_time	INTEGER
            create_time	        INTEGER
            device_type	        TEXT
            platform	        TEXT
            opt_state	        INTEGER
            sync_state	        INTEGER
            modify_flag	        INTEGER
            fingerprint	        TEXT
            ext_int1	        INTEGER
            ext_int2	        INTEGER
            ext_string1	        TEXT
        '''
        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name):
            try:
                if self._is_empty(rec, 'url', 'title') or not self._is_url(rec, 'url'):
                    continue
                bookmark = model_browser.Bookmark()
                bookmark.id        = rec['luid'].Value
                bookmark.owneruser = self.cur_account_name
                bookmark.time      = rec['create_time'].Value
                bookmark.title     = rec['title'].Value
                bookmark.url       = rec['url'].Value
                bookmark.source    = self.cur_db_source
                bookmark.deleted   = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_bookmarks(bookmark)
            except:
                exc()
        self.csm.db_commit()

    def parse_Browserecord_SearchHistory(self, db_path, table_name):
        ''' databases/history - history 浏览记录

            FieldName	    SQLType	    	
            id	            INTEGER
            name	        TEXT
            url	            TEXT
            original_url    TEXT
            visited_time    INTEGER
            host	        TEXT
            visited_count	INTEGER
            state	        INTEGER
            media_type	    INTEGER
            url_hashcode	INTEGER
            from_type	    INTEGER
            source	        TEXT
            daoliu_type	    INTEGER
            article_id	    TEXT
            channel_id	    INTEGER
            icon_url	    TEXT
            temp_1	        TEXT
            temp_2	        TEXT
            temp_3	        TEXT
        '''
        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'url', 'name') or
                    self._is_duplicate(rec, 'id') or
                        not self._is_url(rec, 'url')):
                    continue
                browser_record = model_browser.Browserecord()
                browser_record.id          = rec['id'].Value
                browser_record.name        = rec['name'].Value
                browser_record.url         = rec['url'].Value
                browser_record.datetime    = rec['visited_time'].Value
                browser_record.visit_count = rec['visited_count'].Value if rec['visited_count'].Value > 0 else 1
                browser_record.owneruser   = self.cur_account_name
                browser_record.source      = self.cur_db_source
                browser_record.deleted     = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_browserecords(browser_record)

                if browser_record.name.startswith('网页搜索_'):
                    search_history           = model_browser.SearchHistory()
                    search_history.id        = rec['id'].Value
                    search_history.name      = rec['name'].Value.replace('网页搜索_', '')
                    search_history.url       = rec['url'].Value
                    search_history.datetime  = rec['visited_time'].Value
                    search_history.owneruser = self.cur_account_name
                    search_history.source    = self.cur_db_source
                    search_history.deleted   = 1 if rec.IsDeleted else 0
                    self.csm.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.csm.db_commit()

    @print_run_time
    def parse_DownloadFile(self, db_path, table_name):
        ''' 'databases/RecentFile.db' - recent_file

            FieldName	    SQLType	    	
            id	            integer
            full_path	    text
            display_name	text
            bucket_name	    text
            modify_time	    integer
            data_type	    text
            data_source	    text
            duration	    integer
            thumbnail	    text
            install_state	integer
            size	        integer
            origin_id	    integer
            extra_param	    text
        '''
        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name):
            try:
                if self._is_empty(rec, 'display_name') or self._is_duplicate(rec, 'id'):
                    continue
                downloads = model_browser.DownloadFile()
                downloads.id             = rec['id'].Value
                # downloads.url            = rec['url'].Value
                downloads.filename       = rec['display_name'].Value
                downloads.filefolderpath = self._convert_nodepath(rec['full_path'].Value)
                downloads.totalsize      = rec['size'].Value
                # downloads.createdate     = rec['modify_time'].Value
                downloads.donedate       = rec['modify_time'].Value
                # costtime                 = downloads.donedate - downloads.createdate
                # downloads.costtime       = costtime if costtime > 0 else None  
                downloads.owneruser      = self.cur_account_name
                downloads.source         = self.cur_db_source
                downloads.deleted        = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        self.csm.db_commit()

    def _convert_nodepath(self, raw_path):
        ''' huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
        '''
        try:
            if not raw_path:
                return
            if self.rename_file_path:  # '/storage/emulated', '/data/media'
                raw_path = raw_path.replace(self.rename_file_path[0], self.rename_file_path[1])

            fs = self.root.FileSystem
            for prefix in ['', '/data', ]:
                '/storage/emulated'
                file_node = fs.GetByPath(prefix + raw_path)
                if file_node and file_node.Type == NodeType.File:
                    return file_node.AbsolutePath

                invalid_path = re.search(r'[\\:*?"<>|\r\n]+', raw_path)
                if invalid_path:
                    return 
                nodes = list(fs.Search(raw_path))
                if nodes and nodes[0].Type == NodeType.File:
                    return nodes[0].AbsolutePath
        except:
            tp('android_ucbrowser.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()
