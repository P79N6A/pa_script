# coding=utf-8
__author__ = 'YangLiyuan'

import hashlib

import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
    clr.AddReference('android_chrome')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

from PA_runtime import *
import model_browser
from model_browser import tp, exc, print_run_time, CASE_NAME
from ScriptUtils import parse_decorator, BaseAndroidParser, base_analyze
from android_chrome import analyze_oppo_browser_chrome
import bcp_browser


# app数据库版本
VERSION_APP_VALUE = 2

# 国产安卓手机预装浏览器类型
NATIVE = bcp_browser.NETWORK_APP_OTHER
XIAOMI  = bcp_browser.NETWORK_APP_XIAOMI
HUAWEI  = bcp_browser.NETWORK_APP_HUAWEI
VIVO    = bcp_browser.NETWORK_APP_VIVO
OPPO    = bcp_browser.NETWORK_APP_OPPO
LENOVO  = bcp_browser.NETWORK_APP_LENOVO
MEIZU   = bcp_browser.NETWORK_APP_MEIZU

""" 安卓原生类 com.android.browser/databases/browser2.db
        # xiaomi, huawei, oppo
        # com.vivo.browser/databases/browser2.db
            bookmarks
            history
        # SearchHistory
        mostvisited                 # xiaomi 
        oppo_quicksearch_history    # oppo
        searchs                     # vivo_huawei
    # Download
        oppo
            downloads.db                
        xiaomi:
            com.android.providers.downloads/databases/downloads.db
    # Cookie
        [
            'app_webview/Cookies', 
            'app_miui_webview/Cookies',
            'app_core_ucmobile/Cookies',    # huawei
        ]

    Chrome 类:
        samsung - com.sec.android.app.sbrowser/databases/SBrowser.db
        # oppo 两种都有, 怀疑 chrome 的是旧版本
        oppo - com.android.browser/app_chromeshell/Default/History

    Lenovo
        lenovo - com.zui.browser/databases/lebrowser.db

        ''' databases/browser2.db - history 浏览记录

            'created' is 'create_time' in lebrowser.db:

    TODO 华为 OPPO 分不清, 暂时统一归为 安卓浏览器
"""

def analyze_android_browser(node, extract_deleted, extract_source):
    _root = node.Parent.Parent
    if _root.GetByPath('app_miui_webview'):
        return analyze_xiaomi_browser(node, extract_deleted, extract_source)
    # else:
    #     for _folder in _root.Children:
    #         if 'app_mz_statsapp' in _folder.Name:
    #             return analyze_meizu_browser(node, extract_deleted, extract_source)
    return analyze_native_android_browser(node, extract_deleted, extract_source)


@parse_decorator
def analyze_native_android_browser(node, extract_deleted, extract_source):
    return base_analyze(AndroidBrowserParser, node, NATIVE, VERSION_APP_VALUE, '安卓浏览器', 'Native')
 
@parse_decorator
def analyze_meizu_browser(node, extract_deleted, extract_source):
    return base_analyze(AndroidBrowserParser, node, MEIZU, VERSION_APP_VALUE, '魅族浏览器', 'Meizu')

@parse_decorator
def analyze_xiaomi_browser(node, extract_deleted, extract_source):
    return base_analyze(AndroidBrowserParser, node, XIAOMI, VERSION_APP_VALUE, '小米浏览器', 'Xiaomi')
 
# @parse_decorator
# def analyze_huawei_browser(node, extract_deleted, extract_source):
#     tp(node.AbsolutePath)
#     return base_analyze(AndroidBrowserParser, node, HUAWEI, VERSION_APP_VALUE, '华为浏览器', 'Huawei')

@parse_decorator
def analyze_oppo_browser(node, extract_deleted, extract_source):
    if node.Name == 'downloads.db': # com.android.browser
        return base_analyze(AndroidBrowserParser, node, OPPO, VERSION_APP_VALUE, 'OPPO浏览器', 'OPPO')
    elif node.Name == 'History':    # chrome
        return analyze_oppo_browser_chrome(node, extract_deleted, extract_source)

@parse_decorator
def analyze_vivo_browser(node, extract_deleted, extract_source):
    return base_analyze(AndroidBrowserParser, node, VIVO, VERSION_APP_VALUE, 'VIVO浏览器', 'VIVO')
    
@parse_decorator
def analyze_lenovo_browser(node, extract_deleted, extract_source):
    return base_analyze(AndroidBrowserParser, node, LENOVO, VERSION_APP_VALUE, '联想浏览器', 'Lenovo')


class AndroidBrowserParser(model_browser.BaseBrowserParser, BaseAndroidParser):
    ''' self.root: com.android.browser/
    '''
    def __init__(self, node, db_name):
        super(AndroidBrowserParser, self).__init__(node, db_name)
        self.root = node.Parent.Parent
        self.model_db_name = db_name

    def parse_main(self):
        ''' self.root: /com.android.browser/ '''
        # account_dict = self.parse_Account('databases')
        self.cur_account_name = 'default_user'

        if self.model_db_name == 'Lenovo' and self._read_db('databases/lebrowser.db'):
            self.parse_Bookmark('bookmark')
            self.parse_Browserecord('history')
            self._parse_SearchHistory_lenovo('search_record')                
        elif self._read_db('databases/browser2.db'):
            self.parse_Bookmark('bookmarks')
            self.parse_Browserecord('history')
            self.parse_SearchHistory()

        self.parse_DownloadFile()

        self.parse_Cookie(['app_webview/Cookies', 'app_miui_webview/Cookies', 'app_core_ucmobile/Cookies'], 'cookies')

    def parse_Bookmark(self, table_name):
        ''' 'databases/browser2.db - bookmarks

            FieldName	        SQLType        	
            _id	                INTEGER
            title	            TEXT
            url	                TEXT
            folder	            INTEGER    # lenovo is type
            parent	            INTEGER 
            position	        INTEGER
            insert_after	    INTEGER
            deleted	            INTEGER
            account_name	    TEXT
            account_type	    TEXT
            sourceid	        TEXT
            version	            INTEGER
            created	            INTEGER   # lenovo is type
            modified	        INTEGER
            dirty	            INTEGER
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'url', 'title') or 
                    not self._is_url(rec, 'url') or
                    self._is_duplicate(rec, '_id')):
                    continue
                if 'folder' in rec.Keys and rec['folder'].Value == 1:
                    continue
                bookmark = model_browser.Bookmark()
                bookmark.id        = rec['_id'].Value
                bookmark.owneruser = self.cur_account_name
                if 'created' in rec.Keys:
                    bookmark.time  = rec['created'].Value
                elif 'create_time' in rec.Keys:
                    bookmark.time  = rec['create_time'].Value
                bookmark.title     = rec['title'].Value
                bookmark.url       = rec['url'].Value
                bookmark.source    = self.cur_db_source
                bookmark.deleted   = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_bookmarks(bookmark)
            except:
                exc()
        self.csm.db_commit()

    def parse_Browserecord(self, table_name):
        ''' databases/browser2.db - history 浏览记录

            FieldName	SQLType	         	
            _id	            INTEGER
            title	        TEXT
            url	            TEXT
            created	        INTEGER   # lenovo: create_time
            date	        INTEGER
            visits	        INTEGER
            user_entered	INTEGER
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'url', 'title') or
                    self._is_duplicate(rec, '_id') or
                    not self._is_url(rec, 'url')):
                    continue
                browser_record = model_browser.Browserecord()
                browser_record.id           = rec['_id'].Value
                browser_record.name         = rec['title'].Value
                browser_record.url          = rec['url'].Value
                for data_field in ['created', 'create_time', 'date']:
                    if data_field in rec.Keys and rec[data_field].Value > 0:
                        browser_record.datetime = rec[data_field].Value
                        break
                browser_record.visit_count  = rec['visits'].Value if rec['visits'].Value > 0 else 1
                browser_record.owneruser    = self.cur_account_name
                browser_record.source       = self.cur_db_source
                browser_record.deleted      = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_browserecords(browser_record)
            except:
                exc()
        self.csm.db_commit()

    def parse_SearchHistory(self):
        if self.model_db_name in ['VIVO', 'Huawei', 'Native']:
            self._parse_SearchHistory_vivo_huawei('searches')        
        elif self.model_db_name == 'Xiaomi':
            self._parse_SearchHistory_xiaomi('mostvisited')
        elif self.model_db_name == 'OPPO':
            self._parse_SearchHistory_oppo('oppo_quicksearch_history')

    def _parse_SearchHistory_vivo_huawei(self, table_name):
        ''' databases/browser2.db - searches 无 URL

            FieldName	SQLType   	
            _id	        INTEGER
            search	    TEXT
            date	    LONG
            type	    INTEGER
            extra	    TEXT
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, '_id', 'search') or
                    self._is_duplicate(rec, '_id')):
                    continue
                search_history           = model_browser.SearchHistory()
                search_history.id        = rec['_id'].Value
                search_history.name      = rec['search'].Value
                search_history.datetime  = rec['date'].Value
                search_history.owneruser = self.cur_account_name
                search_history.source    = self.cur_db_source
                search_history.deleted   = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.csm.db_commit()

    def _parse_SearchHistory_oppo(self, table_name):
        ''' databases/browser2.db - oppo_quicksearch_history
                FieldName	                SQLType		
                _id	                        INTEGER			
                keyword	                    TEXT			
                keyword_normalized	        TEXT			
                keyword_pinyin_normalized	TEXT			
                keyword_type	INTEGER	    False	
                time	                    INTEGER			
                url	                        TEXT			
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, '_id', 'keyword') or
                    self._is_duplicate(rec, '_id') or
                    not self._is_url(rec, 'url')):
                    continue
                search_history           = model_browser.SearchHistory()
                search_history.id        = rec['_id'].Value
                search_history.name      = rec['keyword'].Value
                search_history.url       = rec['url'].Value if self._is_url(rec, 'url') else None
                search_history.datetime  = rec['time'].Value
                search_history.owneruser = self.cur_account_name
                search_history.source    = self.cur_db_source
                search_history.deleted   = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.csm.db_commit()

    def _parse_SearchHistory_xiaomi(self, table_name):
        ''' databases/browser2.db - mostvisited

            Table Columns
                FieldName	    SQLType       	
                _id	            INTEGER
                title	        TEXT
                sub_title	    TEXT
                type	        TEXT
                doc_type	    TEXT
                ads_info	    TEXT
                url	            TEXT
                web_url	        TEXT
                date	        LONG
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, '_id', 'title') or
                    self._is_duplicate(rec, '_id') or
                    rec['type'].Value != 'search'):
                    continue
                search_history           = model_browser.SearchHistory()
                search_history.id        = rec['_id'].Value
                search_history.name      = rec['title'].Value
                search_history.url       = rec['url'].Value if self._is_url(rec, 'url') else None
                search_history.datetime  = rec['date'].Value
                search_history.owneruser = self.cur_account_name
                search_history.source    = self.cur_db_source
                search_history.deleted   = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.csm.db_commit()

    def _parse_SearchHistory_lenovo(self, table_name):
        ''' databases/lebrowser.db - search_record

            Table Columns
                FieldName	SQLType     	
                _id	        INTEGER
                search	    TEXT
                date	    INTEGER
                url	        TEXT
                visits	    INTEGER
                created	    INTEGER
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, '_id', 'search') or
                    self._is_duplicate(rec, '_id')):
                    continue
                search_history           = model_browser.SearchHistory()
                search_history.id        = rec['_id'].Value
                search_history.name      = rec['search'].Value
                search_history.url       = rec['url'].Value if self._is_url(rec, 'url') else None
                search_history.datetime  = rec['date'].Value
                search_history.owneruser = self.cur_account_name
                search_history.source    = self.cur_db_source
                search_history.deleted   = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.csm.db_commit()

    @print_run_time
    def parse_DownloadFile(self):
        if self.model_db_name in ['OPPO', 'Lenovo', 'VIVO'] and self._read_db('databases/downloads.db'):
            self._parse_DownloadFile('downloads')
        else:
            download_db_node = self.root.Parent.GetByPath('com.android.providers.downloads/databases/downloads.db')
            # 适配 auto_backup
            if not download_db_node:
                download_db_node = self.root.Parent.GetByPath('com.android.providers.downloads/db/downloads.db')
            if self._read_db(db_path='', node=download_db_node):
                self._parse_DownloadFile('downloads')            

    def _parse_DownloadFile(self, table_name):
        ''' com.android.providers.downloads/databases/downloads.db - downloadmanagement
            VIVO: no notificationpackage, deleted
            
            FieldName	            SQLType         	
            _id	                    INTEGER
            uri	                    TEXT
            method	                INTEGER
            entity	                TEXT
            no_integrity	        BOOLEAN
            hint	                TEXT
            otaupdate	            BOOLEAN
            _data	                TEXT
            mimetype	            TEXT
            destination	            INTEGER
            no_system	            BOOLEAN
            visibility	            INTEGER
            control	                INTEGER
            status	                INTEGER
            numfailed	            INTEGER
            lastmod	                BIGINT
            notificationpackage	    TEXT
            notificationclass	    TEXT
            notificationextras	    TEXT
            cookiedata	            TEXT
            useragent	            TEXT
            referer	                TEXT
            total_bytes	            INTEGER
            current_bytes	        INTEGER
            etag	                TEXT
            uid	                    INTEGER
            otheruid	            INTEGER
            title	                TEXT
            description	            TEXT
            scanned	                BOOLEAN
            is_public_api	                INTEGER
            allow_roaming	                INTEGER
            allowed_network_types	        INTEGER
            is_visible_in_downloads_ui	    INTEGER
            bypass_recommended_size_limit	INTEGER
            mediaprovider_uri	            TEXT
            deleted	                        BOOLEAN
            errorMsg	                    TEXT
        '''
        for rec in self._read_table(table_name):
            try:
                if ('notificationpackage' in rec.Keys and 
                    rec['notificationpackage'].Value != 'com.android.browser'):
                    continue
                if (self._is_empty(rec, '_id', 'title') or
                    self._is_duplicate(rec, '_id')):
                    continue
                downloads = model_browser.DownloadFile()
                downloads.id             = rec['_id'].Value
                if self._is_url(rec, 'uri'):
                    downloads.url        = rec['uri'].Value
                downloads.filename       = rec['title'].Value
                downloads.filefolderpath = self._convert_nodepath(rec['_data'].Value)
                downloads.totalsize      = rec['total_bytes'].Value
                # downloads.createdate     = rec['modify_time'].Value
                downloads.donedate       = rec['lastmod'].Value
                # costtime                 = downloads.donedate - downloads.createdate
                # downloads.costtime       = costtime if costtime > 0 else None  
                downloads.owneruser      = self.cur_account_name
                downloads.source         = self.cur_db_source
                if rec.IsDeleted:
                    downloads.deleted    = 1 
                else:
                    downloads.deleted    = rec['deleted'].Value if 'deleted' in rec.Keys else 0
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
                        
            if self.rename_file_path is not None:  # '/storage/emulated', '/data/media'
                raw_path = raw_path.replace(self.rename_file_path[0], self.rename_file_path[1])
            fs = self.root.FileSystem
            for prefix in ['', '/data', ]:
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
            tp('android_browser.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()
