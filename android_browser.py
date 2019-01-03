# coding=utf-8
__author__ = 'YangLiyuan'

import re
import time
import hashlib

import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
except:
    pass
del clr

from PA_runtime import *
import model_browser
from model_browser import tp, exc, print_run_time, CASE_NAME
import bcp_browser


DEBUG = True
DEBUG = False

# app数据库版本
VERSION_APP_VALUE = 1

# 国产安卓手机预装浏览器类型
XIAOMI = bcp_browser.NETWORK_APP_XIAOMI
HUAWEI = bcp_browser.NETWORK_APP_HUAWEI
VIVO   = bcp_browser.NETWORK_APP_VIVO
OPPO   = bcp_browser.NETWORK_APP_OPPO
LENOVO = bcp_browser.NETWORK_APP_LENOVO


def analyze_xiaomi_browser(node, extract_deleted, extract_source):
    ''' \com.android.browser\databases$ '''

    tp('android_browser.py is running ...')
    tp(node.AbsolutePath)
    if 'media' in node.AbsolutePath:
        return 
    res = []
    pr = ParserResults()
    try:
        res = AndroidBrowserParser(node, extract_deleted, extract_source).parse(DEBUG, 
                                                                                BCP_TYPE=XIAOMI,
                                                                                VERSION_APP_VALUE=VERSION_APP_VALUE)
    except:
        TraceService.Trace(TraceLevel.Debug,
                           'analyze_browser 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('小米浏览器')
    tp('android_browser.py is finished !')
    return pr

def analyze_originbrowser(node, extract_deleted, extract_source):
    ''' \com.android.browser\databases$ '''
    tp('android_browser.py is running ...')
    res = []

    pr = ParserResults()
    try:

        res = AndroidBrowserParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug,
                           'analyze_browser 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('安卓浏览器')
    tp('android_browser.py is finished !')
    return pr


class AndroidBrowserParser(model_browser.BaseBrowserParser):
    ''' self.root: com.android.browser/
    '''
    def __init__(self, node, extract_deleted, extract_source):
        super(AndroidBrowserParser, self).__init__(node, extract_deleted, extract_source, 
                                                   app_name='AndroidBrowser')
        self.root = node.Parent.Parent
        if self.root.FileSystem.Name == 'data.tar':
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None

    def parse_main(self):
        ''' self.root: /com.android.browser/ '''
        # account_dict = self.parse_Account('databases')
        self.cur_account_name = 'default_user'
        if self._read_db('databases/browser2.db'):
            self.parse_Bookmark('bookmarks')
            self.parse_Browserecord('history')
            self.parse_SearchHistory('mostvisited')
        download_db_node = self.root.Parent.GetByPath('com.android.providers.downloads/databases/downloads.db')
        if self._read_db(db_path='', node=download_db_node):
            self.parse_DownloadFile('downloads')
        self.parse_Cookie(['app_webview/Cookies', 'app_miui_webview/Cookies'], 'cookies')

    def parse_Bookmark(self, table_name):
        ''' 'databases/browser2.db - bookmarks

            FieldName	        SQLType        	
            _id	                INTEGER
            title	            TEXT
            url	                TEXT
            folder	            INTEGER
            parent	            INTEGER 
            position	        INTEGER
            insert_after	    INTEGER
            deleted	            INTEGER
            account_name	    TEXT
            account_type	    TEXT
            sourceid	        TEXT
            version	            INTEGER
            created	            INTEGER
            modified	        INTEGER
            dirty	            INTEGER
            sync1	            TEXT
            sync2	            TEXT
            sync3	            TEXT
            sync4	            TEXT
            sync5	            TEXT
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'url', 'title') or 
                    not self._is_url(rec, 'url') or
                    rec['folder'].Value == 1 or
                    self._is_duplicate(rec, '_id')):
                    continue
                bookmark = model_browser.Bookmark()
                bookmark.id        = rec['_id'].Value
                bookmark.owneruser = self.cur_account_name
                bookmark.time      = rec['created'].Value
                bookmark.title     = rec['title'].Value
                bookmark.url       = rec['url'].Value
                bookmark.source    = self.cur_db_source
                bookmark.deleted   = 1 if rec.IsDeleted else 0
                self.mb.db_insert_table_bookmarks(bookmark)
            except:
                exc()
        self.mb.db_commit()

    def parse_Browserecord(self, table_name):
        ''' databases/browser2.db - history 浏览记录

            FieldName	SQLType	         	
            _id	            INTEGER
            title	        TEXT
            url	            TEXT
            created	        INTEGER
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
                browser_record.id          = rec['_id'].Value
                browser_record.name        = rec['title'].Value
                browser_record.url         = rec['url'].Value
                browser_record.datetime    = rec['created'].Value
                browser_record.visit_count = rec['visits'].Value if rec['visits'].Value > 0 else 1
                browser_record.owneruser   = self.cur_account_name
                browser_record.source      = self.cur_db_source
                browser_record.deleted     = 1 if rec.IsDeleted else 0
                self.mb.db_insert_table_browserecords(browser_record)
            except:
                exc()
        self.mb.db_commit()

    def parse_SearchHistory(self, table_name):
        ''' databases/history - mostvisited

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
                self.mb.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.mb.db_commit()

    @print_run_time
    def parse_DownloadFile(self, table_name):
        ''' com.android.providers.downloads/databases/downloads.db - downloadmanagement

            FieldName	            SQLType         	
            _id	                    INTEGER
            uri	                    TEXT
            method	                INTEGER
            entity	                TEXT
            no_integrity	                BOOLEAN
            hint	                TEXT
            otaupdate	                BOOLEAN
            _data	                TEXT
            mimetype	                TEXT
            destination	                INTEGER
            no_system	                BOOLEAN
            visibility	                INTEGER
            control	                INTEGER
            status	                INTEGER
            numfailed	                INTEGER
            lastmod	                    BIGINT
            notificationpackage	        TEXT
            notificationclass	        TEXT
            notificationextras	        TEXT
            cookiedata	                TEXT
            useragent	                TEXT
            referer	                    TEXT
            total_bytes	                INTEGER
            current_bytes	                INTEGER
            etag	                    TEXT
            uid	                        INTEGER
            otheruid	                INTEGER
            title	                    TEXT
            description	                TEXT
            scanned	                    BOOLEAN
            is_public_api	                INTEGER
            allow_roaming	                INTEGER
            allowed_network_types	        INTEGER
            is_visible_in_downloads_ui	    INTEGER
            bypass_recommended_size_limit	INTEGER
            mediaprovider_uri	            TEXT
            deleted	                        BOOLEAN
            errorMsg	                    TEXT
            if_range_id	                    TEXT
            allow_metered	                INTEGER
            allow_write	                    BOOLEAN
            file_create_time	            INTEGER
            downloading_current_speed	    INTEGER
            download_surplus_time	        INTEGER
            xl_accelerate_speed	            INTEGER
            downloaded_time	                INTEGER
            xl_vip_status	                INTEGER
            xl_vip_cdn_url	                TEXT
            xl_task_open_mark	            INTEGER
            download_task_thumbnail	        TEXT
            apk_package_name	            TEXT
            torrent_file_infos_hash	        TEXT
            torrent_file_count	            INTEGER
            download_type	                INTEGER             			
            download_file_hash	            TEXT
            download_extra	                TEXT
            download_apk_install_way	    INTEGER             			
            download_speedup_time	        TEXT
            download_speedup_status	        INTEGER             			
            download_speedup_mode	        INTEGER             			
            flags	                        INTEGER
            download_extra2	                TEXT
        '''
        for rec in self._read_table(table_name):
            try:
                if rec['notificationpackage'].Value != 'com.android.browser':
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
                downloads.deleted        = 1 if rec.IsDeleted else rec['deleted'].Value
                self.mb.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        self.mb.db_commit()


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

 