# coding=utf-8
__author__ = 'YangLiyuan'

import datetime
import hashlib
import json

import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
    clr.AddReference('apple_chrome')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

from PA_runtime import *
import model_browser
from model_browser import tp, exc, print_run_time, CASE_NAME
from ScriptUtils import BaseAndroidParser
from apple_chrome import BaseChromeParser
import bcp_browser


# app数据库版本
VERSION_APP_VALUE = 3


def parse_decorator(func):
    def wrapper(*args, **kw):
        tp('android_chrome.py {} is running ...'.format(func.__name__,))
        res = func(*args, **kw)
        tp('android_chrome.py {} is finished !'.format(func.__name__,))
        return res
    return wrapper 

@parse_decorator
def analyze_chrome(node, extract_deleted, extract_source):
    ''' android: com.android.chrome/databases/WXStorage$ 
        apple:   /Library/Application Support/Google/Chrome/Default/History$
        Patterns:string>/Library/Application Support/Google/Chrome/Default/History$ 
    '''
    res = []
    pr = ParserResults()
    try:
        parser = AndroidChromeParser(node, extract_deleted, extract_source, db_name='Chrome_A')
        res = parser.parse(BCP_TYPE=bcp_browser.NETWORK_APP_CHROME, VERSION_APP_VALUE=VERSION_APP_VALUE)           
    except:
        TraceService.Trace(TraceLevel.Debug,
                           'analyze_chrome 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('Chrome浏览器')
    return pr

@parse_decorator
def analyze_samsung_browser(node, extract_deleted, extract_source):
    if 'media' in node.AbsolutePath:
        return 
    res = []
    pr = ParserResults()
    try:
        parser = SamsungBrowserParser(node, extract_deleted, extract_source, db_name='Samsung')
        res = parser.parse(BCP_TYPE=bcp_browser.NETWORK_APP_OTHER, VERSION_APP_VALUE=VERSION_APP_VALUE)
    except:
        msg = 'analyze_chrome - analyze_samsung_browser 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc())
        TraceService.Trace(TraceLevel.Debug, msg)
    if res:
        pr.Models.AddRange(res)
        pr.Build('三星浏览器')
    return pr

@parse_decorator
def analyze_oppo_browser_chrome(node, extract_deleted, extract_source):
    if 'media' in node.AbsolutePath:
        return 
    res = []
    pr = ParserResults()
    try:
        parser = OPPOBrowserParser(node, extract_deleted, extract_source, db_name='OPPO')
        res = parser.parse(BCP_TYPE=bcp_browser.NETWORK_APP_OPPO, VERSION_APP_VALUE=VERSION_APP_VALUE)
    except:
        msg = 'analyze_chrome - analyze_oppo_browser_chrome 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc())
        TraceService.Trace(TraceLevel.Debug, msg)
    if res:
        pr.Models.AddRange(res)
        pr.Build('OPPO浏览器')
    return pr


class AndroidChromeParser(BaseChromeParser, BaseAndroidParser):
    def __init__(self, node, extract_deleted, extract_source, db_name):
        ''' Patterns: /com\.android\.chrome/app_chrome/Default/History$ 
            self.root: /com.android.chrome/app_chrome 
        '''
        super(AndroidChromeParser, self).__init__(node, extract_deleted, extract_source, db_name)
        if self.root.FileSystem.Name == 'data.tar':
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None
    
    def _convert_nodepath(self, raw_path):
        ''' huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
        '''
        try:
            if not raw_path:
                return
            if self.rename_file_path: 
                # replace: '/storage/emulated', '/data/media'
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
            tp('android_chrome.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()    


class SamsungBrowserParser(BaseChromeParser, BaseAndroidParser):
    def __init__(self, node, extract_deleted, extract_source, db_name):
        ''' Patterns: com\.sec\.android\.app\.sbrowser/app_sbrowser/Default/History '''
        super(SamsungBrowserParser, self).__init__(node, extract_deleted, extract_source, db_name)
        # self.root: com.sec.android.app.sbrowser/
        self.root = node.Parent.Parent.Parent

        if self.root.FileSystem.Name == 'data.tar':
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None

    def parse_main(self):
        accounts = self.parse_Account('app_sbrowser/Default/Preferences')
        self.cur_account_name = accounts[0].get('email', 'default_account')

        self.parse_Bookmark('databases/SBrowser.db', 'BOOKMARKS')
        self.parse_Cookie(['app_sbrowser/Default/Cookies'], 'cookies')

        if self._read_db('app_sbrowser/Default/History'):
            URLS = self._parse_DownloadFile_urls('downloads_url_chains')
            URLID_KEYWORD = self._parse_SearchHistory_keyword('keyword_search_terms')
            self.parse_DownloadFile(URLS, 'downloads')
            self.parse_Browserecord_SearchHistory(URLID_KEYWORD, 'urls')    
    
    def parse_Bookmark(self, db_path, table_name):
        ''' databases/SBrowser.db - BOOKMARKS

            FieldName	        SQLType          	
            _ID	                INTEGER
            BOOKMARK_ID	                INTEGER
            URL	                TEXT
            SURL	                TEXT
            TITLE	                TEXT
            FAVICON	                BLOB
            FOLDER	                INTEGER
            PARENT	                INTEGER
            INSERT_AFTER	                INTEGER
            POSITION	                INTEGER
            CHILDREN_COUNT	                INTEGER
            TAGS	                TEXT
            SOURCEID	                TEXT
            DELETED	                INTEGER
            CREATED	                INTEGER
            MODIFIED	                INTEGER
            DIRTY	                INTEGER
            ACCOUNT_NAME	                TEXT
            ACCOUNT_TYPE	                TEXT
            DEVICE_ID	                TEXT
            DEVICE_NAME	                TEXT
            SYNC1	                TEXT
            SYNC2	                TEXT
            SYNC3	                TEXT
            SYNC4	                TEXT
            SYNC5	                TEXT
            IS_COMMITED	                INTEGER
            bookmark	                INTEGER
            bookmark_type	                INTEGER
            EDITABLE	                INTEGER
            type	                INTEGER
            keyword	                TEXT
            description	                TEXT
            guid	                TEXT
            TOUCH_ICON	                BLOB
            DOMINANT_COLOR	        INTEGER
        '''
        if not self._read_db(db_path):
            return         
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'URL', 'TITLE') or 
                    not self._is_url(rec, 'URL') or
                    self._is_duplicate(rec, '_ID')):
                    continue
                bookmark = model_browser.Bookmark()
                bookmark.id        = rec['_ID'].Value
                bookmark.owneruser = self.cur_account_name
                bookmark.time      = rec['CREATED'].Value
                bookmark.title     = rec['TITLE'].Value
                bookmark.url       = rec['URL'].Value
                bookmark.source    = self.cur_db_source
                bookmark.deleted   = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_bookmarks(bookmark)
            except:
                exc()
        self.csm.db_commit()    

    def _convert_nodepath(self, raw_path):
        ''' huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
        '''
        try:
            if not raw_path:
                return
            if self.rename_file_path: 
                # replace: '/storage/emulated', '/data/media'
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
            tp('android_chrome.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()    


class OPPOBrowserParser(BaseChromeParser, BaseAndroidParser):
    ''' parse OPPO Chrome shell db
        no bookmark
    '''
    def __init__(self, node, extract_deleted, extract_source, db_name):
        ''' Patterns: /com.android.browser/app_chromeshell/Default/History$ '''
        super(OPPOBrowserParser, self).__init__(node, extract_deleted, extract_source, db_name)
        # self.root: com.android.browser/app_chromeshell
        self.root = node.Parent.Parent

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
                if file_node and file_node.Type == NodeType.File:
                    return file_node.AbsolutePath
                invalid_path = re.search(r'[\\:*?"<>|\r\n]+', raw_path)
                if invalid_path:
                    return 
                nodes = list(fs.Search(raw_path))
                if nodes and nodes[0].Type == NodeType.File:
                    return nodes[0].AbsolutePath
        except:
            tp('android_chrome.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()    



