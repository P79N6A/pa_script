# coding=utf-8
import os
import re

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_browser')
except:
    pass
del clr
from model_browser import *

import time

def print_run_time(func):  
    def wrapper(*args, **kw):  
        local_time = time.time()  
        func(*args, **kw) 
        print 'current Function [%s] run time is %.2f' % (func.__name__ ,time.time() - local_time)  
    return wrapper

def exc():
    # exc()
    traceback.print_exc()

def analyze_baidubrowser(node, extract_deleted, extract_source):
    """
        android 华为 (data/data/com.baidu.browser.apps/)
    """
    pr = ParserResults()
    res = BaiduBrowserParser(node, extract_deleted, extract_source).parse()
    pr.Models.AddRange(res)
    pr.Build('百度浏览器')
    return pr


class BaiduBrowserParser(object):
    """  """
    def __init__(self, node, extract_deleted, extract_source):

        self.root = node.Parent.Parent  # data/data/com.baidu.browser.apps/
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mb = MB()
        self.cachepath = ds.OpenCachePath("BaiduBrowser")
        self.cachedb = self.cachepath + "\\BaiduBrowser.db"
        self.mb.db_create(self.cachedb)

    def parse(self):
        '''
            databases/dbbrowser.db
            app_webview_baidu/Cookies
        '''
        # self.parse_Account('app_webview_baidu/Cookies', 'account_userinfo')
        self.parse_Bookmark('databases/dbbrowser.db', 'bookmark')
        self.parse_Browserecord('databases/dbbrowser.db', 'history')
        self.parse_Cookies('app_webview_baidu/Cookies', 'cookies')
        self.parse_DownloadFile('databases/flyflowdownload.db', 'bddownloadtable')
        self.parse_SearchHistory('databases/dbbrowser.db', 'url_input_record')

        self.mb.db_close()
        models = Generate(self.cachedb).get_models()
        return models

    def parse_Bookmark(self, db_path, table_name):
        """
            dbbrowser.db - bookmark
            RecNo  FieldName
            1	   account_uid	        TEXT
            2	   create_time	        INTEGER
            3	   date	        INTEGER
            4	   edit_cmd	        TEXT
            5	   edit_time	        INTEGER
            6	   _id	        INTEGER
            7	   parent	        INTEGER
            8	   parent_uuid	        TEXT
            9	   platform	        TEXT
            10	   position	        INTEGER
            11	   reserve	        TEXT
            12	   sync_time	        INTEGER
            13	   sync_uuid	        TEXT
            14	   title	        TEXT
            15	   type	        INTEGER
            16	   url	        TEXT
            17	   visits	        INTEGER
        """
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if IsDBNull(rec['url'].Value) or IsDBNull(rec['url'].Value):
                continue
            bookmark = Bookmark()
            bookmark.id         = rec['_id'].Value
            # bookmark.owneruser = rec['account_uid'].Value
            bookmark.time       = rec['create_time'].Value
            bookmark.title      = rec['title'].Value
            bookmark.url        = rec['url'].Value
            bookmark.source     = self.cur_db_source
            try:
                self.mb.db_insert_table_bookmarks(bookmark)
            except:
                exc()
        try:
            self.mb.db_commit()
        except:
            exc()

    def parse_Browserecord(self, db_path, table_name):
        """
            dbbrowser.db - history - 浏览记录
            RecNo FieldName	    SQLType	Size
            1	  create_time     INTEGER
            2	  date	          INTEGER
            3	  _id	          INTEGER
            4	  reserve	      TEXT
            5	  should_ask_icon INTEGER
            6	  title	          TEXT
            7	  url	          TEXT
            8	  visits	          INTEGER			    
        """
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return            
            if IsDBNull(rec['url'].Value):
                continue
            browser_record = Browserecord()
            browser_record.id       = rec['_id'].Value
            browser_record.name     = rec['title'].Value
            browser_record.url      = rec['url'].Value
            browser_record.datetime = rec['date'].Value
            browser_record.source   = self.cur_db_source
            try:
                self.mb.db_insert_table_browserecords(browser_record)
            except:
                exc()
        try:
            self.mb.db_commit()
        except:
            exc()

    def parse_Cookies(self, db_path, table_name):
        """ app_webview_baidu/Cookies - cookies
            RecNo	FieldName
            1   	creation_utc	            INTEGER
            2   	host_key	            TEXT
            3   	name	            TEXT
            4   	value	            TEXT
            5   	path	            TEXT
            6   	expires_utc	            INTEGER
            7   	secure	            INTEGER
            8   	httponly	            INTEGER
            9   	last_access_utc	            INTEGER
            10  	has_expires	            INTEGER
            11  	persistent	            INTEGER
            12  	priority	            INTEGER
            13  	encrypted_value	BLOB
            14  	firstpartyonly	            INTEGER
        """        
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if IsDBNull(rec['creation_utc'].Value):
                continue
            cookies = Cookie()
            cookies.id             = rec['creation_utc'].Value
            cookies.host_key       = rec['host_key'].Value
            cookies.name           = rec['name'].Value
            cookies.value          = rec['value'].Value
            cookies.createdate     = rec['creation_utc'].Value
            cookies.expiredate     = rec['expires_utc'].Value
            cookies.lastaccessdate = rec['last_access_utc'].Value
            cookies.hasexipred     = rec['has_expires'].Value
            # cookies.owneruser      = rec['owneruser'].Value
            cookies.source         = self.cur_db_source
            try:
                self.mb.db_insert_table_cookies(cookies)
            except:
                exc()
        try:
            self.mb.db_commit()
        except:
            exc()
    @print_run_time
    def parse_DownloadFile(self, db_path, table_name):
        """
            databases/flyflowdownload.db - bddownloadtable

            下载目录: /storage/emulated/0/baidu/flyflow/downloads/
            RecNo	FieldName	SQLType 
            1	manual	            INTEGER
            2	quiet	            INTEGER
            3	attribute	        TEXT
            4	completetime	    LONG
            5	createdtime	        LONG
            6	style	            TEXT
            7	filename	        TEXT
            8	key	                TEXT
            9	priority	        INTEGER
            10	progressmap	        TEXT
            11	referer	            TEXT
            12	savepath	        TEXT
            13	status	            INTEGER
            14	total	            LONG
            15	current	            LONG
            16	type	            TEXT
            17	url	                TEXT
        """        
        print 'table_name:', self.root.AbsolutePath
        if not self._read_db(db_path):
            return 

        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return            
            if rec['total'].Value < 1 or not rec['url'].Value.startswith('http://'):
                continue
            downloads = DownloadFile()
            downloads.id             = rec['key'].Value
            downloads.url            = rec['url'].Value
            downloads.filename       = rec['filename'].Value
            downloads.filefolderpath = self._convert_2_nodepath(rec['savepath'].Value, downloads.filename)
            downloads.totalsize      = rec['total'].Value
            downloads.createdate     = rec['createdtime'].Value
            downloads.donedate       = rec['completetime'].Value
            costtime = downloads.donedate - downloads.createdate
            downloads.costtime       = costtime if costtime > 0 else None # 毫秒
            # downloads.owneruser      = rec['name'].Value

            downloads.source = self.cur_db_source
            try:
                self.mb.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        try:
            self.mb.db_commit()
        except:
            exc()

    def parse_SearchHistory(self, db_path, table_name):
        """
            dbbrowser.db - url_input_record - 输入记录
            RecNo	FieldName
            1	date	        INTEGER
            2	_id	            INTEGER
            3	title	        TEXT
            4	url	            TEXT
            5	visits	        INTEGER
            """
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):       
            if canceller.IsCancellationRequested:
                return                 
            if IsDBNull(rec['url'].Value) or IsDBNull(rec['url'].Value):
                continue
            search_history = SearchHistory()
            search_history.id       = rec['_id'].Value
            search_history.name     = rec['title'].Value
            search_history.url      = rec['url'].Value
            search_history.datetime = rec['date'].Value
            search_history.source   = self.cur_db_source
            try:
                self.mb.db_insert_table_searchhistory(search_history)
            except:
                exc()
        try:
            self.mb.db_commit()
        except:
            exc()

    def _read_db(self, db_path):
        """ 
            读取手机数据库
        :type table_name: str
        :rtype: bool                              
        """
        node = self.root.GetByPath(db_path)
        self.cur_db = SQLiteParser.Database.FromNode(node,canceller)
        if self.cur_db is None:
            return False
        self.cur_db_source = node.AbsolutePath
        return True

    def _read_table(self, table_name):
        """ 
            读取手机数据库 - 表
        :type table_name: str
        :rtype: db.ReadTableRecords()                                       
        """
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            return self.cur_db.ReadTableRecords(tb, self.extract_deleted, True)
        except:
            exc()


    def _convert_2_nodepath(self, raw_path, file_name):
        # raw_path = '/data/user/0/com.baidu.browser/files/template/profile.zip'
        # huawei: 
        # /data.tar/data/data/com.baidu.searchbox/files/template/profile.zip
        hwwz_pattern = '/data.tar/data/data/com.baidu.searchbox/files/template/'
        fs = self.root.FileSystem
        if not file_name:
            raw_path_list = raw_path.split(r'/')
            file_name = raw_path_list[-1]
        # if  '.' not in file_name:
        #     return 
        # else:
        #     print os.path.join(hwwz_pattern, file_name)

        #     if os.path.isfile(os.path.join(hwwz_pattern, file_name)):
        #         print os.path.join(hwwz_pattern, file_name)
        #         return os.path.join(hwwz_pattern, file_name)
                
        # print 'raw_path, file_name', raw_path, file_name

        _path = None
        if len(file_name) > 0:
            node = fs.Search(r'com\.baidu\.browser\.apps.*?{}$'.format(file_name))
            for i in node:
                _path = i.AbsolutePath
                print 'file_name, _path', file_name, _path
        # print 'baidu.browser _path', _path
        return _path