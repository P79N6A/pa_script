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

from model_browser import *
import bcp_browser


DEBUG = True
DEBUG = False

# app数据库版本
VERSION_APP_VALUE = 1

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

def print_run_time(func):  
    def wrapper(*args, **kw):  
        local_time = time.time()  
        func(*args, **kw) 
        if DEBUG:
            print 'current Function [%s] run time is %.2f' % (func.__name__ ,time.time() - local_time)  
    return wrapper

def analyze_baidubrowser(node, extract_deleted, extract_source):
    """
        android 华为 (data/data/com.baidu.browser.apps/)
    """
    res = []
    pr = ParserResults()
    try:
        res = BaiduBrowserParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug, 
                           'analyze_baidubrowser 解析新案例 "{}" 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
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
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()
        self.cache_db = self.cachepath + '\\{}.db'.format(hash_str)

    def parse(self):
        '''
            databases/dbbrowser.db
            app_webview_baidu/Cookies
        '''
        test_p(self.root.AbsolutePath)
        if DEBUG or self.mb.need_parse(self.cache_db, VERSION_APP_VALUE):
            if not self._read_db('databases/dbbrowser.db'):
                return []
            self.mb.db_create(self.cache_db)
            # self.parse_Account('app_webview_baidu/Cookies', 'account_userinfo')
            self.parse_Bookmark('databases/dbbrowser.db', 'bookmark')
            self.parse_Browserecord('databases/dbbrowser.db', 'history')
            self.parse_Cookies('app_webview_baidu/Cookies', 'cookies')
            self.parse_DownloadFile('databases/flyflowdownload.db', 'bddownloadtable')
            self.parse_SearchHistory('databases/dbbrowser.db', 'url_input_record')

            if not canceller.IsCancellationRequested:
                self.mb.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.mb.db_insert_table_version(VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mb.db_commit()
                
            self.mb.db_close()
        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_browser.NETWORK_APP_BAIDU, self.cache_db, tmp_dir)

        models = Generate(self.cache_db).get_models()
        test_p('匹配 百度浏览器, return models')

        return models

    def parse_Bookmark(self, db_path, table_name):
        """ dbbrowser.db - bookmark

            RecNo  FieldName
            1	   account_uid	        TEXT
            2	   create_time	        INTEGER
            3	   date	            INTEGER
            4	   edit_cmd	        TEXT
            5	   edit_time	    INTEGER
            6	   _id	            INTEGER
            7	   parent	        INTEGER
            8	   parent_uuid	        TEXT
            9	   platform	        TEXT
            10	   position	        INTEGER
            11	   reserve	        TEXT
            12	   sync_time	    INTEGER
            13	   sync_uuid	    TEXT
            14	   title	        TEXT
            15	   type	        INTEGER
            16	   url	        TEXT
            17	   visits	        INTEGER
        """
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if self._is_empty(rec, 'url', 'title') or not self._is_url(rec, 'url'):
                continue
            bookmark = Bookmark()
            bookmark.id         = rec['_id'].Value
            # bookmark.owneruser = rec['account_uid'].Value
            bookmark.time       = rec['create_time'].Value
            bookmark.title      = rec['title'].Value
            bookmark.url        = rec['url'].Value
            bookmark.source     = self.cur_db_source
            bookmark.deleted    = 1 if rec.IsDeleted else 0                  
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
            if self._is_empty(rec, 'url') or not self._is_url(rec, 'url'):
                continue
            browser_record = Browserecord()
            browser_record.id       = rec['_id'].Value
            browser_record.name     = rec['title'].Value
            browser_record.url      = rec['url'].Value
            browser_record.datetime = rec['date'].Value
            browser_record.source   = self.cur_db_source
            browser_record.deleted  = 1 if rec.IsDeleted else 0            
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
            if self._is_empty(rec, 'creation_utc'):
                continue
            cookies = Cookie()
            # cookies.id
            cookies.host_key       = rec['host_key'].Value
            cookies.name           = rec['name'].Value
            cookies.value          = rec['value'].Value
            cookies.createdate     = rec['creation_utc'].Value
            cookies.expiredate     = rec['expires_utc'].Value
            cookies.lastaccessdate = rec['last_access_utc'].Value
            cookies.hasexipred     = rec['has_expires'].Value
            # cookies.owneruser      = rec['owneruser'].Value
            cookies.source         = self.cur_db_source
            cookies.deleted        = 1 if rec.IsDeleted else 0              
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
        """ databases/flyflowdownload.db - bddownloadtable

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
        # test_p 'table_name:', self.root.AbsolutePath
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if self._is_empty(rec, 'filename') or not self._is_url(rec, 'url'):
                continue     
            if canceller.IsCancellationRequested:
                return            
            test_p('filename', rec['filename'].Value)
            downloads = DownloadFile()
            downloads.url            = rec['url'].Value
            downloads.filename       = rec['filename'].Value
            downloads.filefolderpath = self._convert_2_nodepath(rec['savepath'].Value, downloads.filename)
            downloads.totalsize      = rec['total'].Value
            downloads.createdate     = rec['createdtime'].Value
            downloads.donedate       = rec['completetime'].Value
            costtime = downloads.donedate - downloads.createdate
            downloads.costtime       = costtime if costtime > 0 else None # 毫秒
            # downloads.owneruser      = rec['name'].Value
            downloads.source         = self.cur_db_source
            downloads.deleted        = 1 if rec.IsDeleted else 0
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
            if self._is_empty(rec, 'url', 'title') or not self._is_url(rec, 'url'):
                continue
            search_history = SearchHistory()
            search_history.id       = rec['_id'].Value
            search_history.name     = rec['title'].Value
            search_history.url      = rec['url'].Value
            search_history.datetime = rec['date'].Value
            search_history.source   = self.cur_db_source
            search_history.deleted  = 1 if rec.IsDeleted else 0                
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
        self.cur_db = SQLiteParser.Database.FromNode(node, canceller)
        if self.cur_db is None:
            return False
        self.cur_db_source = node.AbsolutePath
        return True

    def _read_table(self, table_name, read_deleted=None):
        """ 
            读取手机数据库 - 表

        默认全部读取, read_deleted 为 True: 只读删除的数据, 为 False 时, 只读未删除的数据
        :type table_name: str
        :type extract_deleted: bool
        :rtype: db.ReadTableRecords()                                       
        """
        try:
            tb = SQLiteParser.TableSignature(table_name)  
            if read_deleted is None:
                return self.cur_db.ReadTableRecords(tb, self.extract_deleted, True)
            elif read_deleted is True:
                return self.cur_db.ReadTableDeletedRecords(tb, False)
            elif read_deleted is False:
                return self.cur_db.ReadTableRecords(tb, False, True)
        except:
            exc()           
            return []

    def _convert_2_nodepath(self, raw_path, file_name):
        try:
            # 判断文件名是否合法
            invalid_file_name = re.search(r'[\\/:*?"<>|\r\n]+', file_name)
            if invalid_file_name:
                return None
            # 已知可能存在的路径 huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
            paths = [
                '/data.tar/data/data/com.baidu.searchbox/files/template/', 
                '/storage/emulated/0/baidu/flyflow/downloads/',
            ]
            fs = self.root.FileSystem
            if not file_name:
                raw_path_list = raw_path.split(r'/')
                file_name = raw_path_list[-1]
            for path in paths:
                if os.path.isfile(os.path.join(path, file_name)):
                    return os.path.join(path, file_name)
            # search
            _path = None
            if len(file_name) > 0:
                try:
                    node = fs.Search(r'com\.baidu\.browser\.apps.*?/{}$'.format(re.escape(file_name)))
                    for i in node:
                        _path = i.AbsolutePath
                except:
                    pass
            return _path if _path else file_name
        except:
            test_p('android_baidubrowser.py _conver_2_nodeapth error, file_name:', file_name)
            exc()
            return file_name

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull, 空数据 
        
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        for i in args:
            if IsDBNull(rec[i].Value) or rec[i].Value in ('', ' ', None, [], {}):
                return True
        return False

    @staticmethod
    def _is_url(rec, *args):
        ''' 匹配 URL IP

        严格匹配
        :type rec:   rec
        :type *args: str
        :rtype: bool
        '''
        URL_PATTERN = r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?'
        IP_PATTERN = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'

        for i in args:
            try:
                match_url = re.match(URL_PATTERN, rec[i].Value)
                match_ip  = re.match(IP_PATTERN, rec[i].Value)
                
                if not match_url and not match_ip:
                    return False
            except:
                exc()
                return False  
        return True