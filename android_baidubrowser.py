# coding=utf-8
__author__ = 'YangLiyuan'

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
VERSION_APP_VALUE = 2


def analyze_baidubrowser(node, extract_deleted, extract_source):
    """
        android 华为 (data/data/com.baidu.browser.apps/)
    """
    res = []
    pr = ParserResults()
    try:
        res = BaiduBrowserParser(node, extract_deleted, extract_source).parse(DEBUG, 
                                                                              BCP_TYPE=bcp_browser.NETWORK_APP_BAIDU,
                                                                              VERSION_APP_VALUE=VERSION_APP_VALUE)        
    except:
        TraceService.Trace(TraceLevel.Debug, 
                           'analyze_baidubrowser 解析新案例 "{}" 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('百度浏览器')
    return pr


class BaiduBrowserParser(model_browser.BaseBrowserParser):
    def __init__(self, node, extract_deleted, extract_source):
        super(BaiduBrowserParser, self).__init__(node, extract_deleted, extract_source, 
                                                 app_name='BaiduBrowser')
        self.root = node.Parent.Parent  # data/data/com.baidu.browser.apps/

    def parse_main(self):
        # self.parse_Account('app_webview_baidu/Cookies', 'account_userinfo')
        self.parse_Bookmark('databases/dbbrowser.db', 'bookmark')
        self.parse_Browserecord('databases/dbbrowser.db', 'history')
        self.parse_Cookie(['app_webview_baidu/Cookies'], 'cookies')
        self.parse_DownloadFile('databases/flyflowdownload.db', 'bddownloadtable')
        self.parse_SearchHistory('databases/dbbrowser.db', 'url_input_record')        

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
            if self._is_duplicate(rec, '_id'):
                continue                  
            bookmark = model_browser.Bookmark()
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
        """ dbbrowser.db - history - 浏览记录

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
            if self._is_empty(rec, 'url', 'create_time') or not self._is_url(rec, 'url'):
                continue
            if self._is_duplicate(rec, 'create_time'):
                continue                
            browser_record = model_browser.Browserecord()
            browser_record.id          = rec['_id'].Value
            browser_record.name        = rec['title'].Value
            browser_record.url         = rec['url'].Value
            browser_record.datetime    = rec['date'].Value
            browser_record.visit_count = rec['visits'].Value if rec['visits'].Value > 0 else 1
            browser_record.source      = self.cur_db_source
            browser_record.deleted     = 1 if rec.IsDeleted else 0
            try:
                self.mb.db_insert_table_browserecords(browser_record)
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
        # tp 'table_name:', self.root.AbsolutePath
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if self._is_empty(rec, 'filename', 'url') or not self._is_url(rec, 'url'):
                continue     
            if self._is_duplicate(rec, 'createdtime'):
                continue
            # tp('filename', rec['filename'].Value)
            downloads = model_browser.DownloadFile()
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
            RecNo	    FieldName
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
            if self._is_duplicate(rec, '_id'):
                continue
            search_history = model_browser.SearchHistory()
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

    def _convert_2_nodepath(self, raw_path, file_name):
        try:
            # huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
            paths = [
                '/data/com.baidu.searchbox/files/template/', 
                '/storage/emulated/0/baidu/flyflow/downloads/',
                'data/media/0/baidu/'
            ]
            fs = self.root.FileSystem
            # 新版本
            file_nodes = list(fs.Search(raw_path))
            if file_nodes:
                file_node = file_nodes[0]
                if file_node.Type == NodeType.File:
                    return file_node.AbsolutePath
            # 老版本
            if not file_name:
                raw_path_list = raw_path.split(r'/')
                file_name = raw_path_list[-1]
            for path in paths:
                # tp(self.root.FileSystem.AbsolutePath)
                # tp(os.path.join(path, file_name))
                #path_node = self.root.FileSystem.GetByPath(os.path.join(path, file_name))
                path_node = self.root.FileSystem.GetByPath('data.tar')
                # tp(self.root.FileSystem.Children)
                if path_node:
                    if path_node.Type == NodeType.File:
                        return path_node.AbsolutePath
            _path = None
            if len(file_name) > 0:
                try:
                    invalid_file_name = re.search(r'[\\/:*?"<>|\r\n]+', file_name)
                    if invalid_file_name:
                        return file_name
                    node = fs.Search(r'baidu.*?/{}$'.format(file_name))
                    for i in node:
                        _path = i.AbsolutePath
                except:
                    pass
            if _path:
                return _path
            return _path if _path else file_name
        except:
            tp('android_baidubrowser.py _conver_2_nodeapth error, file_name:', file_name)
            exc()
            return file_name
