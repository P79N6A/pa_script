# coding=utf-8
__author__ = 'YangLiyuan'

import hashlib

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr
import model_browser
from ScriptUtils import tp, exc, print_run_time, CASE_NAME
import bcp_browser

# app数据库版本
VERSION_APP_VALUE = 3


def analyze_baidumobile(node, extract_deleted, extract_source):
    """
        android 华为 (data/data/com.baidu.searchbox/)
    """
    res = []
    pr = ParserResults()
    try:
        parser = BaiduMobileParser(node, db_name="BaiduMobile")
        res = parser.parse(bcp_browser.NETWORK_APP_BAIDU, VERSION_APP_VALUE)            
    except:
        TraceService.Trace(TraceLevel.Debug, 
                           'analyze_baidumobile 解析新案例 "{}" 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('手机百度')
    return pr

def analyze_baidumobile_lite(node, extract_deleted, extract_source):
    """
        android 华为 (data/data/com.baidu.searchbox.lite/)
    """
    pr = ParserResults()
    try:
        parser = BaiduMobileParser(node, db_name="BaiduMobileLite")
        res = parser.parse(bcp_browser.NETWORK_APP_BAIDU, VERSION_APP_VALUE)        
    except:
        TraceService.Trace(TraceLevel.Debug, 
                           'analyze_baidumobile_lite 解析新案例 "{}" 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('手机百度极速版')
    return pr

class BaiduMobileParser(model_browser.BaseBrowserParser):
    def __init__(self, node, db_name):
        super(BaiduMobileParser, self).__init__(node, db_name)
        self.root = node.Parent.Parent  # data/data/com.baidu.searchbox/
        self.uid_list = ['anony']

    def parse_main(self):
        ''' databases/SearchBox.db
            databases/\d_searchbox.db 
            databases/box_visit_history.db
            databases/downloads.db
            app_webview_baidu/Cookies
            app_webview/Cookies
        '''
        self.parse_Account("databases/SearchBox.db", 'account_userinfo')
        if self.uid_list:
            for uid in self.uid_list:
                self.parse_Bookmark('databases/' + uid + '_searchbox.db', 'favor')
        self.parse_Bookmark('databases/anony_searchbox.db', 'favor')
        self.parse_Browserecord("databases/box_visit_history.db", 'visit_search_history')
        self.parse_Cookie(['app_webview_baidu/Cookies','app_webview/Cookies'], 'cookies')
        self.parse_DownloadFile('databases/downloads.db', 'downloads')
        self.parse_SearchHistory("databases/SearchBox.db", 'clicklog')

    def parse_Account(self, db_path, table_name):
        ''' SearchBox - account_userinfo
                
            clicklog         输入框关键词
            shortcuts        关键词 去重

            SearchBox.db - account_userinfo
            RecNo	FieldName	SQLType	
            1	uid	        TEXT
            2	age	        INTEGER
            3	gender	        INTEGER     gender: 0女1男2未知
            4	province	        TEXT
            5	city	        TEXT
            6	signature	        TEXT
            7	horoscope	        TEXT
            8	vip	        INTEGER
            9	level	        INTEGER
        '''       
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'uid') or rec['uid'].Value in self.uid_list:
                continue
            account = model_browser.Account()
            try:
                account.id = int(rec['uid'].Value)  # TEXT
                self.uid_list.append(rec['uid'].Value)
            except:
                continue
            # account.name    = rec[''].Value
            #account.logindate = rec['date'].Value
            account.source    = self.cur_db_source
            account.deleted = 1 if rec.IsDeleted else 0

            try:
                self.csm.db_insert_table_accounts(account)
            except:
                exc()
        try:
            self.csm.db_commit()
        except:
            exc()

    def parse_Bookmark(self, db_path, table_name):
        ''' 书签 databases/d_searchbox.db - favor

            RecNo	FieldName	
            1	_id	            INTEGER
            2	ukey	            TEXT
            3	serverid	            TEXT
            4	tplid	            TEXT
            5	status	            TEXT
            6	title	            TEXT
            7	desc	            TEXT
            8	img	            TEXT
            9	url	            TEXT
            10	cmd	            TEXT
            11	opentype	            TEXT
            12	feature	            TEXT
            13	datatype	            TEXT
            14	parent	            TEXT
            15	visible	            TEXT
            16	enable	            TEXT
            17	createtime	            TEXT
            18	modifytime	            TEXT
            19	visittime	            TEXT
            20	visits	            INTEGER
            21	extra1	            TEXT
            22	extra2	            TEXT
        '''
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if rec['datatype'].Value == '2' or not self._is_url(rec, 'url'):
                continue # datatype==2 是文件夹
            if self._is_duplicate(rec, '_id'):
                continue
            bookmark = model_browser.Bookmark()
            bookmark.id         = rec['_id'].Value
            # bookmark.account_id = rec['account_uid'].Value
            bookmark.time       = rec['createtime'].Value
            bookmark.title      = rec['title'].Value
            bookmark.url        = rec['url'].Value
            bookmark.source     = self.cur_db_source
            bookmark.deleted    = 1 if rec.IsDeleted else 0           
            try:
                self.csm.db_insert_table_bookmarks(bookmark)
            except:
                exc()
        try:
            self.csm.db_commit()
        except:
            exc()

    def parse_Browserecord(self, db_path, table_name):
        ''' 浏览记录 databases\box_visit_history.db 
                                _ visit_feed_history
                                - visit_history
                                - visit_search_history
                                - visit_swan_history

            - visit_search_history
            RecNo	    
            1	_id	            INTEGER     
            2	ukey	            TEXT        
            3	serverid	            TEXT        
            4	tplid	            TEXT        
            5	status	            TEXT        
            6	title	            TEXT	    
            7	desc	            TEXT        
            8	img	            TEXT        
            9	url	            TEXT        
            10	cmd	            TEXT        
            11	opentype	        TEXT        
            12	feature	            TEXT        
            13	datatype	        TEXT	    
            14	parent	            TEXT        
            15	visible	            TEXT        
            16	enable	            TEXT        
            17	createtime	            TEXT        
            18	modifytime	            TEXT        
            19	visittime	            TEXT        
            20	visits	            INTEGER     
            21	extra1	            TEXT        
            22	extra2	            TEXT        
            23	isfavored	        INTEGER     
            24	uid	            TEXT        
        '''
        if not self._read_db(db_path):
            return 
        if not self.cur_db.GetTable(table_name):
            # lite box_visit_history.db  has no 'visit_search_history' table
            table_name = 'visit_history'
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'title', 'url') or not self._is_url(rec, 'url'):
                continue
            if self._is_duplicate(rec, '_id'):
                continue                    
            browser_record = model_browser.Browserecord()
            browser_record.id          = rec['_id'].Value
            browser_record.name        = rec['title'].Value
            browser_record.url         = rec['url'].Value
            browser_record.datetime    = rec['createtime'].Value
            browser_record.visit_count = rec['visits'].Value if rec['visits'].Value > 0 else 1
            # browser_record.owneruser   = rec['date'].Value
            browser_record.source      = self.cur_db_source
            browser_record.deleted     = 1 if rec.IsDeleted else 0
            try:
                self.csm.db_insert_table_browserecords(browser_record)
            except:
                exc()
        try:
            self.csm.db_commit()
        except:
            exc()

    def parse_DownloadFile(self, db_path, table_name):
        """ downloads.db - downloads
            RecNo	FieldName	SQLType	Size	
            1	_id	            INTEGER			
            2	uri	            TEXT			
            3	method	            INTEGER			
            4	entity	            TEXT			
            5	no_integrity	            BOOLEAN			
            6	hint	            TEXT			
            7	otaupdate	            BOOLEAN			
            8	_data	            TEXT			
            9	mimetype	            TEXT			
            10	destination	            INTEGER			
            11	no_system	            BOOLEAN			
            12	visibility	            INTEGER			
            13	control	            INTEGER			
            14	status	            INTEGER			
            15	numfailed	            INTEGER			
            16	lastmod	            BIGINT			
            17	notificationpackage	            TEXT			
            18	notificationclass	            TEXT			
            19	notificationextras	            TEXT			
            20	cookiedata	            TEXT			
            21	useragent	            TEXT			
            22	referer	            TEXT			
            23	total_bytes	            INTEGER			
            24	current_bytes	            INTEGER			
            25	etag	            TEXT			
            26	uid	            INTEGER			
            27	otheruid	            INTEGER			
            28	title	            TEXT			
            29	description	            TEXT			
            30	scanned	            BOOLEAN			
            31	is_public_api	            INTEGER			
            32	allow_roaming	            INTEGER			
            33	allowed_network_types	            INTEGER			
            34	is_visible_in_downloads_ui	            INTEGER			
            35	bypass_recommended_size_limit	    INTEGER			
            36	mediaprovider_uri	            TEXT			
            37	deleted	            BOOLEAN			
            38	range_start_byte	            INTEGER			
            39	range_end_byte	            INTEGER						
            40	range_byte	            TEXT			
            41	boundary	            TEXT			
            42	downloadMod	            INTEGER			
        """        
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name): 
            if canceller.IsCancellationRequested:
                return   
            if self._is_empty(rec, 'uri', 'title') or not self._is_url(rec, 'uri'):
                continue
            if self._is_duplicate(rec, '_id'):
                continue                    
            downloads = model_browser.DownloadFile()
            downloads.id             = rec['_id'].Value
            downloads.url            = rec['uri'].Value
            downloads.filename       = rec['title'].Value
            downloads.filefolderpath = self._convert_2_nodepath(rec['_data'].Value, downloads.filename)
            downloads.totalsize      = rec['total_bytes'].Value
            # downloads.createdate     = rec['createdtime'].Value
            downloads.donedate       = rec['lastmod'].Value
            # downloads.costtime       = downloads.donedate - downloads.createdate # 毫秒
            # downloads.owneruser      = rec['name'].Value
            downloads.deleted = 1 if rec.IsDeleted else rec['deleted'].Value       
            downloads.source = self.cur_db_source
            try:
                self.csm.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        try:
            self.csm.db_commit()
        except:
            exc()

    def parse_SearchHistory(self, db_path, table_name):
        """ SearchBox.db - clicklog
        RecNo	FieldName	SQLType	Size	Precision	PKDisplay	
        1	_id	            INTEGER
        2	intent_key	        TEXT
        3	query	            TEXT
        4	hit_time	        INTEGER
        5	source	            TEXT
        """        
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'intent_key', 'query') or self._is_duplicate(rec, '_id'):
                continue
            search_history = model_browser.SearchHistory()
            search_history.id       = rec['_id'].Value
            search_history.name     = rec['query'].Value
            # search_history.url      = rec['query'].Value
            search_history.datetime = rec['hit_time'].Value
            search_history.source   = self.cur_db_source
            search_history.deleted = 1 if rec.IsDeleted else 0       
            try:
                self.csm.db_insert_table_searchhistory(search_history)
            except:
                exc()
        try:
            self.csm.db_commit()
        except:
            exc()

    def _convert_2_nodepath(self, raw_path, file_name):
        # raw_path = '/data/user/0/com.baidu.searchbox/files/template/profile.zip'
        try:
            fs = self.root.FileSystem
            file_node = list(fs.Search(raw_path))
            if file_node:
                path = file_node[0].AbsolutePath
                if os.path.isfile(path):
                    return path
            if not file_name:
                raw_path_list = raw_path.split(r'/')
                file_name = raw_path_list[-1]
            # print 'raw_path, file_name', raw_path, file_name
            _path = None
            if len(file_name) > 0:
                try:
                    node = fs.Search(r'baidu.*?/{}$'.format(file_name))
                    for i in node:
                        _path = i.AbsolutePath
                except:
                    # print 'file_name, _path', file_name, _path
                    pass
            return _path if _path else file_name
        except:
            exc()
            print 'raw_path:', raw_path, 'file_name:', file_name
            return file_name

