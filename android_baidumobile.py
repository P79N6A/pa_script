
# coding=utf-8
import os
import traceback
import re
import hashlib


import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
except:
    pass
del clr
from model_browser import *
import bcp_browser

# app数据库版本
VERSION_APP_VALUE = 1


DEBUG = True
DEBUG = False


def exc():
    if DEBUG:
        traceback.print_exc()
    else:
        pass       

def analyze_baidumobile(node, extract_deleted, extract_source):
    """
        android 华为 (data/data/com.baidu.searchbox/)
    """
    pr = ParserResults()
    cache_db_name = "BaiduSearchbox"
    res = BaiduMobileParser(node, extract_deleted, extract_source, cache_db_name).parse()
    pr.Models.AddRange(res)
    pr.Build('手机百度')
    return pr

def analyze_baidumobile_lite(node, extract_deleted, extract_source):
    """
        android 华为 (data/data/com.baidu.searchbox.lite/)
    """
    pr = ParserResults()
    cache_db_name = "BaiduSearchbox_Lite"
    res = BaiduMobileParser(node, extract_deleted, extract_source, cache_db_name).parse()
    pr.Models.AddRange(res)
    pr.Build('手机百度极速版')
    return pr

class BaiduMobileParser(object):

    def __init__(self, node, extract_deleted, extract_source, cache_db_name):

        self.root = node.Parent.Parent  # data/data/com.baidu.searchbox/
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mb = MB()
        self.cachepath = ds.OpenCachePath("BaiduMobile")

        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()
        self.cache_db = self.cachepath + '\\{}_{}.db'.format(cache_db_name, hash_str)

        self.uid_list = ['anony']

    def parse(self):
        '''
            databases/SearchBox.db
            databases/\d_searchbox.db 
            databases/box_visit_history.db
            databases/downloads.db
            app_webview_baidu/Cookies
        '''
        if DEBUG or self.mb.need_parse(self.cache_db, VERSION_APP_VALUE):
            if not self._read_db('databases/SearchBox.db'):
                return              
            self.mb.db_create(self.cache_db)
            self.parse_Account("databases/SearchBox.db", 'account_userinfo')
            if self.uid_list:
                for uid in self.uid_list:
                    self.parse_Bookmark('databases/' + uid + '_searchbox.db', 'favor')
            self.parse_Bookmark('databases/anony_searchbox.db', 'favor')
            self.parse_Browserecord("databases/box_visit_history.db", 'visit_search_history')
            self.parse_Cookies('app_webview_baidu/Cookies', 'cookies')
            self.parse_DownloadFile('databases/downloads.db', 'downloads')
            self.parse_SearchHistory("databases/SearchBox.db", 'clicklog')

            if not canceller.IsCancellationRequested:
                self.mb.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.mb.db_insert_table_version(VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mb.db_commit()
            self.mb.db_close()


        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_browser.NETWORK_APP_BAIDUMOBILE, self.cache_db, tmp_dir)

        models = Generate(self.cache_db).get_models()
        return models

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
            account = Account()
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
                self.mb.db_insert_table_accounts(account)
            except:
                exc()
        try:
            self.mb.db_commit()
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
            bookmark = Bookmark()
            bookmark.id         = rec['_id'].Value
            # bookmark.account_id = rec['account_uid'].Value
            bookmark.time       = rec['createtime'].Value
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
        ''' 浏览记录 databases\box_visit_history.db 
                _ visit_feed_history
                - visit_history
                - visit_search_history
                - visit_swan_history

            - visit_search_history
            RecNo	FieldName	SQLType	
            1	_id	INTEGER False			
            2	ukey	TEXT    False			
            3	serverid	TEXT    False			
            4	tplid	TEXT    False			
            5	status	TEXT    False			
            6	title	TEXT    True		False			
            7	desc	TEXT    False			
            8	img	TEXT    False			
            9	url	TEXT    False			
            10	cmd	TEXT    False			
            11	opentype	TEXT    False			
            12	feature	TEXT    False			
            13	datatype	TEXT    True		False			
            14	parent	TEXT    False			
            15	visible	TEXT    False			
            16	enable	TEXT    False			
            17	createtime	TEXT    False			
            18	modifytime	TEXT    False			
            19	visittime	TEXT    False			
            20	visits	INTEGER False			
            21	extra1	TEXT    False			
            22	extra2	TEXT    False			
            23	isfavored	INTEGER False			
            24	uid	TEXT    False			
        '''
        if not self._read_db(db_path):
            return 
        if not self.cur_db.GetTable(table_name):
            # lite box_visit_history.db  has no visit_search_history table
            table_name = 'visit_history'
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'title', 'url') or not self._is_url(rec, 'url'):
                continue
            browser_record = Browserecord()
            browser_record.id        = rec['_id'].Value
            browser_record.name      = rec['title'].Value
            browser_record.url       = rec['url'].Value
            browser_record.datetime  = rec['createtime'].Value
            # browser_record.owneruser = rec['date'].Value
            browser_record.source    = self.cur_db_source
            browser_record.deleted   = 1 if rec.IsDeleted else 0         
            try:
                self.mb.db_insert_table_browserecords(browser_record)
            except:
                exc()
        try:
            self.mb.db_commit()
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
            downloads = DownloadFile()
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
                self.mb.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        try:
            self.mb.db_commit()
        except:
            exc()

    def parse_Cookies(self, db_path, table_name):
        """ Cookies.db - cookies
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
            cookies.createdate     = rec['path'].Value
            cookies.expiredate     = rec['creation_utc'].Value
            cookies.lastaccessdate = rec['last_access_utc'].Value
            cookies.hasexipred     = rec['has_expires'].Value
            # cookies.owneruser      = rec['owneruser'].Value
            cookies.source         = self.cur_db_source
            cookies.deleted = 1 if rec.IsDeleted else 0            
            try:
                self.mb.db_insert_table_cookies(cookies)
            except:
                exc()
        try:
            self.mb.db_commit()
        except:
            exc()

    def parse_SearchHistory(self, db_path, table_name):
        """ SearchBox.db - clicklog
        RecNo	FieldName	SQLType	Size	Precision	PKDisplay	
        1	_id	            INTEGER
        2	intent_key	        TEXT
        3	query	            TEXT
        4	hit_time	            INTEGER
        5	source	            TEXT
        """        
        if not self._read_db(db_path):
            return 
        for rec in self._read_table(table_name):
            if canceller.IsCancellationRequested:
                return
            if self._is_empty(rec, 'intent_key', 'query'):
                continue
            search_history = SearchHistory()
            search_history.id       = rec['_id'].Value
            search_history.name     = rec['query'].Value
            # search_history.url      = rec['query'].Value
            search_history.datetime = rec['hit_time'].Value
            search_history.source   = self.cur_db_source
            search_history.deleted = 1 if rec.IsDeleted else 0       
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
            return []

    def _convert_2_nodepath(self, raw_path, file_name):
        # raw_path = '/data/user/0/com.baidu.searchbox/files/template/profile.zip'
        try:
            fs = self.root.FileSystem
            if not file_name:
                raw_path_list = raw_path.split(r'/')
                file_name = raw_path_list[-1]
            # print 'raw_path, file_name', raw_path, file_name
            _path = None
            if len(file_name) > 0:
                try:
                    node = fs.Search(r'com\.baidu\.searchbox.*?{}$'.format(re.escape(file_name)))
                    for i in node:
                        _path = i.AbsolutePath
                except:
                    # print 'file_name, _path', file_name, _path
                    pass
            return _path if _path else file_name
        except:
            exc()
            print 'node:', node, 'file_name:', file_name
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