# coding=utf-8
__author__ = 'YangLiyuan'

import datetime
import hashlib
import json

import clr
try:
    clr.AddReference('model_browser')
    clr.AddReference('bcp_browser')
except:
    pass
del clr

from PA_runtime import *
import model_browser
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
            TraceService.Trace(TraceLevel.Warning,
                               (msg+'{}{}').format(traceback.format_exc(), e))
    except:
        pass


def tp(*e):
    ''' Highlight print in vs '''
    if DEBUG:
        TraceService.Trace(TraceLevel.Warning, '{}'.format(e))
    else:
        pass


def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        res = func(*args, **kw)
        if DEBUG:
            msg = 'Current Function <{}> run time is {:.2} s'.format(
                func.__name__, time.time() - local_time)
            TraceService.Trace(TraceLevel.Warning, '{}'.format(msg))
        if res:
            return res
    return wrapper


def analyze_chrome(node, extract_deleted, extract_source):
    ''' Patterns:string>/Library/Application Support/Google/Chrome/Default/History$  '''
    tp('apple_chrome.py is running ...')
    tp(node.AbsolutePath)
    res = []

    pr = ParserResults()
    try:
        res = ChromeParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug,
                           'analyze_chrome 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('Chrome浏览器')
    tp('apple_chrome.py is finished !')
    return pr


class ChromeParser(object):
    def __init__(self, node, extract_deleted, extract_source):
        ''' Patterns:string>/Library/Application Support/Google/Chrome/Default/History$ '''
        self.root = node.Parent.Parent.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mb = model_browser.MB()
        self.cachepath = ds.OpenCachePath('Chrome')
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\i_chrome_{}.db'.format(hash_str)

        self.download_path = None

    def parse(self):
        if DEBUG or self.mb.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.mb.db_create(self.cache_db)

            self.parse_main()

            if not canceller.IsCancellationRequested:
                self.mb.db_insert_table_version(model_browser.VERSION_KEY_DB, model_browser.VERSION_VALUE_DB)
                self.mb.db_insert_table_version(model_browser.VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mb.db_commit()

            self.mb.db_close()
        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_browser.NETWORK_APP_CHROME, self.cache_db, tmp_dir)
        models = model_browser.Generate(self.cache_db).get_models()
        return models

    def parse_main(self):
        ''' self.root: Library/Application Support/Google '''
        accounts = self.parse_Account('Chrome/Default/Preferences')
        self.cur_account_name = accounts[0].get('email', 'default_account')

        self.parse_Bookmark('Chrome/Default/Bookmarks')
        self.parse_Cookie('Chrome/Default/Cookies', 'cookies')
        if self._read_db('Chrome/Default/History'):
            URLS = self._parse_DownloadFile_urls('downloads_url_chains')
            URLID_KEYWORD = self._parse_SearchHistory_keyword('keyword_search_terms')
            self.parse_DownloadFile(URLS, 'downloads')
            self.parse_Browserecord_SearchHistory(URLID_KEYWORD, 'urls')

    def parse_Account(self, json_path):
        ''' Chrome/Default/Preferences 
        
            "account_info": [
                {
                    "account_id": "114861829127406446564",
                    "email": "pangux01@gmail.com",
                    "full_name": "张一",
                    "gaia": "114861829127406446564",
                    "given_name": "一",
                    "hd": "no_hosted_domain",
                    "is_child_account": false,
                    "is_under_advanced_protection": false,
                    "locale": "zh-cn",
                    "picture_url": "https://lh3.googleusercontent.com/-uj9l4pzgqhs/aaaaaaaaaai/aaaaaaaaacc/j3ruasv5kz8/photo.jpg"
                }
            ],        
        '''
        # parse preferencesz
        pfs = self._read_json(json_path)
        if not pfs:
            return []

        default_user = [{
            "account_id": "default_user",
            "full_name": "default_user",
        }]
        accounts = pfs.get('account_info', default_user)
        self.download_path = set([
            pfs.get('download', {}).get('default_directory', None),
            pfs.get('savefile', {}).get('default_directory', None), 
        ])
        # parse account
        for account_dict in accounts:
            try:
                account = model_browser.Account()
                account_id     = account_dict.get('account_id', '111')
                account.id     = int(account_id[:len(account_id)/3])
                account.name   = account_dict.get('email', None)
                account.source = self.cur_json_source
                self.mb.db_insert_table_accounts(account)
            except:
                pass
        self.mb.db_commit()
        return accounts

    def parse_Bookmark(self, json_path):
        ''' Chrome/Default/Bookmarks

            {
                "checksum": "388084ac9ab95b458ccc296b77032310",
                "roots": {
                    "bookmark_bar": {
                        "children": [  ],
                        "date_added": "13182699042594594",
                        "date_modified": "0",
                        "id": "1",
                        "name": "书签栏",
                        "type": "folder"
                    },
                ...
                }
            ...
            }
        '''
        bookmark_dict = self._read_json(json_path)
        if not bookmark_dict:
            return 
        roots = bookmark_dict.get('roots', {})
        for bookmark_folder in roots:
            _folder = roots.get(bookmark_folder, {})
            if not isinstance(_folder, dict):
                continue
            self._bookmark_from_json(_folder.get('children', []))
        self.mb.db_commit()

    def _bookmark_from_json(self, json_list):
        ''' chrome _bookmark_from_json recursively

        Args:
            json_list (list of dict):        
        Returns:
            None
        '''
        for bookmark_dict in json_list:
            bookmark_type = bookmark_dict.get('type', None)
            if  bookmark_type == 'folder' and bookmark_dict.get('children', []):
                self._bookmark_from_json(bookmark_dict.get('children', []))
            elif bookmark_type == 'url':
                bm = model_browser.Bookmark()
                bm.id        = bookmark_dict.get('id', None)
                # bm.owneruser = self.cur_account_name
                bm.time      = self._convert_webkit_ts(bookmark_dict.get('date_added', None))
                bm.title     = bookmark_dict.get('name', None)
                bm.url       = bookmark_dict.get('url', None) 
                bm.owneruser = self.cur_account_name             
                bm.source    = self.cur_json_source             
                self.mb.db_insert_table_bookmarks(bm)
            else:
                tp('>>> chrome new bookmark type:', bookmark_type)

    def parse_Browserecord_SearchHistory(self, URLID_KEYWORD, table_name):
        ''' Chrome/Default/History - urls

            FieldName	    SQLType     	        	
            id	            INTEGER
            url	            LONGVARCHAR
            title	            LONGVARCHAR
            visit_count	        INTEGER
            typed_count	        INTEGER
            last_visit_time	    INTEGER
            hidden	            INTEGER
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'url', 'last_visit_time') or
                    self._is_duplicate(rec, 'id') or
                        not self._is_url(rec, 'url')):
                    continue
                browser_record = model_browser.Browserecord()
                browser_record.id          = rec['id'].Value
                browser_record.name        = rec['title'].Value
                browser_record.url         = rec['url'].Value
                browser_record.datetime    = self._convert_webkit_ts(rec['last_visit_time'].Value)
                browser_record.visit_count = rec['visit_count'].Value if rec['visit_count'].Value > 0 else 1
                browser_record.owneruser   = self.cur_account_name
                browser_record.source      = self.cur_db_source
                browser_record.deleted     = 1 if rec.IsDeleted else rec['hidden'].Value
                self.mb.db_insert_table_browserecords(browser_record)

                if URLID_KEYWORD.has_key(rec['id'].Value):
                    search_history = model_browser.SearchHistory()
                    search_history.id        = rec['id'].Value
                    search_history.name      = URLID_KEYWORD.get(rec['id'].Value, None)
                    search_history.url       = rec['url'].Value
                    search_history.datetime  = browser_record.datetime
                    search_history.owneruser = self.cur_account_name
                    search_history.source    = self.cur_db_source
                    search_history.deleted   = browser_record.deleted              
                    self.mb.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.mb.db_commit()

    def _parse_SearchHistory_keyword(self, table_name):
        ''' Chrome/Default/History - keyword_search_terms

            FieldName	    SQLType    	
            keyword_id	        INTEGER
            url_id	        INTEGER
            lower_term	        LONGVARCHAR
            term	        LONGVARCHAR
        '''
        URLID_KEYWORD = {}
        for rec in self._read_table(table_name): 
            try:      
                if (self._is_empty(rec, 'keyword_id', 'url_id', 'term') or
                    self._is_duplicate(rec, 'url_id')):
                    continue
                url_id  = rec['url_id'].Value
                keyword = rec['term'].Value
                URLID_KEYWORD[url_id] = keyword
            except:
                exc()
        return URLID_KEYWORD

    def parse_Cookie(self, db_path, table_name):
        ''' Chrome/Default/Cookies - keyword_search_terms

            FieldName	        SQLType 	     	
            creation_utc	    INTEGER
            host_key	        TEXT
            name	            TEXT
            value	            TEXT
            path	            TEXT
            expires_utc	            INTEGER
            is_secure	            INTEGER
            is_httponly	            INTEGER
            last_access_utc	        INTEGER
            has_expires	            INTEGER
            is_persistent	        INTEGER
            priority	            INTEGER
            encrypted_value	        BLOB
            firstpartyonly	        INTEGER
        '''

        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name):
            try:
                e = rec.GetEnumerator()
                tp(dir(e))
                tp(e.next)

                if (self._is_empty(rec, 'creation_utc') or
                    self._is_duplicate(rec, 'creation_utc')):
                    continue
                cookie = model_browser.Cookie()
                cookie.id             = rec['creation_utc'].Value
                cookie.host_key       = rec['host_key'].Value
                cookie.name           = rec['name'].Value
                cookie.value          = rec['value'].Value
                cookie.createdate     = rec['creation_utc'].Value
                cookie.expiredate     = rec['expires_utc'].Value
                cookie.lastaccessdate = rec['last_access_utc'].Value
                cookie.hasexipred     = rec['has_expires'].Value
                cookie.owneruser      = self.cur_account_name
                cookie.source         = self.cur_db_source
                cookie.deleted        = 1 if rec.IsDeleted else 0
                self.mb.db_insert_table_cookies(cookie)
            except:
                exc()
        self.mb.db_commit()

    @print_run_time
    def parse_DownloadFile(self, URLS, table_name):
        ''' Chrome/Default/History - downloads

            FieldName	SQLType	Size	Precision	PKDisplay               	
            id	                        INTEGER
            guid	                    VARCHAR
            current_path	            LONGVARCHAR
            target_path	                LONGVARCHAR
            start_time	                INTEGER
            received_bytes	            INTEGER
            total_bytes	                INTEGER
            state	                    INTEGER
            danger_type	                INTEGER
            interrupt_reason	        INTEGER
            hash	                    BLOB
            end_time	                INTEGER
            opened	                    INTEGER
            last_access_time	        INTEGER
            transient	                INTEGER
            referrer	                VARCHAR
            site_url	                VARCHAR
            tab_url	                    VARCHAR
            tab_referrer_url	        VARCHAR
            http_method	                VARCHAR
            by_ext_id	                VARCHAR
            by_ext_name	                VARCHAR
            etag	                    VARCHAR
            last_modified	                VARCHAR
            mime_type	VARCHAR	            255
            original_mime_type	VARCHAR	    255
        '''
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'target_path') or 
                    self._is_duplicate(rec, 'id')):
                    continue
                downloads = model_browser.DownloadFile()
                downloads.id             = rec['id'].Value
                downloads.url            = URLS.get(downloads.id, None)
                downloads.filefolderpath = self._2_nodepath(rec['target_path'].Value)
                downloads.filename       = rec['target_path'].Value.split('/')[-1]
                downloads.totalsize      = rec['total_bytes'].Value
                downloads.createdate     = self._convert_webkit_ts(rec['start_time'].Value)
                downloads.donedate       = self._convert_webkit_ts(rec['end_time'].Value)
                # costtime                 = downloads.donedate - downloads.createdate
                # downloads.costtime       = costtime if costtime > 0 else None  
                downloads.owneruser      = self.cur_account_name
                downloads.source         = self.cur_db_source
                downloads.deleted        = 1 if rec.IsDeleted else 0
                self.mb.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        self.mb.db_commit()

    def _parse_DownloadFile_urls(self, table_name):
        ''' Chrome/Default/History - downloads

        FieldName	    SQLType	     	
        id	            INTEGER
        chain_index	    INTEGER
        url	            LONGVARCHAR
        '''
        URLS = {}
        id_chain = {}
        for rec in self._read_table(table_name, read_delete=False):
            try:
                if (self._is_empty(rec, 'id', 'chain_index', 'url') or 
                    not self._is_url(rec, 'url')):
                    continue
                _id   = rec['id'].Value
                chain = rec['chain_index'].Value
                url   = rec['url'].Value
                # get max chain_index
                if id_chain.has_key(_id) and id_chain[_id] > chain:
                    continue
                id_chain[_id] = chain
                URLS[_id]     = url
            except:
                exc()     
        return URLS

    def _2_nodepath(self, raw_path):
        ''' huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
        '''
        try:
            if not raw_path:
                return
            fs = self.root.FileSystem
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
            tp('apple_chrome.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()

    def _read_json(self, json_path):
        ''' read_json set self.cur_json_source

        Args: 
            json_path (str)
        Returns:
            (bool)
        '''
        try:
            json_node = self.root.GetByPath(json_path)
            file = json_node.Data.read().decode('utf-8')
            json_data = json.loads(file)
            self.cur_json_source = json_node.AbsolutePath
            return json_data
        except:
            exc()
            return False

    def _read_db(self, db_path):
        ''' and set self.cur_db, self.cur_db_source
        
        Args:
            db_path (str): 
        Returns:
            bool: is valid db
        '''
        try:
            tp('db_path', db_path)
            node = self.root.GetByPath(db_path)
            self.cur_db = SQLiteParser.Database.FromNode(node, canceller)
            if self.cur_db is None:
                return False
            self.cur_db_source = node.AbsolutePath
            return True
        except:
            tp('db error', db_path)
            return False

    def _read_table(self, table_name, read_delete=None):
        ''' read_table
        
        Args:
            table_name (str): 
        Returns:
            (iterable): self.cur_db.ReadTableDeletedRecords(tb, ...)
        '''
        # 每次读表清空并初始化 self._PK_LIST
        self._PK_LIST = []
        if read_delete is None:
            read_delete = self.extract_deleted
        try:
            tb = SQLiteParser.TableSignature(table_name)
            ie = self.cur_db.ReadTableRecords(tb, read_delete, True)
            return ie
        except:
            exc()
            return []

    def _is_duplicate(self, rec, pk_name):
        ''' filter duplicate record

        Args:
            rec (record): 
            pk_name (str): 
        Returns:
            bool: rec[pk_name].Value in self._PK_LIST
        '''
        try:
            pk_value = rec[pk_name].Value
            if IsDBNull(pk_value) or pk_value in self._PK_LIST:
                return True
            self._PK_LIST.append(pk_value)
            return False
        except:
            exc()
            return True

    @staticmethod
    def _convert_webkit_ts(webkit_timestamp):
        ''' convert 17 digits webkit timestamp to 10 digits timestamp 

        Args:
            webkit_timestamp(int, str, float): 17 digits
        Returns:
            ts(int): 13 digits, 28800 == 8 hour, assume webkit_timestamp is UTC-8
        '''
        try:
            epoch_start = datetime.datetime(1601,1,1)
            delta = datetime.timedelta(microseconds=int(webkit_timestamp))
            ts = time.mktime((epoch_start + delta).timetuple())
            return int(ts) + 28800
        except:
            return None            

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过
        
        Args:
            rec (rec): 
            args (str): fields
        Returns:
            book:
        '''
        try:
            for i in args:
                value = rec[i].Value
                if IsDBNull(value) or value in ('', ' ', None, [], {}):
                    return True
                if isinstance(value, str) and re.search(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]', str(value)):
                    return True
            return False
        except:
            exc()
            return True

    @staticmethod
    def _is_url(rec, *args):
        ''' 匹配 URL IP
        
        Args:
            rec (rec): 
        Returns:
            bool: 
        '''
        URL_PATTERN = r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?'
        IP_PATTERN = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'

        for i in args:
            try:
                match_url = re.match(URL_PATTERN, rec[i].Value)
                match_ip = re.match(IP_PATTERN, rec[i].Value)
                if not match_url and not match_ip:
                    return False
            except:
                exc()
                return False
        return True