# coding=utf-8
__author__ = 'YangLiyuan'

import datetime
import hashlib
import json

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
from ScriptUtils import tp, exc, print_run_time, CASE_NAME, parse_decorator, base_analyze
import bcp_browser


# app数据库版本
VERSION_APP_VALUE = 4


SEARCH_ENGINES = r'((?P<keyword1>.*?)( - Google 搜尋| - Google Search|- Google 搜索| - 百度| - 搜狗搜索|_360搜索| - 国内版 Bing)$)|(^网页搜索_(?P<keyword2>.*))|'


@parse_decorator
def analyze_chrome(node, extract_deleted, extract_source):
    ''' Patterns:string>/Library/Application Support/Google/Chrome/Default/History$  '''
    return base_analyze(AppleChromeParser, 
                        node, 
                        bcp_browser.NETWORK_APP_CHROME, 
                        VERSION_APP_VALUE,
                        build_name='Chrome浏览器',
                        db_name='Chrome_i')


class BaseChromeParser(model_browser.BaseBrowserParser):
    def __init__(self, node, db_name):
        super(BaseChromeParser, self).__init__(node, db_name)        
        self.root = node.Parent.Parent
        self.download_path = None

    def parse_main(self):
        accounts = self.parse_Account('Default/Preferences')
        self.cur_account_name = accounts[0].get('email', 'default_account')

        self.parse_Bookmark('Default/Bookmarks')
        self.parse_Cookie(['Default/Cookies'], 'cookies') # cookies 由 apple_cookies.py 统一处理
        if self._read_db('Default/History'):
            URLS = self._parse_DownloadFile_urls('downloads_url_chains')
            URLID_KEYWORD = self._parse_SearchHistory_keyword('keyword_search_terms')
            self.parse_DownloadFile(URLS, 'downloads')
            self.parse_Browserecord_SearchHistory(URLID_KEYWORD, 'urls')

    def parse_Account(self, json_path):
        ''' Default/Preferences 

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
        # parse preference
        pfs = self._read_json(json_path)
        if not pfs:
            return []

        default_user = [{
            "account_id": "default_user",
            "email": "default_user",
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
                self.csm.db_insert_table_accounts(account)
            except:
                pass
        self.csm.db_commit()
        return accounts

    def parse_Bookmark(self, json_path):
        ''' Default/Bookmarks

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
                    },...
                }...
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
            is_synced = True if bookmark_folder == 'synced' else False  
            self._bookmark_from_json(_folder.get('children', []), is_synced)
        self.csm.db_commit()

    def _bookmark_from_json(self, json_list, is_synced=False):
        ''' chrome _bookmark_from_json recursively

        Args:
            json_list (list of dict):        
        Returns:
            None
        '''
        for bookmark_dict in json_list:
            bookmark_type = bookmark_dict.get('type', None)
            if  bookmark_type == 'folder' and bookmark_dict.get('children', []):
                self._bookmark_from_json(bookmark_dict.get('children', []), is_synced)
            elif bookmark_type == 'url':
                bm = model_browser.Bookmark()
                bm.id        = bookmark_dict.get('id', None)
                 # bm.owneruser = self.cur_account_name
                bm.time      = self._convert_webkit_ts(bookmark_dict.get('date_added', None))
                bm.title     = bookmark_dict.get('name', None)
                bm.url       = bookmark_dict.get('url', None)
                bm.owneruser = self.cur_account_name
                bm.is_synced = is_synced
                bm.source    = self.cur_json_source
                self.csm.db_insert_table_bookmarks(bm)
            else:
                tp('>>> chrome new bookmark type:', bookmark_type)

    def parse_Browserecord_SearchHistory(self, URLID_KEYWORD, table_name):
        ''' Default/History - urls

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
                self.csm.db_insert_table_browserecords(browser_record)

                # 补充 keyword_search_terms 表中漏掉的搜索关键字
                if not URLID_KEYWORD.has_key(rec['id'].Value):
                    _search_item = self._search_item_from_browser_record(rec['title'].Value)
                else:
                    _search_item = ''

                if URLID_KEYWORD.has_key(rec['id'].Value) or _search_item:
                    search_history = model_browser.SearchHistory()
                    search_history.name = _search_item if _search_item else URLID_KEYWORD.get(rec['id'].Value, None)
                    search_history.id        = rec['id'].Value
                    search_history.url       = rec['url'].Value
                    search_history.datetime  = browser_record.datetime
                    search_history.owneruser = self.cur_account_name
                    search_history.source    = self.cur_db_source
                    search_history.deleted   = browser_record.deleted              
                    self.csm.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.csm.db_commit()

    def _search_item_from_browser_record(self, browser_title):
        '''SEARCH_ENGINES = 
            r'((?P<keyword1>.*?)( - Google 搜尋| - Google Search|- Google 搜索\
            | - 百度| - 搜狗搜索|_360搜索| - 国内版 Bing)$)|(^网页搜索_(?P<keyword2>.*))|'
        '''
        try:
            if browser_title and not IsDBNull(browser_title):
                match_res = re.match(SEARCH_ENGINES, browser_title)
                if match_res and (match_res.group('keyword1') or match_res.group('keyword2')):
                    keyword1 = match_res.group('keyword1')
                    keyword2 = match_res.group('keyword2')
                    keyword = keyword1 if keyword1 else keyword2
                    return keyword
        except:
            exc()
            return ''

    def _parse_SearchHistory_keyword(self, table_name):
        ''' Default/History - keyword_search_terms

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

    @print_run_time
    def parse_DownloadFile(self, URLS, table_name):
        ''' Default/History - downloads

            FieldName	            SQLType
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
                downloads.filefolderpath = self._convert_nodepath(rec['target_path'].Value)
                downloads.filename       = rec['target_path'].Value.split('/')[-1]
                downloads.totalsize      = rec['total_bytes'].Value
                downloads.createdate     = self._convert_webkit_ts(rec['start_time'].Value)
                downloads.donedate       = self._convert_webkit_ts(rec['end_time'].Value)
                # costtime                 = downloads.donedate - downloads.createdate
                # downloads.costtime       = costtime if costtime > 0 else None  
                downloads.owneruser      = self.cur_account_name
                downloads.source         = self.cur_db_source
                downloads.deleted        = 1 if rec.IsDeleted else 0
                self.csm.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        self.csm.db_commit()

    def _parse_DownloadFile_urls(self, table_name):
        ''' Default/History - downloads_url_chains

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

    def _convert_nodepath(self, raw_path):
        pass


class AppleChromeParser(BaseChromeParser):
    def __init__(self, node, db_name):
        ''' Patterns:string>/Library/Application Support/Google/Chrome/Default/History$ '''
        super(AppleChromeParser, self).__init__(node, db_name)

    def _convert_nodepath(self, raw_path):
        ''' huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
        '''
        try:
            if not raw_path:
                return
            fs = self.root.FileSystem

            file_node = fs.GetByPath(raw_path)
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
