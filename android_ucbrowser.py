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
    ''' decorator '''
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


def analyze_ucbrowser(node, extract_deleted, extract_source):
    ''' com.UCMobile/databases/WXStorage$ '''
    tp('android_ucbrowser.py is running ...')
    res = []

    pr = ParserResults()
    try:
        res = UCParser(node, extract_deleted, extract_source).parse()
    except:
        TraceService.Trace(TraceLevel.Debug,
                           'analyze_ucbrowser 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc()))
    if res:
        pr.Models.AddRange(res)
        pr.Build('UC浏览器')
    tp('android_ucbrowser.py is finished !')
    return pr


class UCParser(object):
    def __init__(self, node, extract_deleted, extract_source):

        # PA.InfraLib.Files.NodeType.File
        # PA.InfraLib.Files.NodeType.Directory
        self.root = node.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.mb = model_browser.MB()
        self.cachepath = ds.OpenCachePath('UCBrowser')
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\a_uc_{}.db'.format(hash_str)
        
        if self.root.FileSystem.Name == 'data.tar':
            self.rename_file_path = ['/storage/emulated', '/data/media'] 
        else:
            self.rename_file_path = None

    def parse(self):
        if DEBUG or self.mb.need_parse(self.cache_db, VERSION_APP_VALUE):

            if not self.root.GetByPath('databases').Children:
                return []

            self.mb.db_create(self.cache_db)

            self.parse_main()

            if not canceller.IsCancellationRequested:
                self.mb.db_insert_table_version(
                    model_browser.VERSION_KEY_DB, model_browser.VERSION_VALUE_DB)
                self.mb.db_insert_table_version(
                    model_browser.VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mb.db_commit()

            self.mb.db_close()
        tmp_dir = ds.OpenCachePath('tmp')
        save_cache_path(bcp_browser.NETWORK_APP_UC, self.cache_db, tmp_dir)
        models = model_browser.Generate(self.cache_db).get_models()
        return models

    def parse_main(self):
        ''' self.root: /com.UCMobile/ '''
        account_dict = self.parse_Account('databases')
        if not account_dict:
            account_dict = {
                'default_user': ''
            }
        for account_id in account_dict:
            self.cur_account_id = account_id
            self.parse_Bookmark('databases/' + account_id + '.db', 'bookmark')
            self.parse_Browserecord_SearchHistory('databases/history', 'history')
            self.parse_Cookie(['app_webview/Cookies', 'app_core_ucmobile/Cookies'], 'cookies')
            self.parse_DownloadFile('databases/RecentFile.db', 'recent_file')

    def parse_Account(self, node_path):
        ''' node_path: /databases
            头像位置:
                com.UCMobile\UCMobile\userdata\account\1883626966
            Returns:
                account_dict: {
                    'account_id': 'account_photo',
                }
        '''
        account_id_nodes = self.root.GetByPath(node_path)
        if not account_id_nodes:
            return
        account_dict = {}
        photo_path = ''
        for account_id_node in account_id_nodes.Children:
            raw_account_id = account_id_node.Name
            if raw_account_id and raw_account_id.replace('.db', '').isdigit():
                account_id = raw_account_id.replace('.db', '')
                # account_photo
                photo_node = self.root.GetByPath('UCMobile/userdata/account/'+account_id)
                if photo_node:
                    photo_path = photo_node.AbsolutePath
                    tp('photo_path', photo_path)
                account_dict[account_id] = photo_path if photo_path else None
        
        if account_dict:
            for account_id, photo_path in account_dict.items():
                try:
                    account = model_browser.Account()
                    account.id = account_id
                    # account.name
                    # account.logindate
                    # account.source
                    # account.deleted
                    self.mb.db_insert_table_accounts(account)
                except:
                    exc()
            self.mb.db_commit()

        return account_dict

    def parse_Bookmark(self, db_path, table_name):
        ''' 'databases/' + account_id + '.db', 'bookmark'

            FieldName	        SQLType	         	
            luid	            INTEGER
            guid	            TEXT
            parent_id	        INTEGER
            title	            TEXT
            url	                TEXT
            path	            TEXT
            order_index	        INTEGER
            property	        INTEGER
            folder	            INTEGER
            last_modify_time	INTEGER
            create_time	        INTEGER
            device_type	        TEXT
            platform	        TEXT
            opt_state	        INTEGER
            sync_state	        INTEGER
            modify_flag	        INTEGER
            fingerprint	        TEXT
            ext_int1	        INTEGER
            ext_int2	        INTEGER
            ext_string1	        TEXT
        '''
        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name):
            try:
                if self._is_empty(rec, 'url', 'title') or not self._is_url(rec, 'url'):
                    continue
                bookmark = model_browser.Bookmark()
                bookmark.id        = rec['luid'].Value
                bookmark.owneruser = self.cur_account_id
                bookmark.time      = rec['create_time'].Value
                bookmark.title     = rec['title'].Value
                bookmark.url       = rec['url'].Value
                bookmark.source    = self.cur_db_source
                bookmark.deleted   = 1 if rec.IsDeleted else 0
                self.mb.db_insert_table_bookmarks(bookmark)
            except:
                exc()
        self.mb.db_commit()

    def parse_Browserecord_SearchHistory(self, db_path, table_name):
        ''' databases/history - history 浏览记录

            FieldName	    SQLType	    	
            id	            INTEGER
            name	        TEXT
            url	            TEXT
            original_url    TEXT
            visited_time    INTEGER
            host	        TEXT
            visited_count	INTEGER
            state	        INTEGER
            media_type	    INTEGER
            url_hashcode	INTEGER
            from_type	    INTEGER
            source	        TEXT
            daoliu_type	    INTEGER
            article_id	    TEXT
            channel_id	    INTEGER
            icon_url	    TEXT
            temp_1	        TEXT
            temp_2	        TEXT
            temp_3	        TEXT
        '''
        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name):
            try:
                if (self._is_empty(rec, 'url', 'name') or
                    self._is_duplicate(rec, 'id') or
                        not self._is_url(rec, 'url')):
                    continue
                browser_record = model_browser.Browserecord()
                browser_record.id          = rec['id'].Value
                browser_record.name        = rec['name'].Value
                browser_record.url         = rec['url'].Value
                browser_record.datetime    = rec['visited_time'].Value
                browser_record.visit_count = rec['visited_count'].Value if rec['visited_count'].Value > 0 else 1
                browser_record.owneruser   = self.cur_account_id
                browser_record.source      = self.cur_db_source
                browser_record.deleted     = 1 if rec.IsDeleted else 0
                self.mb.db_insert_table_browserecords(browser_record)

                if browser_record.name.startswith('网页搜索_'):
                    search_history           = model_browser.SearchHistory()
                    search_history.id        = rec['id'].Value
                    search_history.name      = rec['name'].Value.replace('网页搜索_', '')
                    search_history.url       = rec['url'].Value
                    search_history.datetime  = rec['visited_time'].Value
                    search_history.owneruser = self.cur_account_id
                    search_history.source    = self.cur_db_source
                    search_history.deleted   = 1 if rec.IsDeleted else 0
                    self.mb.db_insert_table_searchhistory(search_history)
            except:
                exc()
        self.mb.db_commit()

    def parse_Cookie(self, db_paths, table_name):
        ''' app_webview/Cookies - cookies

            FieldName	       SQLType   	
            creation_utc	    INTEGER
            host_key	        TEXT
            name	            TEXT
            value	            TEXT
            path	            TEXT
            expires_utc	        INTEGER
            secure	            INTEGER
            httponly	        INTEGER
            last_access_utc	    INTEGER
            has_expires	        INTEGER
            persistent	        INTEGER
            priority	        INTEGER
            encrypted_value	    BLOB
            firstpartyonly	    INTEGER
        '''
        for db_path in db_paths:
            if not self._read_db(db_path):
                return
            for rec in self._read_table(table_name):
                try:
                    if (self._is_empty(rec, 'creation_utc') or
                            self._is_duplicate(rec, 'creation_utc')):
                        continue
                    cookies = model_browser.Cookie()
                    cookies.id             = rec['creation_utc'].Value
                    cookies.host_key       = rec['host_key'].Value
                    cookies.name           = rec['name'].Value
                    cookies.value          = rec['value'].Value
                    cookies.createdate     = rec['creation_utc'].Value
                    cookies.expiredate     = rec['expires_utc'].Value
                    cookies.lastaccessdate = rec['last_access_utc'].Value
                    cookies.hasexipred     = rec['has_expires'].Value
                    cookies.owneruser      = self.cur_account_id
                    cookies.source         = self.cur_db_source
                    cookies.deleted        = 1 if rec.IsDeleted else 0
                    self.mb.db_insert_table_cookies(cookies)
                except:
                    exc()
        self.mb.db_commit()

    @print_run_time
    def parse_DownloadFile(self, db_path, table_name):
        ''' 'databases/RecentFile.db' - recent_file

            FieldName	    SQLType	    	
            id	            integer
            full_path	    text
            display_name	text
            bucket_name	    text
            modify_time	    integer
            data_type	    text
            data_source	    text
            duration	    integer
            thumbnail	    text
            install_state	integer
            size	        integer
            origin_id	    integer
            extra_param	    text
        '''
        if not self._read_db(db_path):
            return
        for rec in self._read_table(table_name):
            try:
                if self._is_empty(rec, 'display_name') or self._is_duplicate(rec, 'id'):
                    continue
                downloads = model_browser.DownloadFile()
                downloads.id             = rec['id'].Value
                # downloads.url            = rec['url'].Value
                downloads.filename       = rec['display_name'].Value
                downloads.filefolderpath = self._2_nodepath(rec['full_path'].Value)
                downloads.totalsize      = rec['size'].Value
                # downloads.createdate     = rec['modify_time'].Value
                downloads.donedate       = rec['modify_time'].Value
                # costtime                 = downloads.donedate - downloads.createdate
                # downloads.costtime       = costtime if costtime > 0 else None  
                downloads.owneruser      = self.cur_account_id
                downloads.source         = self.cur_db_source
                downloads.deleted        = 1 if rec.IsDeleted else 0
                self.mb.db_insert_table_downloadfiles(downloads)
            except:
                exc()
        self.mb.db_commit()

    def _2_nodepath(self, raw_path):
        ''' huawei: /data/user/0/com.baidu.searchbox/files/template/profile.zip
        '''
        try:
            if not raw_path:
                return
            if self.rename_file_path:  # '/storage/emulated', '/data/media'
                raw_path = raw_path.replace(self.rename_file_path[0], self.rename_file_path[1])

            fs = self.root.FileSystem
            for prefix in ['', '/data', ]:
                '/storage/emulated'
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
            tp('android_ucbrowser.py _conver_2_nodeapth error, raw_path:', raw_path)
            exc()

    @staticmethod
    def _is_empty(rec, *args):
        ''' 过滤 DBNull 空数据, 有一空值就跳过

        :type rec:   rec
        :type *args: str
        :rtype: bool
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
                match_ip = re.match(IP_PATTERN, rec[i].Value)
                if not match_url and not match_ip:
                    return False
            except:
                exc()
                return False
        return True

    def _read_db(self, db_path):
        ''' read_db

        :type table_name: str
        :rtype: bool                              
        '''
        try:
            node = self.root.GetByPath(db_path)
            self.cur_db = SQLiteParser.Database.FromNode(node, canceller)
            if self.cur_db is None:
                return False
            self.cur_db_source = node.AbsolutePath
            return True
        except:
            return False

    def _read_table(self, table_name, read_delete=None):
        ''' read_table

        :type table_name: str
        :type read_delete: bool
        :rtype: db.ReadTableRecords()                                       
        '''
        # 每次读表清空并初始化 self._PK_LIST
        self._PK_LIST = []
        if read_delete is None:
            read_delete = self.extract_deleted
        try:
            tb = SQLiteParser.TableSignature(table_name)
            return self.cur_db.ReadTableRecords(tb, read_delete, True)
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
