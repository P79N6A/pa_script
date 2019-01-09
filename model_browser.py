# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import json
import hashlib
import datetime
import System
import System.Data.SQLite as SQLite
from PA.InfraLib.Utils import ConvertHelper
import sqlite3
from ScriptUtils import CASE_NAME, exc, tp, print_run_time, BaseParser

VERSION_KEY_DB  = 'db'
VERSION_VALUE_DB = 6

VERSION_KEY_APP = 'app'


SQL_CREATE_TABLE_BOOKMARK = '''
    CREATE TABLE IF NOT EXISTS bookmark(
        id INTEGER,
        time INTEGER,
        title TEXT,
        url TEXT,
        owneruser TEXT,
        is_synced BOOL,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_BOOKMARK = '''
    INSERT INTO 
    bookmark
        (id, time, title, url, owneruser, is_synced, source, deleted, repeated) 
    values
        (? ,? ,? ,? ,? ,? ,?, ?, ?)
    '''

SQL_CREATE_TABLE_SAVEPAGE = '''
    CREATE TABLE IF NOT EXISTS savepage(
        id INTEGER,
        time INTEGER,
        title TEXT,
        url TEXT,
        filename TEXT,
        filesize INTEGER,
        savepath TEXT,
        owneruser TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_SAVEPAGE = '''
    INSERT INTO savepage(id, time, title, url, filename, filesize, savepath, owneruser, source, deleted, repeated) values(? ,? ,? ,? ,? ,? ,? ,? ,? ,?, ?)
    '''

SQL_CREATE_TABLE_SEARCHHISTORY = '''
    CREATE TABLE IF NOT EXISTS search_history(
        id INTEGER,
        name TEXT,
        url TEXT,
        datetime INTEGER,
        owneruser TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_SEARCHHISTORY = '''
    INSERT INTO search_history(id, name, url, datetime, owneruser, source, deleted, repeated) values(? ,? ,? ,? ,? ,? ,?, ?)
    '''

SQL_CREATE_TABLE_DOWNLOADFILES = '''
    CREATE TABLE IF NOT EXISTS download_files(
        id INTEGER,
        url TEXT,
        filename TEXT,
        filefolderpath TEXT,
        totalsize INTEGER,
        createdate INTEGER,
        donedate INTEGER,
        costtime INTEGER,
        owneruser TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_DOWNLOADFILES = '''
    INSERT INTO download_files(id, url, filename, filefolderpath, totalsize, createdate, donedate, costtime, owneruser, source, deleted, repeated) values(? ,?, ? ,? ,? ,? ,? ,? ,? ,? ,? ,?)
    '''

SQL_CREATE_TABLE_BROWSERECORDS = '''
    CREATE TABLE IF NOT EXISTS browse_records(
        id INTEGER,
        name TEXT,
        url TEXT,
        datetime INTEGER,
        owneruser TEXT,
        visit_count INTEGER DEFAULT 1,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_BROWSERECORDS = '''
    INSERT INTO 
    browse_records
        (id, name, url, datetime, owneruser, visit_count, source, deleted, repeated) 
    values
        (? ,? ,? ,? ,? ,? ,?, ?, ?)
    '''

SQL_CREATE_TABLE_ACCOUNTS = '''
    CREATE TABLE IF NOT EXISTS account(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        logindate INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_ACCOUNTS = '''
    INSERT INTO account(id, name, logindate, source, deleted, repeated) values(? ,? ,? ,? ,? ,?)
    '''

SQL_CREATE_TABLE_FILEINFO = '''
    CREATE TABLE IF NOT EXISTS file_info(
        id INTEGER,
        filepath TEXT,
        filename TEXT,
        size INTEGER,
        modifieddate INTEGER,
        title TEXT,
        owneruser TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_FILEINFO = '''
    INSERT INTO file_info(id, filepath, filename, size, modifieddate, title, owneruser, source, deleted, repeated) values(? ,? ,? ,? ,? ,? ,? ,? ,?, ?)
    '''

SQL_CREATE_TABLE_PLUGIN = '''
    CREATE TABLE IF NOT EXISTS plugin(
        id INTEGER,
        title TEXT,
        url TEXT,
        packagename TEXT,
        packagesize INTEGER,
        isinstall INTEGER,
        installdate INTEGER,
        isdelete INTEGER,
        deletedate INTEGER,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_PLUGIN = '''
    INSERT INTO plugin(id, title, url, packagename, packagesize, isinstall, installdate, isdelete, deletedate, source, deleted, repeated) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_COOKIES = '''
    CREATE TABLE IF NOT EXISTS cookies(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        host_key TEXT,
        name TEXT,
        value TEXT,
        createdate INTEGER,
        expiredate INTEGER,
        lastaccessdate INTEGER,
        hasexipred INTEGER,
        owneruser TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_COOKIES = '''
    INSERT INTO cookies(id, host_key, name, value, createdate, expiredate, lastaccessdate, hasexipred, owneruser, source, deleted, repeated) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


class MB(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def db_create(self,db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                print('model_browser db_create() remove %s error: %s' % (db_path, e))
                
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_create_table()
        self.db_commit()

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_remove(self, db_path):
        try:
            os.remove(db_path)
        except Exception as e:
            print('model_browser db_create() remove %s error: %s' % (db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_ACCOUNTS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_BOOKMARK
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_BROWSERECORDS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_DOWNLOADFILES
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FILEINFO
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SAVEPAGE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SEARCHHISTORY
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_PLUGIN
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_COOKIES
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_accounts(self, Account):
        self.db_insert_table(SQL_INSERT_TABLE_ACCOUNTS, Account.get_values())

    def db_insert_table_bookmarks(self, Bookmark):
        self.db_insert_table(SQL_INSERT_TABLE_BOOKMARK, Bookmark.get_values())

    def db_insert_table_browserecords(self, BrowseRecord):
        self.db_insert_table(SQL_INSERT_TABLE_BROWSERECORDS, BrowseRecord.get_values())

    def db_insert_table_downloadfiles(self, DownloadFile):
        self.db_insert_table(SQL_INSERT_TABLE_DOWNLOADFILES, DownloadFile.get_values())

    def db_insert_table_fileinfos(self, FileInfo):
        self.db_insert_table(SQL_INSERT_TABLE_FILEINFO, FileInfo.get_values())

    def db_insert_table_savepages(self, SavePage):
        self.db_insert_table(SQL_INSERT_TABLE_SAVEPAGE, SavePage.get_values())

    def db_insert_table_searchhistory(self, SearchHistory):
        self.db_insert_table(SQL_INSERT_TABLE_SEARCHHISTORY, SearchHistory.get_values())

    def db_insert_table_plugin(self, Plugin):
        self.db_insert_table(SQL_INSERT_TABLE_PLUGIN, Plugin.get_values())

    def db_insert_table_cookies(self, Cookie):
        self.db_insert_table(SQL_INSERT_TABLE_COOKIES, Cookie.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    '''
    版本检测分为两部分
    如果中间数据库结构改变，会修改db_version
    如果app增加了新的内容，需要修改app_version
    只有db_version和app_version都没有变化时，才不需要重新解析
    '''
    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key,version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except:
            pass

        while row is not None:
            if row[0] == VERSION_KEY_DB and row[1] == VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == VERSION_KEY_APP and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)


class Column(object):
    def __init__(self):
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if IsDBNull(value):
            self.__dict__[name] = None
        else:
            if isinstance(value, str):
                value = re.compile('[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]').sub(' ', value)                 
            self.__dict__[name] = value

    def get_values(self):
        return (self.source, self.deleted, self.repeated)


class Account(Column):
    def __init__(self):
        super(Account, self).__init__()
        self.id = None         # INTEGER PRIMARY KEY AUTOINCREMENT
        self.name = None       # TEXT
        self.logindate = None  # INTEGER

    def get_values(self):
        return (self.id, self.name, self.logindate) + super(Account, self).get_values()


class Bookmark(Column):
    def __init__(self):
        super(Bookmark, self).__init__()
        self.id = None
        self.time = None
        self.title = None
        self.url = None
        self.owneruser = 'default_user'
        self.is_synced = False  # 是否为同步的书签

    def get_values(self):
        return (self.id, self.time, self.title, self.url, self.owneruser, self.is_synced) + super(Bookmark, self).get_values()


class Browserecord(Column):
    def __init__(self):
        super(Browserecord, self).__init__()
        self.id = None
        self.name = None
        self.url = None
        self.datetime = None
        self.owneruser = 'default_user'
        self.visit_count = 1

    def get_values(self):
        return (self.id, self.name, self.url, self.datetime, self.owneruser, self.visit_count) + super(Browserecord, self).get_values()


class DownloadFile(Column):
    def __init__(self):
        super(DownloadFile, self).__init__()
        self.id = None
        self.url = None
        self.filename = None
        self.filefolderpath = None
        self.totalsize = None
        self.createdate = None
        self.donedate = None
        self.costtime = None
        self.owneruser = 'default_user'

    def get_values(self):
        return (self.id, self.url, self.filename, self.filefolderpath, self.totalsize, self.createdate, 
            self.donedate, self.costtime, self.owneruser) + super(DownloadFile, self).get_values()


class FileInfo(Column):
    def __init__(self):
        super(FileInfo, self).__init__()
        self.id = None
        self.filepath = None
        self.filename = None
        self.size = None
        self.modified = None
        self.title = None
        self.owneruser = 'default_user'

    def get_values(self):
        return (self.id, self.filepath, self.filename, self.size, self.modified, 
            self.title, self.owneruser) + super(FileInfo, self).get_values()


class SavePage(Column):
    def __init__(self):
        super(SavePage, self).__init__()
        self.id = None
        self.time = None
        self.title = None
        self.url = None
        self.filename = None
        self.filesize = None
        self.savepath = None
        self.owneruser = 'default_user'

    def get_values(self):
        return (self.id, self.time, self.title, self.url, self.filename, self.filesize,
            self.savepath, self.owneruser) + super(SavePage, self).get_values()


class SearchHistory(Column):
    def __init__(self):
        super(SearchHistory, self).__init__()
        self.id = None
        self.name = None
        self.url = None
        self.datetime = None
        self.owneruser = 'default_user'

    def get_values(self):
        return (self.id, self.name, self.url, self.datetime, self.owneruser) + super(SearchHistory, self).get_values()


class Plugin(Column):
    def __init__(self):
        super(Plugin, self).__init__()
        self.id = None
        self.title = None
        self.url = None
        self.packagename = None
        self.packagesize = None
        self.isinstall = None
        self.installdate = None
        self.isdelete = None
        self.deletedate = None

    def get_values(self):
        return (self.id, self.title, self.url, self.packagename, self.packagesize, self.isinstall, self.installdate, 
            self.isdelete, self.deletedate) + super(Plugin, self).get_values()


class Cookie(Column):
    def __init__(self):
        super(Cookie, self).__init__()
        self.id = None
        self.host_key = None
        self.name = None
        self.value = None
        self.createdate = None
        self.expiredate = None
        self.lastaccessdate = None
        self.hasexipred = None
        self.owneruser = None

    def get_values(self):
        return (self.id, self.host_key, self.name, self.value, self.createdate, self.expiredate, self.lastaccessdate,
            self.hasexipred, self.owneruser) + super(Cookie, self).get_values()


class Generate(object):

    def __init__(self, db_cache):
        self.db_cache = db_cache
        self.db = None
        self.owneruser = 'default_user'

        self.db_cmd = None
        self.db_trans = None

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {};ReadOnly=True'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)

        models.extend(self._get_account_models())
        models.extend(self._get_bookmark_models())
        models.extend(self._get_browse_record_models())
        models.extend(self._get_downloadfile_models())
        models.extend(self._get_savepage_models())
        models.extend(self._get_search_history_models())
        models.extend(self._get_cookies_models())
        # models.extend(self._get_fileinfo_models())
        # models.extend(self._get_plugin_models())
        self.db_cmd.Dispose()
        self.db.Close()
        return models

    def _get_account_models(self):
        model = []
        sql = '''select distinct * from account group by name'''
        try:
            self.db_cmd.CommandText = sql
            row = self.db_cmd.ExecuteReader()
            while(row.Read()):
                canceller.ThrowIfCancellationRequested()
                user = Generic.User()
                if not IsDBNull(row[0]):
                    user.Identifier.Value = str(row[0])
                if not IsDBNull(row[1]):
                    user.Name.Value = row[1]
                if not IsDBNull(row[2]):
                    user.TimeLastLogin.Value = self._get_timestamp(row[2])
                if not IsDBNull(row[3]) and row[3] not in [None, '']:
                    user.SourceFile.Value = row[3]
                if not IsDBNull(row[4]):
                    user.Deleted = self._convert_deleted_status(row[4])
                model.append(user)
            row.Close()
        except Exception as e:
            print(e)
        return model

    def _get_bookmark_models(self):
        '''
        0    id INTEGER,
        1    time INTEGER,
        2    title TEXT,
        3    url TEXT,
        4    owneruser TEXT,
        5    is_synced BOOL,
        6    source TEXT,
        7    deleted INTEGER,
        8    repeated INTEGER
        '''
        model = []
        sql = '''select distinct * from bookmark'''
        try:
            self.db_cmd.CommandText = sql
            row = self.db_cmd.ExecuteReader()             
            while(row.Read()):
                canceller.ThrowIfCancellationRequested()
                bookmark = Generic.WebBookmark()
                if not IsDBNull(row[1]):
                    bookmark.TimeStamp.Value = self._get_timestamp(row[1])
                if not IsDBNull(row[2]):
                    bookmark.Title.Value = row[2]
                if not IsDBNull(row[3]):
                    bookmark.Url.Value = row[3]
                # TODO
                # if not IsDBNull(row[5]):
                #     bookmark.IsSynced.Value = row[5]
                if not IsDBNull(row[6]) and row[6] not in [None, '']:
                    bookmark.SourceFile.Value = row[6]
                if not IsDBNull(row[7]):
                    bookmark.Deleted = self._convert_deleted_status(row[7])
                model.append(bookmark)
            row.Close()
        except Exception as e:
            print(e)
        return model

    def _get_browse_record_models(self):
        model = []
        sql = '''select distinct * from browse_records'''
        '''
        0    id INTEGER,
        1    name TEXT,
        2    url TEXT,
        3    datetime INTEGER,
        4    owneruser TEXT,
        5    visit_count INTEGER DEFAULT 1,

        6    source TEXT,
        7    deleted INTEGER,
        8    repeated INTEGER
        '''
        try:
            self.db_cmd.CommandText = sql
            row = self.db_cmd.ExecuteReader()             
            while(row.Read()):
                canceller.ThrowIfCancellationRequested()
                visited = Generic.VisitedPage()
                if not IsDBNull(row[1]):
                    visited.Title.Value = row[1]
                if not IsDBNull(row[2]):
                    visited.Url.Value = row[2]
                if not IsDBNull(row[3]):
                    visited.LastVisited.Value = self._get_timestamp(row[3])
                if not IsDBNull(row[5]):
                    visited.VisitCount.Value = row[5]
                if not IsDBNull(row[6]) and row[6] not in [None, '']:
                    visited.SourceFile.Value = row[6]
                if not IsDBNull(row[7]):
                    visited.Deleted = self._convert_deleted_status(row[7])
                model.append(visited)
            row.Close()
        except Exception as e:
            print(e)
        return model

    def _get_downloadfile_models(self):
        model = []
        sql = '''select distinct * from download_files'''
        '''
        0   id INTEGER,
        1   url TEXT,
        2   filename TEXT,
        3   filefolderpath TEXT,
        4   totalsize INTEGER,
        5   createdate INTEGER,
        6   donedate INTEGER,
        7   costtime INTEGER,
        8   owneruser TEXT,
        9   source TEXT,
        10  deleted INTEGER,
        11  repeated INTEGER
        '''
        try:
            self.db_cmd.CommandText = sql
            row = self.db_cmd.ExecuteReader()             
            while(row.Read()):
                canceller.ThrowIfCancellationRequested()
                download = Generic.Attachment()
                if not IsDBNull(row[3]) and row[3]:
                    download.Uri.Value = self._get_uri(row[3])
                elif not IsDBNull(row[1]) and row[1]:
                    download.Uri.Value = self._get_uri(row[1])
                if not IsDBNull(row[2]):
                    download.Filename.Value = row[2]  
                if not IsDBNull(row[4]):
                    download.Size.Value = row[4]

                if not IsDBNull(row[5]) and row[5]:
                    download.DownloadTime.Value = self._get_timestamp(row[5])
                elif not IsDBNull(row[6]) and row[6]:
                    download.DownloadTime.Value = self._get_timestamp(row[6])

                if not IsDBNull(row[9]) and row[9] not in [None, '']:
                    download.SourceFile.Value = row[9]
                if row[10] is not None:
                    download.Deleted = self._convert_deleted_status(row[10])
                model.append(download)
            row.Close()
        except Exception as e:
            print(e)
        return model

    def _get_fileinfo_models(self):
        model = []
        sql = '''select distinct * from file_info'''
        try:
            self.db_cmd.CommandText = sql
            row = self.db_cmd.ExecuteReader()             
            while(row.Read()):
                canceller.ThrowIfCancellationRequested()
                browseload = Generic.Attachment()
                if not IsDBNull(row[1]):
                    browseload.URL.Value = row[1]
                if not IsDBNull(row[2]):
                    browseload.Filename.Value = row[2]
                if not IsDBNull(row[3]):
                    browseload.Size.Value = row[3]
                if not IsDBNull(row[5]):
                    browseload.Title.Value = row[5]
                if not IsDBNull(row[6]) and row[6] not in [None, '']:
                    browseload.SourceFile.Value = row[6]
                if not IsDBNull(row[7]):
                    browseload.Deleted = self._convert_deleted_status(row[7])
                model.append(browseload)
            row.Close()
        except Exception as e:
            print(e)
        return model

    def _get_savepage_models(self):
        '''统一放到downloadfiles'''
        model = []
        return model

    def _get_search_history_models(self):
        
        model = []
        sql = '''select distinct * from search_history'''
        '''
        0    id INTEGER,
        1    name TEXT,
        2    url TEXT,
        3    datetime INTEGER,
        4    owneruser TEXT,
        5    source TEXT,
        6    deleted INTEGER,
        7    repeated INTEGER
        '''
        try:
            self.db_cmd.CommandText = sql
            row = self.db_cmd.ExecuteReader()             
            while(row.Read()):
                canceller.ThrowIfCancellationRequested()
                search = SearchedItem()
                if not IsDBNull(row[1]):
                    search.Value.Value = row[1]
                if not IsDBNull(row[2]):
                    search.SearchResults.Add(row[2])
                if not IsDBNull(row[3]):
                    search.TimeStamp.Value = self._get_timestamp(row[3])
                if not IsDBNull(row[5]) and row[5] not in [None, '']:
                    search.SourceFile.Value = row[5]
                if not IsDBNull(row[6]):
                    search.Deleted = self._convert_deleted_status(row[6])
                model.append(search)
            row.Close()

        except Exception as e:
            print(e)
        return model

    def _get_plugin_models(self):
        '''插件没有相关模型类，暂时不作处理'''
        model = []
        sql = '''select distinct * from plugin'''
        return model

    def _get_cookies_models(self):
        model = []
        sql = '''select distinct * from cookies'''
        '''
            0    id INTEGER PRIMARY KEY AUTOINCREMENT,
            1    host_key TEXT,
            2    name TEXT,
            3    value TEXT,
            4    createdate INTEGER,
            5    expiredate INTEGER,
            6    lastaccessdate INTEGER,
            7    hasexipred INTEGER,
            8    owneruser TEXT,
            9    source TEXT,
            10    deleted INTEGER,
            11    repeated INTEGER
        '''
        try:
            self.db_cmd.CommandText = sql
            row = self.db_cmd.ExecuteReader()             
            while(row.Read()):
                canceller.ThrowIfCancellationRequested()
                cookie = Generic.Cookie()
                if not IsDBNull(row[1]):
                    cookie.Domain.Value = row[1]
                if not IsDBNull(row[2]):
                    cookie.Name.Value = row[2]
                if not IsDBNull(row[3]):
                    cookie.Value.Value = row[3]
                if not IsDBNull(row[4]):
                    cookie.CreationTime.Value = self._convert_webkit_timestamp(row[4])
                if not IsDBNull(row[5]):
                    cookie.Expiry.Value = self._convert_webkit_timestamp(row[5])
                if not IsDBNull(row[6]):
                    cookie.LastAccessTime.Value = self._convert_webkit_timestamp(row[6])
                if not IsDBNull(row[9]) and row[9] not in [None, '']:
                    cookie.SourceFile.Value = str(row[9])
                if not IsDBNull(row[10]):
                    cookie.Deleted = self._convert_deleted_status(row[10])
                model.append(cookie)
            row.Close()
        except Exception as e:
            print(e)
        return model

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    @staticmethod
    def _get_timestamp(timestamp):
        zero_ts = TimeStamp.FromUnixTime(0, False)
        try:
            if len(str(timestamp)) >= 10:
                timestamp = int(str(timestamp)[:10])
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if ts.IsValidForSmartphone():
                    return ts
            return zero_ts
        except:
            return zero_ts

    @staticmethod
    def _convert_webkit_timestamp(webkit_timestamp):
        ''' convert 17 digits webkit timestamp to 10 digits timestamp '''
        try:
            epoch_start = datetime.datetime(1601, 1, 1)
            delta = datetime.timedelta(microseconds=int(webkit_timestamp))
            timestamp = time.mktime((epoch_start + delta).timetuple())
            ts = TimeStamp.FromUnixTime(int(timestamp) + 28800, False)
            if not ts.IsValidForSmartphone():
                ts = TimeStamp.FromUnixTime(0, False)
            return ts            
        except:
            return None

    def _get_uri(self, path):
        try:
            return ConvertHelper.ToUri(path)
        except:
            pass


##################################################################################
##                              Base Browser Parser                             ##
##################################################################################


def convert_2_SearchedItem(_VisitedPage):
    ''' 有些搜索记录没有存储, 需要将浏览记录转化为搜索记录(SearchedItem), "网页搜索_"为神马搜索
    
    Args:
        VisitedPage: Generic.VisitedPage()
    Return:
        SearchedItem: SearchedItem()
    '''
    SEARCH_ENGINES = r'((?P<keyword1>.*?)( - 百度| - 搜狗搜索|_360搜索| - 国内版 Bing)$)|(^网页搜索_(?P<keyword2>.*))|'
    try:
        if _VisitedPage.Title.Value:
            match_res = re.match(SEARCH_ENGINES, _VisitedPage.Title.Value)
            if match_res and (match_res.group('keyword1') or match_res.group('keyword2')):
                keyword1 = match_res.group('keyword1')
                keyword2 = match_res.group('keyword2')
                keyword = keyword1 if keyword1 else keyword2
                search = SearchedItem()
                if _VisitedPage.Url.Value:
                    search.SearchResults.Add(_VisitedPage.Url.Value)
                search.Value.Value      = keyword
                search.TimeStamp.Value  = _VisitedPage.LastVisited.Value
                search.SourceFile.Value = _VisitedPage.Source.Value
                search.Deleted          = _VisitedPage.Deleted
                return search
    except:
        exc()
        return 


class BaseBrowserParser(BaseParser):
    ''' common func:
            parse_Cookie
        
        need to be implemented func: 
            parse_main
            _convert_nodepath
    '''
    def __init__(self, node, extract_deleted, extract_source, db_name=''):
        super(BaseBrowserParser, self).__init__(node, extract_deleted, extract_source, 'Browser'+db_name)
        self.csm = MB()
        self.Generate = Generate
        self.VERSION_KEY_DB = VERSION_KEY_DB
        self.VERSION_VALUE_DB = VERSION_VALUE_DB
        self.VERSION_KEY_APP = VERSION_KEY_APP        

    def parse_main(self):
        pass

    def parse_Cookie(self, db_paths, table_name):
        ''' Android: app_webview/Cookies 
        
        Args:
            db_paths (list<str>): list of db_path
            table_name (str):

        Table Columns:
             FieldName	        SQL Type 	     	
        1    creation_utc	    INTEGER
        2    host_key	        TEXT
        3    name	            TEXT
        4    value	            TEXT
        5    path	            TEXT
        6    expires_utc	    INTEGER
        7    is_secure	        INTEGER     # or secure
        8    is_httponly	    INTEGER     # or httponly
        9    last_access_utc	INTEGER
        10   has_expires	    INTEGER
        11   is_persistent	    INTEGER     # or persistent
        12   priority	        INTEGER
        13   encrypted_value	BLOB
        14   firstpartyonly	    INTEGER
        '''
        for db_path in db_paths:
            if not self._read_db(db_path):
                continue
            for rec in self._read_table(table_name):
                try:
                    if (self._is_empty(rec, 'creation_utc') or
                        self._is_duplicate(rec, 'creation_utc')):
                        continue
                    cookie = Cookie()
                    cookie.id             = rec['creation_utc'].Value
                    cookie.host_key       = rec['host_key'].Value
                    cookie.name           = rec['name'].Value
                    cookie.value          = rec['value'].Value
                    cookie.createdate     = rec['creation_utc'].Value
                    cookie.expiredate     = rec['expires_utc'].Value
                    cookie.lastaccessdate = rec['last_access_utc'].Value
                    cookie.hasexipred     = rec['has_expires'].Value
                    # cookie.owneruser      = self.cur_account_name
                    cookie.source         = self.cur_db_source
                    cookie.deleted        = 1 if rec.IsDeleted else 0
                    self.csm.db_insert_table_cookies(cookie)
                except:
                    exc()
            self.csm.db_commit()

    def _convert_nodepath(self, raw_path):
        pass

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

