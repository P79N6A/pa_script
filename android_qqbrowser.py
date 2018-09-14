#coding=utf-8
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import logging 
import re
from model_browser import *


class QQBrowserParse(object):
    '''通过遍历数据库文件获取用户名可能导致数据重复 需在model中做去重操作'''
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = False
        self.extractSource = extractSource
        self.user = re.findall(r'^.*/(.+).db', node.AbsolutePath)
        self.db = None
        self.mb = MB()
        self.cache_path = ds.OpenCachePath("QQBrowser")
        self.db_cache = self.cache_path + "\\QQBrowser.db"
        self.mb.db_create(self.db_cache)

    def analyze_bookmarks(self):
        bookmark = Bookmark()
        try:
            self.db = SQLiteParser.Database.FromNode(self.node)
            if self.db is None:
                return 
            ts = SQLiteParser.TableSignature('mtt_bookmarks')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                bookmark.id = rec['_id'].Value if '_id' in rec else None
                bookmark.time = self._timeHandler(rec['created'].Value) if 'created' in rec else None
                bookmark.title = rec['title'].Value if 'title' in rec and rec['title'].Value is not '' else None
                bookmark.url = rec['url'].Value if 'url' in rec and rec['url'].Value is not '' else None
                bookmark.owneruser = self.user[0]
                bookmark.source = self.node.AbsolutePath
                self.mb.db_insert_table_bookmarks(bookmark)
            self.mb.db_commit()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                bookmark.id = row['_id'].Value if '_id' in row else None
                bookmark.time = self._timeHandler(row['created'].Value) if 'created' in row else None
                bookmark.title = repr(row['title'].Value) if 'title' in row and row['title'].Value is not '' else None
                bookmark.url = repr(row['url'].Value) if 'url' in row and row['url'].Value is not '' else None
                bookmark.owneruser = self.user[0]
                bookmark.source = self.node.AbsolutePath
                bookmark.deleted = 1
                self.mb.db_insert_table_bookmarks(bookmark)
            self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_browserecords(self):
        record = Browserecord()
        try:
            node = self.node.Parent.GetByPath('/database')
            self.db = SQLiteParser.Database.FromNode(node)
            if self.db is None:
                return 
            ts = SQLiteParser.TableSignature('history')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                record.id = rec['ID'].Value if 'ID' in rec else None
                record.name = rec['NAME'].Value if 'NAME' in rec and rec['NAME'].Value is not '' else None
                record.url = rec['URL'].Value if 'URL' in rec and rec['URL'].Value is not '' else None
                record.datetime = self._timeHandler(rec['DATETIME'].Value) if 'DATETIME' in rec else None
                self.source = node.AbsolutePath
            self.mb.db_commit()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                record.id = row['ID'].Value if 'ID' in row else None
                record.name = repr(row['NAME'].Value) if 'NAME' in row and row['NAME'].Value is not '' else None
                record.url = repr(row['URL'].Value) if 'URL' in row and row['URL'].Value is not '' else None
                record.datetime = self._timeHandler(row['DATETIME'].Value) if 'DATETIME' in row else None
                record.source = node.AbsolutePath
                record.deleted = 1
                self.mb.db_insert_table_browserecords(record)
            self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_downloadfiles(self):
        downloadfile = DownloadFile()
        try:
            node = self.node.Parent.GetByPath('/download_database')
            self.db = SQLiteParser.Database.FromNode(node)
            if self.db is None:
                return
            ts = SQLiteParser.TableSignature('download')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                downloadfile.id = rec['id'].Value if 'id' in rec else None
                downloadfile.url = rec['url'].Value if 'url' in rec and rec['url'].Value is not '' else None
                downloadfile.filename = rec['filename'].Value if 'filename' in rec and rec['filename'].Value is not '' else None
                downloadfile.filefolderpath = self._transToAbsolutePath(rec['filefolderpath'].Value) if 'filefolderpath' in rec and rec['filefolderpath'].Value is not '' else None
                downloadfile.totalsize = rec['totalsize'].Value if 'totalsize' in rec else None
                downloadfile.createdate = self._timeHandler(rec['createdate'].Value) if 'createdate' in rec else None
                downloadfile.donedate = self._timeHandler(rec['donedate'].Value) if 'donedate' in rec else None
                downloadfile.costtime = rec['costtime'].Value if 'costtime' in rec else None
                downloadfile.source = node.AbsolutePath
                self.mb.db_insert_table_downloadfiles(downloadfile)
            self.mb.db_commit()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                downloadfile.id = row['id'].Value if 'id' in row else None
                downloadfile.url = repr(row['url'].Value) if 'url' in row and row['url'].Value is not '' else None
                downloadfile.filename = repr(row['filename'].Value) if 'filename' in row and row['filename'].Value is not '' else None
                downloadfile.filefolderpath = self._transToAbsolutePath(repr(row['filefolderpath'].Value)) if 'filefolderpath' in row and row['filefolderpath'].Value is not '' else None
                downloadfile.totalsize = row['totalsize'].Value if 'totalsize' in row else None
                downloadfile.createdate = self._timeHandler(row['creatdate'].Value) if 'totalsize' in row else None
                downloadfile.donedate = self._timeHandler(row['donedate'].Value) if 'donedate' in row else None
                downloadfile.costtime = row['costtime'].Value if 'costtime' in row else None
                downloadfile.source = node.AbsolutePath
                downloadfile.deleted = 1
                self.mb.db_insert_table_downloadfiles(downloadfile)
            self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_fileinfo(self):
        fileinfo = FileInfo()
        try:
            node_external = self.node.Parent.GetByPath('/filestore_0000-0000')
            node_internal = self.node.Parent.GetByPath('/filestore_0')
            nodes = [node_internal, node_external]
            for node in nodes:
                self.db = SQLiteParser.Database.FromNode(node)
                if self.db is None:
                    return
                ts = SQLiteParser.TableSignature('file_information')
                for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                    fileinfo.id = rec['FILE_ID'].Value if 'FILE_ID' in rec else None
                    fileinfo.filepath = self._transToAbsolutePath(rec['FILE_PATH'].Value) if 'FILE_PATH' in rec and rec['FILE_PATH'].Value is not '' else None
                    fileinfo.filename = rec['FILE_NAME'].Value if 'FILE_NAME' in rec and rec['FILE_NAME'].Value is not '' else None
                    fileinfo.size = rec['SIZE'].Value if 'SIZE' in rec else None
                    fileinfo.modified = self._timeHandler(rec['MODIFIED_DATE'].Value) if 'MODIFIED_DATE' in rec else None
                    fileinfo.title = rec['TITLE'].Value if 'TITLE' in rec and rec['TITLE'].Value is not '' else None
                    fileinfo.source = node.AbsolutePath
                    self.mb.db_insert_table_fileinfos(fileinfo)
                self.mb.db_commit()
                for row in self.db.ReadTableDeletedRecords(ts, False):
                    fileinfo.id = row['FILE_ID'].Value if 'FILE_ID' in row else None
                    fileinfo.filepath = self._transToAbsolutePath(repr(row['FILE_PATH'].Value)) if 'FILE_PATH' in row and row['FILE_PATH'].Value is not '' else None
                    fileinfo.filename = repr(row['FILE_NAME'].Value) if 'FILE_NAME' in row and row['FILE_NAME'].Value is not '' else None
                    fileinfo.size = row['SIZE'].Value if 'SIZE' in row else None
                    fileinfo.modified = self._timeHandler(row['MODIFIED_DATE'].Value) if 'MODIFIED_DATE' in row else None
                    fileinfo.title = repr(row['TITLE'].Value) if 'TITLE' in row and row['TITLE'].Value is not '' else None
                    fileinfo.source = node.AbsolutePath
                    fileinfo.deleted = 1
                    self.mb.db_insert_table_fileinfos(fileinfo)
                self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_search_history(self):
        searchHistory = SearchHistory()
        try:
            self.db = SQLiteParser.Database.FromNode(self.node)
            if self.db is None:
                return
            ts = SQLiteParser.TableSignature('search_history')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                searchHistory.id = rec['ID'].Value if 'ID' in rec else None
                searchHistory.name = rec['NAME'].Value if 'NAME' in rec and rec['NAME'].Value is not '' else None
                searchHistory.url = rec['URL'].Value if 'URL' in rec and rec['URL'].Value is not '' else None
                searchHistory.datetime = self._timeHandler(rec['DATETIME'].Value) if 'DATETIME' in rec else None
                bookmark.owneruser = self.user[0]
                searchHistory.source = node.AbsolutePath
                self.mb.db_insert_table_searchhistory(searchHistory)
            self.mb.db_commit()
            for row in self.db.ReadTableDeletedRecords(ts, False):
                searchHistory.id = row['ID'].Value if 'ID' in row else None
                searchHistory.name = repr(row['NAME'].Value) if 'NAME' in row and row['NAME'].Value is not '' else None
                searchHistory.url = repr(row['URL'].Value) if 'URL' in row and row['URL'].Value is not '' else None
                searchHistory.datetime = self._timeHandler(row['DATETIME'].Value) if 'DATETIME' in row else None
                bookmark.owneruser = self.user[0]
                searchHistory.source = node.AbsolutePath
                searchHistory.deleted = 1
                self.mb.db_insert_table_searchhistory(searchHistory)
            self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_accounts(self):
        account = Account()
        try:
            account.name = self.user[0]
            account.source = self.node.Parent.AbsolutePath
            self.mb.db_insert_table_accounts(account)
            self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_plugin(self):
        plugin = Plugin()
        node = self.node.Parent.GetByPath('/plugin_db')
        try:
            self.db = SQLiteParser.Database.FromNode(node)
            if self.db is None:
                return
            ts = SQLiteParser.TableSignature('plugins')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                plugin.id = rec['ID'].Value if 'ID' in rec else None
                plugin.title = rec['title'].Value if 'title' in rec and rec['title'].Value is not '' else None
                plugin.url = rec['url'].Value if 'url' in rec and rec['url'].Value is not '' else None
                plugin.packagename = rec['packageName'].Value if 'packageName' in rec and rec['packageName'].Value is not '' else None
                plugin.packagesize = rec['packageSize'].Value if 'packageSize' in rec and rec['packageSize'].Value is not '' else None
                plugin.isinstall = rec['isInstall'].Value if 'isInstall' in rec else None
                plugin.source = node.AbsolutePath
                self.mb.db_insert_table_plugin(plugin)
            self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def analyze_cookies(self):
        cookie = Cookie()
        node = self.node.Parent.Parent.GetByPath('/app_webview/Cookies')
        try:
            self.db = SQLiteParser.Database.FromNode(node)
            if self.db is None:
                return
            ts = SQLiteParser.TableSignature('cookies')
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                cookie.host_key = rec['host_key'].Value if 'host_key' in rec and rec['host_key'].Value is not '' else None
                cookie.name  = rec['name'].Value if 'name' in rec and rec['name'].Value is not '' else None
                cookie.value = rec['value'].Value if 'value' in rec and rec['value'].Value is not '' else None
                cookie.createdate = self._timeHandler(rec['creation_utc'].Value) if 'creation_utc' in rec else None
                cookie.expiredate = self._timeHandler(rec['expires_utc'].Value) if 'expires_utc' in rec else None
                cookie.lastaccessdate = self._timeHandler(rec['last_access_utc'].Value) if 'last_access_utc' in rec else None
                cookie.hasexipred = rec['has_expires'].Value if 'has_expires' in rec else None
                cookie.source = node.AbsolutePath
                self.mb.db_insert_table_cookies(cookie)
            self.mb.db_commit()
        except Exception as e:
            logging.error(e)

    def _timeHandler(self, time):
        if len(str(time)) > 10:
            return int(str(time)[0:10:1])
        elif len(str(time)) == 10:
            return time
        else:
            return 0
        
    def _transToAbsolutePath(self, dir):
        fs = self.node.FileSystem
        if re.match(r'^/storage/emulated/0', dir) is not None:
            subdir = dir.replace('/storage/emulated/0', '/media/0')
            fileNode = fs.Search(subdir + '$')
            for node in fileNode:
                return node.AbsolutePath
        elif  re.match(r'^/data/user/0', dir) is not None:
            subdir = dir.replace('/data/user/0', '/data')
            fileNode = fs.Search(subdir + '$')
            for node in fileNode:
                return node.AbsolutePath
        elif re.match(r'^/storage/0000-0000', dir) is not None:
            return dir
        elif re.match(r'^/$', dir) is not None:
            return fs.AbsolutePath
        else:
            fileNode = fs.Search(dir+ '$')
            for node in fileNode:
                return node.AbsolutePath

    def parse(self):
        self.analyze_accounts()
        self.analyze_bookmarks()
        self.analyze_browserecords()
        self.analyze_cookies()
        self.analyze_downloadfiles()
        self.analyze_fileinfo()
        self.analyze_plugin()
        self.analyze_search_history()
        self.mb.db_close()
        generate = Generate(self.db_cache)
        models = generate.get_models()
        return models

def analyze_android_qqbrowser(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(QQBrowserParse(node, extractDeleted, extractSource).parse())
    return pr

def execute(node, extractDeleted):
    return analyze_android_qqbrowser(node, extractDeleted, False)