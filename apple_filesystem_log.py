#coding=utf-8
__author__ = "sumeng"

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from PA_runtime import *
import System.Data.SQLite as SQLite

import sys
import os
import struct
import gzip
import string
import sqlite3

# EnterPoint: analyze_fsevents(root, extract_deleted, extract_source):
# Patterns: '/\.fseventsd$'

DB_VERSION = 10005


TYPE_SYSTEM = 1
TYPE_USER = 2


SQL_CREATE_TABLE_FSEVENT = '''
    create table if not exists fsevent(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER,
        path TEXT,
        flags INTEGER,
        node_id INTEGER,
        type INTEGER,
        source TEXT)'''

SQL_INSERT_TABLE_FSEVENT = '''
    insert into fsevent(event_id, path, flags, node_id, type, source) 
        values(?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(version) values(?)'''

EVENTMASK = {
    0x00000000: 'None;',
    0x00000001: 'FolderEvent;',
    0x00000002: 'Mount;',
    0x00000004: 'Unmount;',
    0x00000020: 'EndOfTransaction;',
    0x00000800: 'LastHardLinkRemoved;',
    0x00001000: 'HardLink;',
    0x00004000: 'SymbolicLink;',
    0x00008000: 'FileEvent;',
    0x00010000: 'PermissionChange;',
    0x00020000: 'ExtendedAttrModified;',
    0x00040000: 'ExtendedAttrRemoved;',
    0x00100000: 'DocumentRevisioning;',
    0x00400000: 'ItemCloned;',           # macOS HighSierra
    0x01000000: 'Created;',
    0x02000000: 'Removed;',
    0x04000000: 'InodeMetaMod;',
    0x08000000: 'Renamed;',
    0x10000000: 'Modified;',
    0x20000000: 'Exchange;',
    0x40000000: 'FinderInfoMod;',
    0x80000000: 'FolderCreated;',
    0x00000008: 'NOT_USED-0x00000008;',
    0x00000010: 'NOT_USED-0x00000010;',
    0x00000040: 'NOT_USED-0x00000040;',
    0x00000080: 'NOT_USED-0x00000080;',
    0x00000100: 'NOT_USED-0x00000100;',
    0x00000200: 'NOT_USED-0x00000200;',
    0x00000400: 'NOT_USED-0x00000400;',
    0x00002000: 'NOT_USED-0x00002000;',
    0x00080000: 'NOT_USED-0x00080000;',
    0x00200000: 'NOT_USED-0x00200000;',
    0x00800000: 'NOT_USED-0x00800000;'
}

PAGE_HEADER_SIZE = 12
DLS1_RECORD_BIN_SIZE = 12
DLS2_RECORD_BIN_SIZE = 20


def analyze_fsevents(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Project #声明这是微信应用解析的数据集
    if have_fsevents(root):
        progress.Start()
        models = Parser(root, extract_deleted, extract_source).parse()
        mlm = ModelListMerger()
        pr.Models.AddRange(list(mlm.GetUnique(models)))
        pr.Build('文件日志')
    else:
        progress.Skip()
    return pr


def have_fsevents(root):
    return root.GetByPath('.fseventsd') is not None or root.GetByPath('private/var/.fseventsd') is not None


class Parser():

    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.cache_path = ds.OpenCachePath('fsevents')
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

    def parse(self):
        if self.need_parse():
            self.db = None
            self.db_create()
            node = self.root.GetByPath('.fseventsd')
            if node is not None:
                self.parse_fsevent(node, TYPE_SYSTEM)
            node = self.root.GetByPath('private/var/.fseventsd')
            if node is not None:
                self.parse_fsevent(node, TYPE_USER)
            if not canceller.IsCancellationRequested:
                self.db_insert_table_version(DB_VERSION)
            self.db_commit()
            self.db_close()

        models = GenerateModel(self.cache_db).get_sp_models()
        return models

    def get_db_name(self):
        node = self.root.Parent
        if node.AbsolutePath == '/':
            return 'system.db'
        elif node.AbsolutePath == '/private/var':
            return 'user.db'
        else:
            return 'fsevents.db'

    def need_parse(self):
        try:
            if not os.path.exists(self.cache_db):
                return True
            db = sqlite3.connect(self.cache_db)
            cursor = db.cursor()
            sql = 'select version from version'
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_filesystem_log.py Error: LINE {}".format(traceback.format_exc()))

            ret = True
            if row is not None:
                ret = not (row[0] == DB_VERSION)
            cursor.close()
            db.close()
            return ret
        except Exception as e:
            return True

    def db_create(self):
        if os.path.exists(self.cache_db):
            os.remove(self.cache_db)

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_cmd.CommandText = SQL_CREATE_TABLE_FSEVENT
        self.db_cmd.ExecuteNonQuery()
        self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
        self.db_cmd.ExecuteNonQuery()
        self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_fsevent(self, event_id, path, flags, node_id, fs_type, source):
        self.db_insert_table(SQL_INSERT_TABLE_FSEVENT, (event_id, path, flags, node_id, fs_type, source))

    def db_insert_table_version(self, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (version,))

    def parse_fsevent(self, root, fs_type):
        for node in root.GetAllNodes(NodeType.File):
            if canceller.IsCancellationRequested:
                break
            buf = None
            try:
                files = gzip.GzipFile(node.PathWithMountPoint, "rb")
                buf = files.read()
            except Exception as e:
                continue
            if buf == None:
                continue
            buf_size = len(buf)
            offset = 0
            while offset < buf_size - PAGE_HEADER_SIZE:   
                if canceller.IsCancellationRequested:
                    break
                try:
                    (magic, size) = self.parse_page_header(buf[offset:offset+PAGE_HEADER_SIZE])
                    if magic == '1SLD' or magic == '2SLD':
                        if offset + size <= buf_size:
                            self.parse_page_body(magic, buf[offset+PAGE_HEADER_SIZE:offset+size], fs_type, node.AbsolutePath)
                        offset += size
                    else:
                        break
                except Exception as e:
                    pass
            self.db_commit()

    def enumerate_flags(self, flag, f_map):
        f_flag = ''
        for i in f_map:
            if i & flag:
                 f_flag = ''.join([f_flag, f_map[i]])
        return f_flag

    def parse_page_header(self, buf):
        magic = buf[0:4]
        size = struct.unpack('<I', buf[8:12])[0]
        return (magic, size)

    def parse_page_body(self, magic, buf, fs_type, source):
        size = len(buf)
        offset = 0
        while offset < size:
            path = ''
            event_id = 0
            flags = 0
            node_id = 0
            while offset < size:
                c = buf[offset]
                offset += 1
                if c == '\0':
                    break
                path += c

            if magic == '1SLD' and offset >= size - DLS1_RECORD_BIN_SIZE:
                break
            if magic == '2SLD' and offset > size - DLS2_RECORD_BIN_SIZE:
                break

            if len(path) == 0:
                path = 'NULL'
            elif fs_type == TYPE_USER:
                path = '/private/var/' + path
            else:
                path = '/' + path
            # path = filter(lambda x: x in string.printable, path)
            event_id = struct.unpack('<Q', buf[offset:offset+8])[0]
            offset += 8
            flags = struct.unpack('>I', buf[offset:offset+4])[0]
            offset += 4
            
            if event_id == 3669549:
                pass
            if magic == '2SLD':
                node_id = struct.unpack('<Q', buf[offset:offset+8])[0]
                offset += 8

            # fs_flags = self.enumerate_flags(flags, EVENTMASK)
            try:
                self.db_insert_table_fsevent(event_id, path, flags, node_id, fs_type, source)
            except Exception as e:
                pass

    def __check_cancel(self):
        pass

class GenerateModel():
    
    def __init__(self, cache_db):
        self.cache_db = cache_db

    def get_sp_models(self):
        models = []
        try:
            model = KeyValueModel()
            model.Key.Value = 'apple_filesystem_log_db_path'
            model.Value.Value = self.cache_db
            models.append(model)
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_filesystem_log.py Error: LINE {}".format(traceback.format_exc()))
        try:
            db = sqlite3.connect(self.cache_db)
            sql = 'select count(*) from fsevent'
            cursor = db.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is not None:
                model = KeyValueModel()
                model.Key.Value = 'apple_filesystem_log_db_count'
                model.Value.Value = str(row[0])
                models.append(model)
            cursor.close()
            db.close()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_filesystem_log.py Error: LINE {}".format(traceback.format_exc()))
        return models

    def get_models(self):
        models = []

        self.db = sqlite3.connect(self.cache_db)
        models.extend(self._get_fsevent_models())
        self.db.close()
        return models

    def _get_fsevent_models(self):
        models = []
        cursor = self.db.cursor()

        sql = 'select event_id,path,flags,node_id,source from fsevent'
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "apple_filesystem_log.py Error: LINE {}".format(traceback.format_exc()))

        while row is not None:
            if canceller.IsCancellationRequested:
                break

            try:
                model = Generic.FileSystemLog()
                if row[0]:
                    model.EventId.Value = row[0]
                if row[1]:
                    model.Path.Value = row[1]
                if row[2]:
                    model.Flags.Value = row[2]
                if row[3]:
                    model.NodeId.Value = row[3]
                if row[4]:
                    model.LogSource.Value = row[4]

                models.append(model)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "apple_filesystem_log.py Error: LINE {}".format(traceback.format_exc()))
            row = cursor.fetchone()

        cursor.close()
        return models
