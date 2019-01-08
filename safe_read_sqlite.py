#coding=utf-8

__author__ = "Xu Tao"

import clr
import os
import shutil
import hashlib

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

import System
from System.Xml.Linq import *
from System.Data.SQLite import *

del clr

"""
对用c# 去读写数据库含有wal和shm文件进行处理。
默认返回 Cmd 对象。

SqliteByCSharp需要2个参数，第一个参数是数据库的节点，第二个参数是 ds.OpenCachePath("###") 返回的路径。
SqliteByCSharp类自带了读写各个类型的方法。

Use Handbook:
    
    >    from safe_read_sqlite import SqliteByCSharp
    >    conn = SqliteByCSharp(node, cache_dir)
    >    with conn as cmd:
    >       pass


Example Code:
    >    conn = SqliteByCSharp(self.root, self.cache)
    >    with conn as cmd:
    >        cmd.CommandText = '''
    >            select appName, packageName, versionName, permissions from apps
    >        '''
    >        reader = cmd.ExecuteReader()
    >        while reader.Read():
    >            name = SqliteByCSharp.GetString(reader, 0)
    >            bind_id = SqliteByCSharp.GetString(reader, 1)
    >            version = SqliteByCSharp.GetString(reader, 2)

"""


class SqliteByCSharp(object):

    def __init__(self, db_node, cache_path):
        self.path = db_node
        self.cmd = None
        self.conn = None
        self.cache_path = cache_path

    def __enter__(self):
        if self.cmd is not None:
            raise RuntimeError('Already connected')
        path = self.path.PathWithMountPoint
        db_name = os.path.basename(path)
        dest_path = self.cache_path + "\\" + db_name
        if not os.path.exists(dest_path):
            db_path = self.__check_sqlite_maturity(self.path, self.cache_path)
            self.conn = System.Data.SQLite.SQLiteConnection('Data Source = {0}; ReadOnly = True'.format(db_path))
        else:
            self.conn = System.Data.SQLite.SQLiteConnection('Data Source = {0}; ReadOnly = True'.format(dest_path))
        self.conn.Open()
        self.cmd = System.Data.SQLite.SQLiteCommand(self.conn)
        return self.cmd

    def __mapping_file_with_copy(self, src, dst):
        if src is None or src is "":
            print('file not copied as src is empty!')
            return False
        if dst is None or dst is "":
            print('file not copied as dst is lost!')
            return False
        shutil.copy(src, dst)

    def md5(self, string):
        m = hashlib.md5()
        m.update(string.encode(encoding = 'utf-8')) 
        return m.hexdigest()

    @staticmethod
    def GetString(reader, idx):
        return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

    @staticmethod
    def GetInt64(reader, idx):
        return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

    @staticmethod
    def GetBlob(reader, idx):
        return reader.GetValue(idx) if not reader.IsDBNull(idx) else None

    @staticmethod
    def GetFloat(reader, idx):
        return reader.GetFloat(idx) if not reader.IsDBNull(idx) else 0

    def __check_sqlite_maturity(self, db_node, cache):
        wal_is_exists = False
        shm_is_exists = False
        path = db_node.PathWithMountPoint
        wal_path = path + "-wal"
        shm_path = path + "-shm"
        db_name = os.path.basename(path)
        dest_path = cache + "\\" + db_name
        if os.path.exists(wal_path):
            self.__mapping_file_with_copy(wal_path, dest_path + '-wal')
            wal_is_exists = True
        if os.path.exists(shm_path):
            self.__mapping_file_with_copy(shm_path, dest_path + '-shm')
            shm_is_exists = True
        if wal_is_exists or shm_is_exists:
            self.__mapping_file_with_copy(path, dest_path)
            return dest_path
        else:
            return path

    def __exit__(self, exc_ty, exc_val, tb):
        self.conn.Close()
        self.conn = None
        



