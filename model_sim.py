# -*- coding: utf-8 -*-
__author__ = 'YangLiyuan'

from PA_runtime import *
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import sqlite3
import System.Data.SQLite as SQLite
from ScriptUtils import exc


VERSION_KEY_DB  = 'db'
VERSION_VALUE_DB = 1

VERSION_KEY_APP = 'app'

# bcp
'''
1    collect_target_id   手机取证采集目标编号
2    msisdn          	本机号码        MSISDN=CC+DNC+SN，例：86 + 139 + ********
3    imsi            	SIM卡IMSI       IMSI=MCC+MNC+MSIN
4    center_number       短信中心号码
5    delete_status       删除状态
6    delete_time         删除时间
7    iccid           	SIM卡ICCID
8    sim_state           使用状态
'''

SQL_CREATE_TABLE_SIM = '''
    create table if not exists sim(
        _id        INTEGER PRIMARY KEY AUTOINCREMENT,
        name          TEXT,
        msisdn        TEXT,
        imsi          TEXT,
        iccid         TEXT,
        center_num    TEXT,
        is_use        INT,

        source        TEXT,
        deleted       INT DEFAULT 0, 
        repeated      INT DEFAULT 0)                                
    '''

SQL_INSERT_TABLE_SIM = '''
    insert into sim(
        name,
        msisdn,
        imsi,
        iccid,
        center_num,
        is_use,
        source, deleted, repeated) 
        values(?, ?, ?, ?, ?, 
               ?, ?, ?, ?
            )
    '''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


class Model_SIM(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None        

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except:
                print('db_path:', db_path)
                exc()

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_create_table()
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

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SIM
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

    def db_insert_table_sim(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_SIM, column.get_values())

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
            exc()

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
        self.source   = ''
        self.deleted  = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            self.__dict__[name] = value

    def get_values(self):
        return (self.source, self.deleted, self.repeated)

class SIM(Column):
    def __init__(self):
        super(SIM, self).__init__()
        self.name       = None    # TEXT  sim 卡运营商名称
        self.msisdn     = None    # TEXT
        self.imsi       = None    # TEXT
        self.iccid      = None    # TEXT
        self.center_num = None    # TEXT
        self.is_use     = None    #  INT

    def get_values(self):
        return (
           self.name,
           self.msisdn,
           self.imsi,
           self.iccid,
           self.center_num,
           self.is_use,
       ) + super(SIM, self).get_values()

class GenerateModel(object):
    '''
    SIM
        Name     <String>
        Value    <String>
        Category <String>

    "displayName" TEXT
    "phoneNumber" TEXT
    '''

    def __init__(self, cache_db):
        self.cache_db  = cache_db

    def get_models(self):
        models = []
        self.db = sqlite3.connect(self.cache_db)
        self.cursor = self.db.cursor()

        models.extend(self._get_sim_models())

        self.cursor.close()
        self.db.close()
        return models

    def _get_sim_models(self):
        '''
         0   _id        INTEGER PRIMARY KEY AUTOINCREMENT,
         1   name          TEXT,
         2   msisdn         TEXT,
         3   imsi         TEXT,
         4   iccid         TEXT,
         5   center_num    TEXT,
         6   is_use        INT,
         7   source        TEXT,
         8   deleted       INT DEFAULT 0, 
         9   repeated      INT DEFAULT 0
        '''
        models = []
        sql = '''
                select * from sim
              '''
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except:
            exc()
            return []
        while row is not None:
            if canceller.IsCancellationRequested:
                return
            sim = Calls.SIMData()
            # if row[0] is not None:
            #     sim.Name.Value = row[0]
            if row[1] is not None:
                sim.Name.Value = row[1]
            if row[2] is not None:
                sim.Value.Value = row[2]

            if row[-3] is not None:
                sim.SourceFile.Value = self._get_source_file(row[-3])
            if row[-2] is not None:
                sim.Deleted = self._convert_deleted_status(row[-2])
            models.append(sim)
            row = self.cursor.fetchone()
        return models        

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    def _get_source_file(self, source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return ''    