# coding=utf-8
import PA_runtime
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Utils import *

import os
import time
import sqlite3
import traceback
import re


def exc():
    exc()
    # traceback.print_exc()

def analyze_sim(node, extract_deleted, extract_source):
    """
        android 小米 SIM (user_de/0/com.android.providers.telephony/databases/telephony.db)
        user_de/0/com.android.providers.telephony/databases$
    """
    node_path = node.AbsolutePath

    res = None
    if node_path.endswith('sim/sim.db'):
        res = SIMParser_no_tar(node, extract_deleted, extract_source).parse()
    elif node_path.endswith('user_de/0/com.android.providers.telephony/databases'):
        res = SIMParser(node, extract_deleted, extract_source).parse()


    pr = ParserResults()
    if res is not None:
        pr.Models.AddRange(res)
    return pr


class SIMParser(object):
    """ \user_de\0\com.android.providers.telephony\databases\telephony.db	"""
    def __init__(self, node, extract_deleted, extract_source):

        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.m_sim = Model_SIM()
        self.cachepath = ds.OpenCachePath("AndroidSIM")
        self.cachedb = self.cachepath + "\\AndroidSIM.db"
        self.m_sim.db_create(self.cachedb)
        self.m_sim.db_create_table()
        
    def parse(self):
        node = self.root.GetByPath("/telephony.db")
        self.db = SQLiteParser.Database.FromNode(node)
        if self.db is None:
            return
        self.source_telephony_db = node.AbsolutePath
        self.parse_siminfo()
        self.m_sim.db_close()
        models = GenerateModel(self.cachedb).get_models()
        return models

    def parse_siminfo(self):
        """ telephony.db - siminfo
            SIM
        RecNo	FieldName	SQLType	Size	 
        1	_id	                INTEGER
        2	icc_id	                TEXT
        3	sim_id	                INTEGER			
        4	display_name	                TEXT
        5	carrier_name	                TEXT
        6	name_source	                INTEGER
        7	color	                INTEGER
        8	number	                TEXT
        9	display_number_format	INTEGER
        10	data_roaming	                INTEGER
        11	mcc	                INTEGER
        12	mnc	                INTEGER
        """
        for rec in self.my_read_table(table_name='siminfo'):
            if IsDBNull(rec['sim_id'].Value) or IsDBNull(rec['display_name'].Value):
                continue
            sim = SIM()
            sim.name   = rec['display_name'].Value.replace('CMCC', '中国移动')
            sim.value  = rec['number'].Value
            sim.extra  = 'iccid: {}'.format(rec['icc_id'].Value) if rec['icc_id'].Value != '' else None
            sim.source = self.source_telephony_db
            try:
                self.m_sim.db_insert_table_sim(sim)
            except:
                exc()
        try:
            self.m_sim.db_commit()
        except:
            exc()
        
    def my_read_table(self, table_name):
        """
            读取手机数据库, 单数据库模式
        :type table_name: str
        :rtype: db.ReadTableRecords()
        """
        if self.db is None:
            return
        tb = SQLiteParser.TableSignature(table_name)
        return self.db.ReadTableRecords(tb, self.extract_deleted, True)


class SIMParser_no_tar(SIMParser):
    ''' 处理没有 tar 包的案例 '''

    def __init__(self, node, extract_deleted, extract_source):
        super(SIMParse_no_tar, self).__init__(node, extract_deleted, extract_source)
    
    def parse(self):
        node = self.root.GetByPath('sim/sim.db$')
        self.db = SQLiteParser.Database.FromNode(node)
        if self.db is None:
            return
        self.source_sim_db = node.AbsolutePath
        self.parse_sim()
        self.m_sim.db_close()
        models = GenerateModel(self.cachedb).get_models()
        return models    

    def parse_sim(self):
        ''' sim/sim.db - SIM
        RecNo	FieldName	SQLType	
        1	displayName	TEXT		
        2	phoneNumber	TEXT		
        '''
        for rec in self.my_read_table(table_name='SIM'):
            if IsDBNull(rec['body'].Value):
                continue
            sim = SIM()
            sim.name   = rec['displayName'].Value
            sim.value  = rec['phoneNumber'].Value
            sim.source = self.source_sim_db
            try:
                self.m_sim.db_insert_table_sim(sim)
            except:
                exc()
        try:
            self.m_sim.db_commit()
        except:
            exc()


#########################
##      Model_SIM      ##
#########################

VERSION_VALUE_DB = 1
VERSION_KEY_DB  = 'db'
VERSION_KEY_APP = 'app'

SQL_CREATE_TABLE_SIM = '''
    create table if not exists sim(
        _id        INTEGER PRIMARY KEY AUTOINCREMENT,
        name          TEXT,
        value         TEXT,
        extra         TEXT,

        source        TEXT,
        deleted       INT DEFAULT 0, 
        repeated      INT DEFAULT 0)                                
    '''

SQL_INSERT_TABLE_SIM = '''
    insert into sim(
        name,
        value,
        extra,
        source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?)
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
        self.cursor = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)
        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()

        self.db_create_table()

    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_create_table(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_SIM)
            self.cursor.execute(SQL_CREATE_TABLE_VERSION)

    def db_insert_table_sim(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_SIM, column.get_values())

    def db_insert_table_version(self, key, version):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_VERSION, (key, version))

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
        except Exception as e:
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
        self.name = None  # sim 卡运营商名称
        self.value = None  # 
        self.extra = None  # 

    def get_values(self):
        return (
            self.name,
            self.value,
            self.extra,
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

        self.cursor.close()
        self.db.close()
        return models

    def _get_sim_models(self):
        '''
        _id        INTEGER AUTOINCREMENT,
        name          TEXT,
        value         TEXT,
        extra         TEXT,

        source        TEXT,
        deleted       INT DEFAULT 0, 
        repeated      INT DEFAULT 0)   
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
            sim = Calls.SIMData()
            # if row[0] is not None:
            #     sim.Name.Value = row[0]
            if row[1] in range(7):
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
