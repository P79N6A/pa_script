# coding=utf-8
import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_sim')
except:
    pass
del clr

from model_sim import *

import time
import re



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
        self.db = SQLiteParser.Database.FromNode(node,canceller)
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
        self.db = SQLiteParser.Database.FromNode(node,canceller)
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

