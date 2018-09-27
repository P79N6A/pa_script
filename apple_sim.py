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
        apple sim
        \mapping\private\var\wireless\Library\Databases\CellularUsage.db
    """
    res = SIMParser(node, extract_deleted, extract_source).parse()
    pr = ParserResults()
    if res is not None:
        pr.Models.AddRange(res)
    pr.Build('SIM 卡')
    return pr


class SIMParser(object):
    """ iPhone 6_11.1.2_133217541373990_full(5)\
        mapping\private\var\wireless\Library\Databases\CellularUsage.db
    """
    def __init__(self, node, extract_deleted, extract_source):

        self.root = node
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source

        self.m_sim = Model_SIM()
        self.cachepath = ds.OpenCachePath("AppleSIM")
        self.cachedb = self.cachepath + "\\AppleSIM.db"
        self.m_sim.db_create(self.cachedb)
        self.m_sim.db_create_table()
        
    def parse(self):
        # self.db = SQLiteParser.Database.FromNode(self.root,canceller)
        self.db = SQLiteParser.Database.FromNode(self.root, canceller)
        if self.db is None:
            return
        self.source_db = self.root.AbsolutePath
        self.parse_siminfo()
        self.m_sim.db_close()
        models = GenerateModel(self.cachedb).get_models()
        return models

    def parse_siminfo(self):
        """ CellularUsage.db - subscriber_info

        RecNo	FieldName	
        1	ROWID	            INTEGER			
        2	subscriber_id	    TEXT			
        3	subscriber_mdn	            TEXT			
        4	tag	                INTEGER			
        5	last_update_time	            INTEGER			
        6	slot_id	            INTEGER			
        """
        for rec in self.my_read_table(table_name='subscriber_info'):
            if self.is_empty(rec, 'ROWID', 'subscriber_mdn'):
                continue
            sim = SIM()
            sim._id    = rec['ROWID'].Value
            sim.value  = rec['subscriber_mdn'].Value
            sim.source = self.source_db
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

    @staticmethod
    def is_empty(rec, *args):
        ''' 过滤数据 '''
        # 验证手机号, 包含 +86, 86 开头
        if 'subscriber_mdn' in args:
            s = rec['subscriber_mdn'].Value
            try:
                reg_str = r'^((\+86)|(86))?(1)\d{10}$'
                match_obj = re.match(reg_str, s)
                if match_obj is None:
                    return True      
            except:
                return True         

        for i in args:
            if IsDBNull(rec[i].Value) or not rec[i].Value:
                return True
        return False