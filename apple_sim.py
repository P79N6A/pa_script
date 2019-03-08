# coding=utf-8
__author__ = 'YangLiyuan'

import clr
try:
    clr.AddReference('model_sim')
    clr.AddReference('bcp_basic')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr
import bcp_basic
import model_sim
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, base_analyze, parse_decorator, BaseParser


VERSION_APP_VALUE = 2


@parse_decorator
def analyze_sim(node, extract_deleted, extract_source):
    return base_analyze(SIMParser, 
                        node, 
                        bcp_basic.BASIC_SIM_INFORMATION, 
                        VERSION_APP_VALUE,
                        build_name='SIM 卡',
                        db_name='AppleSIM')


class SIMParser(BaseParser):
    """ iPhone 6_11.1.2_133217541373990_full(5)\
        mapping\private\var\wireless\Library\Databases\CellularUsage.db
        
        apple sim
        \mapping\private\var\wireless\Library\Databases\CellularUsage.db
    """
    def __init__(self, node, db_name):   
        super(SIMParser, self).__init__(node, db_name)
        self.root = node
        self.VERSION_VALUE_DB = model_sim.VERSION_VALUE_DB
        self.csm = model_sim.Model_SIM()
        self.Generate = model_sim.GenerateModel

    def parse_main(self):
        if self._read_db(node=self.root):
            self.parse_siminfo()

    def parse_siminfo(self):
        """ CellularUsage.db - subscriber_info

        RecNo	FieldName	
        1	ROWID	            INTEGER			
        2	subscriber_id	    TEXT			
        3	subscriber_mdn	    TEXT			
        4	tag	                INTEGER			
        5	last_update_time	INTEGER			
        6	slot_id	            INTEGER			
        """
        for rec in self._read_table(table_name='subscriber_info'):
            try:
                if self._is_empty(rec, 'ROWID', 'subscriber_mdn'):
                    continue
                sim = model_sim.SIM()
                sim._id    = rec['ROWID'].Value
                sim.msisdn = rec['subscriber_mdn'].Value
                sim.source = self.cur_db_source
                self.csm.db_insert_table_sim(sim)
            except:
                exc()
        self.csm.db_commit()
        