# coding=utf-8
__author__ = 'YangLiyuan'

import json

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_ticketing')
    clr.AddReference('bcp_gis')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import bcp_gis
import model_ticketing 
from ScriptUtils import CASE_NAME, exc, tp, DEBUG, base_analyze, parse_decorator, BaseParser


VERSION_APP_VALUE = 1


@parse_decorator
def analyze_12306(node, extract_deleted, extract_source):
    if 'es_recycle_content' in node.AbsolutePath:
        return ParserResults()
    return base_analyze(Apple12306Parser, 
                        node, 
                        bcp_gis.NETWORK_APP_TICKET_12306, 
                        VERSION_APP_VALUE,
                        '12306',
                        '12306_A')

class Apple12306Parser(BaseParser):
    ''' Library\Preferences\cn.12306.rails12306.plist '''

    def __init__(self, node, db_name):
        super(Apple12306Parser, self).__init__(node, db_name)
        self.VERSION_KEY_DB = 'db'
        self.VERSION_VALUE_DB = model_ticketing.VERSION_VALUE_DB
        self.VERSION_KEY_APP = 'app'
        self.root = node.Parent.Parent
        self.Generate = model_ticketing.GenerateModel
        self.csm = model_ticketing.Ticketing()
        self.accounts = {}

    def parse_main(self):
        datainfo_path = '/shared_prefs/12306data.xml'

        self.parse_account(datainfo_path)

    def parse_account(self, datainfo_path):
        try: 
            location_info = {}
            user_info = {}

            xml_data = self._read_xml(datainfo_path)
            if not xml_data:
                return 
            _data = xml_data.Elements('string') 
            for i in _data:
                if i.Attribute('name').Value == 'locationInfo':
                    location_info = json.loads(i.Value.decode('utf8'))
                elif i.Attribute('name').Value == 'kLoginSuccessKey':
                    user_info = json.loads(i.Value.decode('utf8'))

            if not location_info and not user_info:
                return 
            account = model_ticketing.Account()
            account.account_id = user_info.get('user_name', None)
            account.username   = user_info.get('name', None)
            account.nickname   = user_info.get('user_name', None)
            account.account_id = user_info.get('id_no', None)
            account.birthday   = self._convert_bir(user_info.get('born_date', None))
            account.telephone  = user_info.get('mobileNo', None)
            account.email      = user_info.get('email', None)
            account.city       = location_info.get('city', None)
            self.csm.db_insert_table_account(account)
            self.csm.db_commit()
        except:
            exc()

    @staticmethod
    def _convert_bir(birthday_str):
        '''
        
        Args:
            birthday_str (str): 19920426
        Returns:
            ts (int): timestamp(secoond)
        '''
        try:
            if not birthday_str or len(birthday_str) != 8:
                return None
            struct_time = time.strptime(birthday_str, "%Y%m%d")
            ts = time.mktime(struct_time)
            return ts
        except:
            exc()
            



        





