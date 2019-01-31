# coding=utf-8

__author__ = 'YangLiyuan'


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

import json
import bcp_gis
import model_ticketing
from ScriptUtils import CASE_NAME, exc, tp, DEBUG, base_analyze, parse_decorator, BaseParser, BaseAndroidParser


VERSION_APP_VALUE = 1


@parse_decorator
def analyze_12306(node, extract_deleted, extract_source):
    if 'es_recycle_content' in node.AbsolutePath:
        return ParserResults()
    return base_analyze(Android12306Parser, 
                        node, 
                        bcp_gis.NETWORK_APP_TICKET_12306, 
                        VERSION_APP_VALUE,
                        '12306',
                        '12306_i')

class Android12306Parser(BaseParser):
    ''' Library\Preferences\cn.12306.rails12306.plist '''

    def __init__(self, node, db_name):
        super(Android12306Parser, self).__init__(node, db_name)
        self.VERSION_KEY_DB = 'db'
        self.VERSION_VALUE_DB = model_ticketing.VERSION_VALUE_DB
        self.VERSION_KEY_APP = 'app'

        self.root = node.Parent.Parent.Parent
        self.Generate = model_ticketing.GenerateModel
        self.csm = model_ticketing.Ticketing()

    def parse_main(self):
        ''' r'C5CDD820-C8A7-4E25-911F-1B5C3F72B39D\Documents\Preferences\9c7c560b34de477b.db' '''
        _path = '/Documents/Preferences/'
        _folder = self.root.GetByPath(_path)
        for db_file in _folder.Children:
            if db_file.Name.endswith('.db') and self._read_db(_path+db_file.Name):
                self.parse_account('__DEFAULTS__')

    def parse_account(self, table_name):
        '''
            FieldName	SQLType	
            key	            text
            data	        blob
            type	        integer
            options	        integer

            type:
                3 str
                5 加密
                8 bplist
        '''     
        location_info = {}
        for rec in self._read_table(table_name):
            if rec['type'].Value != 3:
                continue
            if rec['key'].Value=='locationInfo_12306data':
                ''' 
                {
                    "city" : "上海市",
                    "longitude" : "121.3781561957465",
                    "latitude" : "31.112197265625"
                }                
                '''    
                _data = rec['data'].Value
                location_info = json.loads(_data)

            elif rec['key'].Value=='kLoginSuccessKey_12306data':
                ''' 
                {
                    "is_receive" : "Y",
                    "id_no_show" : "510107199204263007",
                    "display_control_flag" : "1",
                    "country_code" : "CN",
                    "user_id" : "1500005532138",
                    "tk" : "uwfSKVScqYBFM3havcNwvh4RLK_k1lotChJkqAnxx2x0",
                    "check_id_flag" : "",
                    "notice_one_sessionMsg" : null,
                    "start_receive" : "Y",
                    "survey_encode_str" : "eGpoMTIzMDYjMCMwI0cjNzY0MzNGMjUxOERBNEQxMDk2NDVEQTg2NUQ4RjRBMzYjRUQ0N0VEREYx\r\nRjlDRkI1NDkxQkVDQjAzOTM1OTlCOTIzNDIxQ0VBMUZENTZDRENENDYyNjZFNUQ=\r\n",
                    "lc_query_cipher" : "F7r5Fmra7uMmL%2FbCBzMUadNQ6q3eV7sWuwum8hjRW7U%3D",
                    "is_bind" : "",
                    "last_msg" : "欢迎使用铁路12306。",
                    "id_type_code" : "1",
                    "name" : "徐xx",
                    "bind_str" : "",
                    "is_active" : "Y",
                    "pic_control_flag" : "",
                    "mobileNo" : "177*****9948",
                    "email" : "112****0518@qq.com",
                    "flag_msg" : "",
                    "user_type" : "1",
                    "user_status" : "1",
                    "is_valid_pay" : "N",
                    "integration_flag" : "1",
                    "id_no" : "510107199204263007",
                    "member_status" : "0",
                    "error_msg" : "",
                    "succ_flag" : "1",
                    "born_date" : "19920426",
                    "notice_one_session" : "Y",
                    "user_name" : "xjh***06"
                }                    
                '''
                _data = rec['data'].Value
                user_info = json.loads(_data)
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

    @staticmethod
    def _convert_bir(birthday_str):
        ''' '19920426' -> TS
        
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
            