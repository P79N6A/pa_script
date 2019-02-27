# coding=utf-8

__author__ = 'YangLiyuan'

import json

import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('model_secure')
    clr.AddReference('bcp_im')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import bcp_im
import model_secure 
from ScriptUtils import CASE_NAME, DEBUG, exc, tp, base_analyze, parse_decorator, BaseParser, BaseAndroidParser


VERSION_APP_VALUE = 1


@parse_decorator
def analyze_tcsecure(node, extract_deleted, extract_source):
    return base_analyze(AndroidTencentSecureParser, 
                        node, 
                        bcp_im.CONTACT_ACCOUNT_TYPE_IM_OTHER, 
                        VERSION_APP_VALUE,
                        bulid_name='腾讯手机管家',
                        db_name='TCSecure_A')


class AndroidTencentSecureParser(BaseParser):
    ''' com.tencent.qqpimsecure
    
        Android
            !encryptqqsecure2.db
            !fea_tunnel_en.db
            
            qqsecure.db
                wx_favorite         
                tb_software_info        
                recent_iden
                wifi_signal_table

                pf_soft_list_profile_db_table_name
                sw_system_software_info     app 包名, 名称, 版本
    '''
    def __init__(self, node, db_name):
        super(AndroidTencentSecureParser, self).__init__(node, db_name)
        self.VERSION_VALUE_DB = model_secure.VERSION_VALUE_DB
        self.root = node.Parent
        self.Generate = model_secure.GenerateModel
        self.csm = model_secure.SM()

    def parse_main(self):
        if self._read_db('qqsecure.db'):
            self.parse_wifi('wifi_signal_table')
            self.parse_callrecord('recent_iden')
        
    def parse_wifi(self, table_name):
        ''' wifi_signal_table

            FieldName	    SQLType         	
            ssid	            TEXT
            bssid	            TEXT
            first_recog_time	TEXT
            last_recog_time	    TEXT
            signal_hist	        TEXT
        '''
        for rec in self._read_table(table_name):
            try:
                if self._is_email_format(rec, 'ssid', 'bssid'):
                    continue
                wifi = model_secure.WifiSignal()
                wifi.ssid = rec['ssid'].Value
                wifi.bssid = rec['bssid'].Value
                wifi.first_time = rec['first_recog_time'].Value
                wifi.last_time = rec['last_recog_time'].Value
                self.csm.db_insert_table_wifi_signal(wifi)
            except:
                exc()
        self.csm.db_commit()

    def parse_callrecord(self, table_name):
        ''' recent_iden

            FieldName	SQLType		
            id	            INTEGER 			
            phone_number	TEXT    			
            value1	        TEXT
            value2	        INT2    			
        '''
        for rec in self._read_table(table_name):
            try:
                cr = model_secure.Callrecord()
                cr.id = rec['id'].Value
                cr.phone_number = rec['phone_number'].Value
                # cr.date
                # cr.call_type = model_secure.
                self.csm.db_insert_table_callrecord(cr)
            except:
                exc()
        self.csm.db_commit()
            