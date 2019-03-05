# -*- coding: utf-8 -*-
__author__ = "xiaoyuge"

from PA_runtime import *
import os
import clr
import shutil
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite
import hashlib

'''
字段说明（基本信息）
0	    COLLECT_TARGET_ID    手机取证采集目标编号
1	    SSID                 SSID
2	    CITY_CODE            城市代码
3	    OPERATOR_NET         运营商
4	    WiFi_UNIT_TYPE       单位类型
5	    ACCESS_TYPE          接入方式
6	    IP_ADDRESS           终端IP地址
7	    ENCRYPT_ALGORITHM    加密方式
8	    PASSWORD             密码
9	    AP_MAC               AP MAC地址
10	    AP_FIRMCODE          AP厂商组织机构代码
11	    DELETE_STATUS        删除状态
12	    DELETE_TIME          删除时间
13	    COMPANY_ADDRESS      详细地址
14	    LONGITUDE            经度
15	    LATITUDE             纬度
16	    ABOVE_SEALEVEL       海拔
17	    VISIT_TIME           连接时间
18	    CONNECT_TYPE         连接状态
'''

SQL_CREATE_TABLE_WA_MFORENSICS_080100 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_080100(
        COLLECT_TARGET_ID TEXT,
        SSID TEXT,
        CITY_CODE TEXT,
        OPERATOR_NET TEXT,
        WiFi_UNIT_TYPE TEXT,
        ACCESS_TYPE TEXT,
        IP_ADDRESS INTEGER,
        ENCRYPT_ALGORITHM TEXT,
        PASSWORD TEXT,
        AP_MAC TEXT,
        AP_FIRMCODE TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER,
        COMPANY_ADDRESS TEXT,
        LONGITUDE TEXT,
        LATITUDE TEXT,
        ABOVE_SEALEVEL TEXT,
        VISIT_TIME INTEGER,
        CONNECT_TYPE TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_080100 = '''
    INSERT INTO WA_MFORENSICS_080100(COLLECT_TARGET_ID,SSID,CITY_CODE,OPERATOR_NET,
    WiFi_UNIT_TYPE,ACCESS_TYPE,IP_ADDRESS,ENCRYPT_ALGORITHM,PASSWORD,AP_MAC,
    AP_FIRMCODE,DELETE_STATUS,DELETE_TIME,COMPANY_ADDRESS,LONGITUDE,LATITUDE,ABOVE_SEALEVEL,VISIT_TIME,CONNECT_TYPE)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（蓝牙设备）
0        COLLECT_TARGET_ID       手机取证采集目标编号
1        FRIEND_BLUETOOTH_MAC    对方蓝牙MAC地址
2        MATERIALS_NAME          设备名称
3        FRIEND_REMARK           备注名称
4        BLUETOOTH_TYPE          蓝牙设备类型
5        LAST_TIME               最后发现时间
6        VISIT_TIME              最后连接时间
7        DELETE_STATUS           删除状态
8        DELETE_TIME             删除时间
'''
SQL_CREATE_TABLE_WA_MFORENSICS_080200 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_080200(
        COLLECT_TARGET_ID TEXT,
        FRIEND_BLUETOOTH_MAC TEXT,
        MATERIALS_NAME TEXT,
        FRIEND_REMARK TEXT,
        BLUETOOTH_TYPE TEXT,
        LAST_TIME INTEGER,
        VISIT_TIME INTEGER,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_080200 = '''
    INSERT INTO WA_MFORENSICS_080200(COLLECT_TARGET_ID,FRIEND_BLUETOOTH_MAC,MATERIALS_NAME,FRIEND_REMARK,
    BLUETOOTH_TYPE,LAST_TIME,VISIT_TIME,DELETE_STATUS,PASSWORD,DELETE_TIME)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（蓝牙传输记录）
0        COLLECT_TARGET_ID       手机取证采集目标编号
1        BLUETOOTH_MAC           本机蓝牙MAC地址
2        FRIEND_BLUETOOTH_MAC    对方蓝牙MAC地址
3        MIME_NAME               文件名
4        MIME_PATH               文件路径
5        MIME_TYPE               文件类型
6        START_TIME              传输开始时间
7        END_TIME                传输结束时间
8        LOCAL_ACTION            本地动作
9        DELETE_STATUS           删除状态
10       DELETE_TIME             删除时间
'''

SQL_CREATE_TABLE_WA_MFORENSICS_080300 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_080300(
        COLLECT_TARGET_ID TEXT,
        BLUETOOTH_MAC TEXT,
        FRIEND_BLUETOOTH_MAC TEXT,
        MIME_NAME TEXT,
        MIME_PATH TEXT,
        MIME_TYPE TEXT,
        START_TIME INTEGER,
        END_TIME INTEGER,
        LOCAL_ACTION TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_080300 = '''
    INSERT INTO WA_MFORENSICS_080300(COLLECT_TARGET_ID,BLUETOOTH_MAC,FRIEND_BLUETOOTH_MAC,MIME_NAME,
    MIME_PATH,MIME_TYPE,START_TIME,END_TIME,LOCAL_ACTION,DELETE_STATUS,DELETE_TIME)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（基站信息）
0        COLLECT_TARGET_ID      手机取证采集目标编号
1        MCC                    移动国家号码
2        MNC                    移动网号
3        ECGI                   基站小区全局标识符
4        LAC                    位置区码
5        CellID                 小区识别
6        SID                    系统识别码
7        NID                    网络识别码
8        BASE_STATION_ID        基站识别码
9        CITY_CODE              地点名称
10       COMPANY_ADDRESS        详细地址
11       LONGITUDE              经度
12       LATITUDE               纬度
13       ABOVE_SEALEVEL         海拔
14       START_TIME             开始时间
15       END_TIME               结束时间
16       DELETE_STATUS          删除状态
17       DELETE_TIME            删除时间
'''

SQL_CREATE_TABLE_WA_MFORENSICS_080400 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_080400(
        COLLECT_TARGET_ID      TEXT,
        MCC                    INTEGER,
        MNC                    INTEGER,
        ECGI                   TEXT,
        LAC                    TEXT,
        CellID                 TEXT,
        SID                    TEXT,
        NID                    TEXT,
        BASE_STATION_ID        TEXT,
        CITY_CODE              TEXT,
        COMPANY_ADDRESS        TEXT,
        LONGITUDE              TEXT,
        LATITUDE               TEXT,
        ABOVE_SEALEVEL         TEXT,
        START_TIME             INTEGER,
        END_TIME               INTEGER,
        DELETE_STATUS          TEXT,
        DELETE_TIME            INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_080400 = '''
    INSERT INTO WA_MFORENSICS_080400(COLLECT_TARGET_ID,MCC,MNC,ECGI,
    LAC,CellID,SID,NID,BASE_STATION_ID,CITY_CODE,COMPANY_ADDRESS,
    LONGITUDE,LATITUDE,ABOVE_SEALEVEL,START_TIME,END_TIME,DELETE_STATUS,DELETE_TIME)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

ENCRYPT_ALGORITHM_WEP = '01'
ENCRYPT_ALGORITHM_WPA = '02'
ENCRYPT_ALGORITHM_WPA2 = '03'
ENCRYPT_ALGORITHM_PSK = '04'
ENCRYPT_ALGORITHM_WPA3 = '05'
ENCRYPT_ALGORITHM_OTHER = '99'

CONNECT_TYPE_LINKED_FALSE = '0'
CONNECT_TYPE_LINKED_TRUE = '1'
CONNECT_TYPE_OTHER = '99'

BLUETOOTH_TYPE_TERMINAL = '01'
BLUETOOTH_TYPE_COMPUTER = '02'
BLUETOOTH_TYPE_EARPHONE = '03'
BLUETOOTH_TYPE_LOUDSPEAKER = '04'
BLUETOOTH_TYPE_CAMERA = '05'
BLUETOOTH_TYPE_OTHER = '99'

LOCAL_ACTION_RECEIVER = '01'
LOCAL_ACTION_SENDER = '02'
LOCAL_ACTION_OTHER = '99'

WIFI_HOTSPOT = "21"
BLUETOOTH_DEVICE = "22"
BLUETOOTH_TRANSMISSION = "23"
BASESTATION_INFORMATION = "24"

class ConnectDeviceBcp(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None
    
    def db_create(self, db_path):
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except Exception as e:
            print("bcp_connectdevice db_create() remove %s error:%s"%(db_path, e))
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
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_080100
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_080200
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_080300
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_080400
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

    def db_insert_table_wifi_hotspot(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_080100, column.get_values())

    def db_insert_table_bluetooth_device(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_080200, column.get_values())

    def db_insert_table_bluetooth_transmission(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_080300, column.get_values())

    def db_insert_table_basestation_information(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_080400, column.get_values())

    
class WifiHotspot(object):
    def __init__(self):
        self.COLLECT_TARGET_ID  = None
        self.SSID               = None
        self.CITY_CODE          = None
        self.OPERATOR_NET       = None
        self.WiFi_UNIT_TYPE     = None
        self.ACCESS_TYPE        = None
        self.IP_ADDRESS         = None
        self.ENCRYPT_ALGORITHM  = None
        self.PASSWORD           = None
        self.AP_MAC             = None
        self.AP_FIRMCODE        = None
        self.DELETE_STATUS      = None
        self.DELETE_TIME        = None
        self.COMPANY_ADDRESS    = None
        self.LONGITUDE          = None
        self.LATITUDE           = None
        self.ABOVE_SEALEVEL     = None
        self.VISIT_TIME         = None
        self.CONNECT_TYPE       = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.SSID, self.CITY_CODE, self.OPERATOR_NET, 
                self.WiFi_UNIT_TYPE, self.ACCESS_TYPE, self.IP_ADDRESS, self.ENCRYPT_ALGORITHM,
                self.PASSWORD, self.AP_MAC, self.AP_FIRMCODE, self.DELETE_STATUS, self.DELETE_TIME,      
                self.COMPANY_ADDRESS, self.LONGITUDE, self.LATITUDE, self.ABOVE_SEALEVEL, 
                self.VISIT_TIME, self.CONNECT_TYPE)

class BluetoothDevice(object):
    def __init__(self):
        self.COLLECT_TARGET_ID      = None
        self.FRIEND_BLUETOOTH_MAC   = None
        self.MATERIALS_NAME         = None
        self.FRIEND_REMARK          = None
        self.BLUETOOTH_TYPE         = None
        self.LAST_TIME              = None
        self.VISIT_TIME             = None
        self.DELETE_STATUS          = None
        self.DELETE_TIME            = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.FRIEND_BLUETOOTH_MAC, self.MATERIALS_NAME, self.FRIEND_REMARK,       
                self.BLUETOOTH_TYPE, self.LAST_TIME, self.VISIT_TIME, self.DELETE_STATUS, self.DELETE_TIME)

class BluetoothTransmission(object):
    def __init__(self):
        self.COLLECT_TARGET_ID      = None
        self.FRIEND_BLUETOOTH_MAC   = None
        self.MATERIALS_NAME         = None
        self.FRIEND_REMARK          = None
        self.BLUETOOTH_TYPE         = None
        self.LAST_TIME              = None
        self.VISIT_TIME             = None
        self.DELETE_STATUS          = None
        self.DELETE_TIME            = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.FRIEND_BLUETOOTH_MAC, self.MATERIALS_NAME, self.FRIEND_REMARK,       
                self.BLUETOOTH_TYPE, self.LAST_TIME, self.VISIT_TIME, self.DELETE_STATUS, self.DELETE_TIME)

class BasestationInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID  = None
        self.MCC                = None
        self.MNC                = None
        self.ECGI               = None
        self.LAC                = None
        self.CellID             = None
        self.SID                = None
        self.NID                = None
        self.BASE_STATION_ID    = None
        self.CITY_CODE          = None
        self.COMPANY_ADDRESS    = None
        self.LONGITUDE          = None
        self.LATITUDE           = None
        self.ABOVE_SEALEVEL     = None
        self.START_TIME         = None
        self.END_TIME           = None
        self.DELETE_STATUS      = None
        self.DELETE_TIME        = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.MCC, self.MNC, self.ECGI, self.LAC, self.CellID, self.SID, self.NID,              
                self.BASE_STATION_ID, self.CITY_CODE, self.COMPANY_ADDRESS, self.LONGITUDE, self.LATITUDE, 
                self.ABOVE_SEALEVEL, self.START_TIME, self.END_TIME, self.DELETE_STATUS, self.DELETE_TIME)

class GenerateBcp(ConnectDeviceBcp):
    def __init__(self, bcp_path, cache_db, bcp_db, collect_target_id, mountDir):
        self.mountDir = mountDir
        self.bcp_path = bcp_path
        self.cache_db = cache_db
        self.collect_target_id = collect_target_id
        self.cache_path = bcp_db

    def generate(self):
        self.db_create(self.cache_path)
        # self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
        # self.db.Open()
        self._generate_wifi_hotspot()
        self._generate_bluetooth_device()
        self._generate_bluetooth_transmission()
        self._generate_basestation_info()
        self.db_close()

    def _generate_wifi_hotspot(self):
        '''生成wifi热点bcp'''
        pass

    def _generate_bluetooth_device(self):
        '''生成蓝牙设备bcp'''
        pass

    def _generate_bluetooth_transmission(self):
        '''生成蓝牙传输bcp'''
        pass

    def _generate_basestation_info(self):
        '''生成基站信息bcp'''
        try:
            db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
            db.Open()
            db_cmd = SQLite.SQLiteCommand(db)
            if db is None:
                return
            db_cmd.CommandText = '''select distinct * from WA_MFORENSICS_080400'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    base = BasestationInfo()
                    base.COLLECT_TARGET_ID = self.collect_target_id
                    base.MCC               = self._db_reader_get_int_value(sr, 1)
                    base.MNC               = self._db_reader_get_int_value(sr, 2)
                    base.ECGI              = self._db_reader_get_string_value(sr, 3)
                    base.LAC               = self._db_reader_get_string_value(sr, 4)
                    base.CellID            = self._db_reader_get_string_value(sr, 5)
                    base.SID               = self._db_reader_get_string_value(sr, 6)
                    base.NID               = self._db_reader_get_string_value(sr, 7)
                    base.BASE_STATION_ID   = self._db_reader_get_string_value(sr, 8)
                    base.CITY_CODE         = self._db_reader_get_string_value(sr, 9)
                    base.COMPANY_ADDRESS   = self._db_reader_get_string_value(sr, 10)
                    base.LONGITUDE         = self._db_reader_get_string_value(sr, 11)
                    base.LATITUDE          = self._db_reader_get_string_value(sr, 12)
                    base.ABOVE_SEALEVEL    = self._db_reader_get_string_value(sr, 13)
                    base.START_TIME        = self._db_reader_get_string_value(sr, 14)
                    base.END_TIME          = self._db_reader_get_string_value(sr, 15)
                    base.DELETE_STATUS     = self._db_reader_get_string_value(sr, 16)
                    base.DELETE_TIME       = self._db_reader_get_int_value(sr, 17)
                    self.db_insert_table_basestation_information(base)
                except:
                    pass
            self.db_commit()
            sr.Close()
            db_cmd.Dispose()
            db.Close()
        except:
            pass

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_float_value(reader, index, default_value=0):
        return reader.GetFloat(index) if not reader.IsDBNull(index) else default_value