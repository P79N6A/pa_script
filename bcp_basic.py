# -*- coding: utf-8 -*-

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

#基本信息脚本，脚本完成后请将字段说明删除！！！

'''
字段说明（基本信息）
0	COLLECT_TARGET_ID	手机取证采集目标编号
1	MANUFACTURER	厂商名称
2	SECURITY_SOFTWARE_ORGCODE	厂商组织机构代码
3	NAME	对象姓名
4	CERTIFICATE_TYPE	对象证件类型
5	CERTIFICATE_CODE	对象证件号码
6	CAPTURE_TIME	采集时间
7	PRINCIPAL_NAME	采集人姓名
8	POLICE_NO	负责人证件号码
9	INCHARGE_WA_DEPARTMENT	管辖地网安部门
10	CASE_NO	案件编号
11	CASE_TYPE	案件类型
12	CASE_NAME	案件名称
13	SEXCODE	对象性别
14	NATION	对象民族
15	BIRTHDAY	对象生日
16	REGISTERED_ADDRESS	对象住址
17	CERT_ISSUE_UNIT	对象证件签发机关
18	CERT_EEFFECT_DATE	对象证件生效日期
19	CERT_INVALID_DATE	对象证件失效日期
20	USER_PHOTO	对象证件头像
21	MATERIALS_NAME	采集设备名称
22	MODEL	采集设备型号
23	VERSION	采集设备硬件版本号
24	SOFTWARE_VERSION	采集设备软件版本号
25	MATERIALS_SERIAL	采集设备序列号
26	IP_ADDRESS	采集点IP
27	DUAL_TIME	采集时长
28	GA_CASE_NO	执法办案系统案件编号
29	GA_CASE_TYPE	执法办案系统案件类别
30	GA_CASE_NAME	执法办案系统案件名称
31	REMARK	备注
32	MIME_NAME	对象人像
33	MAINFILE	对象声纹
34	FINGER_PRINT_VALUE	对象指纹
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010100 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010100(
        COLLECT_TARGET_ID TEXT,
        MANUFACTURER TEXT,
        SECURITY_SOFTWARE_ORGCODE TEXT,
        NAME TEXT,
        CERTIFICATE_TYPE TEXT,
        CERTIFICATE_CODE TEXT,
        CAPTURE_TIME INTEGER,
        PRINCIPAL_NAME TEXT,
        POLICE_NO TEXT,
        INCHARGE_WA_DEPARTMENT TEXT,
        CASE_NO TEXT,
        CASE_TYPE TEXT,
        CASE_NAME TEXT,
        SEXCODE TEXT,
        NATION TEXT,
        BIRTHDAY DATETIME,
        REGISTERED_ADDRESS TEXT,
        CERT_ISSUE_UNIT TEXT,
        CERT_EEFFECT_DATE DATETIME,
        CERT_INVALID_DATE DATETIME,
        USER_PHOTO TEXT,
        MATERIALS_NAME TEXT,
        MODEL TEXT,
        VERSION TEXT,
        SOFTWARE_VERSION TEXT,
        MATERIALS_SERIAL TEXT,
        IP_ADDRESS  INTEGER,
        DUAL_TIME  INTEGER,
        GA_CASE_NO TEXT,
        GA_CASE_TYPE TEXT,
        GA_CASE_NAME TEXT,
        REMARK TEXT,
        MIME_NAME TEXT,
        MAINFILE TEXT,
        FINGER_PRINT_VALUE TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010100 = '''
    INSERT INTO WA_MFORENSICS_010100(COLLECT_TARGET_ID,MANUFACTURER,SECURITY_SOFTWARE_ORGCODE,NAME,
    CERTIFICATE_TYPE,CERTIFICATE_CODE,CAPTURE_TIME,PRINCIPAL_NAME,POLICE_NO,INCHARGE_WA_DEPARTMENT,
    CASE_NO,CASE_TYPE,CASE_NAME,SEXCODE,NATION,BIRTHDAY,REGISTERED_ADDRESS,CERT_ISSUE_UNIT,CERT_EEFFECT_DATE,
    CERT_INVALID_DATE,USER_PHOTO,MATERIALS_NAME,MODEL,VERSION,SOFTWARE_VERSION,MATERIALS_SERIAL,IP_ADDRESS,
    DUAL_TIME,GA_CASE_NO,GA_CASE_TYPE,GA_CASE_NAME,REMARK,MIME_NAME,MAINFILE,FINGER_PRINT_VALUE)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（终端信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	设备名称	MATERIALS_NAME
2	IMEI/ESN/MEID	IMEI_ESN_MEID
3	终端MAC地址	MAC
4	蓝牙MAC地址	BLUETOOTH_MAC
5	厂商名称	MANUFACTURER
6	型号	MODEL
7	特征描述	CHARACTERISTIC_DESC
8	机身容量	C_DISKS
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010200 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010200(
        COLLECT_TARGET_ID TEXT,
        MATERIALS_NAME TEXT,
        IMEI_ESN_MEID TEXT,
        MAC TEXT,
        BLUETOOTH_MAC TEXT,
        MANUFACTURER TEXT,
        MODEL TEXT,
        CHARACTERISTIC_DESC TEXT,
        C_DISKS INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010200 = '''
    INSERT INTO WA_MFORENSICS_010200(COLLECT_TARGET_ID,MATERIALS_NAME,IMEI_ESN_MEID,MAC,
    BLUETOOTH_MAC,MANUFACTURER,MODEL,CHARACTERISTIC_DESC,C_DISKS)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（移动设备身份码信息）
0   手机取证采集目标编号	COLLECT_TARGET_ID
1   IMEI/ESN/MEID	IMEI_ESN_MEID
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010201 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010201(
        COLLECT_TARGET_ID TEXT,
        IMEI_ESN_MEID TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010201 = '''
    INSERT INTO WA_MFORENSICS_010201(COLLECT_TARGET_ID, IMEI_ESN_MEID)
    VALUES(?, ?)'''

'''
字段说明（操作系统信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	操作系统类型	OS_TYPE
2	操作系统版本	OS_NAME
3	安装时间	INSTALLL_TIME
4	系统时区	TIMEZONE
5	破解状态	CRACK_STATUS
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010202 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010202(
        COLLECT_TARGET_ID TEXT,
        OS_TYPE TEXT,
        OS_NAME TEXT,
        INSTALLL_TIME INTEGER,
        TIMEZONE TEXT,
        CRACK_STATUS TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010202 = '''
    INSERT INTO WA_MFORENSICS_010202(COLLECT_TARGET_ID, MAIL_TOOL_TYPE, ACCOUNT, SEQUENCE_NAME,
    MATERIALS_NAME, FILE_NAME, DELETE_STATUS, DELETE_TIME, FILE_SIZE) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（SIM卡信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	本机号码	MSISDN
2	SIM卡IMSI	IMSI
3	短信中心号码	CENTER_NUMBER
4	删除状态	DELETE_STATUS
5	删除时间	DELETE_TIME
6	SIM卡ICCID	ICCID
7	使用状态	SIM_STATE
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010300 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010300(
        COLLECT_TARGET_ID TEXT,
        MSISDN TEXT,
        IMSI TEXT,
        CENTER_NUMBER TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER,
        ICCID TEXT,
        SIM_STATE TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010300 = '''
    INSERT INTO WA_MFORENSICS_010300(COLLECT_TARGET_ID,MSISDN,IMSI,CENTER_NUMBER,DELETE_STATUS,
    DELETE_TIME,ICCID,SIM_STATE) VALUES(?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（通讯录信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	通讯录ID	SEQUENCE_NAME
2	联系人姓名	RELATIONSHIP_NAME
3	加密状态	PRIVACYCONFIG
4	删除状态	DELETE_STATUS
5	删除时间	DELETE_TIME
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010400 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010400(
        COLLECT_TARGET_ID TEXT,
        SEQUENCE_NAME TEXT,
        RELATIONSHIP_NAME TEXT,
        PRIVACYCONFIG TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010400 = '''
    INSERT INTO WA_MFORENSICS_010400(COLLECT_TARGET_ID,SEQUENCE_NAME,RELATIONSHIP_NAME,PRIVACYCONFIG,DELETE_STATUS,DELETE_TIME)
    VALUES(?, ?, ?, ?, ?, ?)'''

'''
字段说明（通讯录详细信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	通讯录ID	SEQUENCE_NAME
2	通讯录字段类型	PHONE_VALUE_TYPE
3	字段标签	PHONE_NUMBER_TYPE
4	字段值	RELATIONSHIP_ACCOUNT
5	删除状态	DELETE_STATUS
6	删除时间	DELETE_TIME
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010500 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010500(
        COLLECT_TARGET_ID TEXT,
        SEQUENCE_NAME TEXT,
        PHONE_VALUE_TYPE TEXT,
        PHONE_NUMBER_TYPE TEXT,
        RELATIONSHIP_ACCOUNT TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010500 = '''
    INSERT INTO WA_MFORENSICS_010500(COLLECT_TARGET_ID,SEQUENCE_NAME,PHONE_VALUE_TYPE,PHONE_NUMBER_TYPE,RELATIONSHIP_ACCOUNT,DELETE_STATUS,DELETE_TIME)
    VALUES(?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（通话记录信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	本机号码	MSISDN
2	对方号码	RELATIONSHIP_ACCOUNT
3	联系人姓名	RELATIONSHIP_NAME
4	通话状态	CALL_STATUS
5	本地动作	LOCAL_ACTION
6	通话开始时间	START_TIME
7	通话结束时间	END_TIME
8	通话时长	DUAL_TIME
9	加密状态	PRIVACYCONFIG
10	删除状态	DELETE_STATUS
11	删除时间	DELETE_TIME
12	拦截状态	INTERCEPT_STATE
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010600 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010600(
        COLLECT_TARGET_ID TEXT,
        MSISDN TEXT,
        RELATIONSHIP_ACCOUNT TEXT,
        RELATIONSHIP_NAME TEXT,
        CALL_STATUS TEXT,
        LOCAL_ACTION TEXT,
        START_TIME INTEGER,
        END_TIME INTEGER,
        DUAL_TIME INTEGER,
        PRIVACYCONFIG TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER,
        INTERCEPT_STATE TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010600 = '''
    INSERT INTO WA_MFORENSICS_010600(COLLECT_TARGET_ID,MSISDN,RELATIONSHIP_ACCOUNT,RELATIONSHIP_NAME,
    CALL_STATUS,LOCAL_ACTION,START_TIME,END_TIME,DUAL_TIME,PRIVACYCONFIG,DELETE_STATUS,DELETE_TIME,
    INTERCEPT_STATE) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（短信记录信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	本机号码	MSISDN
2	对方号码	RELATIONSHIP_ACCOUNT
3	联系人姓名	RELATIONSHIP_NAME
4	本地动作	LOCAL_ACTION
5	发送时间	MAIL_SEND_TIME
6	短消息内容	CONTENT
7	查看状态	MAIL_VIEW_STATUS
8	存储位置	MAIL_SAVE_FOLDER
9	加密状态	PRIVACYCONFIG
10	删除状态	DELETE_STATUS
11	删除时间	DELETE_TIME
12	拦截状态	INTERCEPT_STATE
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010700 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010700(
        COLLECT_TARGET_ID TEXT,
        MSISDN TEXT,
        RELATIONSHIP_ACCOUNT TEXT,
        RELATIONSHIP_NAME TEXT,
        LOCAL_ACTION TEXT,
        MAIL_SEND_TIME INTEGER,
        CONTENT TEXT,
        MAIL_VIEW_STATUS TEXT,
        MAIL_SAVE_FOLDER TEXT,
        PRIVACYCONFIG TEXT,
        DELETE_STATUS INTEGER,
        DELETE_TIME TEXT,
        INTERCEPT_STATE TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010700 = '''
    INSERT INTO WA_MFORENSICS_010700(COLLECT_TARGET_ID,MSISDN,RELATIONSHIP_ACCOUNT,RELATIONSHIP_NAME,
    LOCAL_ACTION,MAIL_SEND_TIME,CONTENT,MAIL_VIEW_STATUS,MAIL_SAVE_FOLDER,PRIVACYCONFIG,DELETE_STATUS,
    DELETE_TIME,INTERCEPT_STATE) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

'''
字段说明（彩信记录信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	本机号码	MSISDN
2	对方号码	RELATIONSHIP_ACCOUNT
3	联系人姓名	RELATIONSHIP_NAME
4	本地动作	LOCAL_ACTION
5	发送时间	MAIL_SEND_TIME
6	彩信文本	CONTENT
7	彩信文件	MAINFILE
8	查看状态	MAIL_VIEW_STATUS
9	存储位置	MAIL_SAVE_FOLDER
10	加密状态	PRIVACYCONFIG
11	删除状态	DELETE_STATUS
12	删除时间	DELETE_TIME
13	拦截状态	INTERCEPT_STATE
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010800 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010800(
        COLLECT_TARGET_ID TEXT,
        MSISDN TEXT,
        RELATIONSHIP_ACCOUNT TEXT,
        RELATIONSHIP_NAME TEXT,
        LOCAL_ACTION TEXT,
        MAIL_SEND_TIME INTEGER,
        CONTENT TEXT,
        MAINFILE TEXT,
        MAIL_VIEW_STATUS TEXT,
        MAIL_SAVE_FOLDER TEXT,
        PRIVACYCONFIG TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER,
        INTERCEPT_STATE TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010800 = '''
    INSERT INTO WA_MFORENSICS_010800(COLLECT_TARGET_ID,MSISDN,RELATIONSHIP_ACCOUNT,RELATIONSHIP_NAME,
    LOCAL_ACTION,MAIL_SEND_TIME,CONTENT,MAINFILE,MAIL_VIEW_STATUS,MAIL_SAVE_FOLDER,PRIVACYCONFIG,
    DELETE_STATUS,DELETE_TIME,INTERCEPT_STATE) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''


'''
字段说明（日历记录信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	标题	TITLE
2	开始时间	START_TIME
3	结束时间	END_TIME
4	地点	EVENT_PLACE
5	描述	DESCRIPTION
6	创建时间	CREATE_TIME
7	最后修改时间	LATEST_MOD_TIME
8	删除状态	DELETE_STATUS
9	删除时间	DELETE_TIME
'''
SQL_CREATE_TABLE_WA_MFORENSICS_010900 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_010900(
        COLLECT_TARGET_ID TEXT,
        TITLE TEXT,
        START_TIME INTEGER,
        END_TIME INTEGER,
        EVENT_PLACE TEXT,
        DESCRIPTION TEXT,
        CREATE_TIME INTEGER,
        LATEST_MOD_TIME INTEGER,
        DELETE_STATUS TEXT,
        DELETE_TIME TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_010900 = '''
    INSERT INTO WA_MFORENSICS_010900(COLLECT_TARGET_ID,TITLE,START_TIME,END_TIME,EVENT_PLACE,
    DESCRIPTION,CREATE_TIME,LATEST_MOD_TIME,DELETE_STATUS,DELETE_TIME) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''


'''
字段说明（同步账号）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	本机同步账号	ACCOUNT
2	删除状态	DELETE_STATUS
3	删除时间	DELETE_TIME
4	密码	PASSWORD
'''
SQL_CREATE_TABLE_WA_MFORENSICS_011000 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_011000(
        COLLECT_TARGET_ID TEXT,
        ACCOUNT TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER,
        PASSWORD TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_011000 = '''
    INSERT INTO WA_MFORENSICS_011000(COLLECT_TARGET_ID,ACCOUNT,DELETE_STATUS,DELETE_TIME,PASSWORD)
    VALUES(?, ?, ?, ?, ?)'''

'''
字段说明（闹钟信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	标题	TITLE
2	闹钟时间	ALARM_TIME
3	重复频率	REPEAT
4	删除状态	DELETE_STATUS
5	删除时间	DELETE_TIME
'''
SQL_CREATE_TABLE_WA_MFORENSICS_011100 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_011100(
        COLLECT_TARGET_ID TEXT,
        ACCOUNT TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER,
        PASSWORD TEXT
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_011100 = '''
    INSERT INTO WA_MFORENSICS_011100(COLLECT_TARGET_ID,ACCOUNT,DELETE_STATUS,DELETE_TIME,PASSWORD)
    VALUES(?, ?, ?, ?, ?)'''

'''
字段说明（操作记录信息）
0	手机取证采集目标编号	COLLECT_TARGET_ID
1	时间	USER_ACTTIME
2	操作记录	ACTION_TYPE
3	删除状态	DELETE_STATUS
4	删除时间	DELETE_TIME
'''
SQL_CREATE_TABLE_WA_MFORENSICS_011200 = '''
    CREATE TABLE IF NOT EXISTS WA_MFORENSICS_011100(
        COLLECT_TARGET_ID TEXT,
        USER_ACTTIME INTEGER,
        ACTION_TYPE TEXT,
        DELETE_STATUS TEXT,
        DELETE_TIME INTEGER
    )'''

SQL_INSERT_TABLE_WA_MFORENSICS_011200 = '''
    INSERT INTO WA_MFORENSICS_011100(COLLECT_TARGET_ID,USER_ACTTIME,ACTION_TYPE,DELETE_STATUS,DELETE_TIME)
    VALUES(?, ?, ?, ?, ?)'''

PRIVACYCONFIG_PUBLIC = "0"
PRIVACYCONFIG_PRIVATE = "1"
PRIVACYCONFIG_LIMITED = "2"
PRIVACYCONFIG_OTHER = "9"

DELETE_STATUS_DELETED = "1"
DELETE_STATUS_INTACT = "0"

PHONE_VALUE_TYPE_PHONENUMBER = "01"
PHONE_VALUE_TYPE_EMAIL = "02"
PHONE_VALUE_TYPE_ADDR = "03"
PHONE_VALUE_TYPE_INSTANT_MSG = "04"
PHONE_VALUE_TYPE_WEB = "05"
PHONE_VALUE_TYPE_ANNIVERSARY = "06"
PHONE_VALUE_TYPE_REMARK = "07"
PHONE_VALUE_TYPE_GROUP = "08"
PHONE_VALUE_TYPE_OTHER = "99"

CALL_STATUS_MISSED = "0"
CALL_STATUS_CONNECTD = "1"
CALL_STATUS_OTHER = "9"

LOCAL_ACTION_RECEIVE = "01"
LOCAL_ACTION_SEND = "02"
LOCAL_ACTION_OTHER = "99"

PRAVACYCONFIG_ENCRYPTED = "1"
PRACACYCONFIG_UNENCRYPTED = "0"

INTERCEPT_STATUS_TRUE = "1"
INTERCEPT_STATUS_FALSE = "0"

BASIC_NORMAL_INFORMATION = "01"
BASIC_TERMINAL_INFOMATION = "02"
BASIC_DEVICE_INFORMATION = "03"
BASIC_OS_INFORMATION = "04"
BASIC_SIM_INFORMATION = "05"
BASIC_CONTACT_INFORMATION = "06"
BASIC_CONTACT_DETAILED_INFORMATION = "07"
BASIC_RECORD_INFORMATION = "08"
BASIC_SMS_INFORMATION = "09"
BASIC_MMS_INFORMATION = "10"
BASIC_CALENDAR_INFOMATION = "11"
BASIC_SYNC_ACCOUNT = "12"
BASIC_CLOCK_INFORMATION = "13"
BASIC_OPERATE_RECORD = "14"


class BasicBcp(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None
    
    def db_create(self, db_path):
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except Exception as e:
            print("bcp_mail db_create() remove %s error:%s"%(db_path, e))
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
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010100
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010200
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010201
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010202
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010300
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010400
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010500
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010600
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010700
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010800
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_010900
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_011000
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_011100
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_011200
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

    def db_insert_table_basic_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010100, column.get_values())

    def db_insert_table_terminal_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010200, column.get_values())

    def db_insert_table_mobile_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010201, column.get_values())

    def db_insert_table_os_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010202, column.get_values())

    def db_insert_table_sim_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010300, column.get_values())

    def db_insert_table_contact_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010400, column.get_values())

    def db_insert_table_contact_detail(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010500, column.get_values())

    def db_insert_table_call_record(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010600, column.get_values())

    def db_insert_table_sms_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010700, column.get_values())

    def db_insert_table_mms_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010800, column.get_values())

    def db_insert_table_calendar_info(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_010900, column.get_values())

    def db_insert_table_sync_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_011000, column.get_values())

    def db_insert_table_alarm_clock(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_011100, column.get_values())

    def db_insert_table_operate_record(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_011200, column.get_values())

    
class BasicInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MANUFACTURER = None
        self.SECURITY_SOFTWARE_ORGCODE = None
        self.NAME = None
        self.CERTIFICATE_TYPE = None
        self.CERTIFICATE_CODE = None
        self.CAPTURE_TIME = None
        self.PRINCIPAL_NAME = None
        self.POLICE_NO = None
        self.INCHARGE_WA_DEPARTMENT = None
        self.CASE_NO = None
        self.CASE_TYPE = None
        self.CASE_NAME = None
        self.SEXCODE = None
        self.NATION = None
        self.BIRTHDAY = None
        self.REGISTERED_ADDRESS = None
        self.CERT_ISSUE_UNIT = None
        self.CERT_EEFFECT_DATE = None
        self.CERT_INVALID_DATE = None
        self.USER_PHOTO = None
        self.MATERIALS_NAME = None
        self.MODEL = None
        self.VERSION = None
        self.SOFTWARE_VERSION = None
        self.MATERIALS_SERIAL = None
        self.IP_ADDRESS = None
        self.DUAL_TIME = None
        self.GA_CASE_NO = None
        self.GA_CASE_TYPE = None
        self.GA_CASE_NAME = None
        self.REMARK = None
        self.MIME_NAME = None
        self.MAINFILE = None
        self.FINGER_PRINT_VALUE = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.MANUFACTURER,self.SECURITY_SOFTWARE_ORGCODE,self.NAME,
        self.CERTIFICATE_TYPE,self.CERTIFICATE_CODE,self.CAPTURE_TIME,self.PRINCIPAL_NAME,self.POLICE_NO,
        self.INCHARGE_WA_DEPARTMENT,self.CASE_NO,self.CASE_TYPE,self.CASE_NAME,self.SEXCODE,self.NATION,
        self.BIRTHDAY,self.REGISTERED_ADDRESS,self.CERT_ISSUE_UNIT,self.CERT_EEFFECT_DATE,self.CERT_INVALID_DATE,
        self.USER_PHOTO,self.MATERIALS_NAME,self.MODEL,self.VERSION,self.SOFTWARE_VERSION,self.MATERIALS_SERIAL,
        self.IP_ADDRESS,self.DUAL_TIME,self.GA_CASE_NO,self.GA_CASE_TYPE,self.GA_CASE_NAME,self.REMARK,self.MIME_NAME,
        self.MAINFILE,self.FINGER_PRINT_VALUE)


class TerminalInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MATERIALS_NAME = None
        self.IMEI_ESN_MEID = None
        self.MAC = None
        self.BLUETOOTH_MAC = None
        self.MANUFACTURER = None
        self.MODEL = None
        self.CHARACTERISTIC_DESC = None
        self.C_DISKS = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.MATERIALS_NAME,self.IMEI_ESN_MEID,self.MAC,
        self.BLUETOOTH_MAC,self.MANUFACTURER,self.MODEL,self.CHARACTERISTIC_DESC,self.C_DISKS)


class MobileInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.IMEI_ESN_MEID = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID, self.IMEI_ESN_MEID)


class OSInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.OS_TYPE = None
        self.OS_NAME = None
        self.INSTALLL_TIME = None
        self.TIMEZONE = None
        self.CRACK_STATUS = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.OS_TYPE,self.OS_NAME,self.INSTALLL_TIME,self.TIMEZONE,self.CRACK_STATUS)


class SIMInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MSISDN = None
        self.IMSI = None
        self.CENTER_NUMBER = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.ICCID = None
        self.SIM_STATE = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.MSISDN,self.IMSI,self.CENTER_NUMBER,self.DELETE_STATUS,self.DELETE_TIME,self.ICCID,self.SIM_STATE)


class ContactInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.SEQUENCE_NAME = None
        self.RELATIONSHIP_NAME = None
        self.PRIVACYCONFIG = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.SEQUENCE_NAME,self.RELATIONSHIP_NAME,self.PRIVACYCONFIG,self.DELETE_STATUS,self.DELETE_TIME)


class ContactDetail(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.SEQUENCE_NAME = None
        self.PHONE_VALUE_TYPE = None
        self.PHONE_NUMBER_TYPE = None
        self.RELATIONSHIP_ACCOUNT = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.SEQUENCE_NAME,self.PHONE_VALUE_TYPE,self.PHONE_NUMBER_TYPE,self.RELATIONSHIP_ACCOUNT,self.DELETE_STATUS,self.DELETE_TIME)


class CallRecords(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MSISDN = None
        self.RELATIONSHIP_ACCOUNT = None
        self.RELATIONSHIP_NAME = None
        self.CALL_STATUS = None
        self.LOCAL_ACTION = None
        self.START_TIME = None
        self.END_TIME = None
        self.DUAL_TIME = None
        self.PRIVACYCONFIG = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.INTERCEPT_STATE = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.MSISDN,self.RELATIONSHIP_ACCOUNT,self.RELATIONSHIP_NAME,self.CALL_STATUS,
        self.LOCAL_ACTION,self.START_TIME,self.END_TIME,self.DUAL_TIME,self.PRIVACYCONFIG,self.DELETE_STATUS,self.DELETE_TIME,self.INTERCEPT_STATE)


class SMSInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MSISDN = None
        self.RELATIONSHIP_ACCOUNT = None
        self.RELATIONSHIP_NAME = None
        self.LOCAL_ACTION = None
        self.MAIL_SEND_TIME = None
        self.CONTENT = None
        self.MAIL_VIEW_STATUS = None
        self.MAIL_SAVE_FOLDER = None
        self.PRIVACYCONFIG = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.INTERCEPT_STATE = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.MSISDN,self.RELATIONSHIP_ACCOUNT,self.RELATIONSHIP_NAME,self.LOCAL_ACTION,
                self.MAIL_SEND_TIME,self.CONTENT,self.MAIL_VIEW_STATUS,self.MAIL_SAVE_FOLDER,self.PRIVACYCONFIG,
                self.DELETE_STATUS,self.DELETE_TIME,self.INTERCEPT_STATE)


class MMSInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.MSISDN = None
        self.RELATIONSHIP_ACCOUNT = None
        self.RELATIONSHIP_NAME = None
        self.LOCAL_ACTION = None
        self.MAIL_SEND_TIME = None
        self.CONTENT = None
        self.MAINFILE = None
        self.MAIL_VIEW_STATUS = None
        self.MAIL_SAVE_FOLDER = None
        self.PRIVACYCONFIG = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.INTERCEPT_STATE = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.MSISDN,self.RELATIONSHIP_ACCOUNT,self.RELATIONSHIP_NAME,
                self.LOCAL_ACTION,self.MAIL_SEND_TIME,self.CONTENT,self.MAINFILE,self.MAIL_VIEW_STATUS,
                self.MAIL_SAVE_FOLDER,self.PRIVACYCONFIG,self.DELETE_STATUS,self.DELETE_TIME,self.INTERCEPT_STATE)


class CalendarInfo(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.TITLE = None
        self.START_TIME = None
        self.END_TIME = None
        self.EVENT_PLACE = None
        self.DESCRIPTION = None
        self.CREATE_TIME = None
        self.LATEST_MOD_TIME = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.TITLE,self.START_TIME,self.END_TIME,self.EVENT_PLACE,
                self.DESCRIPTION,self.CREATE_TIME,self.LATEST_MOD_TIME,self.DELETE_STATUS,self.DELETE_TIME)


class SyncAccount(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.ACCOUNT = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None
        self.PASSWORD = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.ACCOUNT,self.DELETE_STATUS,self.DELETE_TIME,self.PASSWORD)


class AlarmClock(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.TITLE = None
        self.ALARM_TIME = None
        self.REPEAT = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.TITLE,self.ALARM_TIME,self.REPEAT,self.DELETE_STATUS,self.DELETE_TIME)


class OperateRecord(object):
    def __init__(self):
        self.COLLECT_TARGET_ID = None
        self.USER_ACTTIME = None
        self.ACTION_TYPE = None
        self.DELETE_STATUS = None
        self.DELETE_TIME = None

    def get_values(self):
        return (self.COLLECT_TARGET_ID,self.USER_ACTTIME,self.ACTION_TYPE,self.DELETE_STATUS,self.DELETE_TIME)


class GenerateBcp(object):
    def __init__(self, bcp_path, cache_db, bcp_db, collect_target_id, mountDir):
        self.mountDir = mountDir
        self.bcp_path = bcp_path
        self.cache_db = cache_db
        self.collect_target_id = collect_target_id
        self.cache_path = bcp_db
        self.basic = BasicBcp()

    def generate(self):
        self.basic.db_create(self.cache_path)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
        self.db.Open()
        self._generate_basic_info()
        self._generate_terminal_info()
        self._generate_mobile_info()
        self._generate_os_info()
        self._generate_sim_info()
        self._generate_contact_info()
        self._generate_contact_detail()
        self._generate_call_record()
        self._generate_sms_info()
        self._generate_mms_info()
        self._generate_calendar_info()
        self._generate_sync_account()
        self._generate_alarm_clock()
        self._generate_operate_record()
        self.db.Close()
        self.basic.db_close()

    def _generate_demo(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from xxx'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                xxx = XXX()
                xxx.COLLECT_TARGET_ID = self.collect_target_id
                self.mail.db_insert_table_xxx(xxx)
            self.mail.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            print(e)

    def _generate_basic_info(self):
        pass

    def _generate_terminal_info(self):
        pass

    def _generate_mobile_info(self):
        pass

    def _generate_os_info(self):
        pass

    def _generate_sim_info(self):
        '''
         0   _id           INTEGER PRIMARY KEY AUTOINCREMENT,
         1   name          TEXT,
         2   msisdn        TEXT,
         3   imsi          TEXT,
         4   iccid         TEXT,
         5   center_num    TEXT,
         6   is_use        INT,
         7   source        TEXT,
         8   deleted       INT DEFAULT 0, 
         9   repeated      INT DEFAULT 0
        '''
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from sim'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                sim = SIMInfo()
                sim.COLLECT_TARGET_ID = self.collect_target_id
                sim.MSISDN            = sr[2]
                sim.IMSI              = sr[3]
                sim.CENTER_NUMBER     = sr[5]
                sim.DELETE_STATUS     = DELETE_STATUS_DELETED if sr[8] == 1 else DELETE_STATUS_INTACT
                sim.ICCID             = sr[4]
                # sim.SIM_STATE = None
                self.basic.db_insert_table_sim_info(sim)
            self.basic.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            pass

    def _generate_contact_info(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from contacts'''
            sr = self.db_cmd.ExecuteReader()
            count = 0
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                count += 1
                contactinfo = ContactInfo()
                contactinfo.COLLECT_TARGET_ID = self.collect_target_id
                sid = '0000000000000000000000000000000' + str(count)
                contactinfo.SEQUENCE_NAME = sid[-32::]
                contactinfo.RELATIONSHIP_NAME = sr[9]
                a = sr[15]
                contactinfo.DELETE_STATUS = DELETE_STATUS_DELETED if sr[15] == 1 else DELETE_STATUS_INTACT
                self.basic.db_insert_table_contact_info(contactinfo)
            self.basic.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            pass

    def _generate_contact_detail(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from contacts'''
            sr = self.db_cmd.ExecuteReader()
            count = 0
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                count += 1
                contactdetail = ContactDetail()
                contactdetail.COLLECT_TARGET_ID = self.collect_target_id
                sid = '0000000000000000000000000000000' + str(count)
                contactdetail.SEQUENCE_NAME = sid[-32::]
                contactdetail.RELATIONSHIP_ACCOUNT = sr[8]
                a = sr[15]
                contactdetail.DELETE_STATUS = DELETE_STATUS_DELETED if sr[15] == 1 else DELETE_STATUS_INTACT
                self.basic.db_insert_table_contact_detail(contactdetail)
            self.basic.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            pass

    def _generate_call_record(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from records'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                record = CallRecords()
                record.COLLECT_TARGET_ID = self.collect_target_id
                record.RELATIONSHIP_ACCOUNT = sr[1]
                record.RELATIONSHIP_NAME = sr[5]
                record.CALL_STATUS = CALL_STATUS_CONNECTD if sr[4] is 1 else CALL_STATUS_MISSED if sr[4] is 3 else CALL_STATUS_OTHER
                record.LOCAL_ACTION = LOCAL_ACTION_RECEIVE if sr[4] is 1 or sr[4] is 3 else LOCAL_ACTION_SEND if sr[4] is 2 else LOCAL_ACTION_OTHER
                record.DUAL_TIME = sr[3]
                record.DELETE_STATUS = DELETE_STATUS_DELETED if sr[12] == 1 else DELETE_STATUS_INTACT
                self.basic.db_insert_table_call_record(record)
            self.basic.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            pass

    def _generate_sms_info(self):
        ''' table - sms
            0    _id                 TEXT, 
            1    sim_id              INT,
            2    sender_phonenumber  TEXT,
            3    sender_name         TEXT,
            4    read_status         INT DEFAULT 0,
            5    type                INT,
            6    suject              TEXT,
            7    body                TEXT,
            8    send_time           INT,
            9    delivered_date      INT,
            10    is_sender           INT,
            11    source              TEXT,
            12    deleted             INT DEFAULT 0, 
            13    repeated            INT DEFAULT 0          
        '''
        MAIL_TYPE_2_FOLDER = {
            0: '99', # ALL - 其他
            1: '01', # INBOX - 收件箱
            3: '03', # DRAFT - 草稿箱
            4: '02', # OUTBOX - 发件箱
        }
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from sms'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                sms = SMSInfo()
                sms.COLLECT_TARGET_ID = self.collect_target_id
                # sms.MSISDN
                sms.RELATIONSHIP_ACCOUNT = sr[2]
                sms.RELATIONSHIP_NAME    = sr[3]
                if sr[10] == 1:     # is_sender
                    sms.LOCAL_ACTION = '02'
                elif sr[10] == 0:
                    sms.LOCAL_ACTION = '01'
                else:
                    sms.LOCAL_ACTION = '99'
                sms.MAIL_SEND_TIME   = sr[8]
                sms.CONTENT          = sr[7]
                sms.MAIL_VIEW_STATUS = sr[4]
                sms.MAIL_SAVE_FOLDER = MAIL_TYPE_2_FOLDER.get(sr[5], '99')
                sms.DELETE_STATUS    = DELETE_STATUS_DELETED if sr[12] == 1 else DELETE_STATUS_INTACT
                # sms.PRIVACYCONFIG
                # sms.DELETE_TIME          
                # sms.INTERCEPT_STATE
                self.basic.db_insert_table_sms_info(sms)
            self.basic.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            pass

    def _generate_mms_info(self):
        pass

    def _generate_calendar_info(self):
        try:
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            if self.db_cmd is None:
                return
            self.db_cmd.CommandText = '''select distinct * from calendar'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                if canceller.IsCancellationRequested:
                    break
                calendar = CalendarInfo()
                calendar.COLLECT_TARGET_ID = self.collect_target_id
                if not IsDBNull(sr[1]):
                    calendar.TITLE = sr[1]
                if not IsDBNull(sr[5]):
                    calendar.START_TIME = self._get_timestamp(int(sr[5]))
                if not IsDBNull(sr[7]):
                    calendar.END_TIME = self._get_timestamp(int(sr[7]))
                if not IsDBNull(sr[3]):
                    calendar.DESCRIPTION = sr[3]
                if not IsDBNull(sr[12]):
                    calendar.DELETE_STATUS = DELETE_STATUS_DELETED if sr[12] == 1 else DELETE_STATUS_INTACT
                self.basic.db_insert_table_calendar_info(calendar)
            self.basic.db_commit()
            self.db_cmd.Dispose()
        except Exception as e:
            pass

    def _generate_sync_account(self):
        pass

    def _generate_alarm_clock(self):
        pass

    def _generate_operate_record(self):
        pass


    @staticmethod
    def _copy_attachment(mountDir, dir):
        x = mountDir + dir
        sourceDir = x.replace('\\','/')
        targetDir = self.bcp_path + '\\attachment'
        try:
            if not os.path.exists(targetDir):
                shutil.copytree(sourceDir, targetDir)
        except Exception as e:
            print(e)

    @staticmethod
    def _convert_delete_status(status):
        if status == 0:
            return DELETE_STATUS_NOT_DELETED
        else:
            return DELETE_STATUS_DELETED

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)

