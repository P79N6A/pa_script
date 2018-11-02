#coding:utf-8

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

import pickle
from System.Xml.Linq import *
import System.Data.SQLite as SQLite

import os
import sqlite3

# 应用密码信息
SQL_CREATE_TABLE_WA_MFORENSICS_090100 = '''
    create table if not exists WA_MFORENSICS_090100(
        COLLECT_TARGET_ID TEXT,
        SOFTWARE_NAME TEXT,
        USER_NAME TEXT,
        PASSWORD TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
    '''

# 内容详细信息
SQL_CREATE_TABLE_WA_MFORENSICS_090200 = '''
    create table if not exists WA_MFORENSICS_090200(
        COLLECT_TARGET_ID TEXT,
        RECORD_ID TEXT,
        RESOURCE_ORDER TEXT,
        MEDIA_TYPE TEXT,
        CONTENT TEXT,
        MAINFILE TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
'''

# 应用安装信息
SQL_CREATE_TABLE_WA_MFORENSICS_090300 = '''
    create table if not exists WA_MFORENSICS_090300(
        COLLECT_TARGET_ID   TEXT,
        SOFTWARE_NAME   TEXT,
        SOFTWARE_TYPE   TEXT,
        SOFTWARE_VERSION  TEXT,
        SOTFWARE_PATH   TEXT,
        INSTALLL_TIME   INT,
        ANTI_FORENSIC_TYPE  TEXT,
        ANTI_FORENSIC_NAME  TEXT,
        FILE_SIZE   INT,
        PACKAGE_NAME  TEXT,
        SOFTWARE_SIGN  TEXT,
        LATEST_MOD_TIME  INT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
    '''

# 元数据信息
SQL_CREATE_TABLE_WA_MFORENSICS_090400 = '''
    create table if not exists WA_MFORENSICS_090400(
        COLLECT_TARGET_ID TEXT,
        MEDIA_TYPE TEXT,
        MIME_ORIGINAL_NAME TEXT,
        LOCATE_TYPE TEXT,
        LOGIN_TIME INT,
        CITY_CODE TEXT,
        COMPANY_ADDRESS TEXT,
        LONGITUDE TEXT,
        LATITUDE TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
'''

# 账号关联信息
SQL_CREATE_TABLE_WA_MFORENSICS_090500 = '''
    create table if not exists WA_MFORENSICS_090500(
        COLLECT_TARGET_ID TEXT,
        NETWORK_APP TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT TEXT,
        APP_TYPE TEXT,
        FRIEND_ID TEXT,
        RELATE_ACCOUNT TEXT,
        REL_TYPE TEXT,
        DELETE_STATUS TEXT DEFAULT "0",
        DELETE_TIME INT)
'''

# 数据校验信息
SQL_CREATE_TABLE_WA_MFORENSICS_090600 = '''
    create table if not exists WA_MFORENSICS_090600(
        COLLECT_TARGET_ID TEXT,
        MAINFILE TEXT,
        HASH TEXT,
        HASH_TYPE TEXT)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_090100 = '''
    insert into WA_MFORENSICS_090100(COLLECT_TARGET_ID,SOFTWARE_NAME,USER_NAME,PASSWORD,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_090200 = '''
    insert into WA_MFORENSICS_090200(COLLECT_TARGET_ID,RECORD_ID,RESOURCE_ORDER,MEDIA_TYPE,CONTENT,MAINFILE,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_090300 = '''
    insert into WA_MFORENSICS_090300(COLLECT_TARGET_ID,SOFTWARE_NAME,SOFTWARE_TYPE,SOFTWARE_VERSION,SOTFWARE_PATH,INSTALLL_TIME,ANTI_FORENSIC_TYPE,
    ANTI_FORENSIC_NAME,FILE_SIZE,PACKAGE_NAME,SOFTWARE_SIGN,LATEST_MOD_TIME,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_090400 = '''
    insert into WA_MFORENSICS_090400(COLLECT_TARGET_ID,MEDIA_TYPE,MIME_ORIGINAL_NAME,LOCATE_TYPE,LOGIN_TIME,CITY_CODE,
    COMPANY_ADDRESS,LONGITUDE,LATITUDE,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?,?)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_090500 = '''
    insert into WA_MFORENSICS_090500(COLLECT_TARGET_ID,NETWORK_APP,ACCOUNT_ID,ACCOUNT,APP_TYPE,FRIEND_ID,RELATE_ACCOUNT,REL_TYPE,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?)
'''

SQL_INSERT_TABLE_WA_MFORENSICS_090600 = '''
    insert into WA_MFORENSICS_090600(COLLECT_TARGET_ID,NETWORK_APP,ACCOUNT_ID,ACCOUNT,APP_TYPE,FRIEND_ID,RELATE_ACCOUNT,REL_TYPE,DELETE_STATUS,DELETE_TIME)
    values(?,?,?,?,?,?,?,?,?,?)
'''


# 删除状态
DELETE_STATUS_UNDELETED = "0"
DELETE_STATUS_DELETED = "1"


# 类型代码
BCP_OTHER_APP_PWD = "15"
BCP_OTHER_APP_CONTENT = "16"
BCP_OTHER_APP_INSTALLED = "17"
BCP_OTHER_META_DATA = "18"
BCP_OTHER_ACCOUNT_ASS = "19"
BCP_OTHER_DATA_VER = "20"


class OtherInfo(object):

    def __init__(self):
        self.db = None
        self.db_command = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                print("{0} remove failed!".format(db_path))
        
        self.db = SQLite.SQLiteConnection("Data Source = {0}".format(db_path))
        self.db.Open()
        self.db_command = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()

        self.db_create_table()
        self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_command is not None:
            self.db_command.Dispose()
            self.db_command = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            try:
                self.db_trans.Commit()
            except Exception as e:
                self.db_trans.RollBack()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_command is not None:
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_090100
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_090200
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_090300
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_090400
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_090500
            self.db_command.ExecuteNonQuery()
            self.db_command.CommandText = SQL_CREATE_TABLE_WA_MFORENSICS_090600

    def db_insert_table(self, sql, values):
        if self.db_command is not None:
            self.db_command.CommandText = sql
            self.db_command.Parameters.Clear()
            for value in values:
                param = self.db_command.CreateParameter()
                param.Value = value
                self.db_command.Parameters.Add(param)
            self.db_command.ExecuteNonQuery()


    def db_insert_table_apppwd(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_090100, column.get_values())

    def db_insert_table_appcontent(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_090200, column.get_values())
    
    def db_insert_table_installedapp(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_090300, column.get_values())

    def db_insert_table_metadata(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_090400, column.get_values())

    def db_insert_table_accountassociated(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_090500, column.get_values())

    def db_insert_table_dataverification(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_WA_MFORENSICS_090600, column.get_values())


class Apppwd(object):
    
    def __init__(self):
        self.collect_target_id = None
        self.software_name = None
        self.user_name = None
        self.password = None
        self.delete_status = None
        self.delete_time = None

    def get_values(self):
        return(self.collect_target_id,self.software_name,self.user_name,self.password,self.delete_status,self.delete_time)


class Appcontent(object):

    def __init__(self):
        self.collect_target_id = None
        self.record_id = None
        self.resource_order = None
        self.media_type = None
        self.content = None
        self.mainfile = None
        self.delete_status = None
        self.delete_time = None

    def get_values(self):
        return(self.collect_target_id,self.record_id,self.resource_order,self.media_type,self.content,
        self.mainfile,self.delete_status,self.delete_time)


class Appinstalled(object):

    def __init__(self):
        self.collect_target_id = None
        self.software_name = None
        self.software_type = None
        self.software_version = None
        self.sotfware_path = None
        self.installl_time = None
        self.anti_forensic_type = None
        self.anti_forensic_name = None
        self.file_size = None
        self.package_name = None
        self.software_sign = None
        self.latest_mod_time = None
        self.delete_status = None
        self.delete_time = None

    def get_values(self):
        return(self.collect_target_id,self.software_name,self.software_type,self.software_version,self.sotfware_path,
        self.installl_time,self.anti_forensic_type,self.anti_forensic_name,self.file_size,self.package_name,
        self.software_sign,self.latest_mod_time,self.delete_status,self.delete_time)


class Metadata(object):
    
    def __init__(self):
        self.collect_target_id = None
        self.media_type = None
        self.mime_original_name = None
        self.locate_type = None
        self.login_time = None
        self.city_code = None
        self.company_address = None
        self.longitude = None
        self.latitude = None
        self.delete_status = None
        self.delete_time = None

    def get_values(self):
        return(self.collect_target_id,self.media_type,self.mime_original_name,self.locate_type,self.login_time,
        self.city_code,self.company_address,self.longitude,self.latitude,self.delete_status,self.delete_time)


class Accountass(object):

    def __init__(self):
        self.collect_target_id = None
        self.network_app = None
        self.account_id = None
        self.account = None
        self.app_type = None
        self.friend_id = None
        self.relate_account = None
        self.rel_type = None
        self.delete_status = None
        self.delete_time = None

    def get_values(self):
        return(self.collect_target_id,self.network_app,self.account_id,self.account,self.app_type,self.friend_id,
        self.relate_account,self.rel_type,self.delete_status,self.delete_time)


class Dataver(object):

    def __init__(self):
        self.collect_target_id
        self.mainfile
        self.hash
        self.hash_type

    def get_values(self):
        return(self.collect_target_id,self.mainfile,self.hash,self.hash_type)



class BuildBCP(object):

    def __init__(self, bcp_path, cache_db, bcp_db, collect_target_id, network_app, mountdir):
        self.bcp_path = bcp_path
        self.cache_db = cache_db
        self.bcp_db = bcp_db
        self.collect_target_id = collect_target_id
        self.network_app = network_app
        self.mountDir = mountdir
        self.otherinfo = OtherInfo()
        self.db = None

    def genetate(self):
        self.otherinfo.db_create(self.bcp_db)
        self.db = sqlite3.connect(self.cache_db)

        self._get_installedapps()

        self.db.close()
        self.otherinfo.db_close()

    def _get_installedapps(self):
        cursor = self.db.cursor()
        SQL = """
            select bind_id, name, version, permission, installedPath, imgUrl,purchaseDate,deletedDate,description, source, sourceFile, deleted, repeated
            from Applists
        """
        try:
            cursor.execute(SQL)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Debug("[bcp_other.py] :db is not exists table Applists")
            return 

        while row is not None:
            if canceller.IsCancellationRequested:
                return
            
            installedapps = Appinstalled()
            installedapps.collect_target_id = self.collect_target_id
            if row[0]:
                installedapps.package_name = row[0]
            if row[1]:
                installedapps.software_name = row[1]
            if row[2]:
                installedapps.software_version = row[2]
            if row[4]:
                installedapps.sotfware_path = row[4]

            installedapps.delete_status =  self._convert_deleted_type(row[11])

            self.otherinfo.db_insert_table_installedapp(installedapps)
            row = cursor.fetchone()
        self.otherinfo.db_commit()
        cursor.close()


    def _convert_deleted_type(self, value):
        if value == 0:
            return DELETE_STATUS_UNDELETED
        elif value == 1:
            return DELETE_STATUS_DELETED
        else:
            return DELETE_STATUS_UNDELETED