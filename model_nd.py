# coding:utf-8
#
# According to model_eb, we use model_nd for file related solution and bcp, and model_im for chat related, as most of applications today contain a chat-plugin.
# when you create a model_nd object, given name will append a suffix '.im', means the db of im-system. when debuging, delete both of them each time
# C37R, PWNZEN Infomation.
__author__ = "chenfeiyang"
import clr

try:
    clr.AddReference('unity_c37r')
    clr.AddReference('model_im')
    clr.AddReference('System.Data.SQLite')
except:
    pass

import unity_c37r
import model_im
import System.Data.SQLite as sql
import os
import sys
import traceback
from PA.InfraLib.Models import Cloud


class NDBasic(object):
    def __init__(self):
        self.account = 0
        self.res = list()

    def get_values(self):
        return self.res

    def set_value_with_idx(self, idx, val):
        if idx >= len(self.res):
            return
        self.res[idx] = val

    def get_value_with_idx(self, idx):
        if idx >= len(self.res):
            return None
        return self.res[idx]


TBL_CREATE_TRANSFER = '''
    create table if not exists tb_transfer(account_id text, file_name text, file_size int, hash_code text, local_path text, server_path text, url text, torrent_name text, status int, 
                                           is_download int, begin_time int, end_time int, cached_size int, deleted text)
'''

TBL_INSERT_TRANSFER = '''
    insert into tb_transfer values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

NDFileProcessing = 1
NDFileDone = 2


class NDFileTransfer(NDBasic):
    def __init__(self):
        super(NDFileTransfer, self).__init__()
        self.file_name = 1
        self.file_size = 2
        self.hash_code = 3
        self.local_path = 4
        self.server_path = 5
        self.url = 6
        self.torrent_name = 7
        self.status = 8
        self.is_download = 9
        self.begin_time = 10
        self.end_time = 11
        self.cached_size = 12
        self.deleted = 13
        self.res = [None] * 14


TBL_CREATE_FILE_LIST = '''
    create table if not exists tbl_filelist(account_id text, file_name text, file_type int, file_size int, file_hash text, server_path text, url text, create_time int, update_time int, cache_time int, deleted text)
'''

TBL_INSERT_FILE_LIST = '''
    insert into tbl_filelist values(?,?,?,?,?,?,?,?,?,?,?)
'''


class NDFileList(NDBasic):
    def __init__(self):
        super(NDFileList, self).__init__()
        self.file_name = 1
        self.file_type = 2
        self.file_size = 3
        self.file_hash = 4
        self.server_path = 5
        self.url = 6
        self.create_time = 7
        self.update_time = 8
        self.cache_time = 9
        self.deleted = 10
        self.res = [None] * 11


TBL_CREATE_SHARED_FILE = '''
    create table if not exists tb_shared(account_id text, file_name text, server_path text, file_size int, update_time int, create_time int, url text, sender_id text, sender_name text, send_time int, deleted int)
'''

TBL_INSERT_SHARED_FILE = '''
    insert into tb_shared values(?,?,?,?,?,?,?,?,?,?,?)
'''


class NDFileShared(NDBasic):
    def __init__(self):
        super(NDFileShared, self).__init__()
        self.file_name = 1
        self.server_path = 2
        self.file_size = 3
        self.update_time = 4
        self.create_time = 5
        self.url = 6
        self.sender_id = 7
        self.sender_name = 8
        self.send_time = 9
        self.deleted = 10
        self.res = [None] * 11


TBL_CREATE_VERSION = '''
    create table if not exists tbl_version(v_key text, v_value int)
'''

TBL_INSERT_VERSION = '''
    insert into tbl_version(v_key, v_value) values(?,?)
'''

NDDBVersionKey = 'NDDBV'
NDDBVersionValue = 2
NDDBApplicationVersionKey = 'NDDBAPPV'


#
# 透心凉，心飞扬
#
class NetDisk(object):
    def __init__(self, db_name, app_version=1):
        self.db_name = db_name
        self.need_parse = True
        self.conn = None
        self.cmd = None
        self.im = None
        if os.path.exists(self.db_name):
            self.need_parse = NetDisk.checkout(db_name, app_version)
        if self.need_parse:
            self.conn = unity_c37r.create_connection(db_name, False)
            self.cmd = sql.SQLiteCommand(self.conn)
            self.im = model_im.IM()
            self.im.db_create(db_name + '.IM')
            self.events = None
            self.create_tables()

    @staticmethod
    def checkout(db_name, app_version):
        res = 0x0
        try:
            conn = unity_c37r.create_connection(db_name)
            cmd = sql.SQLiteCommand(conn)
            cmd.CommandText = '''
                select * from tbl_version where v_key = '{}'
            '''.format(NDDBVersionKey)
            reader = cmd.ExecuteReader()
            if reader.Read():
                v = unity_c37r.c_sharp_get_long(reader, 1)
                if v == NDDBVersionValue:
                    res |= 0x1
            reader.Close()
            cmd.CommandText = '''
                select * from tbl_version where v_key = '{}'
            '''.format(NDDBApplicationVersionKey)
            reader = cmd.ExecuteReader()
            if reader.Read():
                v = unity_c37r.c_sharp_get_long(reader, 1)
                if v == app_version:
                    res |= 0x2
            reader.Close()
            r = model_im.IM.need_parse(db_name + '.IM', NDDBVersionValue)
            if not r:
                res |= 0x4
            cmd.Dispose()
            conn.Close()
        except:
            conn.Close()
            res = 0x0
        if res != 0x7:
            try:
                os.remove(db_name)
                os.remove(db_name + '.IM')
                return True
            except:
                raise IOError("中间数据库被占用,无法继续分析")
        return False

    def create_tables(self):
        self.cmd.CommandText = TBL_CREATE_FILE_LIST
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_CREATE_TRANSFER
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_CREATE_VERSION
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_CREATE_SHARED_FILE
        self.cmd.ExecuteNonQuery()
        self.begin_events()

    def begin_events(self):
        self.events = self.conn.BeginTransaction()

    def end_events(self):
        self.events.Commit()
        self.events = None

    def db_commit(self):
        self.events.Commit()
        self.begin_events()

    def db_close(self):
        self.im.db_close()
        self.cmd.Dispose()
        self.conn.Close()

    def db_execute(self, command, values):
        self.cmd.CommandText = command
        self.cmd.Parameters.Clear()
        for v in values:
            param = self.cmd.CreateParameter()
            param.Value = v
            self.cmd.Parameters.Add(param)
        self.cmd.ExecuteNonQuery()

    def db_insert_transfer(self, values):
        self.db_execute(TBL_INSERT_TRANSFER, values)

    def db_insert_filelist(self, values):
        self.db_execute(TBL_INSERT_FILE_LIST, values)

    def db_insert_version(self, key, value):
        self.db_execute(TBL_INSERT_VERSION, [key, value])

    def db_insert_im_version(self, value):
        self.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
        self.im.db_insert_table_version(model_im.VERSION_KEY_APP, value)
        self.im.db_commit()

    def db_insert_shared(self, values):
        self.db_execute(TBL_INSERT_SHARED_FILE, values)


class NDModel(object):
    def __init__(self, cache_db):
        self.conn = None
        self.cmd = None
        if not os.path.exists(cache_db):
            return
        if not os.path.exists(cache_db + ".IM"):
            return
        self.conn = unity_c37r.create_connection(cache_db)
        self.cmd = sql.SQLiteCommand(self.conn)

    @staticmethod
    def set_model_value(m, v):
        try:
            m.Value = v
        except:
            traceback.print_exc()

    def __generate_file_basic_model(self):
        self.cmd.CommandText = '''
            select * from tbl_filelist
        '''
        reader = self.cmd.ExecuteReader()
        f = NDFileList()
        models = []
        while reader.Read():
            fb = Cloud.FileBasic()
            fb.OwnerUserID.Value = unity_c37r.c_sharp_get_string(reader, f.account)
            fb.FileName.Value = unity_c37r.c_sharp_get_string(reader, f.file_name)
            fb.FileSize.Value = unity_c37r.c_sharp_get_long(reader, f.file_size)
            fb.HashCode.Value = unity_c37r.c_sharp_get_string(reader, f.file_hash)
            self.set_model_value(fb.FileSize, unity_c37r.c_sharp_get_long(reader, f.file_size))
            # self.set_model_value(fb.Url, unity_c37r.c_sharp_get_string(reader, f.url))
            m_str = unity_c37r.c_sharp_get_string(reader, f.url)
            self.set_model_value(fb.Url, unity_c37r.get_c_sharp_uri(m_str))
            self.set_model_value(fb.ServerPath, unity_c37r.c_sharp_get_string(reader, f.server_path))
            tp = unity_c37r.c_sharp_get_long(reader, f.file_type)
            if tp == 0:
                self.set_model_value(fb.Type, Cloud.FileBasicType.None)
            elif tp == 1:
                self.set_model_value(fb.Type, Cloud.FileBasicType.Txt)
            elif tp == 2:
                self.set_model_value(fb.Type, Cloud.FileBasicType.Image)
            elif tp == 3:
                self.set_model_value(fb.Type, Cloud.FileBasicType.Audio)
            elif tp == 4:
                self.set_model_value(fb.Type, Cloud.FileBasicType.Video)
            elif tp == 5:
                self.set_model_value(fb.Type, Cloud.FileBasicType.Document)
            else:
                self.set_model_value(fb.Type, Cloud.FileBasicType.Other)
            models.append(fb)
        reader.Close()
        return models

    def __generate_file_transfer_model(self):
        self.cmd.CommandText = '''
            select * from tb_transfer
        '''
        reader = self.cmd.ExecuteReader()
        ft = NDFileTransfer()
        models = []
        while reader.Read():
            fts = Cloud.FileTransfer()
            self.set_model_value(fts.OwnerUserID, unity_c37r.c_sharp_get_string(reader, ft.account))
            self.set_model_value(fts.FileName, unity_c37r.c_sharp_get_string(reader, ft.file_name))
            self.set_model_value(fts.FileSize, unity_c37r.c_sharp_get_long(reader, ft.file_size))
            self.set_model_value(fts.HashCode, unity_c37r.c_sharp_get_string(reader, ft.hash_code))
            # self.set_model_value(fts.Url, unity_c37r.c_sharp_get_string(reader, ft.url))
            uri = unity_c37r.c_sharp_get_string(reader, ft.url)
            uri = unity_c37r.get_c_sharp_uri(uri)
            self.set_model_value(fts.Url, uri)
            self.set_model_value(fts.LocalPath, unity_c37r.c_sharp_get_string(reader, ft.local_path))
            self.set_model_value(fts.TorrentName, unity_c37r.c_sharp_get_string(reader, ft.torrent_name))
            st = unity_c37r.c_sharp_get_long(reader, ft.status)
            if st == NDFileProcessing:
                self.set_model_value(fts.Status, Cloud.FileTransferStatus.UnFinished)
            elif st == NDFileDone:
                self.set_model_value(fts.Status, Cloud.FileTransferStatus.Finish)
            else:
                self.set_model_value(fts.Status, Cloud.FileTransferStatus.None)
            isd = unity_c37r.c_sharp_get_long(reader, ft.is_download)
            if isd == 1:
                self.set_model_value(fts.IsDownload, True)
            else:
                self.set_model_value(fts.IsDownload, False)
            self.set_model_value(fts.BeginTime,
                                 unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_try_get_time(reader, ft.begin_time)))
            self.set_model_value(fts.EndTime,
                                 unity_c37r.get_c_sharp_ts(unity_c37r.c_sharp_try_get_time(reader, ft.end_time)))
            self.set_model_value(fts.CachedSize, unity_c37r.c_sharp_get_long(reader, ft.cached_size))
            models.append(fts)
        reader.Close()
        return models

    def __generate_file_share_model(self):
        self.cmd.CommandText = '''
            select * from tb_shared
        '''
        models = []
        ft = NDFileShared()
        reader = self.cmd.ExecuteReader()
        while reader.Read():
            fts = Cloud.FileShare()
            self.set_model_value(fts.OwnerUserID, unity_c37r.c_sharp_get_string(reader, ft.account))
            self.set_model_value(fts.FileName, unity_c37r.c_sharp_get_string(reader, ft.file_name))
            self.set_model_value(fts.ServerPath, unity_c37r.c_sharp_get_string(reader, ft.server_path))
            uri = unity_c37r.c_sharp_get_string(reader, ft.url)
            uri = unity_c37r.get_c_sharp_uri(uri)
            self.set_model_value(fts.Url, uri)
            ctime = unity_c37r.c_sharp_try_get_time(reader, ft.create_time)
            utime = unity_c37r.c_sharp_try_get_time(reader, ft.update_time)
            stime = unity_c37r.c_sharp_try_get_time(reader, ft.send_time)
            self.set_model_value(fts.CreateTime, unity_c37r.get_c_sharp_ts(ctime))
            self.set_model_value(fts.UpdateTime, unity_c37r.get_c_sharp_ts(utime))
            self.set_model_value(fts.ShareTime, unity_c37r.get_c_sharp_ts(stime))
            sid = unity_c37r.c_sharp_get_string(reader, ft.sender_id)
            sname = unity_c37r.c_sharp_get_string(reader, ft.sender_name)
            fts.Sender.Value = unity_c37r.create_user_intro(sid, sname, "")
            models.append(fts)
        reader.Close()
        return models

    def generate_models(self):
        models = []
        models.extend(self.__generate_file_basic_model())
        models.extend(self.__generate_file_share_model())
        models.extend(self.__generate_file_transfer_model())
        self.cmd.Dispose()
        self.conn.Close()
        return models


class NDBCPBASIC(object):
    def __init__(self):
        self.collect_id = 0
        self.app_type = 1
        self.account_id = 2
        self.res = list()

    def set_value_with_idx(self, idx, val):
        if idx >= len(self.res):
            return
        self.res[idx] = val

    def get_value_with_idx(self, idx):
        if idx >= len(self.res):
            return None
        return self.res[idx]

    def get_values(self):
        return self.res


TBL_BCP_CREATE_ACCOUNT = '''
    create table if not exists WA_MFORENSICS_110100(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    REGIS_NICKNAME text,
                                                    PASSWORD text,
                                                    INSTALL_TIME int,
                                                    AREA text,
                                                    CITY_CODE text,
                                                    FIXED_PHONE text,
                                                    MSISDN text,
                                                    EMAIL_ACCOUNT text,
                                                    CERTIFICATE_TYPE text,
                                                    CERTIFICATE_CODE text,
                                                    SEXCODE text,
                                                    AGE int,
                                                    POSTAL_ADDRESS text,
                                                    POSTAL_CODE text,
                                                    OCCUPATION_NAME text,
                                                    BLOOD_TYPE text,
                                                    NAME text,
                                                    SIGN_NAME text,
                                                    PERSONAL_DESC text,
                                                    REG_CITY text,
                                                    GRADUATESCHOOL text,
                                                    ZODIAC text,
                                                    CONSTALLATION text,
                                                    BIRTHDAY text,
                                                    HASH_TYPE text,
                                                    USER_PHOTO text,
                                                    ACCOUNT_REG_DATE int,
                                                    LAST_LOGIN_TIME int,
                                                    LATEST_MOD_TIME int,
                                                    DELETE_STATUS int,
                                                    DELETE_TIME int
)
'''

TBL_BCP_INSERT_ACCOUNT = '''
    insert into WA_MFORENSICS_110100 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''


class NDBCPACCOUNT(NDBCPBASIC):
    def __init__(self):
        super(NDBCPACCOUNT, self).__init__()
        self.account = 3
        self.nick = 4
        self.password = 5
        self.install_time = 6
        self.area = 7
        self.city_code = 8
        self.telephone = 9
        self.mobile = 10
        self.email = 11
        self.certificate_type = 12
        self.certificate_code = 13
        self.gender = 14
        self.age = 15
        self.address = 16
        self.post_code = 17
        self.occupation = 18
        self.blood_type = 19
        self.real_name = 20
        self.signature = 21
        self.descrition = 22
        self.city = 23
        self.graduation = 24
        self.zodiac = 25
        self.constallation = 26
        self.birthday = 27
        self.hash_type = 28
        self.photo = 29
        self.reg_date = 30
        self.login_time = 31
        self.update_time = 32
        self.delete_status = 33
        self.delete_time = 34
        self.res = [None] * 35


TBL_BCP_CREATE_TRANSFER = '''
create table if not exists WA_MFORENSICS_110200(COLLECT_TARGET_ID text,
                                                NETWORK_APP text,
                                                ACCOUNT_ID text,
                                                ACCOUNT text,
                                                FILE_NAME text,
                                                MEDIA_TYPE text,
                                                FILE_SIZE int,
                                                HASH text,
                                                HASH_TYPE text,
                                                FILE_PATH text,
                                                TRANSFILE text,
                                                URL text,
                                                REFERER text,
                                                MIME_NAME text,
                                                ASYNC_STATUS text,
                                                ACTION_TYPE text, 
                                                START_TIME int,
                                                END_TIME int,
                                                CACHE_VALUE int,
                                                DELETE_STATUS text,
                                                DELETE_TIME int
)
'''

TBL_BCP_INSERT_TRANSFER = '''
insert into WA_MFORENSICS_110200 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''


class NDBCPTRANSFER(NDBCPBASIC):
    def __init__(self):
        super(NDBCPTRANSFER, self).__init__()
        self.account = 3
        self.file_name = 4
        self.media_type = 5
        self.file_size = 6
        self.hash = 7
        self.hash_type = 8
        self.file_path = 9
        self.server_path = 10
        self.url = 11
        self.reference = 12
        self.torrent = 13
        self.status = 14
        self.action_type = 15
        self.begin_time = 16
        self.end_time = 17
        self.cached_size = 18
        self.delete_status = 19
        self.delete_time = 20
        self.res = [None] * 21


TBL_BCP_CREATE_FILELIST = '''
    create table if not exists WA_MFORENSICS_110300(COLLECT_TARGET_ID text,
                                                    NETWORK_APP text,
                                                    ACCOUNT_ID text,
                                                    ACCOUNT text,
                                                    FILE_NAME text,
                                                    MEDIA_TYPE text,
                                                    FILE_SIZE int,
                                                    HASH text,
                                                    HASH_TYPE text,
                                                    TRANSFILE text,
                                                    URL text,
                                                    REFERER text,
                                                    MIME_NAME text,
                                                    CREATE_TIME int,
                                                    LATEST_MOD_TIME int,
                                                    UPDATE_TIME int,
                                                    LOCAL_STATE text,
                                                    DELETE_STATUS int,
                                                    DELETE_TIME int
)
'''

TBL_BCP_INSERT_FILELIST = '''
    insert into WA_MFORENSICS_110300 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''


class NDBCPFILELIST(NDBCPBASIC):
    def __init__(self):
        super(NDBCPFILELIST, self).__init__()
        self.account = 3
        self.file_name = 4
        self.media_type = 5
        self.file_size = 6
        self.hash = 7
        self.hash_type = 8
        self.server_path = 9
        self.url = 10
        self.reference = 11
        self.torrent = 12
        self.create_time = 13
        self.update_time = 14
        self.cached_time = 15
        self.status = 16
        self.delete_status = 17
        self.delete_time = 18
        self.res = [None] * 19


class NDBCP(object):
    def __init__(self, bcp_path, mount_path, cache_db, bcp_db, collect_target_id, contact_account_type):
        self.bcp_path = bcp_path
        self.db = bcp_db
        self.mnt = mount_path
        self.cache_db = cache_db
        self.cid = collect_target_id
        self.app_type = contact_account_type
        if os.path.exists(self.db):
            os.remove(self.db)
        self.conn = unity_c37r.create_connection(self.db, False)
        self.cmd = sql.SQLiteCommand(self.conn)
        self.transaciton = None
        self.create_tables()

    def create_tables(self):
        self.cmd.CommandText = TBL_BCP_CREATE_ACCOUNT
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_FILELIST
        self.cmd.ExecuteNonQuery()
        self.cmd.CommandText = TBL_BCP_CREATE_TRANSFER
        self.cmd.ExecuteNonQuery()
        self.begin_transaction()

    def begin_transaction(self):
        if self.transaciton is None:
            self.transaciton = self.conn.BeginTransaction()

    def db_commit(self):
        self.transaciton.Commit()
        self.transaciton = None
        self.begin_transaction()

    def db_close(self):
        self.cmd.Dispose()
        self.conn.Close()

    def db_insert_account(self, a):
        unity_c37r.execute_query(self.cmd, TBL_BCP_INSERT_ACCOUNT, a.get_values())

    def db_insert_transfer(self, t):
        unity_c37r.execute_query(self.cmd, TBL_BCP_INSERT_TRANSFER, t.get_values())

    def db_insert_file_list(self, fl):
        unity_c37r.execute_query(self.cmd, TBL_BCP_INSERT_FILELIST, fl.get_values())

    def generate(self):
        self.__generate_transfer()
        self.__generate_file_list()
        self.__generate_account()
        self.db_close()

    def __generate_account(self):
        connection = unity_c37r.create_connection(self.cache_db + ".IM", True)
        command = sql.SQLiteCommand(connection)
        command.CommandText = '''
            select * from account
        '''
        reader = command.ExecuteReader()
        while reader.Read():
            a = NDBCPACCOUNT()
            a.set_value_with_idx(a.account_id, unity_c37r.c_sharp_get_string(reader, 0))
            a.set_value_with_idx(a.account, a.get_value_with_idx(a.account_id))
            a.set_value_with_idx(a.nick, unity_c37r.c_sharp_get_string(reader, 1))
            a.set_value_with_idx(a.password, unity_c37r.c_sharp_get_string(reader, 3))
            photo = unity_c37r.c_sharp_get_string(reader, 4)
            photo = os.path.join(self.mnt, photo)
            if os.path.exists(photo):
                pass  # ===>copy file fix that
            else:
                a.set_value_with_idx(a.photo, photo)
            a.set_value_with_idx(a.telephone, unity_c37r.c_sharp_get_string(reader, 5))
            a.set_value_with_idx(a.email, unity_c37r.c_sharp_get_string(reader, 6))
            gender = unity_c37r.c_sharp_get_long(reader, 7)
            gender = '0%d' % gender
            a.set_value_with_idx(a.gender, gender)
            a.set_value_with_idx(a.age, unity_c37r.c_sharp_get_long(reader, 8))
            a.set_value_with_idx(a.city, unity_c37r.c_sharp_get_string(reader, 11))
            a.set_value_with_idx(a.address, unity_c37r.c_sharp_get_string(reader, 12))
            a.set_value_with_idx(a.birthday, unity_c37r.c_sharp_get_string(reader, 13))
            a.set_value_with_idx(a.signature, unity_c37r.c_sharp_get_string(reader, 14))
            a.set_value_with_idx(a.delete_status, unity_c37r.c_sharp_get_string(reader, 15))
            a.set_value_with_idx(a.collect_id, self.cid)
            a.set_value_with_idx(a.app_type, self.app_type)
            self.db_insert_account(a)
        self.db_commit()
        reader.Close()
        command.Dispose()
        connection.Close()

    def __generate_transfer(self):
        connection = unity_c37r.create_connection(self.cache_db)
        cmd = sql.SQLiteCommand(connection)
        cmd.CommandText = '''
            select * from tb_transfer
        '''
        fts = NDFileTransfer()
        reader = cmd.ExecuteReader()
        while reader.Read():
            t = NDBCPTRANSFER()
            t.set_value_with_idx(t.account_id, unity_c37r.c_sharp_get_string(reader, fts.account))
            t.set_value_with_idx(t.file_name, unity_c37r.c_sharp_get_string(reader, fts.file_name))
            t.set_value_with_idx(t.file_size, unity_c37r.c_sharp_get_long(reader, fts.file_size))
            t.set_value_with_idx(t.hash, unity_c37r.c_sharp_get_string(reader, fts.hash_code))
            t.set_value_with_idx(t.server_path, unity_c37r.c_sharp_get_string(reader, fts.server_path))
            t.set_value_with_idx(t.file_path, unity_c37r.c_sharp_get_string(reader, fts.local_path))
            t.set_value_with_idx(t.url, unity_c37r.c_sharp_get_string(reader, fts.url))
            t.set_value_with_idx(t.torrent, unity_c37r.c_sharp_get_string(reader, fts.torrent_name))
            t.set_value_with_idx(t.status, unity_c37r.c_sharp_get_long(reader, fts.status))
            t.set_value_with_idx(t.action_type, unity_c37r.c_sharp_get_long(reader, fts.is_download))
            t.set_value_with_idx(t.begin_time, unity_c37r.c_sharp_get_long(reader, fts.begin_time))
            t.set_value_with_idx(t.end_time, unity_c37r.c_sharp_get_long(reader, fts.end_time))
            t.set_value_with_idx(t.cached_size, unity_c37r.c_sharp_get_long(reader, fts.cached_size))
            t.set_value_with_idx(t.delete_status, unity_c37r.c_sharp_get_long(reader, fts.deleted))
            t.set_value_with_idx(t.collect_id, self.cid)
            t.set_value_with_idx(t.app_type, self.app_type)
            self.db_insert_transfer(t)
        self.db_commit()
        reader.Close()
        cmd.Dispose()
        connection.Close()

    def __generate_file_list(self):
        connection = unity_c37r.create_connection(self.cache_db)
        cmd = sql.SQLiteCommand(connection)
        cmd.CommandText = '''
            select * from tbl_filelist
        '''
        ftl = NDFileList()
        reader = cmd.ExecuteReader()
        while reader.Read():
            fl = NDBCPFILELIST()
            fl.set_value_with_idx(fl.account_id, unity_c37r.c_sharp_get_string(reader, ftl.account))
            fl.set_value_with_idx(fl.app_type, self.app_type)
            fl.set_value_with_idx(fl.collect_id, self.cid)
            fl.set_value_with_idx(fl.file_name, unity_c37r.c_sharp_get_string(reader, ftl.file_name))
            fl.set_value_with_idx(fl.hash, unity_c37r.c_sharp_get_string(reader, ftl.file_hash))
            fl.set_value_with_idx(fl.file_size, unity_c37r.c_sharp_get_long(reader, ftl.file_size))
            fl.set_value_with_idx(fl.server_path, unity_c37r.c_sharp_get_string(reader, ftl.server_path))
            fl.set_value_with_idx(fl.create_time, unity_c37r.c_sharp_get_long(reader, ftl.create_time))
            fl.set_value_with_idx(fl.update_time, unity_c37r.c_sharp_get_long(reader, ftl.update_time))
            fl.set_value_with_idx(fl.cached_time, unity_c37r.c_sharp_get_long(reader, ftl.cache_time))
            fl.set_value_with_idx(fl.delete_status, unity_c37r.c_sharp_get_long(reader, ftl.deleted))
            self.db_insert_file_list(fl)
        ftl = NDFileShared()
        reader.Close()
        cmd.CommandText = '''
            select * from tb_shared
        '''
        reader = cmd.ExecuteReader()
        while reader.Read():
            fl = NDBCPFILELIST()
            fl.set_value_with_idx(fl.app_type, self.app_type)
            fl.set_value_with_idx(fl.collect_id, self.cid)
            fl.set_value_with_idx(fl.account_id, unity_c37r.c_sharp_get_string(reader, ftl.account))
            fl.set_value_with_idx(fl.file_name, unity_c37r.c_sharp_get_string(reader, ftl.file_name))
            fl.set_value_with_idx(fl.file_size, unity_c37r.c_sharp_get_long(reader, ftl.file_size))
            fl.set_value_with_idx(fl.url, unity_c37r.c_sharp_get_string(reader, ftl.url))
            fl.set_value_with_idx(fl.create_time, unity_c37r.c_sharp_get_long(reader, ftl.create_time))
            fl.set_value_with_idx(fl.update_time, unity_c37r.c_sharp_get_long(reader, ftl.update_time))
            fl.set_value_with_idx(fl.cached_time, unity_c37r.c_sharp_get_long(reader, ftl.send_time))
            fl.set_value_with_idx(fl.server_path, unity_c37r.c_sharp_get_string(reader, ftl.server_path))
            self.db_insert_file_list(fl)
        self.db_commit()
        reader.Close()
        cmd.Dispose()
        connection.Close()
