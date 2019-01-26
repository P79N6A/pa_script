# -*- coding: utf-8 -*-
import codecs
import json

__author__ = "TaoJianping"

import hashlib
import os
import shutil

import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    # clr.AddReference('unity_c37r')
    clr.AddReference('model_eb')
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
except Exception as e:
    print("debug", e)
del clr

import model_eb
import model_im
import PA_runtime
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.Common.Utilities.Types import TimeStampFormats

# CONST
Tianmao_VERSION = 1
DEBUG = False


class ColHelper(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = System.Data.SQLite.SQLiteConnection(
            'Data Source = {}; Readonly = True'.format(db_path))
        self.cmd = None
        self.is_opened = False
        self.in_context = False
        self.reader = None

    def open(self):
        self.conn.Open()
        self.cmd = System.Data.SQLite.SQLiteCommand(self.conn)
        self.is_opened = True

    def close(self):
        if self.reader is not None:
            self.reader.Close()
        self.cmd.Dispose()
        self.conn.Close()
        self.is_opened = False

    def __enter__(self):
        if self.is_opened is False:
            self.open()
        self.in_context = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.in_context = False
        return True

    def __repr__(self):
        return "this db exists in {path}".format(path=self.db_path)

    def execute_sql(self, sql):
        self.cmd.CommandText = sql
        self.reader = self.cmd.ExecuteReader()
        return self.reader

    def fetch_reader(self, sql):
        cmd = System.Data.SQLite.SQLiteCommand(self.conn)
        cmd.CommandText = sql
        return cmd.ExecuteReader()

    def has_rest(self):
        return self.reader.Read()

    def get_string(self, idx):
        return self.reader.GetString(idx) if not self.reader.IsDBNull(idx) else ""

    def get_int64(self, idx):
        return self.reader.GetInt64(idx) if not self.reader.IsDBNull(idx) else 0

    def get_blob(self, idx):
        return self.reader.GetValue(idx) if not self.reader.IsDBNull(idx) else None

    def get_float(self, idx):
        return self.reader.GetFloat(idx) if not self.reader.IsDBNull(idx) else 0

    @staticmethod
    def fetch_string(reader, idx):
        return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

    @staticmethod
    def fetch_int64(reader, idx):
        return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

    @staticmethod
    def fetch_blob(reader, idx):
        return reader.GetValue(idx) if not reader.IsDBNull(idx) else None

    @staticmethod
    def fetch_float(reader, idx):
        return reader.GetFloat(idx) if not reader.IsDBNull(idx) else 0


class RecoverTableHelper(object):
    """对恢复数据的库进行了一下包装"""

    def __init__(self, node):
        self.db = SQLiteParser.Database.FromNode(node, canceller)
        self.db_path = node.PathWithMountPoint
        self.type_relation = {
            "Int": SQLiteParser.FieldType.Int,
            "Text": SQLiteParser.FieldType.Text,
        }

    def is_valid(self):
        return True if self.db else False

    def fetch_table(self, table_name, table_config):
        ts = SQLiteParser.TableSignature(table_name)
        for column_name, _type in table_config.items():
            SQLiteParser.Tools.AddSignatureToTable(ts, column_name, self.type_relation[_type],
                                                   SQLiteParser.FieldConstraints.NotNull)
        return ts

    def read_deleted_record(self, table):
        return self.db.ReadTableDeletedRecords(table, False)


class Utils(object):
    @staticmethod
    def open_file(file_path, encoding="utf-8"):
        with codecs.open(file_path, 'r', encoding=encoding) as f:
            return f.read()

    @staticmethod
    def copy_file(old_path, new_path):
        try:
            shutil.copyfile(old_path, new_path)
            return True
        except Exception as e:
            return False

    @staticmethod
    def copy_dir(old_path, new_path):
        try:
            if os.path.exists(new_path):
                shutil.rmtree(new_path)
            shutil.copytree(old_path, new_path)
            return True
        except Exception as e:
            print(e)
            return False

    @staticmethod
    def list_dir(path):
        return os.listdir(path)

    @staticmethod
    def convert_timestamp(ts):
        try:
            if not ts:
                return None
            ts = str(ts)
            if len(ts) > 13:
                return None
            elif float(ts) < 0:
                return None
            elif len(ts) == 13:
                return int(float(ts[:-3]))
            elif len(ts) <= 10:
                return int(float(ts))
            else:
                return None
        except:
            return None

    @staticmethod
    def convert_ts_for_ios(ts):
        try:
            dstart = DateTime(1970, 1, 1, 0, 0, 0)
            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(ts)
            return ((cdate - dstart).TotalSeconds)
        except Exception as e:
            return None

    @staticmethod
    def json_loads(data):
        try:
            return json.loads(data)
        except:
            return None


class Logger(object):
    def __init__(self):
        self.module = None
        self.class_name = None
        self.func_name = None

    def error(self):
        if DEBUG:
            TraceService.Trace(TraceLevel.Error, "{module} error: {class_name} {func} ==> {log_info}".format(
                module=self.module,
                class_name=self.class_name,
                func=self.func_name,
                log_info=traceback.format_exc()
            ))

    def info(self, info):
        TraceService.Trace(TraceLevel.Info, "{module} info: {class_name} {func} ==> {log_info}".format(
            module=self.module,
            class_name=self.class_name,
            func=self.func_name,
            log_info=info
        ))


class TmallParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.app_name = 'Tmall'
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache_db = self.__get_cache_db()
        self.model_eb_col, self.model_im_col = self.__load_eb_models(self.cache_db, Tianmao_VERSION, self.app_name)
        self.data_path = self.__copy_data_dir_files(data_dir="databases")
        self.logger = Logger()
        self.checking_account_col = None
        self.checking_account_recover_col = None
        self.eb_shop_col = None
        self.eb_products_col = None
        self.using_account = None

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath(self.app_name)
        m = hashlib.md5()
        m.update(Encoding.UT8.GetBytes(self.root.AbsolutePath))
        return os.path.join(self.cache_path, m.hexdigest().upper())

    @staticmethod
    def __load_eb_models(cache_db, app_version, app_name):
        eb = model_eb.EB(cache_db, app_version, app_name)
        im = eb.im
        return eb, im

    def __add_media_path(self, obj, file_name):
        try:
            searchkey = file_name
            nodes = self.root.FileSystem.Search(searchkey + '$')
            for node in nodes:
                obj.media_path = node.AbsolutePath
                if obj.media_path.endswith('.mp3'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                elif obj.media_path.endswith('.amr'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                elif obj.media_path.endswith('.slk'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                elif obj.media_path.endswith('.mp4'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                elif obj.media_path.endswith('.jpg'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                elif obj.media_path.endswith('.png'):
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                else:
                    obj.type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
                return True
        except Exception as e:
            print (e)
        return False

    def __copy_data_dir_files(self, data_dir):
        node = self.root.GetByPath(data_dir)
        if node is None:
            print("not found data")
            return False
        old_dir = node.PathWithMountPoint
        new_dir = os.path.join(self.cache_path, "tmp")
        Utils.copy_dir(old_dir, new_dir)
        return new_dir

    def __search_account_col(self, account_name):
        if not self.data_path:
            return None, None
        files = Utils.list_dir(self.data_path)
        for f in files:
            if f.startswith("MessageDB_") and not (f.endswith("-shm") or f.endswith("-wal")):
                db_file = os.path.join(self.data_path, f)
                db_col = ColHelper(db_file)
                with db_col as db_col:
                    sql = """SELECT _id from profile where NICK = '{}'""".format(account_name)
                    db_col.execute_sql(sql)
                    while db_col.has_rest():
                        return db_col, self.__search_account_recover_col(f)

    def __search_account_recover_col(self, file_name):
        path = "/databases/{}".format(file_name)
        node = self.root.GetByPath(path)
        if node is None:
            return
        return RecoverTableHelper(node)

    def __search_messages_db_files(self):
        files = Utils.list_dir(self.data_path)
        for f in files:
            if f.startswith("MessageDB_") and not (f.endswith("-shm") or f.endswith("-wal")):
                yield f

    @staticmethod
    def __convert_friend_type(_type):
        if _type == "imba":
            return model_im.FRIEND_TYPE_SUBSCRIBE
        elif _type == "im_bc":
            return model_im.FRIEND_TYPE_RECENT
        elif _type == "im_cc":
            return model_im.FRIEND_TYPE_FRIEND
        else:
            return None

    def __add_profile_table_friend(self):
        with self.checking_account_col as db_col:
            sql = """SELECT PROFILE_ID, 
                            IDENTITY_TYPE, 
                            DISPLAY_NAME, 
                            AVATAR_URL, 
                            NICK, 
                            GENDER, 
                            BIRTH_TIME, 
                            MODIFY_TIME, 
                            SIGNATURE, 
                            ACCOUNT_TYPE, 
                            BIZ_TYPE,
                            EXT_INFO 
                    FROM profile;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend = model_im.Friend()

                    if db_col.get_string(0) in (self.using_account.account_id, self.using_account.username):
                        continue

                    friend.account_id = self.using_account.account_id
                    friend.source = self.checking_account_col.db_path
                    friend.friend_id = db_col.get_string(0)
                    friend.photo = db_col.get_string(3)
                    friend.nickname = db_col.get_string(4)
                    friend.signature = db_col.get_string(8)
                    friend.type = self.__convert_friend_type(db_col.get_string(1))

                    # 没有测试数据，不知道长啥样
                    friend.gender = None
                    friend.birthday = None

                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    self.logger.error()
            self.model_im_col.db_commit()

    def __recover_profile_table_friend(self):
        if not self.checking_account_recover_col.is_valid():
            return
        ts = self.checking_account_recover_col.fetch_table("profile", {
            "PROFILE_ID": "Text",
            "AVATAR_URL": "Text",
            "NICK": "Text",
            "IDENTITY_TYPE": "Text",
            "SIGNATURE": "Text",
        })
        for rec in self.checking_account_recover_col.read_deleted_record(ts):

            try:

                friend = model_im.Friend()
                friend.deleted = 1
                friend.source = self.checking_account_recover_col.db_path
                friend.account_id = self.using_account.account_id
                friend.source = self.checking_account_col.db_path
                friend.friend_id = rec['PROFILE_ID'].Value
                friend.photo = rec['AVATAR_URL'].Value
                friend.nickname = rec['NICK'].Value
                friend.signature = rec['SIGNATURE'].Value
                friend.type = self.__convert_friend_type(rec['IDENTITY_TYPE'].Value)

                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def __in_profile_table(self, friend_id):
        sql = """SELECT _id FROM profile where PROFILE_ID = '{}'""".format(friend_id)
        reader = self.checking_account_col.fetch_reader(sql)
        answer = False
        while reader.Read():
            answer = True
        reader.Close()
        return answer

    def __add_relation_table_friend(self):
        with self.checking_account_col as db_col:
            sql = """SELECT RELATION_ID, 
                            IDENTITY_TYPE, 
                            TARGET_REMARK_NAME, 
                            MODIFY_TIME, 
                            CREATE_TIME, 
                            IS_BLACK, 
                            RELATION_TYPE, 
                            TARGET_ACCOUNT_TYPE,
                            BIZ_TYPE, 
                            AVATAR_URL, 
                            LOCAL_TIME, 
                            EXT_INFO 
                    FROM relation;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend = model_im.Friend()

                    friend.friend_id = db_col.get_string(0)

                    if self.__in_profile_table(friend.friend_id):
                        continue

                    friend.account_id = self.using_account.account_id
                    friend.photo = db_col.get_string(9)
                    friend.nickname = db_col.get_string(2)
                    friend.type = self.__convert_friend_type(db_col.get_string(1))

                    if db_col.get_string(11):
                        extra_info = json.loads(db_col.get_string(11))
                        friend.telephone = extra_info.get("uLogPhone", None)

                    self.model_im_col.db_insert_table_friend(friend)
                except Exception:
                    self.logger.error()
            self.model_im_col.db_commit()

    def __recover_relation_table_friend(self):
        if not self.checking_account_recover_col.is_valid():
            return
        ts = self.checking_account_recover_col.fetch_table("relation", {
            "RELATION_ID": "Text",
            "IDENTITY_TYPE": "Text",
            "TARGET_REMARK_NAME": "Text",
            "AVATAR_URL": "Text",
        })
        for rec in self.checking_account_recover_col.read_deleted_record(ts):

            try:

                friend = model_im.Friend()
                friend.deleted = 1
                friend.source = self.checking_account_recover_col.db_path
                friend.friend_id = rec['RELATION_ID'].Value
                if self.__in_profile_table(friend.friend_id):
                    continue
                friend.account_id = self.using_account.account_id
                friend.photo = rec['AVATAR_URL'].Value
                friend.nickname = rec['TARGET_REMARK_NAME'].Value
                friend.type = self.__convert_friend_type(rec['IDENTITY_TYPE'].Value)

                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_account_table(self):
        with self.checking_account_col as db_col:
            sql = """SELECT PROFILE_ID, 
                            IDENTITY_TYPE, 
                            DISPLAY_NAME, 
                            AVATAR_URL, 
                            NICK, 
                            GENDER, 
                            BIRTH_TIME, 
                            MODIFY_TIME, 
                            SIGNATURE, 
                            ACCOUNT_TYPE, 
                            BIZ_TYPE, 
                            EXT_INFO 
                    FROM profile 
                    WHERE BIZ_TYPE = -1;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                a = model_im.Account()
                a.account_id = db_col.get_string(0)
                a.username = db_col.get_string(4)
                a.source = self.checking_account_col.db_path
                a.nickname = db_col.get_string(8)
                a.photo = db_col.get_string(3)
                # TODO add
                a.gender = None
                a.birthday = None

                self.using_account = a
                self.model_im_col.db_insert_table_account(a)
        self.model_im_col.db_commit()

    def _generate_friend_table(self):
        if self.checking_account_col:
            self.__add_profile_table_friend()
            self.__add_relation_table_friend()
        if self.checking_account_recover_col:
            self.__recover_profile_table_friend()
            self.__recover_relation_table_friend()

    def __fetch_sender_name(self, sender_id):
        sql = """SELECT DISPLAY_NAME FROM profile where PROFILE_ID = '{}'""".format(sender_id)
        if self.checking_account_col.is_opened is False:
            self.checking_account_col.open()
        reader = self.checking_account_col.fetch_reader(sql)
        answer = None
        while reader.Read():
            answer = reader.GetString(0) if not reader.IsDBNull(0) else ""
            reader.Close()
            break
        reader.Close()
        if self.checking_account_col.in_context is False:
            self.checking_account_col.close()
        return answer

    @staticmethod
    def __fetch_system_message_content(msg):
        if not msg:
            return
        msg = json.loads(msg)
        content = msg.get("content", "")
        title = msg.get("title", "")
        summary = msg.get("summary", "")
        url = msg.get("url", "")
        return "{} {} {} {}".format(title, summary, content, url)

    @staticmethod
    def __fetch_message_content(m, _type, msg):

        if not msg:
            return
        try:
            msg = json.loads(msg)
        except:
            return

        if _type == 101:
            return msg.get("text", None)
        elif _type == 111 or type == 112:
            title = msg.get("title", "")
            url = msg.get("extActionUrl", "")
            return "{} {}".format(title, url)
        elif _type == 102:
            url = msg.get("url", "")
            if "&" in url:
                url = url.split("&")[0]
            m.media_path = url
            return
        elif _type in (104, 105):
            m.media_path = msg.get("url", "")
            return
        elif _type == 110:
            packet_type = msg.get("wxDisplayType", "")
            words = msg.get("wxDisplayName", "")
            return "[{}] {}".format(packet_type, words)
        elif _type == 106:
            return msg.get("content", None)
        else:
            return None

    @staticmethod
    def __convert_message_type(type_code):
        if not type_code:
            return
        if type_code == 101:
            return model_im.MESSAGE_CONTENT_TYPE_TEXT
        elif type_code == 111 or type == 112:
            return model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
        elif type_code == 102:
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif type_code == 104:
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif type_code == 105:
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif type_code == 110:
            return model_im.MESSAGE_CONTENT_TYPE_RED_ENVELPOE
        elif type_code == 106:
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM

    def __add_system_messages(self):
        with self.checking_account_col as db_col:
            sql = """SELECT _id, 
                            MSG_ID, 
                            CLIENT_ID, 
                            MSG_TIME, 
                            MODIFY_TIME, 
                            SORTED_TIME, 
                            SENDER_ID, 
                            SENDER_TYPE, 
                            RECEIVER_ID, 
                            RECEIVER_TYPE, 
                            CONV_TARGET_TYPE, 
                            CONV_TARGET_ENTITY_TYPE, 
                            CONV_TARGET_CVS_TYPE, 
                            CONV_CODE, 
                            MSG_DATA, 
                            MSG_TYPE, 
                            READ_STATUS, 
                            SUMMARY, 
                            COLUMN_TYPE,
                            LOCAL_DATA, 
                            EXT_INFO 
                    FROM message_imba;"""
            reader = db_col.execute_sql(sql)
            while reader.Read():
                try:
                    m = model_im.Message()

                    m.account_id = self.using_account.account_id
                    m.msg_id = db_col.get_string(1)
                    m.send_time = Utils.convert_timestamp(db_col.get_int64(3))
                    m.sender_id = db_col.get_string(6)
                    m.sender_name = self.__fetch_sender_name(m.sender_id)
                    m.source = self.checking_account_col.db_path
                    m.talker_type = model_im.CHAT_TYPE_SYSTEM
                    m.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
                    m.talker_id = db_col.get_string(13)
                    m.is_sender = 1 if m.sender_id == self.using_account.account_id or m.sender_id.endswith(
                        self.using_account.username) else 0
                    m.content = self.__fetch_system_message_content(db_col.get_string(14))

                    self.model_im_col.db_insert_table_message(m)
                except Exception:
                    self.logger.error()
        self.model_im_col.db_commit()

    def __add_shop_messages(self):
        with self.checking_account_col as db_col:
            sql = """SELECT _id, 
                            MSG_ID, 
                            CLIENT_ID, 
                            MSG_TIME, 
                            MODIFY_TIME, 
                            SORTED_TIME, 
                            SENDER_ID, 
                            SENDER_TYPE, 
                            RECEIVER_ID, 
                            RECEIVER_TYPE, 
                            CONV_TARGET_TYPE, 
                            CONV_TARGET_ENTITY_TYPE, 
                            CONV_TARGET_CVS_TYPE, 
                            CONV_CODE, 
                            MSG_DATA, 
                            MSG_TYPE, 
                            READ_STATUS, 
                            SUMMARY, 
                            COLUMN_TYPE,
                            LOCAL_DATA, 
                            EXT_INFO 
                    FROM message_im_bc;"""
            reader = db_col.execute_sql(sql)
            while reader.Read():
                try:
                    m = model_im.Message()

                    m.account_id = self.using_account.account_id
                    m.msg_id = db_col.get_string(1)
                    m.send_time = Utils.convert_timestamp(db_col.get_int64(3))
                    m.sender_id = db_col.get_string(6)
                    m.sender_name = self.__fetch_sender_name(m.sender_id)
                    m.source = self.checking_account_col.db_path
                    m.talker_type = model_im.CHAT_TYPE_FRIEND
                    m.type = self.__convert_message_type(db_col.get_int64(15))
                    m.talker_id = db_col.get_string(13)
                    m.is_sender = 1 if m.sender_id == self.using_account.account_id or m.sender_id.endswith(
                        self.using_account.username) else 0
                    m.content = self.__fetch_message_content(m, db_col.get_int64(15), db_col.get_string(14))

                    self.model_im_col.db_insert_table_message(m)
                except Exception:
                    self.logger.error()
        self.model_im_col.db_commit()

    def __add_friend_messages(self):
        with self.checking_account_col as db_col:
            sql = """SELECT _id, 
                            MSG_ID, 
                            CLIENT_ID, 
                            MSG_TIME, 
                            MODIFY_TIME, 
                            SORTED_TIME, 
                            SENDER_ID, 
                            SENDER_TYPE, 
                            RECEIVER_ID, 
                            RECEIVER_TYPE, 
                            CONV_TARGET_TYPE, 
                            CONV_TARGET_ENTITY_TYPE, 
                            CONV_TARGET_CVS_TYPE, 
                            CONV_CODE, 
                            MSG_DATA, 
                            MSG_TYPE, 
                            READ_STATUS, 
                            SUMMARY, 
                            COLUMN_TYPE,
                            LOCAL_DATA, 
                            EXT_INFO 
                    FROM message_im_cc;"""
            reader = db_col.execute_sql(sql)
            while reader.Read():
                try:
                    m = model_im.Message()

                    m.account_id = self.using_account.account_id
                    m.msg_id = db_col.get_string(1)
                    m.send_time = Utils.convert_timestamp(db_col.get_int64(3))
                    m.sender_id = db_col.get_string(6)
                    m.sender_name = self.__fetch_sender_name(m.sender_id)
                    m.source = self.checking_account_col.db_path
                    m.talker_type = model_im.CHAT_TYPE_FRIEND
                    m.type = self.__convert_message_type(db_col.get_int64(15))
                    m.talker_id = db_col.get_string(13)
                    m.is_sender = 1 if m.sender_id == self.using_account.account_id or m.sender_id.endswith(
                        self.using_account.username) else 0
                    m.content = self.__fetch_message_content(m, db_col.get_int64(15), db_col.get_string(14))

                    self.model_im_col.db_insert_table_message(m)
                except Exception:
                    self.logger.error()
        self.model_im_col.db_commit()

    def __recover_system_messages(self):
        if not self.checking_account_recover_col.is_valid():
            return
        ts = self.checking_account_recover_col.fetch_table("message_imba", {
            "MSG_ID": "Text",
            "MSG_TIME": "Int",
            "SENDER_ID": "Text",
            "CONV_CODE": "Text",
            "MSG_DATA": "Text",
        })
        for rec in self.checking_account_recover_col.read_deleted_record(ts):

            try:
                m = model_im.Message()

                m.account_id = self.using_account.account_id
                m.msg_id = rec["MSG_ID"].Value
                m.send_time = Utils.convert_timestamp(rec["MSG_TIME"].Value)
                m.sender_id = rec["SENDER_ID"].Value
                m.sender_name = self.__fetch_sender_name(m.sender_id)
                m.source = self.checking_account_col.db_path
                m.talker_type = model_im.CHAT_TYPE_SYSTEM
                m.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
                m.talker_id = rec["CONV_CODE"].Value
                m.is_sender = 1 if m.sender_id in (self.using_account.account_id, self.using_account.username) else 0
                m.content = self.__fetch_system_message_content(rec["MSG_DATA"].Value)
                m.deleted = 1

                self.model_im_col.db_insert_table_message(m)
            except Exception:
                self.logger.error()
        self.model_im_col.db_commit()

    def __recover_shop_messages(self):
        if not self.checking_account_recover_col.is_valid():
            return
        ts = self.checking_account_recover_col.fetch_table("message_im_bc", {
            "MSG_ID": "Text",
            "MSG_TIME": "Int",
            "SENDER_ID": "Text",
            "CONV_CODE": "Text",
            "MSG_DATA": "Text",
            "MSG_TYPE": "Int",
        })
        for rec in self.checking_account_recover_col.read_deleted_record(ts):

            try:
                m = model_im.Message()

                m.account_id = self.using_account.account_id
                m.msg_id = rec["MSG_ID"].Value
                m.send_time = Utils.convert_timestamp(rec["MSG_TIME"].Value)
                m.sender_id = rec["SENDER_ID"].Value
                m.sender_name = self.__fetch_sender_name(m.sender_id)
                m.source = self.checking_account_col.db_path
                m.talker_type = model_im.CHAT_TYPE_FRIEND
                m.type = self.__convert_message_type(rec["MSG_TYPE"].Value)
                m.talker_id = rec["CONV_CODE"].Value
                m.is_sender = 1 if m.sender_id == self.using_account.account_id or m.sender_id.endswith(
                        self.using_account.username) else 0
                m.content = self.__fetch_message_content(m, rec["MSG_TYPE"].Value, rec["MSG_DATA"].Value)
                m.deleted = 1

                self.model_im_col.db_insert_table_message(m)
            except Exception:
                self.logger.error()
        self.model_im_col.db_commit()

    def __recover_friend_messages(self):
        if not self.checking_account_recover_col.is_valid():
            return
        ts = self.checking_account_recover_col.fetch_table("message_im_cc", {
            "MSG_ID": "Text",
            "MSG_TIME": "Int",
            "SENDER_ID": "Text",
            "CONV_CODE": "Text",
            "MSG_DATA": "Text",
            "MSG_TYPE": "Int",
        })
        for rec in self.checking_account_recover_col.read_deleted_record(ts):

            try:
                m = model_im.Message()

                m.account_id = self.using_account.account_id
                m.msg_id = rec["MSG_ID"].Value
                m.send_time = Utils.convert_timestamp(rec["MSG_TIME"].Value)
                m.sender_id = rec["SENDER_ID"].Value
                m.sender_name = self.__fetch_sender_name(m.sender_id)
                m.source = self.checking_account_col.db_path
                m.talker_type = model_im.CHAT_TYPE_FRIEND
                m.type = self.__convert_message_type(rec["MSG_TYPE"].Value)
                m.talker_id = rec["CONV_CODE"].Value
                m.is_sender = 1 if m.sender_id == self.using_account.account_id or m.sender_id.endswith(
                        self.using_account.username) else 0
                m.content = self.__fetch_message_content(m, rec["MSG_TYPE"].Value, rec["MSG_DATA"].Value)
                m.deleted = 1

                self.model_im_col.db_insert_table_message(m)
            except Exception:
                self.logger.error()
        self.model_im_col.db_commit()

    def _generate_message_table(self):
        if self.checking_account_col:
            self.__add_system_messages()
            self.__add_shop_messages()
            self.__add_friend_messages()
        if self.checking_account_recover_col:
            self.__recover_system_messages()
            self.__recover_shop_messages()
            self.__recover_friend_messages()

    def __query_product_info(self, item_id):
        with self.eb_products_col as db_col:
            sql = """SELECT _id, 
                            type, 
                            title, 
                            auction_url, 
                            word, 
                            word_type, 
                            gmt_create, 
                            seller, 
                            address, 
                            fee, 
                            picUrl, 
                            item_id 
                    FROM history
                    WHERE item_id = '{}';""".format(item_id)
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    return {
                        "title": db_col.get_string(2),
                        "fee": db_col.get_string(9),
                    }

                except Exception:
                    self.logger.info(item_id)
            return None

    def _generate_eb_products(self, item_id, product_type, shop_id=None, ts=None, is_deleted=False):
        with self.eb_products_col as db_col:
            sql = """SELECT _id, 
                            type, 
                            title, 
                            auction_url, 
                            word, 
                            word_type, 
                            gmt_create, 
                            seller, 
                            address, 
                            fee, 
                            picUrl, 
                            item_id 
                    FROM history
                    WHERE item_id = '{}';""".format(item_id)
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    p = model_eb.EBProduct()

                    p.set_value_with_idx(p.source_file, self.eb_products_col.db_path)
                    p.set_value_with_idx(p.product_id, db_col.get_string(11))
                    p.set_value_with_idx(p.product_name, db_col.get_string(2))
                    # 价格只能获取到一个区间 10-100 这样的，属于string
                    # p.set_value_with_idx(p.price, db_col.get_string(9))
                    p.set_value_with_idx(p.description, db_col.get_string(9))
                    p.set_value_with_idx(p.shop_id, db_col.get_string(7))
                    p.set_value_with_idx(p.time, ts)
                    p.set_value_with_idx(p.source, product_type)
                    p.set_value_with_idx(p.url, db_col.get_string(10))
                    p.set_value_with_idx(p.shop_id, shop_id)
                    if is_deleted:
                        p.set_value_with_idx(p.deleted, 1)

                    self.model_eb_col.db_insert_table_product(p.get_value())
                except Exception:
                    self.logger.error()
        self.model_eb_col.db_commit()

    def _generate_eb_deal(self, deal_info, is_deleted=False):
        """

        :param deal_info: {'event': 'Page_Detail_Button-Buy', 'ts': '1542267016561', 'spm': 'a2141.7631564.buy', 'seller_id': '690559374', 'item_id': '544649024499', 'shop_id': '66036897', 'container_type': 'xdetail', 'pageName': 'Page_Detail'}
        :param item_id:
        :return:
        """
        item_id = deal_info.get("item_id", None)
        ts = Utils.convert_timestamp(deal_info.get("ts", None))
        seller_id = deal_info.get("seller_id", None)
        shop_id = deal_info.get("shop_id", None)

        if not all((item_id, seller_id, shop_id)):
            return False

        product_info = self.__query_product_info(item_id)

        d = model_eb.EBDeal()
        d.set_value_with_idx(d.source_file, self.eb_log_col.db_path)
        d.set_value_with_idx(d.target, item_id)
        d.set_value_with_idx(d.begin_time, ts)
        d.set_value_with_idx(d.source_file, self.eb_log_col.db_path)

        if is_deleted:
            d.set_value_with_idx(d.deleted, 1)

        if product_info:
            d.set_value_with_idx(d.content, product_info['title'])
            # d.set_value_with_idx(d.money, product_info['fee'])
            d.set_value_with_idx(d.desc, product_info['fee'])

        self.model_eb_col.db_insert_table_deal(d.get_value())
        self.model_eb_col.db_commit()

    def _generate_eb_data(self):

        self.eb_shop_col = self.__load_data_col("AmpData")
        self.eb_products_col = self.__load_data_col("data_history")
        self.eb_log_col = self.__load_data_col("MLTK.db")

        if not all((self.eb_log_col, self.eb_shop_col, self.eb_products_col)):
            return

        with self.eb_log_col as db_col:
            sql = """SELECT id,
                            data,
                            ts
                    FROM UT"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    info_data = json.loads(db_col.get_string(1))
                    event_name = info_data.get("event")
                    item_id = info_data.get("item_id", None)
                    shop_id = info_data.get("shop_id", None)
                    ts = Utils.convert_timestamp(info_data.get("ts", None))

                    if not item_id:
                        continue

                    if event_name == "Page_Detail_Button-Buy":
                        self._generate_eb_deal(info_data)
                    elif event_name == "Page_Detail_Button-AddToCart":
                        self._generate_eb_products(item_id, model_eb.EB_PRODUCT_SHOPCART, shop_id, ts)
                    elif event_name == "Page_Detail_Button-AddToFavorite":
                        self._generate_eb_products(item_id, model_eb.EB_PRODUCT_FAVORITE, shop_id, ts)
                    else:
                        self.logger.info(info_data)

                except Exception:
                    self.logger.error()

    def _recover_eb_data(self):
        # self.recover_eb_shop_col = self.root.GetByPath("/databases/AmpData")
        # self.recover_eb_products_col = self.root.GetByPath("/databases/data_history")
        self.recover_eb_log_col = RecoverTableHelper(self.root.GetByPath("/databases/MLTK.db"))

        if not self.recover_eb_log_col.is_valid():
            return
        ts = self.recover_eb_log_col.fetch_table("UT", {
            "data": "Text",
        })
        for rec in self.recover_eb_log_col.read_deleted_record(ts):
            try:
                info_data = Utils.json_loads(rec["data"].Value)
                if not info_data:
                    continue
                event_name = info_data.get("event")
                item_id = info_data.get("item_id", None)
                shop_id = info_data.get("shop_id", None)
                ts = Utils.convert_timestamp(info_data.get("ts", None))

                if not item_id:
                    continue

                if event_name == "Page_Detail_Button-Buy":
                    self._generate_eb_deal(info_data, is_deleted=True)
                elif event_name == "Page_Detail_Button-AddToCart":
                    self._generate_eb_products(item_id, model_eb.EB_PRODUCT_SHOPCART, shop_id, ts, is_deleted=True)
                elif event_name == "Page_Detail_Button-AddToFavorite":
                    self._generate_eb_products(item_id, model_eb.EB_PRODUCT_FAVORITE, shop_id, ts, is_deleted=True)
                else:
                    self.logger.info(info_data)

            except Exception:
                self.logger.error()

    def __load_data_col(self, file_name):
        db_path = os.path.join(self.data_path, file_name)
        if os.path.exists(db_path):
            return ColHelper(db_path)
        return None

    def __load_recover_col(self, file_name):
        nodes = self.root.Search(file_name)
        for node in nodes:
            return RecoverTableHelper(node)

    def __load_cols(self, file_name):
        data_col = self.__load_data_col(file_name)
        recover_col = self.__load_recover_col(file_name)
        return data_col, recover_col

    def _main(self):
        """解析的逻辑主函数"""
        if not self.data_path:
            return
        for message_db_file in self.__search_messages_db_files():
            self.checking_account_col, self.checking_account_recover_col = self.__load_cols(message_db_file)

            if not all((self.checking_account_col, self.checking_account_recover_col)):
                continue

            self._generate_account_table()
            self._generate_friend_table()
            self._generate_message_table()
        
        # 没有找到eb的数据
        # self._generate_eb_data()
        # self._recover_eb_data()

    def _db_update_finished_info(self):
        """当更新数据完成之后，更新version表的内容，方便日后检查"""
        self.model_eb_col.db_insert_table_version(model_eb.EB_VERSION_KEY, model_eb.EB_VERSION_VALUE)
        self.model_eb_col.db_insert_table_version(model_eb.EB_APP_VERSION_KEY, Tianmao_VERSION)
        self.model_eb_col.db_commit()
        self.model_eb_col.sync_im_version()

    def parse(self):
        """程序入口"""
        if DEBUG or self.model_eb_col.need_parse:
            self.model_eb_col.db_create()

            self._main()

            self._db_update_finished_info()

        generate = model_eb.GenerateModel(self.cache_db)
        results = generate.get_models()

        return results


def parse_tmall(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Tmall
    results = TmallParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("天猫")
    return pr
