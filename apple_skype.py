# -*- coding: utf-8 -*-
import hashlib
import os
import codecs
import json
import base64
import shutil

import xml.etree.ElementTree as ET
import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

del clr

import model_im
import PA_runtime
import re
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions

__author__ = "TaoJianping"

# CONST
Skype_VERSION = 1


class ColHelper(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = System.Data.SQLite.SQLiteConnection(
            'Data Source = {}; Readonly = True'.format(db_path))
        self.reader = None

    def __enter__(self):
        self.conn.Open()
        self.cmd = System.Data.SQLite.SQLiteCommand(self.conn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cmd.Dispose()
        if self.reader is not None:
            self.reader.Close()
        self.conn.Close()
        return True

    def __repr__(self):
        return "this db exists in {path}".format(path=self.db_path)

    def execute_sql(self, sql):
        self.cmd.CommandText = sql
        self.reader = self.cmd.ExecuteReader()
        return self.reader

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


class RecoverTableHelper(object):
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
    def base64_to_str(encoded_data):
        pass

    @staticmethod
    def str_to_base64(origin_data):
        pass


class SkypeParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.model_im_col = model_im.IM()
        self.cache_db = self.__get_cache_db()
        self.model_im_col.db_create(self.cache_db)
        self.account_db_nodes = self.__fetch_data_path()
        self.recovering_helper = None
        self.checking_col = None
        self.using_account = None
        self.table_name = None

    def __fetch_data_path(self):
        node = self.root.GetByPath("Library/LocalDatabase")
        if not node:
            return
        account_db_nodes = node.Search('/*\.db$')
        old_version_nodes = node.Search('/*\.dbb$')
        for node in account_db_nodes:
            file_name = os.path.basename(node.PathWithMountPoint)
            new_db_path = os.path.join(self.cache_path, file_name)
            shutil.copy(node.PathWithMountPoint, new_db_path)
            self.recovering_helper = RecoverTableHelper(node)
            yield new_db_path
        for node in old_version_nodes:
            file_name = os.path.basename(node.PathWithMountPoint)
            new_db_path = os.path.join(self.cache_path, file_name)
            shutil.copy(node.PathWithMountPoint, new_db_path)
            self.recovering_helper = RecoverTableHelper(node)
            yield new_db_path

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath("Skype")
        m = hashlib.md5()
        m.update(self.root.AbsolutePath)
        return os.path.join(self.cache_path, m.hexdigest().upper())

    def _get_account_table(self):
        file_name = os.path.basename(self.checking_col.db_path)
        account_sign = file_name.split(".")[0].split("-")[1]
        account_info = json.loads(self.__query_account_info(account_sign))

        account = model_im.Account()
        account.account_id = account_info["mri"].split(":", 1)[1]
        account.username = account_info.get("fullName", None)
        account.nickname = account_info.get("displayNameOverride", None)
        account.photo = account_info.get("thumbUrl", None)
        account.gender = self.__convert_gender(account_info.get("gender", 0))
        account.source = self.checking_col.db_path
        account.signature = account_info.get("mood", None)
        account.telephone = self.__choose_phone_number(account_info.get("phones", []))
        account.birthday = account_info.get("birthday", None)
        account.country = account_info.get("country", None)
        account.city = account_info.get("city", None)

        # 挂载正在使用的account
        self.using_account = account

        self.model_im_col.db_insert_table_account(account)
        self.model_im_col.db_commit()

    def _get_friend_table(self):
        with self.checking_col as db_col:
            sql = """SELECT nsp_data 
                    FROM {};""".format(self.table_name["profilecache"])
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend_info = json.loads(db_col.get_string(0))
                    if friend_info.get("isManuallyAdded", None) is None:
                        continue

                    friend = model_im.Friend()
                    friend.source = db_col.db_path
                    friend.account_id = self.using_account.account_id
                    friend_type, friend.friend_id = friend_info["mri"].split(":", 1)
                    if friend.account_id == friend.friend_id or friend_type not in ("4", "8"):
                        continue
                    friend.signature = friend_info.get("mood", None)
                    friend.gender = self.__convert_gender(friend_info.get("gender", 0))
                    friend.nickname = friend_info.get("displayNameOverride", None)
                    friend.telephone = self.__choose_phone_number(friend_info.get("phones", []))
                    friend.photo = friend_info.get("thumbUrl", None)
                    friend.type = model_im.FRIEND_TYPE_FRIEND if friend_type == "8" else None
                    friend.birthday = friend_info.get("birthday", None)
                    if friend_info.get("city") or friend_info.get("country"):
                        friend.address = friend_info.get("city", "") + " " + friend_info.get("country", "")

                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    print("_get_friend_table error", e)
            self.model_im_col.db_commit()

    def _get_chatroom_table(self):
        chatroom_member = []
        with self.checking_col as db_col:
            sql = """SELECT nsp_data 
                    FROM {};""".format(self.table_name["conversations"])
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    chatroom_info = json.loads(db_col.get_string(0))

                    chatroom = model_im.Chatroom()
                    chatroom.source = db_col.db_path
                    chatroom.account_id = self.using_account.account_id
                    chatroom.chatroom_id = chatroom_info["conv"]["id"].split(":", 1)[1]
                    if not chatroom.chatroom_id.endswith("@thread.skype"):
                        continue
                    if chatroom_info["conv"].get("_convProps", None):
                        chatroom.create_time = self.__convert_timestamp(
                            chatroom_info["conv"]["_convProps"].get("created", None))
                    if chatroom_info["conv"].get("_threadProps", None):
                        chatroom.member_count = int(chatroom_info["conv"]["_threadProps"].get("membercount", 0))
                        chatroom.name = chatroom_info["conv"]["_threadProps"].get("topic", None)
                        if chatroom_info["conv"]["_threadProps"].get("creator", None):
                            chatroom.creator_id = \
                                chatroom_info["conv"]["_threadProps"].get("creator", "").split(":", 1)[1]
                    # 获取成员并return 出去方便拿到chatroom_member表
                    if chatroom_info["conv"].get("_threadMembers", None):
                        for member in chatroom_info["conv"]["_threadMembers"]:
                            member["id"] = member["id"].split(":")[1]
                            member["chatroom_id"] = chatroom.chatroom_id
                            chatroom_member.append(member)

                    self.model_im_col.db_insert_table_chatroom(chatroom)
                except Exception as e:
                    print("_get_chatroom_table error", e)

            self.model_im_col.db_commit()
        return chatroom_member

    def _get_message_table(self):
        with self.checking_col as db_col:
            sql = """SELECT nsp_data 
                    FROM {};""".format(self.table_name["messages"])
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    message_info = json.loads(db_col.get_string(0))

                    message = model_im.Message()
                    message.source = self.checking_col.db_path
                    message.msg_id = message_info.get("cuid", None)
                    message.account_id = self.using_account.account_id
                    message.talker_id = message_info.get("conversationId").split(":", 1)[1]
                    message.sender_id = message_info.get("creator").split(":", 1)[1]
                    message.is_sender = 1 if message.account_id == message.sender_id else 0
                    message.content = message_info.get("content", None)
                    message.send_time = self.__convert_timestamp(message_info.get("createdTime", None))
                    message.type = self.__convert_message_content_type(message_info.get("messagetype", None))
                    message.talker_type = model_im.CHAT_TYPE_GROUP if "@" in message.talker_id else model_im.CHAT_TYPE_FRIEND
                    if message.type in (model_im.MESSAGE_CONTENT_TYPE_IMAGE, model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                                        model_im.MESSAGE_CONTENT_TYPE_VOICE):
                        self.__add_media_path(message)
                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    print("_get_message_table error", e)
            self.model_im_col.db_commit()

    def _get_chatroom_member_table(self, member_list):
        for member in member_list:
            member_id = member["id"]
            chatroom_id = member["chatroom_id"]
            account_info = json.loads(self.__query_account_info(member_id))

            chatroom_member = model_im.ChatroomMember()
            chatroom_member.chatroom_id = chatroom_id
            chatroom_member.member_id = member_id
            chatroom_member.account_id = self.using_account.account_id
            chatroom_member.photo = account_info.get("thumbUrl", None)
            if account_info.get("city") or account_info.get("country"):
                chatroom_member.address = account_info.get("city", "") + " " + account_info.get("country", "")
            chatroom_member.telephone = self.__choose_phone_number(account_info.get("phones", []))
            chatroom_member.gender = self.__convert_gender(account_info.get("gender", 0))
            chatroom_member.birthday = account_info.get("birthday", None)
            chatroom_member.signature = account_info.get("mood", None)
            chatroom_member.display_name = account_info.get("displayNameOverride", None)
            chatroom_member.source = self.checking_col.db_path

            self.model_im_col.db_insert_table_chatroom_member(chatroom_member)
        self.model_im_col.db_commit()

    def decode_recover_friend(self):
        table_name = self.table_name["profilecache"]
        if not self.recovering_helper.is_valid():
            return
        ts = self.recovering_helper.fetch_table(table_name, {
            "nsp_data": "Text",
        })
        for rec in self.recovering_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                friend_info = json.loads(rec["nsp_data"].Value)
                if friend_info.get("isManuallyAdded", None) is None:
                    continue

                friend = model_im.Friend()
                friend.source = self.checking_col.db_path
                friend.account_id = self.using_account.account_id
                friend_type, friend.friend_id = friend_info["mri"].split(":", 1)
                if friend.account_id == friend.friend_id:
                    continue
                if friend_type not in ("4", "8"):
                    continue
                friend.signature = friend_info.get("mood", None)
                friend.gender = self.__convert_gender(friend_info.get("gender", 0))
                friend.nickname = friend_info.get("displayNameOverride", None)
                friend.telephone = self.__choose_phone_number(friend_info.get("phones", []))
                friend.photo = friend_info.get("thumbUrl", None)
                if friend_type == "8":
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                friend.birthday = friend_info.get("birthday", None)
                if friend_info.get("city") or friend_info.get("country"):
                    friend.address = friend_info.get("city", "") + " " + friend_info.get("country", "")

                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                print("decode_recover_friend debug error", e)
        self.model_im_col.db_commit()

    def decode_recover_chatroom(self):
        table_name = self.table_name["conversations"]
        if not self.recovering_helper.is_valid():
            return
        ts = self.recovering_helper.fetch_table(table_name, {
            "nsp_data": "Text",
        })
        for rec in self.recovering_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                chatroom_info = json.loads(rec["nsp_data"].Value)

                chatroom = model_im.Chatroom()
                chatroom.deleted = 1
                chatroom.source = self.checking_col.db_path
                chatroom.account_id = self.using_account.account_id
                chatroom.chatroom_id = chatroom_info["conv"]["id"].split(":", 1)[1]
                if not chatroom.chatroom_id.endswith("@thread.skype"):
                    continue
                if chatroom_info["conv"].get("_convProps", None):
                    chatroom.create_time = self.__convert_timestamp(
                        chatroom_info["conv"]["_convProps"].get("created", None))
                if chatroom_info["conv"].get("_threadProps", None):
                    chatroom.member_count = int(chatroom_info["conv"]["_threadProps"].get("membercount", 0))
                    chatroom.name = chatroom_info["conv"]["_threadProps"].get("topic", None)
                    if chatroom_info["conv"]["_threadProps"].get("creator", None):
                        chatroom.creator_id = chatroom_info["conv"]["_threadProps"].get("creator", "").split(":", 1)[1]
                self.model_im_col.db_insert_table_chatroom(chatroom)
            except Exception as e:
                print("decode_recover_chatroom debug error", e)
        self.model_im_col.db_commit()

    def decode_recover_message(self):
        table_name = self.table_name["messages"]
        if not self.recovering_helper.is_valid():
            return
        ts = self.recovering_helper.fetch_table(table_name, {
            "nsp_data": "Text",
        })
        for rec in self.recovering_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                message_info = json.loads(rec["nsp_data"].Value)

                message = model_im.Message()
                message.source = self.checking_col.db_path
                message.msg_id = message_info.get("cuid", None)
                message.account_id = self.using_account.account_id
                message.talker_id = message_info.get("conversationId").split(":", 1)[1]
                message.sender_id = message_info.get("creator").split(":", 1)[1]
                message.is_sender = 1 if message.account_id == message.sender_id else 0
                message.content = message_info.get("content", None)
                message.send_time = self.__convert_timestamp(message_info.get("createdTime", None))
                message.type = self.__convert_message_content_type(message_info.get("messagetype", None))
                message.talker_type = model_im.CHAT_TYPE_GROUP if "@" in message.talker_id else model_im.CHAT_TYPE_FRIEND
                if message.type in (model_im.MESSAGE_CONTENT_TYPE_IMAGE, model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                                    model_im.MESSAGE_CONTENT_TYPE_VOICE):
                    self.__add_media_path(message)
                message.deleted = 1

                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                print("decode_recover_message debug error", e)
        self.model_im_col.db_commit()

    @staticmethod
    def __fetch_file_name(content):
        ans_list = re.findall("(?<=originalName=\\\").*?(?=\\\">)", content)
        if ans_list:
            return ans_list[0]
        return None

    def __add_media_path(self, m_obj):
        searchkey = self.__fetch_file_name(m_obj.content)
        if not searchkey:
            return
        nodes = self.root.FileSystem.Search(searchkey + '$')
        for node in nodes:
            m_obj.media_path = node.AbsolutePath
            return True

    @staticmethod
    def __choose_phone_number(phone_list, _type=2):
        for phone in phone_list:
            if phone["type"] == 2:
                return phone["number"]
        return None

    def __query_account_info(self, account_sign):
        with self.checking_col as db_col:
            sql = """select nsp_pk,
                            nsp_data
                    from profilecachev8 
                    where nsp_pk like '%{}%';""".format(account_sign)
            db_col.execute_sql(sql)
            while db_col.has_rest():
                return db_col.get_string(1)
            return None

    def __query_table_names(self):
        ret = {
            "messages": "messagesv12",
            "profilecache": "profilecachev8",
            "conversations": "conversationsv14",
        }
        table_name_list = ["messages", "profilecache", "conversations"]
        for table_name in table_name_list:
            with self.checking_col as db_col:
                sql = """SELECT name _id FROM sqlite_master 
                        WHERE type ='table' and _id like '{}%' limit 1;""".format(table_name)
                db_col.execute_sql(sql)
                while db_col.has_rest():
                    try:
                        ret[table_name] = db_col.get_string(0)
                    except Exception as e:
                        print("error:__query_table_names", e)
        return ret

    @staticmethod
    def __convert_gender(gender):
        if gender == 1:
            return model_im.GENDER_MALE
        elif gender == 2:
            return model_im.GENDER_FEMALE
        else:
            return model_im.GENDER_NONE

    @staticmethod
    def __convert_timestamp(ts):
        if not ts:
            return
        ts = str(ts)
        if len(ts) > 13:
            return
        elif int(ts) < 0:
            return
        elif len(ts) == 13:
            return int(ts[:-3])
        elif len(ts) <= 10:
            return int(ts)
        else:
            return

    @staticmethod
    def __convert_message_content_type(_type):
        if _type == "RichText" or _type == "Text":
            return model_im.MESSAGE_CONTENT_TYPE_TEXT
        elif _type.startswith("ThreadActivity"):
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        elif _type == "RichText/Media_Video":
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif _type == "RichText/Media_AudioMsg":
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif _type == "RichText/UriObject":
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif _type == "Event/Call":
            return model_im.MESSAGE_CONTENT_TYPE_VOIP
        else:
            return None

    def parse(self):
        """解析的主函数"""

        for checking_db_path in self.__fetch_data_path():
            self.checking_col = ColHelper(checking_db_path)

            # 因为查询的表的名字里面有版本号，所以做一些处理，挂载一个表名字的状态
            self.table_name = self.__query_table_names()

            self._get_account_table()
            self._get_friend_table()
            self._get_message_table()
            chatroom_members = self._get_chatroom_table()
            self._get_chatroom_member_table(chatroom_members)
            self.decode_recover_friend()
            self.decode_recover_message()
            self.decode_recover_chatroom()

        generate = model_im.GenerateModel(self.cache_db)
        results = generate.get_models()

        return results


def analyze_skype(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Skype
    results = SkypeParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Skype")
    return pr
