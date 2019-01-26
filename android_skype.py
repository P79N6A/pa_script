# -*- coding: utf-8 -*-
import hashlib
import os
import codecs
import json
import base64
import shutil

import xml.etree.ElementTree as ET
import clr

try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
except:
    pass

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

del clr
import time

import model_im
import PA_runtime
import re
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from HTMLParser import HTMLParser

__author__ = "TaoJianping"

# CONST
Skype_VERSION = 1


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
    def decode_base64(encoded_data):
        try:
            return base64.b64decode(encoded_data)
        except Exception as e:
            return None

    @staticmethod
    def str_to_base64(origin_data):
        pass

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


class HtmlNode(object):
    def __init__(self, tag):
        self.tag_name = tag
        self.property = {}
        self.child = []
        self.data = None

    def __getitem__(self, item):
        """暂时只支持返回找到的第一个元素"""
        for node in self.child:
            if node.tag_name == item:
                return node

    def get_all(self, tag_name):
        answer = []
        for node in self.child:
            if node.tag_name == tag_name:
                answer.append(node)
        return answer

    def get(self, key, default=None):
        for node in self.child:
            if node.tag_name == key:
                return node
        return default


class PaHtmlParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.__inner_node_list = []
        self.body = {}
        self.root = HtmlNode(tag="html")

    def handle_starttag(self, tag, attrs):
        node = HtmlNode(tag)
        node.property = {k: v for k, v in attrs}
        self.__inner_node_list.append(node)

    def handle_data(self, data):
        if not self.__inner_node_list:
            return
        self.__inner_node_list[-1].data = data

    def handle_endtag(self, tag):
        node = self.__inner_node_list.pop()
        if len(self.__inner_node_list) != 0:
            self.__inner_node_list[-1].child.append(node)
        else:
            self.root.child.append(node)

    @property
    def first_dom(self):
        if len(self.root.child) == 0:
            return
        return self.root.child[0]


class SkypeParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.model_im_col = model_im.IM()
        self.cache_db = self.__get_cache_db()
        self.model_im_col.db_create(self.cache_db)
        self.recovering_helper = None
        self.checking_col = None
        self.using_account = None
        self.table_name = None

    def __change_db_config(self):
        node = self.root.GetByPath("databases")
        if not node:
            return
        account_db_nodes = node.Search('/*\.db$')
        for node in account_db_nodes:
            file_name = os.path.basename(node.PathWithMountPoint)
            if not file_name.startswith("s4l-"):
                continue
            new_db_path = os.path.join(self.cache_path, file_name)
            shutil.copy(node.PathWithMountPoint, new_db_path)
            # 配置model
            self.recovering_helper = RecoverTableHelper(node)
            self.checking_col = ColHelper(new_db_path)
            yield

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath("Skype")
        m = hashlib.md5()
        m.update(Encoding.UT8.GetBytes(self.root.AbsolutePath))
        return os.path.join(self.cache_path, m.hexdigest().upper())

    @staticmethod
    def _handle_birthday(birthday):
        if not birthday:
            return
        time_tuple = time.strptime(birthday, "%Y-%m-%d")
        ts = int(time.mktime(time_tuple))
        return ts

    def _generate_account_table(self):
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
        account.birthday = self._handle_birthday(account_info.get("birthday", None))
        account.country = account_info.get("country", None)
        account.city = account_info.get("city", None)

        # 挂载正在使用的account
        self.using_account = account

        self.model_im_col.db_insert_table_account(account)
        self.model_im_col.db_commit()

    def _generate_friend_table(self):
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
                    friend.birthday = self._handle_birthday(friend_info.get("birthday", None))
                    if friend_info.get("city") or friend_info.get("country"):
                        friend.address = friend_info.get("city", "") + " " + friend_info.get("country", "")

                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    pass
            self.model_im_col.db_commit()

    def _generate_chatroom_table(self):
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
                    if (not chatroom.chatroom_id.endswith("@thread.skype")) and (
                            not chatroom.chatroom_id.endswith("highlights.skype")):
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
                            member["id"] = member["id"].split(":", 1)[1]
                            member["chatroom_id"] = chatroom.chatroom_id
                            chatroom_member.append(member)

                    self.model_im_col.db_insert_table_chatroom(chatroom)
                except Exception as e:
                    pass

            self.model_im_col.db_commit()
        return chatroom_member

    def _generate_message_table(self):
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
                    message.sender_name = self.__query_sender_name(message_info.get("creator"))
                    message.is_sender = 1 if message.account_id == message.sender_id else 0
                    content = self.__convert_message_content(message_info.get("content", None), message)
                    message.content = content
                    message.send_time = self.__convert_timestamp(message_info.get("createdTime", None))
                    if not message.type:
                        message.type = self.__convert_message_content_type(message_info.get("messagetype", None))
                    message.talker_type = model_im.CHAT_TYPE_GROUP if "@" in message.talker_id else model_im.CHAT_TYPE_FRIEND
                    if message.type in (model_im.MESSAGE_CONTENT_TYPE_IMAGE, model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                                        model_im.MESSAGE_CONTENT_TYPE_VOICE):
                        self.__add_media_path(message)
                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    pass
            self.model_im_col.db_commit()

    def _generate_chatroom_member_table(self, member_list):
        for member in member_list:
            try:
                member_id = member["id"]
                chatroom_id = member["chatroom_id"]
                serialized_info = self.__query_account_info(member_id)
                if not serialized_info:
                    continue
                account_info = json.loads(serialized_info)

                chatroom_member = model_im.ChatroomMember()
                chatroom_member.chatroom_id = chatroom_id
                chatroom_member.member_id = member_id
                chatroom_member.account_id = self.using_account.account_id
                chatroom_member.photo = account_info.get("thumbUrl", None)
                if account_info.get("city") or account_info.get("country"):
                    chatroom_member.address = account_info.get("city", "") + " " + account_info.get("country", "")
                chatroom_member.telephone = self.__choose_phone_number(account_info.get("phones", []))
                chatroom_member.gender = self.__convert_gender(account_info.get("gender", 0))
                chatroom_member.birthday = self._handle_birthday(account_info.get("birthday", None))
                chatroom_member.signature = account_info.get("mood", None)
                chatroom_member.display_name = account_info.get("displayNameOverride", None)
                chatroom_member.source = self.checking_col.db_path
                self.model_im_col.db_insert_table_chatroom_member(chatroom_member)
            except Exception as e:
                pass
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
                friend.birthday = self._handle_birthday(friend_info.get("birthday", None))
                if friend_info.get("city") or friend_info.get("country"):
                    friend.address = friend_info.get("city", "") + " " + friend_info.get("country", "")

                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                pass
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
                if (not chatroom.chatroom_id.endswith("@thread.skype")) and (
                        not chatroom.chatroom_id.endswith("highlights.skype")):
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
                pass
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
                message.sender_name = self.__query_sender_name(message_info.get("creator"))
                message.is_sender = 1 if message.account_id == message.sender_id else 0
                message.content = self.__convert_message_content(message_info.get("content", None), message)
                message.send_time = self.__convert_timestamp(message_info.get("createdTime", None))
                if not message.type:
                    message.type = self.__convert_message_content_type(message_info.get("messagetype", None))
                message.talker_type = model_im.CHAT_TYPE_GROUP if "@" in message.talker_id else model_im.CHAT_TYPE_FRIEND
                if message.type in (model_im.MESSAGE_CONTENT_TYPE_IMAGE, model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                                    model_im.MESSAGE_CONTENT_TYPE_VOICE):
                    self.__add_media_path(message)
                message.deleted = 1

                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                pass
        self.model_im_col.db_commit()

    def __add_media_path(self, m_obj):
        nodes = self.root.FileSystem.Search(m_obj.content + '$')
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
                        pass
        return ret

    def __query_sender_name(self, sender_id):
        sql = """SELECT nsp_data 
                    FROM {table_name}
                    WHERE nsp_pk = 'C{sender_id}';""".format(
            table_name=self.table_name["profilecache"],
            sender_id=sender_id,
        )
        if self.checking_col.is_opened is False:
            self.checking_col.open()
        reader = self.checking_col.fetch_reader(sql)
        while reader.Read():
            try:
                user_info = json.loads(ColHelper.fetch_string(reader, 0))

                nickname = user_info.get("displayNameOverride", None)
                if self.checking_col.in_context is False:
                    self.checking_col.close()
                return nickname
            except Exception as e:
                pass
        if self.checking_col.in_context is False:
            self.checking_col.close()
        return ""

    def __add_location(self, msg, address, latitude, longitude, ts=None):
        location = model_im.Location()
        location.account_id = self.using_account.account_id
        location.address = address
        location.latitude = latitude
        location.longitude = longitude
        location.timestamp = ts
        self.model_im_col.db_insert_table_location(location)

        msg.location_id = location.location_id
        msg.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION

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

    def __assemble_message_delete_member(self, executor, target):
        executor_name = self.__query_sender_name(executor)
        target_name = self.__query_sender_name(target)

        if not executor_name:
            executor_name = ":".join(executor.split(":")[-2:])
        if not target_name:
            target_name = ":".join(target.split(":")[-2:])

        if executor == target:
            return "{} 已离开此对话".format(executor_name)
        return "{} 已将 {} 从此对话中移除".format(executor_name, target_name)

    def __assemble_message_add_member(self, executor, target):
        executor_name = self.__query_sender_name(executor)
        target_name = self.__query_sender_name(target)

        if not executor_name:
            executor_name = ":".join(executor.split(":")[-2:])
        if not target_name:
            target_name = ":".join(target.split(":")[-2:])

        if executor == target:
            return "{} 已加入此对话".format(executor_name)
        return "{} 已将 {} 添加进此对话".format(executor_name, target_name)

    def __assemble_message_emoji(self, nodes):
        if not nodes:
            return None
        return "".join(node.data for node in nodes)

    def __assemble_message_call(self, root_node):
        call_type = root_node['partlist'].property['type']
        if call_type == 'missed':
            caller_id = root_node['partlist'].child[0].property['identity']
            if caller_id == self.using_account.account_id:
                return "未接电话"
            return "{} 未接电话".format(root_node['partlist'].child[0]['name'].data)
        elif call_type == 'started':
            return "开始通话"
        elif call_type == 'ended':
            return "通话 {} 秒".format(root_node['partlist'].child[0]['duration'].data)
        return None

    def __assemble_message_rename_group(self, root_node):
        changer = root_node['topicupdate']['initiator'].data
        new_name = root_node['topicupdate']['value'].data
        changer_display_name = self.__query_sender_name(changer)
        return "{} 已将此对话重命名为“{}”".format(changer_display_name, new_name)

    def __assemble_message_history_closure(self, root):
        if root['historydisclosedupdate']['value'].data == "true":
            return "{} 已将聊天历史记录隐藏，对新参与者不可见".format(
                self.__query_sender_name(root['historydisclosedupdate']['initiator'].data))
        else:
            return "{} 已将历史聊天记录设为对所有人可见".format(
                self.__query_sender_name(root['historydisclosedupdate']['initiator'].data))

    def __assemble_message_close_add_memeber(self, root_node):
        if root_node['joiningenabledupdate']['value'].data == "true":
            return "{} 已启用使用链接加入此对话。转到“组设置”获取邀请其他人加入的链接".format(
                self.__query_sender_name(root_node['joiningenabledupdate']['initiator'].data))
        else:
            return "{} 已禁用加入此对话".format(
                self.__query_sender_name(root_node['joiningenabledupdate']['initiator'].data))

    def __assemble_message_change_pic_bak(self, root_node):
        return "{} 已更改对话图片".format(self.__query_sender_name(root_node['pictureupdate']['initiator'].data))

    def __convert_message_content(self, content, msg_obj):
        content = content.strip()
        if not (content.startswith("<") and content.endswith(">")):
            return content
        try:
            hp = PaHtmlParser()
            hp.feed(content)
            hp.close()

            tag_name = hp.first_dom.tag_name
            if tag_name == "deletemember":
                executor = hp.root['deletemember']['initiator'].data
                target = hp.root['deletemember']['target'].data
                return self.__assemble_message_delete_member(executor, target)
            elif tag_name == "addmember":
                executor = hp.root['addmember']['initiator'].data
                target = hp.root['addmember']['target'].data
                return self.__assemble_message_add_member(executor, target)
            elif tag_name == "ss":
                nodes = hp.root.get_all("ss")
                return self.__assemble_message_emoji(nodes)
            elif tag_name == "uriobject":
                if hp.root['uriobject'].get("swift", None):
                    encoded_info = hp.root['uriobject']['swift'].property['b64']
                    decoded_info = Utils.decode_base64(encoded_info)
                    attachment = json.loads(decoded_info)["attachments"][0]
                    url = attachment["content"]['images'][0]['url']
                    return url
                return hp.root['uriobject']['originalname'].property['v']
            elif tag_name == 'partlist':
                return self.__assemble_message_call(hp.root)
            elif tag_name == 'topicupdate':
                return self.__assemble_message_rename_group(hp.root)
            elif tag_name == 'location':
                address = hp.root['location'].property['address']
                latitude = float(hp.root['location'].property['latitude']) / 1000000
                longitude = float(hp.root['location'].property['longitude']) / 1000000
                ts = Utils.convert_timestamp(hp.root['location'].property.get("timestamp", None))
                self.__add_location(msg_obj, address, latitude, longitude, ts)
                return None
            elif tag_name == 'historydisclosedupdate':
                return self.__assemble_message_history_closure(hp.root)
            elif tag_name == 'joiningenabledupdate':
                return self.__assemble_message_close_add_memeber(hp.root)
            elif tag_name == "sms":
                return hp.root['sms']['defaults']['content'].data
            elif tag_name == "pictureupdate":
                return self.__assemble_message_change_pic_bak(hp.root)
            else:
                return content
        except Exception as e:
            return content

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

        for _ in self.__change_db_config():
            # 因为查询的表的名字里面有版本号，所以做一些处理，挂载一个表名字的状态
            self.table_name = self.__query_table_names()

            self._generate_account_table()
            self._generate_friend_table()
            self._generate_message_table()
            chatroom_members = self._generate_chatroom_table()
            self._generate_chatroom_member_table(chatroom_members)
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
