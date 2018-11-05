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
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from PA.Common.Utilities.Types import TimeStampFormats


__author__ = "TaoJianping"

# CONST
ICQ_VERSION = 1


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
    def __init__(self, node_path):
        # node = Node(node_path)
        self.db = SQLiteParser.Database.FromNode(node_path, canceller)
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


class ICQParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.model_im_col = model_im.IM()
        self.cache_db = self.__get_cache_db()
        self.account_info_path, self.cl_db_path, self.agent_db_path, self.files_db_path = self.__fetch_data_node()
        self.model_im_col.db_create(self.cache_db)
        self.cl_db_col = ColHelper(self.cl_db_path)
        self.agent_db_col = ColHelper(self.agent_db_path)
        self.files_db_col = ColHelper(self.files_db_path)
        # self.message_recover_helper = RecoverTableHelper(self.agent_db_path)
        # self.cl_recover_helper = RecoverTableHelper(self.cl_db_path)
        self.using_account = None

    def __fetch_data_node(self):
        account_info_node = self.root.GetByPath(r"/Documents/profiles")
        cl_db_node = self.root.GetByPath(r"/Documents/cl.sqlite")
        agent_db_node = self.root.GetByPath(r"/Documents/Agent.sqlite")
        files_db_node = self.root.GetByPath(r"/Documents/files.sqlite")
        self.message_recover_helper = RecoverTableHelper(agent_db_node)
        self.cl_recover_helper = RecoverTableHelper(cl_db_node)
        path_name_list = []
        if all((account_info_node, cl_db_node, agent_db_node, files_db_node)):
            for node in (account_info_node, cl_db_node, agent_db_node, files_db_node):
                copy_file_path = os.path.join(
                    self.cache_path,
                    os.path.split(node.PathWithMountPoint)[-1]
                )
                path_name_list.append(copy_file_path)
                shutil.copy(node.PathWithMountPoint, copy_file_path)
            return path_name_list
        else:
            miss_node = [i for i in (account_info_node, cl_db_node, agent_db_node, files_db_node) if i is None]
            raise Exception("{} => 没有找到相应的文件", miss_node)

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath("ICQ")
        m = hashlib.md5()
        m.update(self.root.AbsolutePath)
        return os.path.join(self.cache_path, m.hexdigest().upper())

    def _get_account_table(self):
        account = model_im.Account()

        profile_data = json.loads(Utils.open_file(self.account_info_path))
        decrypted_user_info = base64.decodestring(profile_data['profiles'][0].get("myInfo"))
        user_info = json.loads(decrypted_user_info)

        account.account_id = profile_data['profiles'][0].get("uid")
        account.gender = self.__convert_gender(user_info["gender"])
        account.telephone = user_info.get("attachedPhoneNumber")
        account.nickname = user_info.get("displayId", "").decode("utf-8")
        account.birthday = self.__convert_timestamp(user_info.get("birthDate"))
        account.signature = user_info.get("about", None)
        if user_info["homeAddress"].get("city", None):
            account.city = user_info["homeAddress"].get("city").decode("utf-8")
        if user_info["homeAddress"].get("country", None):
            account.country = user_info["homeAddress"].get("country").decode("utf-8")
        account.source = self.account_info_path

        self.using_account = account
        self.model_im_col.db_insert_table_account(account)
        self.model_im_col.db_commit()

    def _get_friend_table(self):
        with self.cl_db_col as db_col:
            sql = """SELECT contact.pid, 
                            anketa.about, 
                            anketa.birthdate, 
                            anketa.city, 
                            anketa.country, 
                            anketa.emails, 
                            anketa.gender, 
                            anketa.nickname, 
                            anketa.smsNumber, 
                            anketa.abContactName
                    FROM contact LEFT JOIN anketa ON contact._rowid_ = anketa.contactID
                    WHERE contact.groupId in (1, 2, 3) AND contact.userType = 2;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend = model_im.Friend()
                    friend.source = self.cl_db_path
                    friend.account_id, _, friend.friend_id = db_col.get_string(0).split("|")
                    if friend.account_id == friend.friend_id:
                        continue
                    friend.signature = db_col.get_string(1)
                    friend.birthday = self.__convert_timestamp(db_col.get_int64(2))
                    friend.address = db_col.get_string(4) + db_col.get_string(3)
                    friend.email = db_col.get_string(5)
                    friend.gender = self.__convert_gender(db_col.get_int64(6))
                    friend.nickname = db_col.get_string(9)
                    friend.telephone = db_col.get_string(8)
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

    def _get_chatroom_table(self):
        with self.cl_db_col as db_col:
            sql = """SELECT contact.pid, 
                            contact.userType, 
                            contact.isTemporary, 
                            contact.displayName,
                            chat_info.chatDescription, 
                            chat_info.chatParticipantsCount, 
                            chat_info.rules, 
                            chat_info.youRole
                    FROM contact left join chat_info on contact._rowid_ = chat_info.contactID
                    where chat_info.youRole != 0;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    chatroom = model_im.Chatroom()
                    chatroom.source = self.cl_db_path
                    chatroom.account_id, _, chatroom.chatroom_id = db_col.get_string(0).split("|")
                    chatroom.name = db_col.get_string(3)
                    chatroom.description = db_col.get_string(4)
                    chatroom.member_count = db_col.get_int64(5)
                    chatroom.notice = db_col.get_string(6)
                    chatroom.type = model_im.CHATROOM_TYPE_NORMAL if db_col.get_int64(
                        2) == 0 else model_im.CHATROOM_TYPE_TEMP
                    self.model_im_col.db_insert_table_chatroom(chatroom)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

    def _get_message_table(self):
        with self.agent_db_col as db_col:
            sql = """SELECT ZMRMESSAGE.ZHISTORYID, 
                            ZMRMESSAGE.ZTYPE, 
                            ZMRMESSAGE.ZTEXT, 
                            ZMRMESSAGE.ZTIME, 
                            ZMRMESSAGE.ZWASREAD, 
                            ZMRMESSAGE.ZPARTICIPANTUID, 
                            ZMRCONVERSATION.ZPID,
                            ZMRMESSAGE.ZOUTGOING,
                            ZMRMESSAGE.ZFILEID
                    FROM ZMRMESSAGE 
                    LEFT JOIN ZMRCONVERSATION ON ZMRMESSAGE.ZCONVERSATION = ZMRCONVERSATION.Z_PK;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    message = model_im.Message()
                    message.source = self.cl_db_path
                    message.account_id, _, message.talker_id = db_col.get_string(6).split("|")
                    message.msg_id = db_col.get_int64(0)
                    message.content = db_col.get_string(2)
                    message.send_time = self._get_timestamp(db_col.get_int64(3))
                    message.is_sender = 1 if db_col.get_int64(7) == 1 else 0

                    if "@" in message.talker_id:
                        message.sender_id = db_col.get_string(5)
                        message.talker_type = model_im.CHAT_TYPE_GROUP
                    else:
                        message.talker_type = model_im.CHAT_TYPE_FRIEND
                        if message.is_sender == 1:
                            message.sender_id = message.account_id
                        else:
                            message.sender_id = message.talker_id

                    _type = db_col.get_int64(1)
                    if _type == 510:
                        file_id = db_col.get_int64(8)
                        file_info = self.__query_file_info(file_id)
                        message.media_path = file_info[0] if not file_info[0] else file_info[2]
                        if file_info[1] is not None:
                            file_type = file_info[1].split("/")[0]
                            if file_type == "image":
                                message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            elif file_type == "audio":
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                            elif file_type == "video":
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                            else:
                                message.type = None
                    else:
                        message.type = self.__convert_message_content_type(_type)

                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

    def _get_chatroom_member_table(self):
        for chatroom_member_info in self.__query_chatroom_list():
            member_id = chatroom_member_info["member_id"]
            account_id = chatroom_member_info["account_id"]
            conversation_id = chatroom_member_info["conversation_id"]
            display_name = self.__query_member_name(member_id)

            if not all((member_id, account_id, conversation_id)):
                continue

            chatroom_member = model_im.ChatroomMember()
            chatroom_member.member_id = member_id
            chatroom_member.display_name = display_name
            chatroom_member.account_id = account_id
            chatroom_member.chatroom_id = conversation_id
            self.model_im_col.db_insert_table_chatroom_member(chatroom_member)
        self.model_im_col.db_commit()

    def decode_recover_friend(self):
        if not self.cl_recover_helper.is_valid():
            return
        ts = self.cl_recover_helper.fetch_table("anketa", {
            "about": "Text",
            "birthdate": "Int",
            "city": "Text",
            "country": "Text",
            "emails": "Text",
            "gender": "Int",
            "nickname": "Text",
            "smsNumber": "Text",
            "abContactName": "Text",
        })
        for rec in self.cl_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                friend = model_im.Friend()
                friend.source = self.cl_db_path
                friend.signature = rec["about"].Value
                friend.birthday = self.__convert_timestamp(rec["birthdate"].Value)
                friend.address = rec["country"].Value + rec["city"].Value
                friend.email = rec["emails"].Value
                friend.gender = self.__convert_gender(rec["gender"].Value)
                friend.nickname = rec["nickname"].Value
                friend.telephone = rec["smsNumber"].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                friend.deleted = 1
                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                print("debug error", e)
        self.model_im_col.db_commit()

    def decode_recover_chatroom(self):
        if not self.cl_recover_helper.is_valid():
            return
        ts = self.cl_recover_helper.fetch_table("contact", {
            "_rowid_": "Int",
            "pid": "Text",
            "isTemporary": "Int",
            "displayName": "Text",
            "groupId": "Int",
            "userType": "Int"
        })
        for rec in self.cl_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                if rec["groupId"].Value == 0 or rec["userType"].Value != 5:
                    continue
                chatroom = model_im.Chatroom()
                chatroom.source = self.cl_db_path
                chatroom.account_id, _, chatroom.chatroom_id = rec["pid"].Value.split("|")
                chatroom.name = rec["displayName"].Value
                chatroom.deleted = 1
                self.model_im_col.db_insert_table_chatroom(chatroom)
            except Exception as e:
                print("debug error", e)
        self.model_im_col.db_commit()

    def decode_recover_message(self):
        if not self.message_recover_helper.is_valid():
            return
        ts = self.message_recover_helper.fetch_table("ZMRMESSAGE", {
            "ZHISTORYID": "Int",
            "ZTYPE": "Int",
            "ZTEXT": "Text",
            "ZTIME": "Int",
            "ZWASREAD": "Int",
            "ZPARTICIPANTUID": "Text",
            "ZOUTGOING": "Int",
            "ZFILEID": "Int",
        })
        for rec in self.message_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                message = model_im.Message()
                message.account_id = self.using_account.account_id
                message.source = self.cl_db_path
                message.msg_id = rec["ZHISTORYID"].Value
                message.content = rec["ZTEXT"].Value
                message.send_time = self._get_timestamp(rec["ZTIME"].Value)
                message.is_sender = 1 if rec["ZOUTGOING"].Value == 1 else 0
                message.sender_id = message.account_id if message.is_sender == 1 else rec["ZPARTICIPANTUID"].Value
                message.deleted = 1

                _type = rec["ZTYPE"].Value
                if _type == 510:
                    file_id = rec["ZFILEID"].Value
                    file_info = self.__query_file_info(file_id)
                    if file_info:
                        message.media_path = file_info[0] if not file_info[0] else file_info[2]
                        if file_info[1] is not None:
                            file_type = file_info[1].split("/")[0]
                            if file_type == "image":
                                message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            elif file_type == "audio":
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                            elif file_type == "video":
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                            else:
                                message.type = None
                        else:
                            message.type = self.__convert_message_content_type(_type)

                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                print("debug error", e)
        self.model_im_col.db_commit()

    def __query_file_info(self, file_id):
        with self.files_db_col as db_col:
            sql = """SELECT file_id,
                            content_url,
                            filename,
                            mimetype,
                            storage_content_filename,
                            url
                    FROM file
                    WHERE file_id = {};""".format(file_id)
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    content_url = db_col.get_string(1)
                    mime_type = db_col.get_string(3)
                    url = db_col.get_string(5)
                    storage_content_filename = db_col.get_string(4)
                    return (
                        content_url,
                        mime_type,
                        url,
                        storage_content_filename
                    )
                except Exception as e:
                    print("debug error", e)

    def __query_chatroom_list(self):
        with self.agent_db_col as db_col:
            sql = """SELECT DISTINCT ZMRMESSAGE.ZPARTICIPANTUID, 
                            ZMRCONVERSATION.Z_PK, 
                            ZMRCONVERSATION.ZPID
                        FROM ZMRMESSAGE left join ZMRCONVERSATION
                        on ZMRMESSAGE.ZCONVERSATION = ZMRCONVERSATION.Z_PK
                        WHERE ZMRMESSAGE.ZPARTICIPANTUID is not null;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    member_id = db_col.get_string(0)
                    account_id, _, conversation_id = db_col.get_string(2).split("|")
                    yield {
                        "member_id": member_id,
                        "account_id": account_id,
                        "conversation_id": conversation_id,
                    }
                except Exception as e:
                    print("debug error", e)

    def __query_member_name(self, member_id):
        with self.cl_db_col as db_col:
            sql = """SELECT displayName
                        FROM contact
                        WHERE uid = '{}';""".format(member_id)
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    display_name = db_col.get_string(0)
                    return display_name
                except Exception as e:
                    print("debug error", e)

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
        try:
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
        except Exception as e:
            print("__convert_timestamp error", ts)
            print(e)
            return None

    @staticmethod
    def __convert_message_content_type(type):
        if type == 100:
            return model_im.MESSAGE_CONTENT_TYPE_TEXT
        elif type == 719:
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        elif type == 709:
            return model_im.MESSAGE_CONTENT_TYPE_EMOJI
        elif type == 710:
            return model_im.MESSAGE_CONTENT_TYPE_VOIP
        else:
            return None

    def _get_timestamp(self, timestamp):
        try:
            dstart = DateTime(1970, 1, 1, 0, 0, 0)
            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(timestamp)
            return ((cdate - dstart).TotalSeconds)
        except Exception as e:
            return None

    def parse(self):
        """解析的主函数"""

        # 获取缓存数据
        self._get_account_table()
        self._get_friend_table()
        self._get_message_table()
        self._get_chatroom_table()
        self._get_chatroom_member_table()
        self.decode_recover_friend()
        self.decode_recover_message()
        self.decode_recover_chatroom()

        generate = model_im.GenerateModel(self.cache_db)
        results = generate.get_models()

        return results


def analyze_icq(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.ICQ
    results = ICQParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("ICQ")
    return pr
