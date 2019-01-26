# -*- coding: utf-8 -*-
import json

__author__ = "TaoJianping"
import clr

try:
    clr.AddReference('System.Core')
    clr.AddReference('System.Xml.Linq')
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('model_im')
except:
    pass
del clr
import hashlib
import codecs

import xml.etree.ElementTree as ET
import model_im
import PA_runtime
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions

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
    def __init__(self, node):
        self.db = SQLiteParser.Database.FromNode(node, canceller)
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


class ICQParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root.Parent.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.model_im_col = model_im.IM()
        self.cache_db = self.__get_cache_db()
        self.account_info_path, self.user_info_path, self.icq_data_path = self.__init_data_path()
        if all((self.user_info_path, self.icq_data_path)):
            self.model_im_col.db_create(self.cache_db)
        self.icq_db_col = ColHelper(self.icq_data_path)
        self.recover_helper = RecoverTableHelper(self.root.GetByPath("databases/agent-dao"))
        self.using_account = None
        self.using_account_name = None

    def __init_data_path(self):
        account_info_node = self.root.GetByPath(r"shared_prefs/AccountPrefs.xml")
        user_info_node = self.root.GetByPath(r"shared_prefs/SummaryPrefs.xml")
        icq_data_node = self.root.GetByPath(r"databases/agent-dao")
        if all((account_info_node, user_info_node, icq_data_node)):
            return (
                account_info_node.PathWithMountPoint,
                user_info_node.PathWithMountPoint,
                icq_data_node.PathWithMountPoint
            )
        else:
            return None, None, None

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath("ICQ")
        m = hashlib.md5()
        m.update(Encoding.UT8.GetBytes(self.root.AbsolutePath))
        return os.path.join(self.cache_path, m.hexdigest().upper())

    def _get_account_table(self):
        account = model_im.Account()
        account.source = self.account_info_path

        account_info_tree = ET.parse(self.account_info_path)
        account_root = account_info_tree.getroot()
        for child_node in account_root:
            if child_node.attrib.get("name") == "uin":
                account.account_id = child_node.text
            elif child_node.attrib.get("name") == "attachedPhone":
                account.telephone = child_node.text
            else:
                pass

        user_node = self.root.GetByPath("shared_prefs/SummaryPrefs.xml")
        if user_node is None:
            return
        es = []
        long_es = []
        try:
            data = self.__open_xml_file(user_node.PathWithMountPoint)
            user_node.Data.seek(0)
            xml = XElement.Parse(data)
            es = xml.Elements("string")
            long_es = xml.Elements("long")
        except Exception as e:
            print(e)
        for rec in es:
            if not rec.FirstNode:
                continue
            if rec.Attribute("name") and rec.Attribute("name").Value == "country":
                account.country = rec.FirstNode.Value
            elif rec.Attribute("name") and rec.Attribute("name").Value == "city":
                account.city = rec.FirstNode.Value
            elif rec.Attribute("name") and rec.Attribute("name").Value == "gender":
                account.gender = self.__convert_gender(rec.FirstNode.Value)
            elif rec.Attribute("name") and rec.Attribute("name").Value == "birthDate":
                ts = int(rec.Attribute("value").Value) / 1000
                account.birthday = ts
            elif rec.Attribute("name") and rec.Attribute("name").Value == "about":
                print(dir(rec.FirstNode))
                account.signature = rec.FirstNode.Value
            elif rec.Attribute("name") and rec.Attribute("name").Value == "nickname":
                account.nickname = rec.FirstNode.Value
                self.using_account_name = account.nickname
            else:
                print(rec.Attribute("name").Value)
                pass

        for rec in long_es:
            if rec.Attribute("name") and rec.Attribute("name").Value == "birthDate":
                ts = int(rec.Attribute("value").Value) / 1000
                account.birthday = ts

        self.using_account = account.account_id

        self.model_im_col.db_insert_table_account(account)
        self.model_im_col.db_commit()

    def _get_friend_table(self):
        with self.icq_db_col as db_col:
            sql = """select PERSON.SN,
                            PERSON.NAME,
                            ICQ_CONTACT_DATA.LAST_USED_PHONE,
                            ICQ_CONTACT_DATA.PROFILE_ID
                            from PERSON left join ICQ_CONTACT_DATA 
                            on PERSON.SN = ICQ_CONTACT_DATA.CONTACT_ID;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend = model_im.Friend()
                    friend.source = self.icq_data_path
                    friend.account_id = db_col.get_string(3)
                    friend.friend_id = db_col.get_string(0)
                    friend.nickname = db_col.get_string(1)
                    friend.telephone = db_col.get_int64(2)
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

    def _get_chatroom_table(self):
        with self.icq_db_col as db_col:
            sql = """SELECT ICQ_CONTACT_DATA.PROFILE_ID,
                            ICQ_CONTACT_DATA.CONTACT_ID,
                            ICQ_CONTACT_DATA.NAME,
                            CHAT_INFO.ABOUT,
                            CHAT_INFO.MEMBERS_COUNT
                    FROM ICQ_CONTACT_DATA LEFT JOIN CHAT_INFO 
                    on ICQ_CONTACT_DATA.CHAT_INFO_ID = CHAT_INFO._id
                    where ICQ_CONTACT_DATA.CONTACT_ID like '%@%';"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    chatroom = model_im.Chatroom()
                    chatroom.source = self.icq_data_path
                    chatroom.account_id = db_col.get_string(0)
                    chatroom.chatroom_id = db_col.get_string(1)
                    chatroom.name = db_col.get_string(2)
                    chatroom.description = db_col.get_string(3)
                    chatroom.member_count = db_col.get_int64(4)
                    self.model_im_col.db_insert_table_chatroom(chatroom)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

    def _get_message_table(self):
        with self.icq_db_col as db_col:
            sql = """SELECT MESSAGE_DATA.PROFILE_ID,
                            MESSAGE_DATA.CONTACT_ID,
                            MESSAGE_DATA.TYPE,
                            MESSAGE_DATA.CONTENT,
                            MESSAGE_DATA.TIMESTAMP,
                            MESSAGE_DATA.SENDER,
                            MESSAGE_DATA.HISTORY_ID,
                            GALLERY_ENTRY_DATA.URL,
                            ICQ_CONTACT_DATA.NAME
                    FROM MESSAGE_DATA 
                    left join GALLERY_ENTRY_DATA 
                    on MESSAGE_DATA.HISTORY_ID = GALLERY_ENTRY_DATA.MESSAGE_ID
                    left join ICQ_CONTACT_DATA
                    on MESSAGE_DATA.CONTACT_ID = ICQ_CONTACT_DATA.CONTACT_ID"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    message = model_im.Message()
                    message.source = self.icq_data_path
                    message.account_id = db_col.get_string(0)
                    message.talker_id = db_col.get_string(1)
                    message.sender_id = db_col.get_string(5)
                    message.msg_id = db_col.get_int64(6)
                    message.type = self.__convert_message_content_type(db_col.get_int64(2))
                    message.content = db_col.get_string(3)
                    message.send_time = self.__convert_timestamp(db_col.get_int64(4))
                    message.media_path = db_col.get_string(7)
                    message.sender_name = db_col.get_string(8)
                    message.is_sender = 1 if message.account_id == message.sender_id else 0

                    if message.type in (
                        model_im.MESSAGE_CONTENT_TYPE_IMAGE,
                        model_im.MESSAGE_CONTENT_TYPE_VIDEO,
                        model_im.MESSAGE_CONTENT_TYPE_VOICE,
                    ):
                        if message.media_path:
                            message.content = message.media_path

                    if message.type == model_im.MESSAGE_CONTENT_TYPE_VOIP:
                        caller = message.sender_name if message.sender_name else message.sender_id
                        responser = "您"

                        if message.is_sender == 1:
                            caller, responser = responser, caller

                        call_info = json.loads(message.content)
                        duration_time = call_info['duration']
                        videocall = call_info['videocall']

                        message.content = "{caller}呼叫了{responser} \n 持续运行了{s}秒".format(
                            caller=caller,
                            responser=responser,
                            s=duration_time,
                        )
                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

    def _get_search_table(self):
        with self.icq_db_col as db_col:
            sql = """SELECT _id,
                            KEY_WORD
                    FROM SEARCH_QUERY_PROMT"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                search = model_im.Search()
                search.key = db_col.get_string(1)
                search.source = self.icq_data_path
                self.model_im_col.db_insert_table_search(search)
            self.model_im_col.db_commit()

    def decode_recover_friend(self):
        node = self.root.GetByPath("databases/agent-dao")
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = 'PERSON'
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, "SN", SQLiteParser.FieldType.Text,
                                               SQLiteParser.FieldConstraints.NotNull)
        SQLiteParser.Tools.AddSignatureToTable(ts, "NAME", SQLiteParser.FieldType.Text,
                                               SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableDeletedRecords(ts, False):
            try:
                friend = model_im.Friend()
                friend.account_id = self.using_account
                friend.source = self.icq_data_path
                friend.friend_id = rec["SN"].Value
                friend.nickname = rec["NAME"].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def decode_recover_message(self):
        if not self.recover_helper.is_valid():
            return
        ts = self.recover_helper.fetch_table("MESSAGE_DATA", {
            "PROFILE_ID": "Text",
            "CONTACT_ID": "Text",
            "TYPE": "Int",
            "CONTENT": "Text",
            "TIMESTAMP": "Int",
            "SENDER": "Text",
            "HISTORY_ID": "Int",
        })
        for rec in self.recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                message = model_im.Message()
                message.account_id = rec["PROFILE_ID"].Value
                message.talker_id = rec["CONTACT_ID"].Value
                message.sender_id = rec["SENDER"].Value
                message.msg_id = rec["HISTORY_ID"].Value
                message.send_time = self.__convert_timestamp(rec["TIMESTAMP"].Value)
                message.source = self.icq_data_path
                message.type = self.__convert_message_content_type(rec["TYPE"].Value)
                message.content = rec["CONTENT"].Value
                message.is_sender = 1 if message.account_id == message.sender_id else 0
                message.deleted = 1

                if message.type == model_im.MESSAGE_CONTENT_TYPE_VOIP:
                    caller = message.sender_name if message.sender_name else message.sender_id
                    responser = "您"

                    if message.is_sender == 1:
                        caller, responser = responser, caller

                    call_info = json.loads(message.content)
                    duration_time = call_info['duration']
                    videocall = call_info['videocall']

                    message.content = "{caller}呼叫了{responser} \n 持续运行了{s}秒".format(
                        caller=caller,
                        responser=responser,
                        s=duration_time,
                    )

                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def decode_recover_search(self):
        if not self.recover_helper.is_valid():
            return
        ts = self.recover_helper.fetch_table("SEARCH_QUERY_PROMT", {
            "KEY_WORD": "Text",
        })
        for rec in self.recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                search = model_im.Search()
                search.key = rec["KEY_WORD"].Value
                search.source = self.icq_data_path
                self.model_im_col.db_insert_table_search(search)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def decode_recover_chatroom(self):
        if not self.recover_helper.is_valid():
            return
        ts = self.recover_helper.fetch_table("ICQ_CONTACT_DATA", {
            "PROFILE_ID": "Text",
            "CONTACT_ID": "Text",
            "NAME": "Text",
            "CHAT_INFO_ID": "Int",
        })
        for rec in self.recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                name = rec["CONTACT_ID"].Value
                if "@" in name:
                    chatroom = model_im.Chatroom()
                    chatroom.account_id = rec["PROFILE_ID"].Value
                    chatroom.name = name
                    chatroom.chatroom_id = rec["CONTACT_ID"].Value
                    chatroom.source = self.icq_data_path
                    chatroom.deleted = 1
                    chat_info_id = rec["CHAT_INFO_ID"].Value
                    if chat_info_id:
                        _, chatroom.description, chatroom.member_count = self.__query_chatroom_info(chat_info_id)
                    self.model_im_col.db_insert_table_chatroom(chatroom)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def __query_chatroom_info(self, chat_info_id):
        with self.icq_db_col as db_col:
            sql = """SELECT _id,
                            ABOUT,
                            MEMBERS_COUNT
                        FROM CHAT_INFO 
                        WHERE _id = {chat_info_id}""".format(chat_info_id=chat_info_id)
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    return (
                        db_col.get_int64(0),
                        db_col.get_string(1),
                        db_col.get_int64(2)
                    )
                except Exception as e:
                    print("debug error", e)
            return None, None, None

    @staticmethod
    def __convert_gender(gender):
        if gender == "MALE":
            return model_im.GENDER_MALE
        elif gender == "FEMALE":
            return model_im.GENDER_FEMALE
        else:
            return model_im.GENDER_NONE

    @staticmethod
    def __convert_timestamp(ts):
        if isinstance(ts, str):
            return ts[:-3]
        elif isinstance(ts, int):
            ts = str(ts)[:-3]
            return int(ts)
        else:
            ts = str(ts)[:-3]
            return int(ts)

    @staticmethod
    def __convert_message_content_type(type):
        if type == 1:
            return model_im.MESSAGE_CONTENT_TYPE_TEXT
        elif type == 9:
            return model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        elif type == 8:
            return model_im.MESSAGE_CONTENT_TYPE_EMOJI
        elif type == 5:
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif type == 6:
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif type == 20:
            return model_im.MESSAGE_CONTENT_TYPE_LINK
        elif type == 17:
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif type == 4:
            return model_im.MESSAGE_CONTENT_TYPE_VOIP
        else:
            return None

    @staticmethod
    def __open_xml_file(file_path):
        with codecs.open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

    def parse(self):
        """解析的主函数"""

        if not all((self.account_info_path, self.user_info_path, self.icq_data_path)):
            return

        # 获取缓存数据
        self._get_account_table()
        self._get_friend_table()
        self._get_message_table()
        self._get_chatroom_table()
        self._get_search_table()
        self.decode_recover_friend()
        self.decode_recover_message()
        self.decode_recover_search()
        self.decode_recover_chatroom()

        generate = model_im.GenerateModel(self.cache_db)
        results = generate.get_models()

        return results


def parse_icq(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.ICQ
    results = ICQParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("ICQ")
    return pr
