# -*- coding: utf-8 -*-
import hashlib
import os

__author__ = "TaoJianping"

from PA_runtime import *
import PA_runtime
import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
clr.AddReference('TelegramDecoder')
try:
    clr.AddReference('model_im')
except:
    pass
del clr

import System
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from System.Data.SQLite import *
from com.telegram.decoder import *
import model_im
import json

MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT = 1
MESSAGE_STATUS_SENT = 2
MESSAGE_STATUS_UNREAD = 3
MESSAGE_STATUS_READ = 4

CONTACT_ACCOUNT_TYPE_IM_TELEGRAM = '1030063'


def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""


def GetInt64(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0


def GetBlob(reader, idx):
    return reader.GetValue(idx) if not reader.IsDBNull(idx) else None


def GetFloat(reader, idx):
    return reader.GetFloat(idx) if not reader.IsDBNull(idx) else 0


class TelegramDecodeHelper(object):
    """
    TelegramDecoder插件中的类方法进行简单的封装
    decode_user：解析users表中的data字段
    decode_message：解析messages表中的data字段
    decode_account：解析userconfing.xml文件中的对象
    """

    @staticmethod
    def decode_user(byte_data):
        """
        解析users表中的data字段
        :param byte_data: Array[bytes]
        :return: dict
        """
        try:
            user = DecoderReader.decodeUser(byte_data)
            return user
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def decode_message(byte_data):
        """
        解析messages表中的data字段
        :param byte_data: Array[bytes]
        :return: dict
        """
        try:
            res = DecoderReader.decodeMessage(byte_data)
            return res
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def decode_account(user_str):
        """
        解析userconfing.xml文件中的对象的字符串
        :param user_str: (str)
        :return:
        """
        try:
            account = DecoderReader.decodeAccount(user_str)
            return account
        except Exception as e:
            print(e)
            return None


class Telegram(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.account = None
        self.account_config_path = None
        self.account_db_path = None
        self.cache_db = self.__get_cache_db()
        self.model_col = model_im.IM()
        if self.root.GetByPath(r"/cache4.db"):
            self.model_col.db_create(self.cache_db)

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath("Telegram")
        m = hashlib.md5()
        m.update(self.root.AbsolutePath)
        return os.path.join(self.cache_path, (m.hexdigest().upper() + ".db"))

    def __get_con(self):
        """获取应用缓存的db"""
        tg_node = self.root.GetByPath(self.account_db_path)
        if tg_node is None:
            return
        return System.Data.SQLite.SQLiteConnection(
            'Data Source = {}; Readonly = True'.format(tg_node.PathWithMountPoint))

    def __change_account_config(self):
        """telegram可以有三个account,因此这个函数可以切换account的配置"""
        account_config_path_list = (
            r"/shared_prefs/userconfing.xml",
            r"/shared_prefs/userconfig1.xml",
            r"/shared_prefs/userconfig2.xml",
        )
        account_data_db_list = (
            r"/cache4.db",
            r"/account1/cache4.db",
            r"/account2/cache4.db",
        )
        for config in zip(account_config_path_list, account_data_db_list):
            self.account_config_path = config[0]
            self.account_db_path = config[1]
            yield

    def __find_account(self):
        user_node = self.root.Parent.GetByPath(self.account_config_path)
        if user_node is None:
            return
        es = []
        try:
            user_node.Data.seek(0)
            xml = XElement.Parse(user_node.read())
            es = xml.Elements("string")
        except Exception as e:
            print(e)
        for rec in es:
            if rec.Attribute("name") and rec.Attribute("name").Value == "user":
                user_id = rec.FirstNode.Value[:64]
                account_info = TelegramDecodeHelper.decode_account(user_id)
                return account_info

    def __query_user_info(self, user_id):
        """
        根据user_id查找用户的资料
        :param user_id: 用户id
        :return: (dict)
        """
        # TODO 有机会可以优化
        conn = self.__get_con()
        conn.Open()
        cmd = System.Data.SQLite.SQLiteCommand(conn)
        cmd.CommandText = """select data
                            from users where uid={}""".format(user_id)
        reader = cmd.ExecuteReader()
        friend_info = None
        while reader.Read():
            friend_info = TelegramDecodeHelper.decode_user(GetBlob(reader, 0))
        cmd.Dispose()
        reader.Close()
        conn.Close()
        return friend_info

    def _get_account(self):
        account_info = self.__find_account()
        if not account_info:
            return
        account = model_im.Account()
        account.account_id = account_info.id
        first_name = account_info.first_name if account_info.first_name else ""
        last_name = account_info.last_name if account_info.last_name else ""
        account.username = first_name + " " + last_name
        account.telephone = account_info.phone
        account.deleted = 0 if account_info.deleted is False else 1
        account.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint

        self.account = account.account_id
        self.model_col.db_insert_table_account(account)
        self.model_col.db_commit()
        return account.account_id

    def _get_friends(self):
        conn = self.__get_con()
        conn.Open()
        cmd = System.Data.SQLite.SQLiteCommand(conn)
        cmd.CommandText = """select uid,
                                    name,
                                    status,
                                    data
                            from users"""
        reader = cmd.ExecuteReader()
        while reader.Read():
            try:
                friend = model_im.Friend()
                friend.friend_id = GetInt64(reader, 0)
                friend.account_id = self.account
                friend.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                friend_info = TelegramDecodeHelper.decode_user(GetBlob(reader, 3))
                if friend_info:
                    friend.deleted = 0 if friend_info.deleted is False else 1
                    friend.telephone = friend_info.phone
                    first_name = friend_info.first_name if friend_info.first_name else ""
                    last_name = friend_info.last_name if friend_info.last_name else ""
                    friend.nickname = first_name + " " + last_name
                self.model_col.db_insert_table_friend(friend)
            except Exception as e:
                print(e)
        reader.Close()
        cmd.Dispose()
        conn.Close()
        self.model_col.db_commit()

    def _get_messages(self):
        conn = self.__get_con()
        conn.Open()
        cmd = System.Data.SQLite.SQLiteCommand(conn)
        cmd.CommandText = """select messages.mid,
                                    messages.uid,
                                    chats.name,
                                    messages.read_state,
                                    messages.send_state,
                                    messages.date,
                                    messages.data,
                                    messages.out,
                                    messages.ttl,
                                    messages.media,
                                    messages.replydata,
                                    messages.imp,
                                    messages.mention
                            from messages left join chats on abs(messages.uid) = abs(chats.uid)"""
        reader = cmd.ExecuteReader()
        while reader.Read():
            try:
                message = model_im.Message()
                message.account_id = self.account
                message.talker_id = GetInt64(reader, 1)
                message.talker_name = GetString(reader, 2)
                message.msg_id = GetInt64(reader, 0)
                message.send_time = GetInt64(reader, 5)
                message.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                # 下面的这些通过TelegramDecoder解码
                message_info = TelegramDecodeHelper.decode_message(GetBlob(reader, 6))
                if message_info:
                    message.sender_id = message_info.from_id
                    message.is_sender = 1 if self.account == message.sender_id else 0
                    sender_info = self.__query_user_info(message.sender_id)
                    if sender_info is not None:
                        first_name = sender_info.first_name if sender_info.first_name else ""
                        last_name = sender_info.last_name if sender_info.last_name else ""
                        message.sender_name = first_name + " " + last_name
                    message.content = message_info.message
                    message.status = MESSAGE_STATUS_READ if message_info.unread is False else MESSAGE_STATUS_UNREAD
                self.model_col.db_insert_table_message(message)
            except Exception as e:
                print(e)
        cmd.Dispose()
        reader.Close()
        conn.Close()
        self.model_col.db_commit()

    def _get_chatroom(self):
        conn = self.__get_con()
        conn.Open()
        cmd = System.Data.SQLite.SQLiteCommand(conn)
        cmd.CommandText = """select uid,
                                    name,
                                    data
                            from chats"""
        reader = cmd.ExecuteReader()
        while reader.Read():
            try:
                chat_room = model_im.Chatroom()
                chat_room.account_id = self.account
                chat_room.chatroom_id = GetInt64(reader, 0)
                chat_room.name = GetString(reader, 1)
                chat_room.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                # 没有找到解析chats表data的decoder类
                self.model_col.db_insert_table_chatroom(chat_room)
            except Exception as e:
                print(e)
        cmd.Dispose()
        reader.Close()
        conn.Close()
        self.model_col.db_commit()

    def _get_chatroom_member(self):
        conn = self.__get_con()
        conn.Open()
        cmd = System.Data.SQLite.SQLiteCommand(conn)
        cmd.CommandText = """select channel_users_v2.did,
                                    channel_users_v2.uid,
                                    channel_users_v2.date,
                                    channel_users_v2.data,
                                    users.data
                            from channel_users_v2 left join users on abs(channel_users_v2.uid) = abs(users.uid)"""
        reader = cmd.ExecuteReader()
        while reader.Read():
            try:
                chatroom_member = model_im.ChatroomMember()
                chatroom_member.account_id = self.account
                chatroom_member.chatroom_id = GetInt64(reader, 0)
                chatroom_member.member_id = GetInt64(reader, 1)
                chatroom_member.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                user_info = TelegramDecodeHelper.decode_user(GetBlob(reader, 4))
                if user_info:
                    chatroom_member.deleted = 0 if user_info.deleted is False else 1
                    chatroom_member.telephone = user_info.phone
                    first_name = user_info.first_name if user_info.first_name else ""
                    last_name = user_info.last_name if user_info.last_name else ""
                    chatroom_member.display_name = first_name + " " + last_name
                self.model_col.db_insert_table_chatroom_member(chatroom_member)
            except Exception as e:
                print(e)
        cmd.Dispose()
        reader.Close()
        conn.Close()
        self.model_col.db_commit()

    def _add_unknown_resource(self):
        """
        因为无法找到本地文件缓存和message的对应方式，暂时以unknow的形式添加进去
        :return:
        """
        resource_path = self.root.PathWithMountPoint
        dir_name = os.path.dirname(os.path.dirname(os.path.dirname(resource_path)))
        resource_path = os.path.join(dir_name, "storage", "emulated", "0", "Telegram")
        file_dir_list = os.listdir(resource_path)
        for file_dir in file_dir_list:
            if file_dir == "Telegram Audio":
                file_type = model_im.MESSAGE_CONTENT_TYPE_VOICE
            elif file_dir == "Telegram Images":
                file_type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
            elif file_dir == "Telegram Video":
                file_type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
            else:
                file_type = model_im.MESSAGE_CONTENT_TYPE_ATTACHMENT
            file_list = os.listdir(os.path.join(resource_path, file_dir))
            file_list.remove(".nomedia")
            for _file in file_list:
                file_path = os.path.join(resource_path, file_dir, _file)
                message = model_im.Message()
                message.media_path = file_path
                message.type = file_type
                message.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                self.model_col.db_insert_table_message(message)
        self.model_col.db_commit()

    def decode_recover_messages(self):
        node = self.root.GetByPath(self.account_db_path)
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = 'messages'
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, "data", SQLiteParser.FieldType.Text,
                                               SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                message = model_im.Message()
                message.account_id = self.account
                message.msg_id = rec["mid"].Value
                message.send_time = rec["date"].Value
                message.talker_id = rec["uid"].Value
                message.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                talker_info = self.__query_user_info(message.talker_id)
                if talker_info is not None:
                    first_name = talker_info.first_name if talker_info.first_name else ""
                    last_name = talker_info.last_name if talker_info.last_name else ""
                    message.talker_name = first_name + " " + last_name
                # 下面的这些通过TelegramDecoder解码
                message_info = TelegramDecodeHelper.decode_message(rec["data"].Value)
                if message_info:
                    message.sender_id = message_info.from_id
                    sender_info = self.__query_user_info(message.sender_id)
                    if sender_info is not None:
                        first_name = sender_info.first_name if sender_info.first_name else ""
                        last_name = sender_info.last_name if sender_info.last_name else ""
                        message.sender_name = first_name + " " + last_name
                    message.content = message_info.message
                    message.status = MESSAGE_STATUS_READ if message_info.unread is False else MESSAGE_STATUS_UNREAD
                self.model_col.db_insert_table_message(message)
            except Exception as e:
                print("error happen", e)
        self.model_col.db_commit()

    def decode_recover_friends(self):
        node = self.root.GetByPath(self.account_db_path)
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = 'users'
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Int,
                                               SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                friend = model_im.Friend()
                data = rec['data'].Value
                name = rec['name'].Value
                friend.account_id = self.account
                friend.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                if name:
                    friend.nickname = name.replace(";;;", " ")
                friend.friend_id = rec['uid'].Value
                friend_info = TelegramDecodeHelper.decode_user(data)
                if friend_info is not None:
                    friend.deleted = 0 if friend_info.deleted is False else 1
                    friend.telephone = friend_info.phone
                    first_name = friend_info.first_name if friend_info.first_name else ""
                    last_name = friend_info.last_name if friend_info.last_name else ""
                    friend.nickname = first_name + " " + last_name
                self.model_col.db_insert_table_friend(friend)

            except Exception as e:
                print("error happen", e)
        self.model_col.db_commit()

    def decode_recover_chatroom(self):
        node = self.root.GetByPath(self.account_db_path)
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = 'chats'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                chat_room = model_im.Chatroom()
                chat_room.account_id = self.account
                chat_room.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                chat_room.chatroom_id = rec['uid'].Value
                chat_room.name = rec['name'].Value
                # 没有找到解析chats表data的decoder类
                self.model_col.db_insert_table_chatroom(chat_room)
            except Exception as e:
                print("error happen", e)
        self.model_col.db_commit()

    def decode_recover_chatroom_member(self):
        node = self.root.GetByPath(self.account_db_path)
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = 'channel_users_v2'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                chatroom_member = model_im.ChatroomMember()
                chatroom_member.account_id = self.account
                chatroom_member.source = self.root.GetByPath(self.account_db_path).PathWithMountPoint
                chatroom_member.chatroom_id = rec['did'].Value
                chatroom_member.member_id = rec['uid'].Value
                chatroom_member_data = rec['data'].Value
                if chatroom_member_data:
                    user_info = TelegramDecodeHelper.decode_user(chatroom_member_data)
                    chatroom_member.deleted = 0 if user_info.deleted is False else 1
                    chatroom_member.telephone = user_info.phone
                    first_name = user_info.first_name if user_info.first_name else ""
                    last_name = user_info.last_name if user_info.last_name else ""
                    chatroom_member.display_name = first_name + " " + last_name
                self.model_col.db_insert_table_chatroom_member(chatroom_member)
            except Exception as e:
                print("error happen", e)
        self.model_col.db_commit()

    def parse(self):
        """解析的主函数"""
        for _ in self.__change_account_config():
            account = self._get_account()
            if account:
                self._get_friends()
                self._get_messages()
                self._get_chatroom()
                self._get_chatroom_member()
                self.decode_recover_chatroom()
                self.decode_recover_chatroom_member()
                self.decode_recover_friends()
                self.decode_recover_messages()
        try:
            self._add_unknown_resource()
        except Exception as e:
            print("error happen", e)
        generate = model_im.GenerateModel(self.cache_db)
        results = generate.get_models()

        return results


def parse_telegram(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.Telegram
    results = Telegram(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Telegram")
    return pr
