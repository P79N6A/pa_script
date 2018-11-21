# -*- coding: utf-8 -*-
__author__ = "TaoJianping"


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

try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
except:
    pass

del clr

import model_im
import model_nd
import PA_runtime
import re
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions


# CONST
BaiduNetDisk_VERSION = 1


class ColBase(object):
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
            return False

    @staticmethod
    def convert_timestamp(ts):
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


class BaiduNetDiskParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache_db = self.__get_cache_db()
        self.has_data = self.__copy_db_file()

        self.model_nd_col = model_nd.NetDisk(self.cache_db)
        self.model_im_col = self.model_nd_col.im
        self.account_col = self.__fetch_account_col()

        self.cloud_p2p_col = None
        self.file_list_col = None
        self.c_recover_helper = None
        self.f_recover_helper = None

        self.using_account = None

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath("BaiduNetDisk")
        m = hashlib.md5()
        m.update(self.root.AbsolutePath)
        return os.path.join(self.cache_path, m.hexdigest().upper())

    def __copy_db_file(self):
        if not self.root.GetByPath("databases"):
            return False
        old_dir = self.root.GetByPath("databases").PathWithMountPoint
        new_dir = os.path.join(self.cache_path, "tmp")
        Utils.copy_dir(old_dir, new_dir)
        return True

    def __fetch_account_col(self):
        path = os.path.join(self.cache_path, "tmp", "account.db")
        return ColBase(path)

    def __fetch_cols(self, account):
        """获取要用到的几个数据库"""
        prefix = account.account_id

        cloud_p2p_db = prefix + "cloudp2p"
        cloud_p2p_path = os.path.join(self.cache_path, "tmp", "{}.db".format(cloud_p2p_db))
        cloud_p2p_col = ColBase(cloud_p2p_path)

        filelist_db = prefix + "filelist"
        filelist_path = os.path.join(self.cache_path, "tmp", "{}.db".format(filelist_db))
        file_list_col = ColBase(filelist_path)

        return cloud_p2p_col, file_list_col

    def __fetch_recover_helper(self, account):
        prefix = account.account_id

        cloud_p2p_db = prefix + "cloudp2p.db"
        cloud_p2p_node = self.root.GetByPath(r'/databases/{}'.format(cloud_p2p_db))
        c_recover_helper = RecoverTableHelper(cloud_p2p_node)

        filelist_db = prefix + "filelist.db"
        filelist_node = self.root.GetByPath(r'/databases/{}'.format(filelist_db))
        f_recover_helper = RecoverTableHelper(filelist_node)

        return c_recover_helper, f_recover_helper

    def _generate_account_table(self):
        with self.account_col as db_col:
            sql = """SELECT account_uid, 
                            account_name, 
                            account_phone, 
                            account_email, 
                            uk, 
                            name, 
                            nick_name, 
                            intro, 
                            avatar_url, 
                            display_name, 
                            remark
                    FROM info;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    account = model_im.Account()

                    account.account_id = db_col.get_string(0)
                    account.username = db_col.get_string(1)
                    account.nickname = db_col.get_string(6)
                    account.telephone = db_col.get_string(2)
                    account.email = db_col.get_string(3)
                    account.photo = db_col.get_string(8)
                    account.uk = db_col.get_int64(4)

                    self.using_account = account
                    self.model_im_col.db_insert_table_account(account)
                    yield account
                except Exception as e:
                    pass
            self.model_im_col.db_commit()

    def _generate_friend_table(self):
        with self.cloud_p2p_col as db_col:
            sql = """SELECT uk, 
                            source, 
                            name, 
                            uname, 
                            nick_name, 
                            remark, 
                            avatar_url, 
                            intro, 
                            third
                    FROM v_followlist;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend = model_im.Friend()

                    friend.account_id = self.using_account.account_id
                    friend.photo = db_col.get_string(6)
                    friend.nickname = db_col.get_string(4)
                    friend.friend_id = db_col.get_int64(0)
                    friend.type = model_im.FRIEND_TYPE_FOLLOW if db_col.get_int64(1) == 2 else model_im.FRIEND_TYPE_FRIEND

                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    pass
            self.model_im_col.db_commit()

    def _generate_chatroom_table(self):
        with self.cloud_p2p_col as db_col:
            sql = """SELECT v_groups_detail.name, 
                            v_groups_detail.desc, 
                            v_groups_detail.announce, 
                            v_groups_detail.type, 
                            v_groups_detail.ctime, 
                            v_groups_detail.create_uk, 
                            v_groups_detail.people_limit,
                            v_groups_detail.group_id, 
                            v_groups_detail.is_banded, 
                            count(v_groups_people.uk) 
                    FROM v_groups_detail left join v_groups_people 
                    on v_groups_detail.group_id = v_groups_people.group_id 
                    group by v_groups_detail.group_id;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    chatroom = model_im.Chatroom()

                    chatroom.chatroom_id = db_col.get_int64(7)
                    chatroom.account_id = self.using_account.account_id
                    chatroom.name = db_col.get_string(0)
                    chatroom.member_count = db_col.get_int64(9)
                    chatroom.max_member_count = db_col.get_int64(6)
                    chatroom.create_time = db_col.get_int64(4)
                    chatroom.creator_id = db_col.get_int64(5)
                    chatroom.source = db_col.db_path
                    chatroom.description = db_col.get_string(1)
                    chatroom.notice = db_col.get_string(2)

                    self.model_im_col.db_insert_table_chatroom(chatroom)
                except Exception as e:
                    pass

            self.model_im_col.db_commit()

    def __insert_people_messages(self):
        with self.cloud_p2p_col as db_col:
            sql = """SELECT conversation_uk, 
                            uk, 
                            name, 
                            avatar_url, 
                            msg_id, 
                            msg_content, 
                            msg_type, 
                            ctime, 
                            status, 
                            file_status, 
                            send_state, 
                            files_count, 
                            path, 
                            server_filename, 
                            server_mtime, 
                            thumbnail_url, 
                            server_ctime,
                            category, 
                            image_prev_url2 
                    FROM v_people_messages;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    message = model_im.Message()
                    message.account_id = self.using_account.account_id
                    message.source = db_col.db_path
                    message.sender_id = db_col.get_int64(1)
                    message.sender_name = db_col.get_string(2)
                    message.talker_id = db_col.get_int64(0)
                    message.msg_id = db_col.get_int64(4)
                    message.content = db_col.get_string(5)
                    message.send_time = Utils.convert_timestamp(db_col.get_int64(7))
                    message.is_sender = 1 if message.sender_id == self.using_account.uk else 0
                    message.talker_type = model_im.CHAT_TYPE_FRIEND

                    if db_col.get_int64(11) > 0:
                        self.__add_media_path(message, db_col.get_string(13))

                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    pass
            self.model_im_col.db_commit()

    def __insert_chatroom_messages(self):
        with self.cloud_p2p_col as db_col:
            sql = """SELECT group_id, 
                            uk, 
                            name, 
                            avatar_url, 
                            msg_id, 
                            msg_content, 
                            msg_type, 
                            ctime, 
                            status, 
                            file_status, 
                            send_state, 
                            files_count, 
                            path, 
                            server_filename, 
                            server_mtime, 
                            thumbnail_url, 
                            server_ctime,
                            category, 
                            image_prev_url2 
                    FROM v_groups_messages;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    message = model_im.Message()
                    message.account_id = self.using_account.account_id
                    message.source = db_col.db_path
                    message.sender_id = db_col.get_int64(1)
                    message.sender_name = db_col.get_string(2)
                    message.talker_id = db_col.get_int64(0)
                    message.msg_id = db_col.get_int64(4)
                    message.content = db_col.get_string(5)
                    message.send_time = Utils.convert_timestamp(db_col.get_int64(7))
                    message.is_sender = 1 if message.sender_id == self.using_account.uk else 0
                    message.talker_type = model_im.CHAT_TYPE_GROUP
                    if db_col.get_int64(11) > 0:
                        self.__add_media_path(message, db_col.get_string(13))

                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    pass
            self.model_im_col.db_commit()

    def _generate_message_table(self):
        self.__insert_people_messages()
        self.__insert_chatroom_messages()

    def _generate_chatroom_member_table(self):
        with self.cloud_p2p_col as db_col:
            sql = """SELECT uk, 
                            group_id, 
                            name, 
                            avatar_url, 
                            ctime, 
                            role 
                    FROM v_groups_people;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    chatroom_member = model_im.ChatroomMember()

                    chatroom_member.chatroom_id = db_col.get_string(1)
                    chatroom_member.account_id = self.using_account.account_id
                    chatroom_member.source = db_col.db_path
                    chatroom_member.display_name = db_col.get_string(2)
                    chatroom_member.photo = db_col.get_string(3)
                    chatroom_member.member_id = db_col.get_int64(0)

                    self.model_im_col.db_insert_table_chatroom_member(chatroom_member)
                except Exception as e:
                    pass
            self.model_im_col.db_commit()

    def _generate_file_list_table(self):
         with self.file_list_col as db_col:
            sql = """SELECT _id, 
                            fid, 
                            server_path, 
                            file_name, 
                            isdir, 
                            state, 
                            file_category, 
                            parent_path, 
                            file_md5, 
                            server_ctime, 
                            server_mtime, 
                            client_ctime, 
                            client_mtime,
                            file_size
                    FROM cachefilelist;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    f = model_nd.NDFileList()
                    f.set_value_with_idx(f.account, self.using_account.account_id)
                    f.set_value_with_idx(f.file_name, db_col.get_string(3))
                    f.set_value_with_idx(f.file_hash, db_col.get_string(8))
                    f.set_value_with_idx(f.file_size, db_col.get_int64(13))
                    f.set_value_with_idx(f.create_time, db_col.get_int64(10))
                    f.set_value_with_idx(f.server_path, db_col.get_string(2))

                    self.model_nd_col.db_insert_filelist(f.get_values())
                except Exception as e:
                    pass
            self.model_nd_col.db_commit()

    def _generate_download_task_table(self):
        with self.file_list_col as db_col:
            sql = """select _id, 
                            type, 
                            local_url, 
                            remote_url, 
                            size, 
                            state, 
                            offset_size, 
                            date, 
                            file_md5 
                    from download_tasks;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    t = model_nd.NDFileTransfer()
                    t.set_value_with_idx(t.account, self.using_account.account_id)
                    t.set_value_with_idx(t.server_path, db_col.get_string(3))
                    t.set_value_with_idx(t.local_path, db_col.get_string(2))
                    t.set_value_with_idx(t.file_size, db_col.get_int64(4))
                    t.set_value_with_idx(t.file_name, db_col.get_string(2).split(r"/")[-1])
                    t.set_value_with_idx(t.cached_size, db_col.get_int64(6))
                    t.set_value_with_idx(t.hash_code, db_col.get_string(8))
                    t.set_value_with_idx(t.is_download, model_nd.NDFileDone if t.file_size == t.cached_size else model_nd.NDFileProcessing)
                    t.set_value_with_idx(t.begin_time, Utils.convert_timestamp(db_col.get_int64(7)))

                    self.model_nd_col.db_insert_transfer(t.get_values())
                except Exception as e:
                    pass
            self.model_nd_col.db_commit()

    def _generate_share_file_table(self):
        with self.cloud_p2p_col as db_col:
            sql = """select _id, 
                            conversation_uk,
                            uk,
                            uname,
                            fsid,
                            path,
                            server_filename,
                            size, 
                            server_ctime,
                            server_mtime, 
                            local_ctime,
                            local_mtime,
                            is_dir,
                            thumbnail_url,
                            dlink,
                            md5,
                            msg_id,
                            ctime,
                            status,
                            category,
                            file_status
                    from people_messages_files;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    s = model_nd.NDFileShared()
                    s.set_value_with_idx(s.account, self.using_account.account_id)
                    s.set_value_with_idx(s.file_name, db_col.get_string(6))
                    s.set_value_with_idx(s.server_path, db_col.get_string(5))
                    s.set_value_with_idx(s.file_size, db_col.get_int64(7))
                    s.set_value_with_idx(s.create_time, Utils.convert_timestamp(db_col.get_int64(9)))
                    s.set_value_with_idx(s.url, db_col.get_string(14))
                    s.set_value_with_idx(s.sender_id, db_col.get_int64(2))
                    s.set_value_with_idx(s.sender_name, db_col.get_string(3))
                    s.set_value_with_idx(s.send_time, Utils.convert_timestamp(db_col.get_int64(17)))

                    self.model_nd_col.db_insert_shared(s.get_values())
                except Exception as e:
                    pass
            self.model_nd_col.db_commit()

    def decode_recover_chatroom(self):
        if not self.c_recover_helper.is_valid():
            return
        ts = self.c_recover_helper.fetch_table("groups", {
            "group_id": "Int",
            "name": "Text",
            "ctime": "Int",
            "create_uk": "Int",
            "people_limit": "Int",
        })
        for rec in self.c_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:

                chatroom = model_im.Chatroom()
                chatroom.deleted = 1
                chatroom.source = self.c_recover_helper.db_path
                chatroom.account_id = self.using_account.account_id
                chatroom.chatroom_id = rec['group_id'].Value
                chatroom.name = rec['name'].Value
                chatroom.creator_id = rec['create_uk'].Value
                chatroom.create_time = rec['ctime'].Value
                chatroom.max_member_count = rec['people_limit'].Value

                self.model_im_col.db_insert_table_chatroom(chatroom)
            except Exception as e:
                pass
        self.model_im_col.db_commit()

    def decode_recover_chatroom_member(self):
        if not self.c_recover_helper.is_valid():
            return
        ts = self.c_recover_helper.fetch_table("groups_people", {
            "role": "Int",
            "ctime": "Int",
            "uk": "Int",
            "group_id": "Int",
        })
        for rec in self.c_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:

                m = model_im.ChatroomMember()
                m.deleted = 1
                m.source = self.c_recover_helper.db_path
                m.account_id = self.using_account.account_id
                m.chatroom_id = rec['group_id'].Value
                m.member_id = rec['uk'].Value

                self.model_im_col.db_insert_table_chatroom_member(m)
            except Exception as e:
                pass
        self.model_im_col.db_commit()

    def decode_recover_groups_messages(self):
        if not self.c_recover_helper.is_valid():
            return
        ts = self.c_recover_helper.fetch_table('groups_messages', {
            "msg_id": "Int",
            "group_id": "Int",
            "msg_content": "Text",
            "msg_type": "Int",
            "uk": "Int",
            "uname": "Text",
            "ctime": "Int",
        })
        for rec in self.c_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:

                message = model_im.Message()
                message.source = self.c_recover_helper.db_path
                message.deleted = 1
                message.account_id = self.using_account.account_id
                message.msg_id = rec['msg_id'].Value
                message.talker_id = rec['group_id'].Value
                message.content = rec['msg_content'].Value
                message.sender_id = rec['uk'].Value
                message.send_time = Utils.convert_timestamp(rec['ctime'].Value)
                message.is_sender = 1 if rec['uk'].Value == self.using_account.uk else 0
                message.talker_type = model_im.CHAT_TYPE_GROUP

                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                pass
        self.model_im_col.db_commit()

    def decode_recover_people_messages(self):
        if not self.c_recover_helper.is_valid():
            return
        ts = self.c_recover_helper.fetch_table('people_messages', {
            "msg_id": "Int",
            "conversation_uk": "Int",
            "msg_content": "Text",
            "msg_type": "Int",
            "uk": "Int",
            "uname": "Text",
            "ctime": "Int",
            "files_count": "Int",
        })
        for rec in self.c_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:

                message = model_im.Message()
                message.account_id = self.using_account.account_id
                message.deleted = 1
                message.source = self.c_recover_helper.db_path
                message.sender_id = rec['uk'].Value
                message.sender_name = rec['uname'].Value
                message.talker_id = rec['conversation_uk'].Value
                message.msg_id = rec['msg_id'].Value
                message.content = rec['msg_content'].Value
                message.send_time = Utils.convert_timestamp(rec['ctime'].Value)
                message.is_sender = 1 if message.sender_id == self.using_account.uk else 0
                message.talker_type = model_im.CHAT_TYPE_FRIEND

                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                pass
        self.model_im_col.db_commit()

    def decode_recover_file_list(self):
        if not self.f_recover_helper.is_valid():
            return
        ts = self.f_recover_helper.fetch_table('cachefilelist', {
            "_id": "Int",
            "fid": "Int",
            "server_path": "Text",
            "file_name": "Text",
            "isdir": "Int",
            "state": "Int",
            "file_category": "Int",
            "parent_path": "Text",
            "file_md5": "Text",
            "server_ctime": "Int",
            "server_mtime": "Int",
            "client_ctime": "Int",
            "client_mtime": "Int",
            "file_size": "Int",
        })
        for rec in self.f_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                f = model_nd.NDFileList()
                f.set_value_with_idx(f.account, self.using_account.account_id)
                f.set_value_with_idx(f.file_name, rec['uname'].Value)
                f.set_value_with_idx(f.file_hash, rec['file_md5'].Value)
                f.set_value_with_idx(f.file_size, rec['file_size'].Value)
                f.set_value_with_idx(f.create_time, rec['server_mtime'].Value)
                f.set_value_with_idx(f.server_path, rec['server_path'].Value)
                f.set_value_with_idx(f.deleted, 1)

                self.model_nd_col.db_insert_filelist(f.get_values())
            except Exception as e:
                pass
        self.model_nd_col.db_commit()

    def decode_recover_download_task(self):
        if not self.f_recover_helper.is_valid():
            return
        ts = self.f_recover_helper.fetch_table('download_tasks', {
            "_id": "Int",
            "type": "Int",
            "local_url": "Text",
            "remote_url": "Text",
            "size": "Int",
            "state": "Int",
            "offset_size": "Int",
            "date": "Int",
            "file_md5 ": "Text",
        })
        for rec in self.f_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                t = model_nd.NDFileTransfer()
                t.set_value_with_idx(t.account, self.using_account.account_id)
                t.set_value_with_idx(t.deleted, 1)
                t.set_value_with_idx(t.server_path, rec['remote_url'].Value)
                t.set_value_with_idx(t.local_path, rec['local_url'].Value)
                t.set_value_with_idx(t.file_size, rec['size'].Value)
                t.set_value_with_idx(t.file_name, rec['remote_url'].Value.split(r"/")[-1])
                t.set_value_with_idx(t.cached_size, rec['offset_size'].Value)
                t.set_value_with_idx(t.hash_code, rec['file_md5'].Value)
                t.set_value_with_idx(t.is_download, model_nd.NDFileDone if t.file_size == t.cached_size else model_nd.NDFileProcessing)
                t.set_value_with_idx(t.begin_time, Utils.convert_timestamp(rec['date'].Value))

                self.model_nd_col.db_insert_transfer(t.get_values())
            except Exception as e:
                pass
        self.model_nd_col.db_commit()

    def decode_recover_share_file(self):
        if not self.c_recover_helper.is_valid():
            return
        ts = self.c_recover_helper.fetch_table('people_messages_files', {
            "_id": "Int",
            "conversation_uk": "Int",
            "uk": "Int",
            "uname": "Text",
            "fsid": "Int",
            "path": "Text",
            "server_filename": "Text",
            "size": "Int",
            "server_ctime": "Int",
            "server_mtime": "Int",
            "local_ctime": "Int",
            "local_mtime": "Int",
            "dlink": "Text",
            "md5": "Text",
            "msg_id": "Int",
            "ctime": "Int",
            "status": "Int",
        })
        for rec in self.c_recover_helper.read_deleted_record(ts):
            if canceller.IsCancellationRequested:
                return
            try:
                s = model_nd.NDFileShared()
                s.set_value_with_idx(s.account, self.using_account.account_id)
                s.set_value_with_idx(s.deleted, 1)
                s.set_value_with_idx(s.file_name, rec['server_filename'].Value)
                s.set_value_with_idx(s.server_path, rec['path'].Value)
                s.set_value_with_idx(s.file_size, rec['size'].Value)
                s.set_value_with_idx(s.create_time, Utils.convert_timestamp(rec['server_mtime'].Value))
                s.set_value_with_idx(s.url, rec['dlink'].Value)
                s.set_value_with_idx(s.sender_id, rec['uk'].Value)
                s.set_value_with_idx(s.sender_name, rec['uname'].Value)
                s.set_value_with_idx(s.send_time, Utils.convert_timestamp(rec['ctime'].Value))

                self.model_nd_col.db_insert_shared(s.get_values())
            except Exception as e:
                pass
        self.model_nd_col.db_commit()

    def __add_media_path(self, m_obj, file_name):
        if not file_name:
            return
        nodes = self.root.FileSystem.Search(file_name + '$')
        for node in nodes:
            m_obj.media_path = node.AbsolutePath
        return True

    def parse(self):
        """解析的主函数"""
        if self.has_data is False:
            return

        for account in self._generate_account_table():
            self.cloud_p2p_col, self.file_list_col = self.__fetch_cols(account)
            self.c_recover_helper, self.f_recover_helper = self.__fetch_recover_helper(account)

            self._generate_friend_table()
            self._generate_message_table()
            self._generate_chatroom_table()
            self._generate_chatroom_member_table()
            self._generate_file_list_table()
            self._generate_download_task_table()
            self._generate_share_file_table()

            self.decode_recover_chatroom()
            self.decode_recover_chatroom_member()
            self.decode_recover_groups_messages()
            self.decode_recover_people_messages()
            self.decode_recover_file_list()
            self.decode_recover_download_task()
            self.decode_recover_share_file()

        generate = model_nd.NDModel(self.cache_db)
        nd_results = generate.generate_models()

        generate = model_im.GenerateModel(self.cache_db + ".IM")
        im_results = generate.get_models()

        return nd_results + im_results


def analyze_nd(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.BDY
    results = BaiduNetDiskParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("百度云网盘")
    return pr
