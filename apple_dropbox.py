# -*- coding: utf-8 -*-
__author__ = "TaoJianping"

import hashlib
import os
import codecs
import json
import base64
import shutil

from xml.dom import minidom
import xml.etree.ElementTree as ET
import clr

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

try:
    clr.AddReference('model_im')
    clr.AddReference('model_nd')
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
from PA.InfraLib.Extensions import PlistHelper

# CONST
DropBox_VERSION = 1
DEBUG = True


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
            "Float": SQLiteParser.FieldType.Float,
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
            ts = str(int(float(ts)))
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

    @staticmethod
    def calculate_file_size(file_path):
        if file_path is None:
            return
        if not os.path.exists(file_path):
            return
        return int(os.path.getsize(file_path))

    @staticmethod
    def hash_md5(words):
        m = hashlib.md5()
        m.update(words)
        return m.hexdigest().upper()

    @staticmethod
    def create_sub_node(node, rpath, vname):
        mem = MemoryRange.CreateFromFile(rpath)
        r_node = Node(vname, Files.NodeType.File)
        r_node.Data = mem
        node.Children.Add(r_node)
        return r_node

    @staticmethod
    def open_plist(file_path):
        res = PlistHelper.ReadPlist(file_path)
        return res

    @staticmethod
    def get_xml_node(file_path):
        doc = minidom.parse(file_path)
        root = doc.documentElement
        return root

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


class DropBoxParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = self.__get_root_node(root)
        self.app_name = 'DropBox'
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache_db = self.__get_cache_db()
        self.model_nd_col, self.model_im_col = self.__load_nd_models(self.cache_db, DropBox_VERSION)
        self.data_path = self.__copy_data_dir_files(data_dir="/Documents/Users")
        self.user_info_data_path = self.__copy_data_dir_files(data_dir="/Library/Application Support/Dropbox")
        self.logger = Logger()
        self.using_account = None

    @staticmethod
    def __get_root_node(node):
        """根据传入的节点拿到要检测的根节点"""
        return node.Parent.Parent

    @staticmethod
    def __load_nd_models(cache_db, app_version):
        model_nd_col = model_nd.NetDisk(cache_db, app_version)
        model_im_col = model_nd_col.im
        return model_nd_col, model_im_col

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath(self.app_name)
        m = hashlib.md5()
        m.update(Encoding.UT8.GetBytes(self.root.AbsolutePath))
        return os.path.join(self.cache_path, m.hexdigest().upper())

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

    # TODO 可以优化
    @staticmethod
    def __determine_file_type(file_name):
        if file_name.endswith('.mp3'):
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif file_name.endswith('.amr'):
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif file_name.endswith('.slk'):
            return model_im.MESSAGE_CONTENT_TYPE_VOICE
        elif file_name.endswith('.mp4'):
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif file_name.endswith('.mov'):
            return model_im.MESSAGE_CONTENT_TYPE_VIDEO
        elif file_name.endswith('.jpg'):
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif file_name.endswith('.gif'):
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        elif file_name.endswith('.png'):
            return model_im.MESSAGE_CONTENT_TYPE_IMAGE
        else:
            return None

    def __copy_data_dir_files(self, data_dir):
        node = self.root.GetByPath(data_dir)
        if node is None:
            print("not found data")
            return False
        old_dir = node.PathWithMountPoint

        new_dir = os.path.join(self.cache_path, Utils.hash_md5(data_dir))
        Utils.copy_dir(old_dir, new_dir)
        return new_dir

    @staticmethod
    def __process_account_id(_id):
        if not _id:
            return
        if not _id.startswith("dbid:"):
            return _id
        return _id[5:]

    def _generate_account_table(self, account_file):
        """
        创建account table，dropbox 不支持多账户
        :param account_file: 保存账户信息的文件地址
        :return:
        """
        print(account_file)
        account_info = Utils.json_loads(Utils.open_file(account_file)).get("contacts", [None])[0]
        if not account_info:
            return

        account = model_im.Account()
        account.account_id = self.__process_account_id(account_info.get("account_info", {}).get("id", None))
        account.username = account_info.get("name", {}).get("display", None)
        account.nickname = account.username
        account.email = account_info.get("email_addresses", [None])[0]
        account.telephone = account_info.get("phone_numbers", [None])[0]
        account.photo = account_info.get("account_info", {}).get("photo_url", None)
        account.source = account_file

        # 挂载正在使用的account
        self.using_account = account

        self.model_im_col.db_insert_table_account(account)
        self.model_im_col.db_commit()

    def _generate_friend_table(self, friends_file):
        """
        创建friend table， 注意，dropbox 并没有联系人聊天之类的东西，这个联系人只在分享的有用
        :param friends_file: 保存联系人的信息
        :return:
        """
        friends_info = Utils.json_loads(Utils.open_file(friends_file)).get("contacts", None)
        if not friends_info:
            return

        for friend_info in friends_info:
            try:

                friend = model_im.Friend()
                friend.account_id = self.using_account.account_id
                friend.friend_id = self.__process_account_id(friend_info.get("account_info", {}).get("id", None))
                friend.source = friends_file
                friend.photo = friend_info.get("account_info", {}).get("photo_url", None)
                friend.nickname = friend_info.get("name", {}).get("display", None)
                friend.email = friend_info.get("email_addresses", [None])[0]
                friend.telephone = friend_info.get("phone_numbers", [None])[0]

                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                self.logger.error()
        self.model_im_col.db_commit()

    @staticmethod
    def __process_file_name(origin_name):
        return origin_name.split("/")[-1]

    def __add_net_disk_file_record(self):
        with self.using_net_disk_col as db_col:
            sql = """SELECT normalized_path, 
                            metadata_last_modified_date, 
                            metadata_client_mtime, 
                            metadata_path, 
                            metadata_icon, 
                            metadata_is_deleted, 
                            file_content_hash,
                            metadata_total_bytes
                    FROM metadata
                    WHERE metadata_is_dir != 1;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:

                    f = model_nd.NDFileList()
                    f.set_value_with_idx(f.account, self.using_account.account_id)
                    f.set_value_with_idx(f.server_path, db_col.get_string(3))
                    f.set_value_with_idx(f.file_name, self.__process_file_name(db_col.get_string(3)))
                    f.set_value_with_idx(f.file_type, self.__determine_file_type(f.get_value_with_idx(f.file_name)))
                    f.set_value_with_idx(f.file_hash, db_col.get_string(6))
                    f.set_value_with_idx(f.file_size, db_col.get_int64(7))
                    f.set_value_with_idx(f.create_time, Utils.convert_timestamp(db_col.get_float(2)))
                    f.set_value_with_idx(f.update_time, Utils.convert_timestamp(db_col.get_float(1)))

                    self.model_nd_col.db_insert_filelist(f.get_values())
                except Exception as e:
                    self.logger.error()
            self.model_nd_col.db_commit()

    def __decode_recover_net_disk_file(self):
        if not self.file_list_recover_col.is_valid():
            return
        ts = self.file_list_recover_col.fetch_table('metadata', {
            "normalized_path": "Text",
            "metadata_last_modified_date": "Float",
            "metadata_client_mtime": "Float",
            "metadata_path": "Text",
            "file_content_hash": "Text",
            "metadata_total_bytes": "Int",
        })
        for rec in self.file_list_recover_col.read_deleted_record(ts):
            try:
                f = model_nd.NDFileList()
                f.set_value_with_idx(f.account, self.using_account.account_id)
                f.set_value_with_idx(f.file_name, self.__process_file_name(rec['normalized_path'].Value))
                f.set_value_with_idx(f.file_hash, rec['file_content_hash'].Value)
                f.set_value_with_idx(f.file_size, rec['metadata_total_bytes'].Value)
                f.set_value_with_idx(f.create_time, rec['metadata_client_mtime'].Value)
                f.set_value_with_idx(f.server_path, rec['normalized_path'].Value)
                f.set_value_with_idx(f.deleted, 1)

                self.model_nd_col.db_insert_filelist(f.get_values())
            except Exception as e:
                self.logger.error()
        self.model_nd_col.db_commit()

    def _generate_net_disk_file_table(self):
        self.__add_net_disk_file_record()
        self.__decode_recover_net_disk_file()

    def __search_cache_file(self, file_size):
        """
        根据file_size 找到相应的文件，不保证完全正确
        :param file_size: (int)
        :return:
        """
        if not os.path.exists(self.current_cache_files_dir):
            return

        for d in Utils.list_dir(self.current_cache_files_dir):
            d_path = os.path.join(self.current_cache_files_dir, d)
            for f in Utils.list_dir(d_path):
                f_path = os.path.join(d_path, f)
                if f.startswith("original") and (Utils.calculate_file_size(f_path) == file_size):
                    return f_path

    def __query_hash_code(self, file_path):
        """通过文件在服务器的路径找到文件的hash_code"""
        with self.using_net_disk_col as db_col:
            sql = """SELECT file_content_hash 
                        FROM metadata 
                        WHERE normalized_path = '{}';""".format(file_path)
            db_col.execute_sql(sql)
            if db_col.has_rest():
                return db_col.get_string(0)

    def __add_download_task_record(self):
        with self.using_offline_files_col as db_col:
            sql = """SELECT normalized_path, 
                            total_bytes, 
                            ordering_index 
                    FROM offline_files 
                    ORDER BY ordering_index;"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:

                    t = model_nd.NDFileTransfer()
                    t.set_value_with_idx(t.account, self.using_account.account_id)
                    t.set_value_with_idx(t.server_path, db_col.get_string(0))
                    t.set_value_with_idx(t.file_size, db_col.get_int64(1))
                    t.set_value_with_idx(t.file_name, self.__process_file_name(db_col.get_string(0)))
                    t.set_value_with_idx(t.cached_size, db_col.get_int64(1))
                    t.set_value_with_idx(t.is_download, model_nd.NDFileDone)
                    t.set_value_with_idx(t.hash_code, self.__query_hash_code(db_col.get_string(0)))
                    t.set_value_with_idx(t.local_path, self.__search_cache_file(t.get_value_with_idx(t.file_size)))

                    self.model_nd_col.db_insert_transfer(t.get_values())
                except Exception as e:
                    self.logger.error()
            self.model_nd_col.db_commit()

    def __decode_recover_download_task(self):
        if not self.offline_files_recover_col.is_valid():
            return
        ts = self.offline_files_recover_col.fetch_table('offline_files', {
            "normalized_path": "Text",
            "total_bytes": "Int",
        })
        for rec in self.offline_files_recover_col.read_deleted_record(ts):
            try:
                t = model_nd.NDFileTransfer()
                t.set_value_with_idx(t.account, self.using_account.account_id)
                t.set_value_with_idx(t.deleted, 1)
                t.set_value_with_idx(t.server_path, rec['normalized_path'].Value)
                t.set_value_with_idx(t.file_size, rec['total_bytes'].Value)
                t.set_value_with_idx(t.file_name, self.__process_file_name(t.get_value_with_idx(t.server_path)))
                t.set_value_with_idx(t.cached_size, rec['total_bytes'].Value)
                t.set_value_with_idx(t.hash_code, self.__query_hash_code(rec['normalized_path'].Value))
                t.set_value_with_idx(t.is_download, model_nd.NDFileDone)

                self.model_nd_col.db_insert_transfer(t.get_values())
            except Exception as e:
                self.logger.error()
        self.model_nd_col.db_commit()

    def _generate_download_task_table(self):
        """生成download task table"""
        self.__add_download_task_record()
        self.__decode_recover_download_task()

    def _generate_search_table(self, file_path):
        """
        搜索记录是存储在一个plist文件里面的
        :param file_path: 搜索记录文件的地址
        :return:
        """
        if not file_path:
            return

        root = Utils.open_plist(file_path)
        nodes = list(root["$objects"])
        for node in nodes:
            if hasattr(node, "Content"):
                key_word = getattr(node, "Content", None)
                if key_word == "$null":
                    continue

                s = model_im.Search()
                s.account_id = self.using_account.account_id
                s.key = key_word
                s.source = file_path
                self.model_im_col.db_insert_table_search(s)

        self.model_im_col.db_commit()
        return

    def __search_db_files(self):
        """返回云盘文件的数据库文件"""
        dirs = [d for d in Utils.list_dir(self.data_path) if os.path.isdir(os.path.join(self.data_path, d))]

        ret = []
        for d in dirs:
            db_file = os.path.join(self.data_path, d, "metadata.db")
            offline_db_file = os.path.join(self.data_path, d, "offline.db")
            cache_files_dir = os.path.join(self.data_path, d, "FileCache", "Loaded")
            if os.path.exists(db_file) and os.path.exists(offline_db_file):
                ret.extend((db_file, offline_db_file, cache_files_dir))

        if not ret:
            return None, None, None

        return ret

    def __search_user_info_files(self):
        """
        搜索用户信息文件和联系人信息文件
        :return:
        """
        target_node = self.root.GetByPath("/Library/Application Support/Dropbox")
        if not target_node:
            return None, None
        account_nodes = list(target_node.Search("me$"))
        friend_nodes = list(target_node.Search("all_searchable$"))
        if account_nodes and friend_nodes:
            return account_nodes[0].PathWithMountPoint, friend_nodes[0].PathWithMountPoint
        return None, None

    def __search_query_file(self):
        target_node = self.root.GetByPath("/Library/Caches/Users")
        if not target_node:
            return
        search_query_node = list(target_node.FileSystem.Search("SearchQueryHistory$"))
        if search_query_node:
            return search_query_node[0].PathWithMountPoint

    def _main(self):
        """解析的逻辑主函数"""
        if not self.data_path or not self.user_info_data_path:
            return

        # 获取必要的文件
        db_file, offline_db_file, cache_files_dir = self.__search_db_files()
        account_info_file, friends_info_file = self.__search_user_info_files()
        search_file = self.__search_query_file()

        if not all((account_info_file, friends_info_file, db_file, offline_db_file, cache_files_dir)):
            return

        # 因为用户信息和联系人信息都在文件里面以json的方式存储的,所以就不挂载实例的状态里面了
        self.using_net_disk_col = ColHelper(db_file)
        self.file_list_recover_col = RecoverTableHelper(Utils.create_sub_node(self.root, db_file, "file_db"))
        self.using_offline_files_col = ColHelper(offline_db_file)
        self.offline_files_recover_col = RecoverTableHelper(
            Utils.create_sub_node(self.root, offline_db_file, "offline_db"))
        self.current_cache_files_dir = cache_files_dir

        self._generate_account_table(account_info_file)
        self._generate_friend_table(friends_info_file)
        self._generate_search_table(search_file)
        self._generate_net_disk_file_table()
        self._generate_download_task_table()

    def _db_update_finished_info(self):
        """当更新数据完成之后，更新version表的内容，方便日后检查"""
        self.model_nd_col.db_insert_im_version(DropBox_VERSION)

    def parse(self):
        """程序入口"""
        if DEBUG or self.model_nd_col.need_parse:
            self._main()
            self._db_update_finished_info()

        generate = model_nd.NDModel(self.cache_db)
        nd_results = generate.generate_models()

        generate = model_im.GenerateModel(self.cache_db + ".IM")
        im_results = generate.get_models()

        return nd_results + im_results


def analyze_dropbox(root, extract_deleted, extract_source):
    pr = ParserResults()
    pr.Categories = DescripCategories.DropBox
    results = DropBoxParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("DropBox")
    return pr
