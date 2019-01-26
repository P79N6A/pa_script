# -*- coding: utf-8 -*-
import hashlib
import os

import clr

__author__ = "TaoJianping"

clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('unity_c37r')
    clr.AddReference('model_eb')
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
except Exception as e:
    print("debug", e)

import model_eb
import model_im
import PA_runtime
import System
from PA_runtime import *
from System.Data.SQLite import *
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions

del clr
# CONST
JD_VERSION = 1
# 消息状态
MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT = 1
MESSAGE_STATUS_SENT = 2
MESSAGE_STATUS_UNREAD = 3
MESSAGE_STATUS_READ = 4
# 00未知、01收藏夹、02购物车、03已购买、04普通浏览、99其他
EB_PRODUCT_UNKWON = "0"
EB_PRODUCT_FAVORITE = "1"
EB_PRODUCT_SHOPCART = "2"
EB_PRODCUT_BUIED = "3"
EB_PRODUCT_BROWSE = "4"
EB_PRODUCT_OTHER = "99"
# 消息类型
MESSAGE_CONTENT_TYPE_TEXT = 1  # 文本
MESSAGE_CONTENT_TYPE_IMAGE = 2  # 图片
MESSAGE_CONTENT_TYPE_VOICE = 3  # 语音
MESSAGE_CONTENT_TYPE_VIDEO = 4  # 视频
MESSAGE_CONTENT_TYPE_EMOJI = 5  # 表情
MESSAGE_CONTENT_TYPE_CONTACT_CARD = 6  # 名片
MESSAGE_CONTENT_TYPE_LOCATION = 7  # 坐标
MESSAGE_CONTENT_TYPE_LINK = 8  # 链接
MESSAGE_CONTENT_TYPE_VOIP = 9  # 网络电话
MESSAGE_CONTENT_TYPE_ATTACHMENT = 10  # 附件
MESSAGE_CONTENT_TYPE_RED_ENVELPOE = 11  # 红包
MESSAGE_CONTENT_TYPE_RECEIPT = 12  # 转账
MESSAGE_CONTENT_TYPE_AA_RECEIPT = 13  # 群收款
MESSAGE_CONTENT_TYPE_SYSTEM = 99  # 系统


class ColHelper(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = System.Data.SQLite.SQLiteConnection(
            'Data Source = {}; Readonly = True'.format(db_path))

    def __enter__(self):
        self.conn.Open()
        self.cmd = System.Data.SQLite.SQLiteCommand(self.conn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cmd.Dispose()
        if hasattr(self, "reader"):
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


class JDParser(object):
    def __init__(self, root, extract_deleted, extract_source):
        self.root = root
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.cache_db = self.__get_cache_db()
        self.eb = model_eb.EB(self.cache_db, JD_VERSION, u'Jingdong')
        self.model_im_col = self.eb.im
        self.need_parse = self.eb.need_parse
        self.jd_db_path, self.user_db_path = self.__get_data_db()
        self.jd_db_col = ColHelper(self.jd_db_path)
        self.user_db_col = ColHelper(self.user_db_path)
        if self.need_parse and all((self.jd_db_path, self.user_db_path)):
            self.eb.db_create()
        self.using_account = None

    def __get_data_db(self):
        """获取需要用到的两张表的地址"""
        jd_db_node = self.root.GetByPath("databases/jd.db")
        user_db_node = self.root.GetByPath("databases/__icssdk_database.db")
        if all((jd_db_node, user_db_node)):
            return jd_db_node.PathWithMountPoint, user_db_node.PathWithMountPoint
        else:
            return None, None

    def __get_cache_db(self):
        """获取中间数据库的db路径"""
        self.cache_path = ds.OpenCachePath("Jingdong")
        m = hashlib.md5()
        m.update(Encoding.UT8.GetBytes(self.root.AbsolutePath))
        return os.path.join(self.cache_path, m.hexdigest().upper())

    def __process_media(self, msg):
        try:
            sdcard = '/storage/emulated/0/'
            searchkey = ''
            nodes = list()
            if msg.content.find(sdcard) != -1:
                searchkey = msg.content[msg.content.find(sdcard) + len(sdcard):]
                nodes = self.root.FileSystem.Search(searchkey + '$')
                if len(list(nodes)) == 0:
                    searchkey = msg.content[msg.content.rfind('/') + 1:]
                    nodes = self.root.FileSystem.Search(searchkey + '$')
            for node in nodes:
                msg.media_path = node.AbsolutePath
                if msg.media_path.endswith('.mp3'):
                    msg.type = MESSAGE_CONTENT_TYPE_VOICE
                elif msg.media_path.endswith('.amr'):
                    msg.type = MESSAGE_CONTENT_TYPE_VOICE
                elif msg.media_path.endswith('.slk'):
                    msg.type = MESSAGE_CONTENT_TYPE_VOICE
                elif msg.media_path.endswith('.mp4'):
                    msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                elif msg.media_path.endswith('.jpg'):
                    msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                elif msg.media_path.endswith('.png'):
                    msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                else:
                    msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
                return True
        except Exception as e:
            print (e)
        return False

    def __config_using_account(self):
        with self.user_db_col as db_col:
            sql = """SELECT pin
                    FROM my_info"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                self.using_account = db_col.get_string(0)

    def _get_account_table(self):
        with self.user_db_col as db_col:
            sql = """SELECT mypin
                    FROM my_config"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                account = model_im.Account()
                account.account_id = db_col.get_string(0)
                self.model_im_col.db_insert_table_account(account)
            self.model_im_col.db_commit()

    def _get_friend_table(self):
        with self.user_db_col as db_col:
            sql = """SELECT _id,
                            localPin,
                            venderId,
                            appId,
                            venderName,
                            avatar
                    FROM _MSG_LIST_"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    friend = model_im.Friend()
                    friend.account_id = db_col.get_string(1)
                    friend.source = self.user_db_path
                    friend.friend_id = db_col.get_string(2)
                    friend.nickname = db_col.get_string(4)
                    friend.photo = db_col.get_string(5)
                    self.model_im_col.db_insert_table_friend(friend)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

    def _get_message_table(self):
        with self.user_db_col as db_col:
            sql = """SELECT _id,
                            UUID,
                            localPin,
                            type,
                            datetime,
                            timestamp,
                            mid,
                            from_pin,
                            body_type,
                            body_content,
                            body_url,
                            readed
                    FROM _MSG_"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                try:
                    message = model_im.Message()
                    message.account_id = db_col.get_string(2)
                    message.sender_id = db_col.get_string(7)
                    message.sender_name = db_col.get_string(7)
                    message.msg_id = db_col.get_string(6)
                    message.send_time = self.__convert_timestamp(db_col.get_int64(5))
                    message.source = self.user_db_path
                    message_type = db_col.get_string(8)
                    if message_type == "text":
                        message.type = MESSAGE_CONTENT_TYPE_TEXT
                    elif message_type == "image":
                        message.content = db_col.get_string(10)
                        self.__process_media(message)
                    else:
                        message.type = MESSAGE_CONTENT_TYPE_SYSTEM
                    message.content = db_col.get_string(9)
                    message.status = MESSAGE_STATUS_READ if db_col.get_int64(11) == 1 else MESSAGE_STATUS_UNREAD
                    message.is_sender = 1 if message.account_id == message.sender_id else 0
                    self.model_im_col.db_insert_table_message(message)
                except Exception as e:
                    print("debug error", e)
            self.model_im_col.db_commit()

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

    def _get_search_table(self):
        with self.jd_db_col as db_col:
            sql = """SELECT word,
                            search_time
                    FROM search_history"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                search = model_im.Search()
                search.key = db_col.get_string(0)
                search.create_time = self.__convert_timestamp(db_col.get_int64(1))
                search.source = self.jd_db_path
                self.model_im_col.db_insert_table_search(search)
            self.model_im_col.db_commit()

    def _get_product_table(self):
        # 购物车的商品
        with self.jd_db_col as db_col:
            sql = """SELECT id,
                            name,
                            productCode,
                            buyCount
                    FROM CartTable"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                product = model_eb.EBProduct()
                product.set_value_with_idx(product.account_id, self.using_account)
                product.set_value_with_idx(product.product_id, db_col.get_int64(2))
                product.set_value_with_idx(product.product_name, db_col.get_string(1))
                product.set_value_with_idx(product.source, EB_PRODUCT_SHOPCART)
                self.eb.db_insert_table_product(product.get_value())
            self.eb.db_commit()
        # 普通的浏览记录
        with self.jd_db_col as db_col:
            sql = """SELECT id,
                            productCode
                    FROM BrowseHistoryTable"""
            db_col.execute_sql(sql)
            while db_col.has_rest():
                product = model_eb.EBProduct()
                product.set_value_with_idx(product.product_id, db_col.get_int64(1))
                product.set_value_with_idx(product.source, EB_PRODUCT_BROWSE)
                self.eb.db_insert_table_product(product.get_value())
            self.eb.db_commit()

    def decode_recover_account(self):
        node = self.root.GetByPath("databases/__icssdk_database.db")
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = 'my_config'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                account = model_im.Account()
                account.account_id = rec["mypin"].Value
                self.model_im_col.db_insert_table_account(account)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def decode_recover_friend(self):
        node = self.root.GetByPath("databases/__icssdk_database.db")
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = '_MSG_LIST_'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                friend = model_im.Friend()
                friend.account_id = rec["localPin"].Value
                friend.source = self.user_db_path
                friend.friend_id = rec["venderId"].Value
                friend.nickname = rec["venderName"].Value
                friend.photo = rec["avatar"].Value
                self.model_im_col.db_insert_table_friend(friend)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def decode_recover_message(self):
        node = self.root.GetByPath("databases/__icssdk_database.db")
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = '_MSG_'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                message = model_im.Message()
                message.account_id = rec["localPin"].Value
                message.sender_id = rec["from_pin"].Value
                message.sender_name = rec["from_pin"].Value
                message.msg_id = rec["mid"].Value
                message.send_time = self.__convert_timestamp(rec["timestamp"].Value)
                message.source = self.user_db_path
                message_type = rec["body_type"].Value
                if message_type == "text":
                    message.type = MESSAGE_CONTENT_TYPE_TEXT
                elif message_type == "image":
                    message.content = rec["body_content"].Value
                    self.__process_media(message)
                else:
                    message.type = MESSAGE_CONTENT_TYPE_SYSTEM
                message.content = rec["body_content"].Value
                message.status = MESSAGE_STATUS_READ if rec["readed"].Value == 1 else MESSAGE_STATUS_UNREAD
                message.is_sender = 1 if message.account_id == message.sender_id else 0
                self.model_im_col.db_insert_table_message(message)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def decode_recover_search(self):
        node = self.root.GetByPath("databases/jd.db")
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        table = 'search_history'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                search = model_im.Search()
                search.key = rec["word"].Value
                search.create_time = self.__convert_timestamp(rec["search_time"].Value)
                search.source = self.jd_db_path
                self.model_im_col.db_insert_table_search(search)
            except Exception as e:
                print("error happen", e)
        self.model_im_col.db_commit()

    def decode_recover_product(self):
        node = self.root.GetByPath("databases/jd.db")
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        # 购物车
        table = 'CartTable'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                product = model_eb.EBProduct()
                product.set_value_with_idx(product.account_id, self.using_account)
                product.set_value_with_idx(product.product_id, rec["productCode"].Value)
                product.set_value_with_idx(product.product_name, rec["name"].Value)
                product.set_value_with_idx(product.source, EB_PRODUCT_SHOPCART)
                self.eb.db_insert_table_product(product.get_value())
            except Exception as e:
                print("error happen", e)
        # 浏览记录
        table = 'BrowseHistoryTable'
        ts = SQLiteParser.TableSignature(table)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            try:
                product = model_eb.EBProduct()
                product.set_value_with_idx(product.product_id, rec["productCode"].Value)
                product.set_value_with_idx(product.source, EB_PRODUCT_BROWSE)
                self.eb.db_insert_table_product(product.get_value())
            except Exception as e:
                print("error happen", e)
        self.eb.db_commit()

    def parse(self):
        """解析的主函数"""
        if not all((self.user_db_path, self.jd_db_path)):
            return
        # 配置当前正在使用的用户
        self.__config_using_account()
        # 获取缓存数据
        self._get_account_table()
        self._get_friend_table()
        self._get_message_table()
        self._get_product_table()
        self._get_search_table()
        self.decode_recover_account()
        self.decode_recover_friend()
        self.decode_recover_message()
        self.decode_recover_search()
        self.decode_recover_product()

        generate = model_eb.GenerateModel(self.cache_db)
        results = generate.get_models()

        return results


def parse_jd(root, extract_deleted, extract_source):

    pr = ParserResults()
    pr.Categories = DescripCategories.JingDong
    results = JDParser(root, extract_deleted, extract_source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("京东")
    return pr
