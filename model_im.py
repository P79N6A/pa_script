# -*- coding: utf-8 -*-
__author__ = "sumeng"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite
from System import Convert

import os
import sqlite3
import json
import uuid

VERSION_VALUE_DB = 14

GENDER_NONE = 0
GENDER_MALE = 1
GENDER_FEMALE = 2

FRIEND_TYPE_NONE = 0  # 未知
FRIEND_TYPE_FRIEND = 1  # 好友
FRIEND_TYPE_GROUP_FRIEND = 2  # 群好友
FRIEND_TYPE_FANS = 3  # 粉丝
FRIEND_TYPE_FOLLOW = 4  # 关注
FRIEND_TYPE_SPECAIL_FOLLOW = 5  # 特别关注
FRIEND_TYPE_MUTUAL_FOLLOW = 6  # 互相关注
FRIEND_TYPE_RECENT = 7  # 最近
FRIEND_TYPE_SUBSCRIBE = 8  # 公众号
FRIEND_TYPE_STRANGER = 9  # 陌生人
FRIEND_TYPE_SHOP = 10  # 商家

CHATROOM_TYPE_NORMAL = 1  # 普通群
CHATROOM_TYPE_TEMP = 2  # 临时群
CHATROOM_TYPE_TIEBA = 3  #贴吧

CHAT_TYPE_FRIEND = 1  # 好友聊天
CHAT_TYPE_GROUP = 2  # 群聊天
CHAT_TYPE_SYSTEM = 3  # 系统消息
CHAT_TYPE_OFFICIAL = 4  # 公众号
CHAT_TYPE_SUBSCRIBE = 5  # 订阅号
CHAT_TYPE_SHOP = 6  # 商家

MESSAGE_TYPE_SYSTEM = 1
MESSAGE_TYPE_SEND = 2
MESSAGE_TYPE_RECEIVE = 3

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
MESSAGE_CONTENT_TYPE_CHARTLET = 14
MESSAGE_CONTENT_TYPE_SYSTEM = 99  # 系统

MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT = 1
MESSAGE_STATUS_SENT = 2
MESSAGE_STATUS_UNREAD = 3
MESSAGE_STATUS_READ = 4

LABEL_DEFAULT = 0
LABEL_LIKED = 1
LABEL_DISLIKED = 2
LABEL_STAR = 3

PLATFORM_PC = 1
PLATFORM_MOBILE = 2

DEAL_TYPE_RED_ENVELPOE = 1  # 红包
DEAL_TYPE_RECEIPT = 2  # 转账
DEAL_TYPE_AA_RECEIPT = 3  # 群收款

RECEIPT_STATUS_UNRECEIVE = 1  # 未领取
RECEIPT_STATUS_RECEIVE = 2  # 已领取
RECEIPT_STATUS_EXPIRE = 3  # 已失效

FAVORITE_TYPE_TEXT = 1  # 文本
FAVORITE_TYPE_IMAGE = 2  # 图片
FAVORITE_TYPE_VOICE = 3  # 语音
FAVORITE_TYPE_VIDEO = 4  # 视频
FAVORITE_TYPE_LINK = 5  # 链接
FAVORITE_TYPE_LOCATION = 6  # 位置
FAVORITE_TYPE_ATTACHMENT = 7  # 附件
FAVORITE_TYPE_CHAT = 8  # 聊天记录

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

SQL_CREATE_TABLE_ACCOUNT = '''
    create table if not exists account(
        account_id TEXT, 
        nickname TEXT,
        username TEXT,
        password TEXT, 
        photo TEXT, 
        telephone TEXT, 
        email TEXT, 
        gender INT, 
        age INT, 
        country TEXT,
        province TEXT,
        city TEXT,
        address TEXT, 
        birthday INT, 
        signature TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_ACCOUNT = '''
    insert into account(account_id, nickname, username, password, photo, telephone, email, gender, age, 
                        country, province, city, address, birthday, signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FRIEND = '''
    create table if not exists friend(
        account_id TEXT, 
        friend_id TEXT, 
        nickname TEXT, 
        fullname TEXT,
        remark TEXT,
        photo TEXT, 
        type INT,
        telephone TEXT, 
        email TEXT, 
        gender INT, 
        age INT, 
        address TEXT, 
        birthday INT, 
        signature TEXT,
        location_id INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FRIEND = '''
    insert into friend(account_id, friend_id, nickname, fullname, remark, photo, type, telephone, email, gender, 
                       age, address, birthday, signature, location_id, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_CHATROOM = '''
    create table if not exists chatroom(
        account_id TEXT, 
        chatroom_id TEXT, 
        name TEXT, 
        photo TEXT, 
        type INT,
        notice TEXT,
        description TEXT,
        creator_id TEXT,
        owner_id TEXT,
        member_count INT,
        max_member_count INT,
        create_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_CHATROOM = '''
    insert into chatroom(account_id, chatroom_id, name, photo, type, notice, description, creator_id, 
                         owner_id, member_count, max_member_count, create_time, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        

SQL_CREATE_TABLE_CHATROOM_MEMBER = '''
    create table if not exists chatroom_member(
        account_id TEXT, 
        chatroom_id TEXT, 
        member_id TEXT, 
        display_name TEXT, 
        photo TEXT, 
        telephone TEXT, 
        email TEXT, 
        gender INT, 
        age INT, 
        address TEXT, 
        birthday INT, 
        signature TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_CHATROOM_MEMBER = '''
    insert into chatroom_member(account_id, chatroom_id, member_id, display_name, photo, telephone, 
                                email, gender, age, address, birthday, signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_CHATROOM_MEMBER = '''
    create index idxChatroomMember on chatroom_member (account_id, chatroom_id)
'''

SQL_CREATE_TABLE_MESSAGE = '''
    create table if not exists message(
        account_id TEXT, 
        talker_id TEXT,
        talker_name TEXT,
        sender_id TEXT,
        sender_name TEXT,
        is_sender INT,
        msg_id TEXT, 
        type INT,
        content TEXT,
        media_path TEXT,
        send_time INT,
        location_id INT,
        deal_id INT,
        link_id INT,
        status INT,
        talker_type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_MESSAGE = '''
    insert into message(account_id, talker_id, talker_name, sender_id, sender_name, is_sender, msg_id, type, content, 
                        media_path, send_time, location_id, deal_id, link_id, status, talker_type, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED = '''
    create table if not exists feed(
        account_id TEXT, 
        sender_id TEXT,
        content TEXT,
        image_path TEXT,
        video_path TEXT,
        url TEXT,
        url_title TEXT,
        url_desc TEXT,
        send_time INT,
        like_id INT,
        likecount INT,
        rtcount INT,
        comment_id INT,
        commentcount INT,
        device TEXT,
        location_id INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED = '''
    insert into feed(account_id, sender_id, content, image_path, video_path, url, url_title, url_desc, send_time, 
                     like_id, likecount, rtcount, comment_id, commentcount, device, location_id, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED_LIKE = '''
    create table if not exists feed_like(
        like_id INT,
        sender_id TEXT,
        sender_name TEXT,
        create_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED_LIKE = '''
    insert into feed_like(like_id, sender_id, sender_name, create_time, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_FEED_LIKE = '''
    create index idxFeedLike on feed_like (like_id)
'''

SQL_CREATE_TABLE_FEED_COMMENT = '''
    create table if not exists feed_comment(
        comment_id INT,
        sender_id TEXT,
        sender_name TEXT,
        ref_user_id TEXT,
        ref_user_name TEXT,
        content TEXT,
        create_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED_COMMENT = '''
    insert into feed_comment(comment_id, sender_id, sender_name, ref_user_id, ref_user_name, content, create_time, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_FEED_COMMENT = '''
    create index idxFeedComment on feed_comment (comment_id)
'''

SQL_CREATE_TABLE_LOCATION = '''
    create table if not exists location(
        location_id INT,
        latitude REAL,
        longitude REAL,
        elevation REAL,
        address TEXT,
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_LOCATION = '''
    insert into location(location_id, latitude, longitude, elevation, address, timestamp, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        
SQL_CREATE_INDEX_ON_TABLE_LOCATION = '''
    create index idxLocation on location (location_id)
'''

SQL_CREATE_TABLE_DEAL = '''
    create table if not exists deal(
        deal_id TEXT,
        type INT,
        money TEXT,
        description TEXT,
        remark TEXT,
        status INT,
        create_time INT,
        expire_time INT,
        receive_info TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_DEAL = '''
    insert into deal(deal_id, type, money, description, remark, status, create_time, expire_time, 
                     receive_info, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_DEAL = '''
    create index idxDeal on deal (deal_id)
'''

SQL_CREATE_TABLE_SEARCH = '''
    create table if not exists search(
        account_id TEXT, 
        key TEXT,
        create_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_SEARCH = '''
    insert into search(account_id, key, create_time, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FAVORITE = '''
    create table if not exists favorite(
        account_id TEXT, 
        favorite_id INT,
        type INT,
        talker TEXT,
        talker_name TEXT,
        talker_type INT,
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FAVORITE = '''
    insert into favorite(account_id, favorite_id, type, talker, talker_name, talker_type, 
            timestamp, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FAVORITE_ITEM = '''
    create table if not exists favorite_item(
        favorite_id INT, 
        type INT,
        sender TEXT,
        sender_name TEXT,
        content TEXT,
        media_path TEXT,
        link_id INT,
        location_id INT,
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FAVORITE_ITEM = '''
    insert into favorite_item(favorite_id, type, sender, sender_name,
            content, media_path, link_id, location_id, timestamp, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_FAVORITE_ITEM = '''
    create index idxFavoriteItem on favorite_item (favorite_id)
'''

SQL_CREATE_TABLE_LINK = '''
    create table if not exists link(
        link_id INT,
        url TEXT,
        title TEXT,
        content TEXT,
        image TEXT,
        from_app TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_LINK = '''
    insert into link(link_id, url, title, content, image, from_app, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_LINK = '''
    create index idxLink on link (link_id)
'''

SQL_CREATE_TABLE_BROWSE_HISTORY = '''
    create table if not exists browsehistory(
         account_id TEXT, 
         browse_id TEXT,
         browse_name TEXT,
         lastbrowsetime INT,
         followerscount INT,
         followeingcount INT,
         coverurl TEXT,
         profileurl TEXT,
         verified_reason TEXT,
         description TEXT,
         gender INT,
         source TEXT,
         deleted INT DEFAULT 0, 
         repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_BROWSE_HISTORY = '''
    insert into browsehistory(account_id, browse_id, browse_name, lastbrowsetime, followerscount, followeingcount,
                              coverurl, profileurl, verified_reason, description, gender, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

SQL_CREATE_TABLE_LOGS = '''
    create table if not exists tb_logs(log_id int, log_description text, log_content text, log_result int, log_time int)
'''

SQL_ISNERT_TABLE_LOGS = '''
    insert into tb_logs values(?,?,?,?,?)
'''


g_location_id = 1
g_deal_id = 1
g_link_id = 1
g_feed_like_id = 1
g_feed_comment_id = 1
g_favorite_id = 1

class IM(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
                return False

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        if self.db is not None:
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
            self.db_trans = self.db.BeginTransaction()

            self.db_create_table()
            self.db_commit()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_ACCOUNT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FRIEND
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CHATROOM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CHATROOM_MEMBER
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MESSAGE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FEED
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FEED_LIKE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FEED_COMMENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_LOCATION
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_DEAL
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SEARCH
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FAVORITE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FAVORITE_ITEM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_LINK
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_BROWSE_HISTORY
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_LOGS
            self.db_cmd.ExecuteNonQuery()

    def db_create_index(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_CHATROOM_MEMBER
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_DEAL
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_LOCATION
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_FAVORITE_ITEM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_FEED_COMMENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_FEED_LIKE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_LINK
            self.db_cmd.ExecuteNonQuery()
            self.db_commit()

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_account(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_ACCOUNT, column.get_values())

    def db_insert_table_friend(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FRIEND, column.get_values())

    def db_insert_table_chatroom(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_CHATROOM, column.get_values())

    def db_insert_table_chatroom_member(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_CHATROOM_MEMBER, column.get_values())

    def db_insert_table_message(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_MESSAGE, column.get_values())

    def db_insert_table_feed(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FEED, column.get_values())

    def db_insert_table_feed_like(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FEED_LIKE, column.get_values())

    def db_insert_table_feed_comment(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FEED_COMMENT, column.get_values())

    def db_insert_table_location(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_LOCATION, column.get_values())

    def db_insert_table_deal(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_DEAL, column.get_values())

    def db_insert_table_search(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_SEARCH, column.get_values())

    def db_insert_table_favorite(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FAVORITE, column.get_values())

    def db_insert_table_favorite_item(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FAVORITE_ITEM, column.get_values())

    def db_insert_table_link(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_LINK, column.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    def db_insert_table_log(self, column):
        self.db_insert_table(SQL_ISNERT_TABLE_LOGS, column.get_values())
    '''
    版本检测分为两部分
    如果中间数据库结构改变，会修改db_version
    如果app增加了新的内容，需要修改app_version
    只有db_version和app_version都没有变化时，才不需要重新解析
    '''
    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key,version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))

        while row is not None:
            if row[0] == VERSION_KEY_DB and row[1] == VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == VERSION_KEY_APP and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)


class Column(object):
    def __init__(self):
        self.source = ''
        self.deleted = 0
        self.repeated = 0

    def get_values(self):
        return self.source, self.deleted, self.repeated


class Account(Column):
    def __init__(self):
        super(Account, self).__init__()
        self.account_id = None  # 账户ID[TEXT]
        self.nickname = None  # 昵称[TEXT]
        self.username = None  # 用户名[TEXT]
        self.password = None  # 密码[TEXT]
        self.photo = None  # 头像[TEXT]
        self.telephone = None  # 电话[TEXT]
        self.email = None  # 电子邮件[TEXT]
        self.gender = GENDER_NONE  # 性别[INT]
        self.age = None  # 年龄[INT]
        self.country = None  # 国家[TEXT]
        self.province = None  # 省份[TEXT]
        self.city = None  # 城市[TEXT]
        self.address = None  # 地址[TEXT]
        self.birthday = None  # 生日[INT]
        self.signature = None  # 签名[TEXT]

    def get_values(self):
        return (self.account_id, self.nickname, self.username, self.password, self.photo, self.telephone, self.email, 
                self.gender, self.age, self.country, self.province, self.city, self.address, self.birthday, 
                self.signature) + super(Account, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_account(self)


class Friend(Column):
    def __init__(self):
        super(Friend, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.friend_id = None  # 好友ID[TEXT]
        self.nickname = None  # 昵称[TEXT]
        self.fullname = None  # 全名[TEXT]
        self.remark = None  # 备注[TEXT]
        self.photo = None  # 头像[TEXT]
        self.type = FRIEND_TYPE_NONE  # 类型[INT] FRIEND_TYPE
        self.telephone = None  # 电话[TEXT]
        self.email = None  # 电子邮箱[TEXT]
        self.gender = GENDER_NONE  # 性别[INT]
        self.age = None  # 年龄[INT]
        self.address = None  # 地址[TEXT]
        self.birthday = None  # 生日[INT]
        self.signature = None  # 签名[TEXT]
        self.location_id = 0  # 位置ID[INT]

        self.location_obj = None

    def get_values(self):
        return (self.account_id, self.friend_id, self.nickname, self.fullname, self.remark, self.photo, self.type, self.telephone, 
                self.email, self.gender, self.age, self.address, self.birthday, self.signature, self.location_id) + super(Friend, self).get_values()

    def create_location(self):
        self.location_obj = Location()
        self.location_id = self.location_obj.location_id
        self.location_obj.deleted = self.deleted
        self.location_obj.source = self.source
        return self.location_obj

    def insert_db(self, im):
        if isinstance(im, IM):
            if self.location_id and self.location_id > 0 and self.location_obj:
                self.location_obj.insert_db(im)
            im.db_insert_table_friend(self)


class Chatroom(Column):
    def __init__(self):
        super(Chatroom, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.chatroom_id = None  # 群ID[TEXT]
        self.name = None  # 群名称[TEXT]
        self.photo = None  # 群头像[TEXT]
        self.type = None  # 群类型[INT]
        self.notice = None  # 群声明[TEXT]
        self.description = None  # 群描述[TEXT]
        self.creator_id = None  # 创建者[TEXT]
        self.owner_id = None  # 管理员[TEXT]
        self.member_count = None  # 群成员数量[INT]
        self.max_member_count = None  # 群最大成员数量[INT]
        self.create_time = None  # 创建时间[INT]

    def get_values(self):
        return (self.account_id, self.chatroom_id, self.name, self.photo, self.type, self.notice, self.description, 
                self.creator_id, self.owner_id, self.member_count, self.max_member_count, self.create_time) + super(Chatroom, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_chatroom(self)


class ChatroomMember(Column):
    def __init__(self):
        super(ChatroomMember, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.chatroom_id = None  # 群ID[TEXT]
        self.member_id = None  # 成员ID[TEXT]
        self.display_name = None  # 群内显示名称[TEXT]
        self.photo = None  # 头像[TEXT]
        self.telephone = None  # 电话[TEXT]
        self.email = None  # 电子邮箱[TEXT]
        self.gender = GENDER_NONE  # 性别[INT]
        self.age = None  # 年龄[INT]
        self.address = None  # 地址[TEXT]
        self.birthday = None  # 生日[INT]
        self.signature = None  # 签名[TEXT]

    def get_values(self):
        return (self.account_id, self.chatroom_id, self.member_id, self.display_name, self.photo, self.telephone, 
                self.email, self.gender, self.age, self.address, self.birthday, self.signature) + super(ChatroomMember, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_chatroom_member(self)


class Message(Column):
    def __init__(self):
        super(Message, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.talker_id = None  # 会话ID[TEXT]
        self.talker_name = None  # 会话昵称[TEXT]
        self.sender_id = None  # 发送者ID[TEXT]
        self.sender_name = None  # 发送者昵称[TEXT]
        self.is_sender = None  # 自己是否为发送发[INT]
        self.msg_id = None  # 消息ID[TEXT]
        self.type = None  # 消息类型[INT]，MESSAGE_CONTENT_TYPE
        self.content = None  # 内容[TEXT]
        self.media_path = None  # 媒体文件地址[TEXT]
        self.send_time = None  # 发送时间[INT]
        self.location_id = 0
        self.deal_id = 0
        self.link_id = 0
        self.status = None  # 消息状态[INT]  MESSAGE_STATUS
        self.talker_type = None  # 聊天类型[INT]  CHAT_TYPE

        self.location_obj = None
        self.deal_obj = None
        self.link_obj = None

    def get_values(self):
        return (self.account_id, self.talker_id, self.talker_name, self.sender_id, self.sender_name, 
                self.is_sender, self.msg_id, self.type, self.content, self.media_path, self.send_time, 
                self.location_id, self.deal_id, self.link_id, self.status, self.talker_type) + super(Message, self).get_values()

    def create_location(self):
        self.location_obj = Location()
        self.location_id = self.location_obj.location_id
        self.location_obj.deleted = self.deleted
        self.location_obj.source = self.source
        return self.location_obj

    def create_deal(self):
        self.deal_obj = Deal()
        self.deal_id = self.deal_obj.deal_id
        self.deal_obj.deleted = self.deleted
        self.deal_obj.source = self.source
        return self.deal_obj

    def create_link(self):
        self.link_obj = Link()
        self.link_id = self.link_obj.link_id
        self.link_obj.deleted = self.deleted
        self.link_obj.source = self.source
        return self.link_obj

    def insert_db(self, im):
        if isinstance(im, IM):
            if self.location_id and self.location_id > 0 and self.location_obj:
                self.location_obj.insert_db(im)
            if self.deal_id and self.deal_id > 0 and self.deal_obj:
                self.deal_obj.insert_db(im)
            if self.link_id and self.link_id > 0 and self.link_obj:
                self.link_obj.insert_db(im)
            im.db_insert_table_message(self)


class Feed(Column):
    def __init__(self):
        global g_feed_comment_id, g_feed_like_id
        super(Feed, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.content = None  # 文本[TEXT]
        self.image_path = None  # 图片地址[TEXT]  多个文件以逗号分隔  image_path1,image_path2,...
        self.video_path = None  # 视频地址[TEXT]  多个文件以逗号分隔  video_path1,video_path2,...
        self.url = None  # 链接[TEXT]
        self.url_title = None  # 链接标题[TEXT]
        self.url_desc = None  # 链接描述[TEXT]
        self.send_time = None  # 发布时间[INT]
        self.like_id = g_feed_like_id  # 赞ID[INT]
        g_feed_like_id += 1
        self.likecount = 0  # 赞数量[INT]
        self.rtcount = 0  # 转发数量[INT]
        self.comment_id = g_feed_comment_id  # 评论ID[INT]
        g_feed_comment_id += 1
        self.commentcount = 0  # 评论数量[INT]
        self.device = None  # 设备名称[TEXT]
        self.location_id = 0  # 地址ID[INT]

        self.likes = []
        self.comments = []
        self.location_obj = None
        
    def get_values(self):
        return (self.account_id, self.sender_id, self.content, self.image_path, self.video_path, self.url, 
                self.url_title, self.url_desc, self.send_time, self.like_id, self.likecount, self.rtcount, 
                self.comment_id, self.commentcount, self.device, self.location_id) + super(Feed, self).get_values()
    
    def create_like(self):
        like = FeedLike()
        like.like_id = self.like_id
        like.deleted = self.deleted
        like.source = self.source
        self.likes.append(like)
        return like

    def create_comment(self):
        comment = FeedComment()
        comment.comment_id = self.comment_id
        comment.deleted = self.deleted
        comment.source = self.source
        self.comments.append(comment)
        return comment

    def create_location(self):
        self.location_obj = Location()
        self.location_id = self.location_obj.location_id
        self.location_obj.deleted = self.deleted
        self.location_obj.source = self.source
        return self.location_obj

    def insert_db(self, im):
        if isinstance(im, IM):
            for like in self.likes:
                like.insert_db(im)
            for comment in self.comments:
                comment.insert_db(im)
            if self.location_id and self.location_id > 0 and self.location_obj:
                self.location_obj.insert_db(im)
            im.db_insert_table_feed(self)


class FeedLike(Column):
    def __init__(self):
        super(FeedLike, self).__init__()
        self.like_id = None  # 赞ID[INT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.sender_name = None  # 发布者昵称[TEXT]
        self.create_time = None  # 发布时间[INT]

    def get_values(self):
        return (self.like_id, self.sender_id, self.sender_name, self.create_time) + super(FeedLike, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_feed_like(self)

class FeedComment(Column):
    def __init__(self):
        super(FeedComment, self).__init__()
        self.comment_id = None  # 评论ID[INT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.sender_name = None  # 发布者昵称[TEXT]
        self.ref_user_id = None  # 回复用户ID[TEXT]
        self.ref_user_name = None  # 回复用户昵称[TEXT]
        self.content = None  # 评论内容[TEXT]
        self.create_time = None  # 发布时间[INT]

    def get_values(self):
        return (self.comment_id, self.sender_id, self.sender_name, self.ref_user_id, self.ref_user_name, 
                self.content, self.create_time) + super(FeedComment, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_feed_comment(self)


class Location(Column):
    def __init__(self):
        super(Location, self).__init__()
        global g_location_id
        self.location_id = g_location_id  # 地址ID[INT]
        g_location_id += 1
        self.latitude = None  # 纬度[REAL]
        self.longitude = None  # 经度[REAL]
        self.elevation = None  # 海拔[REAL]
        self.address = None  # 地址名称[TEXT]
        self.timestamp = None  # 时间戳[INT]

    def get_values(self):
        return (self.location_id, self.latitude, self.longitude, self.elevation, self.address, 
                self.timestamp) + super(Location, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_location(self)


class Deal(Column):
    def __init__(self):
        super(Deal, self).__init__()
        global g_deal_id
        self.deal_id = g_deal_id  # 交易ID[TEXT]
        g_deal_id += 1
        self.type = None  # 类型[INT]
        self.money = None  # 金额[TEXT]
        self.description = None  # 描述[TEXT]
        self.remark = None  # 备注[TEXT]
        self.status = None  # 状态[INT]
        self.create_time = None  # 转账时间[INT]
        self.expire_time = None  # 失效时间[INT]
        self.receive_info = None  # 收款信息[TEXT]

    def get_values(self):
        return (self.deal_id, self.type, self.money, self.description, self.remark, self.status,
                self.create_time, self.expire_time, self.receive_info) + super(Deal, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_deal(self)


class Search(Column):
    def __init__(self):
        super(Search, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.key = None  # 搜索关键字[TEXT]
        self.create_time = None  # 搜索时间[INT]

    def get_values(self):
        return (self.account_id, self.key, self.create_time) + super(Search, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_search(self)


class Favorite(Column):
    def __init__(self):
        super(Favorite, self).__init__()
        global g_favorite_id
        self.account_id = None  # 账号ID[TEXT]
        self.favorite_id = g_favorite_id  # ID[INT]
        g_favorite_id += 1
        self.type = None  # 类型[INT] FAVORITE_TYPE
        self.talker = None  # 会话ID[TEXT]
        self.talker_name = None  # 会话昵称[TEXT]
        self.talker_type = None  # 聊天类型[INT] CHAT_TYPE
        self.timestamp = None  # 时间戳[INT]

        self.items = []

    def get_values(self):
        return (self.account_id, self.favorite_id, self.type, self.talker, self.talker_name, self.talker_type,
                self.timestamp) + super(Favorite, self).get_values()

    def create_item(self):
        item = FavoriteItem()
        item.favorite_id = self.favorite_id
        item.deleted = self.deleted
        item.source = self.source
        self.items.append(item)
        return item

    def insert_db(self, im):
        if isinstance(im, IM):
            for item in self.items:
                item.insert_db(im)
            im.db_insert_table_favorite(self)


class FavoriteItem(Column):
    def __init__(self):
        super(FavoriteItem, self).__init__()
        self.favorite_id = 0  # ID[INT]
        self.type = None  # 类型[INT] FAVORITE_TYPE
        self.sender = None  # 发送者[TEXT]
        self.sender_name = None  # 发送者昵称[TEXT]
        self.content = None  # 内容[TEXT]
        self.media_path = None  # 文件路径[TEXT]
        self.link_id = 0  # 链接ID[INT]
        self.location_id = 0  # 地址ID[INT]
        self.timestamp = None  # 时间戳[TEXT]

        self.link_obj = None
        self.location_obj = None

    def get_values(self):
        return (self.favorite_id, self.type, self.sender, self.sender_name, self.content, self.media_path, 
                self.link_id, self.location_id, self.timestamp) + super(FavoriteItem, self).get_values()

    def create_link(self):
        self.link_obj = Link()
        self.link_id = self.link_obj.link_id
        self.link_obj.deleted = self.deleted
        self.link_obj.source = self.source
        return self.link_obj

    def create_location(self):
        self.location_obj = Location()
        self.location_id = self.location_obj.location_id
        self.location_obj.deleted = self.deleted
        self.location_obj.source = self.source
        return self.location_obj

    def insert_db(self, im):
        if isinstance(im, IM):
            if self.link_id and self.link_id > 0 and self.link_obj:
                self.link_obj.insert_db(im)
            if self.location_id and self.location_id > 0 and self.location_obj:
                self.location_obj.insert_db(im)
            im.db_insert_table_favorite_item(self)


class Link(Column):
    def __init__(self):
        super(Link, self).__init__()
        global g_link_id
        self.link_id = g_link_id  # ID[INT]
        g_link_id += 1
        self.url = None  # 网址[TEXT]
        self.title = None  # 标题[TEXT]
        self.content = None  # 描述[TEXT]
        self.image = None  # 图片[TEXT]
        self.from_app = None  # 来自[TEXT]

    def get_values(self):
        return (self.link_id, self.url, self.title, self.content, self.image, self.from_app) + super(Link, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_link(self)


class BrowseHistory(Column):
    def __init__(self):
        super(BrowseHistory, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.browse_id = None  # 被浏览人ID[TEXT]
        self.browse_name = None  # 被浏览人昵称[TEXT]
        self.lastbrowsetime = None  # 最后浏览时间[INT]
        self.followerscount = 0  # 粉丝数量[INT]
        self.followeingcount = 0  # 关注数量[INT]
        self.coverurl = None  # 背景图片[TEXT]
        self.profileurl = None  # 头像[TEXT]
        self.verified_reason = None  # 认证信息[TEXT]
        self.description = None  # 简介[TEXT]
        self.gender = GENDER_NONE  # 性别[INT]
    
    def get_values(self):
        return (self.account_id, self.browse_id, self.browse_name, self.lastbrowsetime, 
                self.followerscount, self.followeingcount, self.coverurl, self.verified_reason, 
                self.description, self.gender) + super(BrowseHistory, self).get_values()


class APPLog(Column):
    def __init__(self):
        super(APPLog, self).__init__()
        self.log_id = str(uuid.uuid1())
        self.log_description = ""
        self.log_content = ""
        self.log_result = -1
        self.log_time = 0
    
    def get_values(self):
        return (self.log_id, self.log_description, self.log_content, self.log_result, self.log_time)


class GenerateModel(object):
    def __init__(self, cache_db, mount_dir=None):
        self.cache_db = cache_db
        self.mount_dir = mount_dir
        self.friends = {}
        self.chatrooms = {}

    def get_models(self):
        models = []

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
        self.db.Open()

        models.extend(self._get_account_models())
        models.extend(self._get_friend_models())
        models.extend(self._get_group_models())
        models.extend(self._get_chat_models())
        models.extend(self._get_feed_models())
        models.extend(self._get_search_models())
        models.extend(self._get_favorite_models())
        models.extend(self._get_browse_history_models())


        self.db.Close()
        return models

    def _get_account_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('account'):
            return []
        models = []

        sql = '''select account_id, nickname, username, password, photo, telephone, email, gender, age, country, 
                        province, city, address, birthday, signature, source, deleted, repeated
                 from account'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    account_id = self._db_reader_get_string_value(r, 0)
                    nickname = self._db_reader_get_string_value(r, 1)
                    photo = self._db_reader_get_string_value(r, 4, None)
                    birthday = self._db_reader_get_int_value(r, 13, None)
                    source = self._db_reader_get_string_value(r, 15)
                    deleted = self._db_reader_get_int_value(r, 16, None)

                    user = Common.User()
                    user.SourceFile.Value = source
                    user.Deleted = self._convert_deleted_status(deleted)
                    user.ID.Value = account_id
                    user.Name.Value = nickname
                    user.Username.Value = self._db_reader_get_string_value(r, 2)
                    user.Password.Value = self._db_reader_get_string_value(r, 3)
                    if photo not in [None, '']:
                        user.PhotoUris.Add(self._get_uri(photo))
                    user.PhoneNumber.Value = self._db_reader_get_string_value(r, 5)
                    user.Email.Value = self._db_reader_get_string_value(r, 6)
                    user.Sex.Value = self._convert_sex_type(self._db_reader_get_int_value(r, 7))
                    user.Age.Value = self._db_reader_get_int_value(r, 8)
                    if birthday:
                        ts = self._get_timestamp(birthday)
                        if ts:
                            user.Birthday.Value = ts
                    user.Signature.Value = self._db_reader_get_string_value(r, 14)

                    address = Contacts.StreetAddress()
                    address.Country.Value = self._db_reader_get_string_value(r, 9)
                    address.Neighborhood.Value = self._db_reader_get_string_value(r, 10)
                    address.City.Value = self._db_reader_get_string_value(r, 11)
                    address.FullName.Value = self._db_reader_get_string_value(r, 12)
                    user.Addresses.Add(address)
                    models.append(user)

                    if account_id is not None:
                        contact = {}
                        contact['user_id'] = account_id
                        if nickname:
                            contact['nickname'] = nickname
                        if photo:
                            contact['photo'] = photo
                        self.friends[self._get_user_key(account_id, account_id)] = contact
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models 

    def _get_friend_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('friend'):
            return []
        models = []

        sql = '''select friend.account_id, friend.friend_id, friend.nickname, friend.remark, friend.photo, 
                        friend.type, friend.telephone, friend.email, friend.gender, friend.age, friend.address, 
                        friend.birthday, friend.signature, friend.location_id, friend.source, friend.deleted, 
                        friend.repeated, friend.fullname,
                        location.latitude, location.longitude, location.elevation, location.address, location.timestamp,
                        location.source, location.deleted
                 from friend
                 left join location on friend.location_id = location.location_id'''

        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    account_id = self._db_reader_get_string_value(r, 0)
                    user_id = self._db_reader_get_string_value(r, 1)
                    nickname = self._db_reader_get_string_value(r, 2)
                    remark = self._db_reader_get_string_value(r, 3)
                    photo = self._db_reader_get_string_value(r, 4, None)
                    birthday = self._db_reader_get_int_value(r, 11, None)
                    location_id = self._db_reader_get_int_value(r, 13)
                    source = self._db_reader_get_string_value(r, 14, None)
                    deleted = self._db_reader_get_int_value(r, 15)
                    location_latitude = self._db_reader_get_float_value(r, 19)
                    location_longitude = self._db_reader_get_float_value(r, 20)
                    location_elevation = self._db_reader_get_float_value(r, 21)
                    location_address = self._db_reader_get_string_value(r, 22)
                    location_timestamp = self._db_reader_get_int_value(r, 23)

                    friend = Common.Friend()
                    friend.SourceFile.Value = source
                    friend.Deleted = self._convert_deleted_status(deleted)
                    friend.OwnerUserID.Value = account_id
                    friend.ID.Value = user_id
                    friend.NickName.Value = nickname
                    friend.Remarks.Value = remark
                    if photo not in [None, '']:
                        friend.PhotoUris.Add(self._get_uri(photo))
                    friend.FriendType.Value = self._convert_friend_type(self._db_reader_get_int_value(r, 5))
                    friend.PhoneNumber.Value = self._db_reader_get_string_value(r, 6)
                    friend.Email.Value = self._db_reader_get_string_value(r, 7)
                    friend.Sex.Value = self._convert_sex_type(self._db_reader_get_int_value(r, 8))
                    friend.Age.Value = self._db_reader_get_int_value(r, 9)
                    address = Contacts.StreetAddress()
                    address.FullName.Value = self._db_reader_get_string_value(r, 10)
                    friend.Addresses.Add(address)
                    if birthday:
                        ts = self._get_timestamp(birthday)
                        if ts:
                            friend.Birthday.Value = ts
                    friend.Signature.Value = self._db_reader_get_string_value(r, 12)
                    if location_id not in [None, 0]:
                        location = self._get_location(location_latitude, location_longitude, location_elevation, location_address, location_timestamp, source, deleted)
                        friend.Location.Value = location
                        models.append(location)
                    friend.FullName.Value = self._db_reader_get_string_value(r, 17)
                    models.append(friend)

                    if account_id not in [None, ''] and user_id not in [None, '']:
                        contact = {}
                        contact['user_id'] = user_id
                        if nickname:
                            contact['nickname'] = nickname
                        if photo:
                            contact['photo'] = photo
                        self.friends[self._get_user_key(account_id, user_id)] = contact
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models 

    def _get_group_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('chatroom'):
            return []
        models = []

        sql = '''select account_id, chatroom_id, name, photo, type, notice, description, creator_id, 
                        owner_id, member_count, max_member_count, create_time, source, deleted, repeated
                 from chatroom'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    account_id = self._db_reader_get_string_value(r, 0)
                    user_id = self._db_reader_get_string_value(r, 1)
                    nickname = self._db_reader_get_string_value(r, 2)
                    photo = self._db_reader_get_string_value(r, 3, None)
                    timestamp = self._db_reader_get_int_value(r, 11)
                    source = self._db_reader_get_string_value(r, 12)
                    deleted = self._db_reader_get_int_value(r, 13, None)
                    
                    group = Common.Group()
                    group.SourceFile.Value = source
                    group.Deleted = self._convert_deleted_status(deleted)
                    group.OwnerUserID.Value = account_id
                    group.ID.Value = user_id
                    group.Name.Value = nickname
                    group.Members.AddRange(self._get_chatroom_member_models(account_id, user_id))
                    if photo not in [None, '']:
                        group.PhotoUris.Add(self._get_uri(photo))
                    group.Status.Value = self._convert_group_status(self._db_reader_get_int_value(r, 4))
                    group.Description.Value = self._db_reader_get_string_value(r, 6)
                    group.Creator.Value = self._get_user_intro(account_id, self._db_reader_get_string_value(r, 7, None))
                    group.Managers.Value = self._get_user_intro(account_id, self._db_reader_get_string_value(r, 8, None))
                    group.MemberCount.Value = self._db_reader_get_int_value(r, 9)
                    group.MemberMaxCount.Value = self._db_reader_get_int_value(r, 10)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            group.JoinTime.Value = ts
                    models.append(group)

                    if account_id not in [None, ''] and user_id not in [None, '']:
                        contact = {}
                        contact['user_id'] = user_id
                        if nickname:
                            contact['nickname'] = nickname
                        if photo:
                            contact['photo'] = photo
                        self.chatrooms[self._get_user_key(account_id, user_id)] = contact
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models 

    def _get_chat_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('message'):
            return []
        chats = {}
        models = []
        sql = '''select message.account_id, message.talker_id, message.talker_name, message.sender_id, message.sender_name, 
                        message.is_sender, message.msg_id, message.type, message.content, message.media_path, message.send_time, 
                        message.location_id, message.deal_id, message.link_id, message.status, message.talker_type, 
                        message.source, message.deleted, message.repeated, 
                        location.latitude, location.longitude, location.elevation, location.address, location.timestamp,
                        location.source, location.deleted,
                        deal.type, deal.money, deal.description, deal.remark, deal.status, deal.create_time, deal.expire_time, 
                        deal.receive_info, deal.source, deal.deleted, 
                        link.url, link.title, link.content, link.image, link.source, link.deleted
                 from message
                 left join location on message.location_id = location.location_id
                 left join deal on message.deal_id = deal.deal_id
                 left join link on message.link_id = link.link_id '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    account_id = self._db_reader_get_string_value(r, 0)
                    talker_id = self._db_reader_get_string_value(r, 1)
                    talker_name = self._db_reader_get_string_value(r, 2, None)
                    sender_id = self._db_reader_get_string_value(r, 3)
                    sender_name = self._db_reader_get_string_value(r, 4, None)
                    msg_type = self._db_reader_get_int_value(r, 7)
                    content = self._db_reader_get_string_value(r, 8)
                    media_path = self._db_reader_get_string_value(r, 9)
                    send_time = self._db_reader_get_int_value(r, 10, None)
                    location_id = self._db_reader_get_int_value(r, 11)
                    deal_id = self._db_reader_get_int_value(r, 12)
                    link_id = self._db_reader_get_int_value(r, 13)
                    talker_type = self._db_reader_get_int_value(r, 15)
                    source = self._db_reader_get_string_value(r, 16)
                    deleted = self._db_reader_get_int_value(r, 17, None)
                    location_latitude = self._db_reader_get_float_value(r, 19)
                    location_longitude = self._db_reader_get_float_value(r, 20)
                    location_elevation = self._db_reader_get_float_value(r, 21)
                    location_address = self._db_reader_get_string_value(r, 22)
                    location_timestamp = self._db_reader_get_int_value(r, 23)
                    deal_money = self._db_reader_get_string_value(r, 27)
                    deal_description = self._db_reader_get_string_value(r, 28)
                    deal_remark = self._db_reader_get_string_value(r, 29)
                    deal_status = self._db_reader_get_int_value(r, 30)
                    deal_create_time = self._db_reader_get_int_value(r, 31)
                    deal_expire_time = self._db_reader_get_int_value(r, 32)
                    deal_receive_info = self._db_reader_get_string_value(r, 33)
                    link_url = self._db_reader_get_string_value(r, 36)
                    link_title = self._db_reader_get_string_value(r, 37)
                    link_desc = self._db_reader_get_string_value(r, 38)
                    link_image_path = self._db_reader_get_string_value(r, 39)

                    message = Common.Message()
                    message.Content.Value = Common.MessageContent()
                    message.SourceFile.Value = source
                    message.Deleted = self._convert_deleted_status(deleted)
                    message.OwnerUserID.Value = account_id
                    message.ID.Value = talker_id
                    message.Sender.Value = self._get_user_intro(account_id, sender_id, sender_name, talker_type==CHAT_TYPE_GROUP)
                    if sender_id == account_id:
                        message.Type.Value = Common.MessageType.Send
                    else:
                        message.Type.Value = Common.MessageType.Receive
                    if send_time:
                        ts = self._get_timestamp(send_time)
                        if ts:
                            message.TimeStamp.Value = ts
                            message.SendTime.Value = ts
                    message.Content.Value.Text.Value = content
                    if msg_type == MESSAGE_CONTENT_TYPE_IMAGE:
                        message.Content.Value.Image.Value = self._get_uri(media_path)
                    elif msg_type == MESSAGE_CONTENT_TYPE_VOICE:
                        message.Content.Value.Audio.Value = self._get_uri(media_path)
                    elif msg_type == MESSAGE_CONTENT_TYPE_VIDEO:
                        message.Content.Value.Video.Value = self._get_uri(media_path)
                    elif msg_type == MESSAGE_CONTENT_TYPE_EMOJI:
                        message.Content.Value.Gif.Value = self._get_uri(media_path)
                    #elif msg_type == MESSAGE_CONTENT_TYPE_CONTACT_CARD:
                    #    pass
                    elif msg_type == MESSAGE_CONTENT_TYPE_LOCATION:
                        if location_id not in [None, 0]:
                            location = self._get_location(location_latitude, location_longitude, location_elevation, location_address, location_timestamp, source, deleted)
                            message.Content.Value.Location.Value = location
                            models.append(location)
                    elif msg_type == MESSAGE_CONTENT_TYPE_RED_ENVELPOE:
                        if deal_id not in [None, 0]:
                            message.Content.Value.RedEnvelope.Value = self._get_aareceipts(deal_money, deal_description, deal_remark, deal_status, deal_create_time, deal_expire_time, deal_receive_info, source, deleted)
                            message.Content.Value.RedEnvelope.Value.OwnerUserID.Value = message.OwnerUserID.Value
                            message.Content.Value.RedEnvelope.Value.OwnerMessage.Value = message
                    elif msg_type == MESSAGE_CONTENT_TYPE_RECEIPT:
                        if deal_id not in [None, 0]:
                            message.Content.Value.Receipt.Value = self._get_receipt(deal_money, deal_description, deal_remark, deal_status, deal_create_time, deal_expire_time, deal_receive_info, source, deleted)
                            message.Content.Value.Receipt.Value.OwnerUserID.Value = message.OwnerUserID.Value
                            message.Content.Value.Receipt.Value.OwnerMessage.Value = message
                    elif msg_type == MESSAGE_CONTENT_TYPE_AA_RECEIPT:
                        if deal_id not in [None, 0]:
                            message.Content.Value.AAReceipts.Value = self._get_aareceipts(deal_money, deal_description, deal_remark, deal_status, deal_create_time, deal_expire_time, deal_receive_info, source, deleted)
                            message.Content.Value.AAReceipts.Value.OwnerUserID.Value = message.OwnerUserID.Value
                            message.Content.Value.AAReceipts.Value.OwnerMessage.Value = message
                    elif msg_type == MESSAGE_CONTENT_TYPE_LINK:
                        if link_id not in [None, 0]:
                            message.Content.Value.Link.Value = self._get_link(link_url, link_title, link_desc, link_image_path, source, deleted)
                    #elif msg_type == MESSAGE_CONTENT_TYPE_VOIP:
                    #    pass
                    elif msg_type == MESSAGE_CONTENT_TYPE_SYSTEM:
                        message.Type.Value = Common.MessageType.System

                    if account_id not in [None, ''] and talker_id not in [None, '']:
                        key = self._get_user_key(account_id, talker_id)
                        if key in chats:
                            chat = chats[key]
                            chat.Messages.Add(message)
                            chat.Count.Value += 1
                            if message.Deleted != DeletedState.Intact:
                                chat.DeletedCount.Value += 1
                            message.OwnerChat.Value = chat
                        else:
                            chat = Generic.Chat()
                            chat.SourceFile.Value = message.SourceFile.Value
                            chat.Deleted = self._convert_deleted_status(0)
                            chat.OwnerUserID.Value = account_id
                            chat.ChatId.Value = talker_id
                            chat.ChatType.Value = self._convert_chat_type(talker_type)
                            if talker_type == CHAT_TYPE_GROUP:
                                if talker_name is not None:
                                    chat.ChatName.Value = talker_name
                                else:
                                    chat.ChatName.Value = self.chatrooms.get(key, {}).get('nickname', '')
                                chat.Participants.AddRange(self._get_chatroom_member_models(account_id, talker_id))
                            else:
                                if talker_name is not None:
                                    chat.ChatName.Value = talker_name
                                else:
                                    chat.ChatName.Value = self.friends.get(key, {}).get('nickname', '')
                                chat.Participants.Add(self._get_user_intro(account_id, talker_id))
                                chat.Participants.Add(self._get_user_intro(account_id, account_id))
                            chat.Messages.Add(message)
                            chat.Count.Value = 1
                            chat.DeletedCount.Value = 0 if message.Deleted == DeletedState.Intact else 1
                            chats[key] = chat
                            message.OwnerChat.Value = chat
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))

        v = chats.values()
        v.extend(models)
        return v

    def _get_chatroom_member_models(self, account_id, chatroom_id):
        if account_id in [None, ''] or chatroom_id in [None, '']:
            return []
        models = []
        sql = '''select account_id, chatroom_id, member_id, display_name, photo, telephone, email, 
                        gender, age, address, birthday, signature, source, deleted, repeated
                 from chatroom_member
                 where account_id='{0}' and chatroom_id='{1}' '''.format(account_id, chatroom_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    member_id = self._db_reader_get_string_value(r, 2)
                    if member_id not in [None, '']:
                        nickname = self._db_reader_get_string_value(r, 3, None)

                        model = self._get_user_intro(account_id, member_id)
                        if nickname not in [None, '']:
                            model.Name.Value = nickname
                        model.SourceFile.Value = self._db_reader_get_string_value(r, 12)
                        model.Deleted = self._convert_deleted_status(self._db_reader_get_int_value(r, 13, None))
                        models.append(model)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models

    def _get_feed_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('feed'):
            return []
        models = []

        sql = '''select feed.account_id, feed.sender_id, feed.content, feed.image_path, feed.video_path, 
                        feed.url, feed.url_title, feed.url_desc, feed.send_time, feed.like_id, feed.likecount, 
                        feed.rtcount, feed.comment_id, feed.commentcount, feed.device, feed.location_id, 
                        feed.source, feed.deleted, feed.repeated,
                        location.latitude, location.longitude, location.elevation, location.address, location.timestamp,
                        location.source, location.deleted
                 from feed
                 left join location on feed.location_id = location.location_id '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    account_id = self._db_reader_get_string_value(r, 0)
                    sender_id = self._db_reader_get_string_value(r, 1)
                    url = self._db_reader_get_string_value(r, 5, None)
                    timestamp = self._db_reader_get_int_value(r, 8, None)
                    like_id = self._db_reader_get_int_value(r, 9)
                    comment_id = self._db_reader_get_int_value(r, 12)
                    location_id = self._db_reader_get_int_value(r, 15)
                    source = self._db_reader_get_string_value(r, 16)
                    deleted = self._db_reader_get_int_value(r, 17, None)
                    location_latitude = self._db_reader_get_float_value(r, 19)
                    location_longitude = self._db_reader_get_float_value(r, 20)
                    location_elevation = self._db_reader_get_float_value(r, 21)
                    location_address = self._db_reader_get_string_value(r, 22)
                    location_timestamp = self._db_reader_get_int_value(r, 23)

                    moment = Common.Moment()
                    moment.Content.Value = Common.MomentContent()
                    moment.SourceFile.Value = source
                    moment.Deleted = self._convert_deleted_status(deleted)
                    moment.OwnerUserID.Value = account_id
                    moment.ID.Value = sender_id
                    moment.Sender.Value = self._get_user_intro(account_id, sender_id)
                    moment.Content.Value.Text.Value = self._db_reader_get_string_value(r, 2)
                    images = self._db_reader_get_string_value(r, 3).split(',')
                    for image in images:
                        if image not in [None, '']:
                            moment.Content.Value.Images.Add(self._get_uri(image))
                    videos = self._db_reader_get_string_value(r, 4).split(',')
                    for video in videos:
                        if video not in [None, '']:
                            moment.Content.Value.Videos.Add(self._get_uri(video))
                    if url not in [None, '']:
                        link = Common.MomentSharp()
                        link.Uri.Value = self._get_uri(url)
                        link.Title.Value = self._db_reader_get_string_value(r, 6)
                        link.Description.Value = self._db_reader_get_string_value(r, 7)
                        moment.Content.Value.Share.Add(link)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            moment.TimeStamp.Value = ts
                    if like_id not in [None, 0]:
                        moment.Likes.AddRange(self._get_feed_likes(account_id, like_id))
                    moment.LikeCount.Value = self._db_reader_get_int_value(r, 10)
                    moment.RtCount.Value = self._db_reader_get_int_value(r, 11)
                    if comment_id not in [None, 0]:
                        moment.Comments.AddRange(self._get_feed_comments(account_id, comment_id))
                    moment.CommentCount.Value = self._db_reader_get_int_value(r, 13)
                    moment.Device.Value = self._db_reader_get_string_value(r, 14)
                    if location_id not in [None, 0]:
                        location = self._get_location(location_latitude, location_longitude, location_elevation, location_address, location_timestamp, source, deleted)
                        moment.Location.Value = location
                    models.append(moment)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models 

    def _get_search_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('search'):
            return []
        models = []

        sql = '''select account_id, key, create_time, source, deleted, repeated
                 from search'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    timestamp = self._db_reader_get_int_value(r, 2, None)

                    search = SearchedItem()
                    search.SourceFile.Value = self._db_reader_get_string_value(r, 3)
                    search.Deleted = self._convert_deleted_status(self._db_reader_get_int_value(r, 4, None))
                    search.OwnerUserID.Value = self._db_reader_get_string_value(r, 0)
                    search.Value.Value = self._db_reader_get_string_value(r, 1)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            search.TimeStamp.Value = ts
                    models.append(search)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models 

    def _get_favorite_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('favorite'):
            return []
        models = []

        sql = '''select account_id, favorite_id, type, talker, talker_name, talker_type,
                        timestamp, source, deleted, repeated
                 from favorite'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    account_id = self._db_reader_get_string_value(r, 0)
                    favorite_id = self._db_reader_get_int_value(r, 1)
                    talker_id = self._db_reader_get_string_value(r, 3)
                    talker_name = self._db_reader_get_string_value(r, 4, None)
                    talker_type = self._db_reader_get_int_value(r, 5)
                    timestamp = self._db_reader_get_int_value(r, 6, None)
                    source = self._db_reader_get_string_value(r, 7)
                    deleted = self._db_reader_get_int_value(r, 8, None)

                    favorite = Common.Collection()
                    favorite.SourceFile.Value = source
                    favorite.Deleted = self._convert_deleted_status(deleted)
                    favorite.OwnerUserID.Value = account_id
                    if favorite_id not in [None, 0]:
                        favorite.Content.AddRange(self._get_favorite_item_models(favorite_id, account_id))
                    favorite.From.Value = self._get_user_intro(account_id, talker_id, talker_name, talker_type == CHAT_TYPE_GROUP)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            favorite.CreateTime.Value = ts
                    models.append(favorite)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models 

    def _get_favorite_item_models(self, favorite_id, account_id):
        if canceller.IsCancellationRequested:
            return []
        models = []

        sql = '''select favorite_item.favorite_id, favorite_item.type, favorite_item.sender, favorite_item.sender_name, 
                        favorite_item.content, favorite_item.media_path, favorite_item.link_id, favorite_item.location_id,
                        favorite_item.timestamp, favorite_item.source, favorite_item.deleted, favorite_item.repeated,
                        location.latitude, location.longitude, location.elevation, location.address, location.timestamp,
                        location.source, location.deleted,
                        link.url, link.title, link.content, link.image, link.source, link.deleted
                 from favorite_item
                 left join location on favorite_item.location_id = location.location_id
                 left join link on favorite_item.link_id = link.link_id
                 where favorite_item.favorite_id = {} '''.format(favorite_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    fav_type = self._db_reader_get_int_value(r, 1)
                    sender_id = self._db_reader_get_string_value(r, 2)
                    sender_name = self._db_reader_get_string_value(r, 3, None)
                    media_path = self._db_reader_get_string_value(r, 5)
                    link_id = self._db_reader_get_int_value(r, 6)
                    location_id = self._db_reader_get_int_value(r, 7)
                    timestamp = self._db_reader_get_int_value(r, 8, None)
                    source = self._db_reader_get_string_value(r, 9)
                    deleted = self._db_reader_get_int_value(r, 10, None)
                    location_latitude = self._db_reader_get_float_value(r, 12)
                    location_longitude = self._db_reader_get_float_value(r, 13)
                    location_elevation = self._db_reader_get_float_value(r, 14)
                    location_address = self._db_reader_get_string_value(r, 15)
                    location_timestamp = self._db_reader_get_int_value(r, 16)
                    link_url = self._db_reader_get_string_value(r, 19)
                    link_title = self._db_reader_get_string_value(r, 20)
                    link_desc = self._db_reader_get_string_value(r, 21)
                    link_image_path = self._db_reader_get_string_value(r, 22)

                    message = Common.Message()
                    message.Content.Value = Common.MessageContent()
                    message.OwnerUserID.Value = account_id
                    message.SourceFile.Value = source
                    message.Deleted = self._convert_deleted_status(deleted)
                    message.Sender.Value = self._get_user_intro(account_id, sender_id, sender_name)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            message.TimeStamp.Value = ts

                    message.Content.Value.Text.Value = self._db_reader_get_string_value(r, 4)
                    if fav_type == FAVORITE_TYPE_IMAGE:
                        message.Content.Value.Image.Value = self._get_uri(media_path)
                    elif fav_type == FAVORITE_TYPE_VOICE:
                        message.Content.Value.Audio.Value = self._get_uri(media_path)
                    elif fav_type == FAVORITE_TYPE_VIDEO:
                        message.Content.Value.Video.Value = self._get_uri(media_path)
                    elif fav_type == FAVORITE_TYPE_LINK:
                        if link_id not in [None, 0]:
                            message.Content.Value.Link.Value = self._get_link(link_url, link_title, link_desc, link_image_path, source, deleted)
                    elif fav_type == FAVORITE_TYPE_LOCATION:
                        if location_id not in [None, 0]:
                            location = self._get_location(location_latitude, location_longitude, location_elevation, location_address, location_timestamp, source, deleted)
                            message.Content.Value.Location.Value = location
                    elif fav_type == FAVORITE_TYPE_ATTACHMENT:
                        message.Content.Value.File.Value = self._get_uri(media_path)

                    models.append(message)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models

    def _get_browse_history_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('browsehistory'):
            return []
        models = []

        sql = '''select account_id, browse_id, browse_name, lastbrowsetime, followerscount, followeingcount,
                        coverurl, profileurl, verified_reason, description, gender, source, deleted, repeated
                 from browsehistory'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    timestamp = self._db_reader_get_int_value(r, 3, None)
                    cover = self._db_reader_get_string_value(r, 6, None)
                    photo = self._db_reader_get_string_value(r, 7, None)
                    source = self._db_reader_get_string_value(r, 11)
                    deleted = self._db_reader_get_int_value(r, 12)

                    history = Generic.VisitedPage()
                    history.SourceFile.Value = source
                    history.Deleted = self._convert_deleted_status(deleted)
                    history.OwnerUserID.Value = self._db_reader_get_string_value(r, 0)
                    history.ID.Value = self._db_reader_get_string_value(r, 1)
                    history.NickName.Value = self._db_reader_get_string_value(r, 2)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            history.LastVisited.Value = ts
                    history.FansCount.Value = self._db_reader_get_int_value(r, 4)
                    history.FocusCount.Value = self._db_reader_get_int_value(r, 5)
                    if cover not in [None, '']:
                        history.CoverUris.Add(self._get_uri(cover))
                    if photo not in [None, '']:
                        history.PhotoUris.Add(self._get_uri(photo))
                    history.VerifiedReason.Value = self._db_reader_get_string_value(r, 8)
                    history.Description.Value = self._db_reader_get_string_value(r, 9)
                    history.Sex.Value = self._convert_sex_type(self._db_reader_get_int_value(r, 10))
                    models.append(history)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models 

    def _db_has_table(self, table_name):
        try:
            sql = "select count(*) from sqlite_master where type='table' and name='{}' ".format(table_name)
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            r.Read()
            if r and self._db_reader_get_int_value(r, 0) >= 1:
                return True
            else:
                return False
        except Exception as e:
            return False

    def _get_user_key(self, account_id, user_id):
        return account_id + "#*#" + user_id

    def _get_user_intro(self, account_id, user_id, user_name=None, is_group=False):
        user = Common.UserIntro()
        user.ID.Value = user_id

        if account_id is not None and user_id is not None:
            key = self._get_user_key(account_id, user_id)
            contact = None
            if is_group:
                contact = self.chatrooms.get(key)
            else:
                contact = self.friends.get(key)
            
            if contact is not None:
                user.Name.Value = contact.get('nickname', '')
                photo = contact.get('photo', '')
                if len(photo) > 0:
                    user.Photo.Value = self._get_uri(photo)

            if user_name is not None:
                user.Name.Value = user_name
        return user

    def _get_timestamp(self, timestamp):
        try:
            ts = TimeStamp.FromUnixTime(timestamp, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except Exception as e:
            return None

    def _get_uri(self, path):
        if path.startswith('http') or len(path) == 0:
            return ConvertHelper.ToUri(path)
        else:
            if self.mount_dir:
                return ConvertHelper.ToUri(self.mount_dir + path.replace("/", "\\"))
            else:
                return ConvertHelper.ToUri(path)

    def _get_feed_likes(self, account_id, like_id):
        models = []
        sql = '''select sender_id, sender_name, create_time, source, deleted, repeated
                 from feed_like
                 where like_id='{0}' '''.format(like_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    sender_id = self._db_reader_get_string_value(r, 0)
                    sender_name = self._db_reader_get_string_value(r, 1, None)
                    timestamp = self._db_reader_get_int_value(r, 2, None)
                    source = self._db_reader_get_string_value(r, 3)
                    deleted = self._db_reader_get_int_value(r, 4, None)
                    
                    like = Common.MomentLike()
                    like.User.Value = self._get_user_intro(account_id, sender_id, sender_name)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            like.TimeStamp.Value = ts
                    like.SourceFile.Value = source
                    like.Deleted = self._convert_deleted_status(deleted)
                    models.append(like)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models

    def _get_feed_comments(self, account_id, comment_id):
        models = []
        sql = '''select sender_id, sender_name, ref_user_id, ref_user_name, content, create_time, source, deleted, repeated
                 from feed_comment
                 where comment_id='{0}' '''.format(comment_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    sender_id = self._db_reader_get_string_value(r, 0)
                    sender_name = self._db_reader_get_string_value(r, 1, None)
                    ref_user_id = self._db_reader_get_string_value(r, 2, None)
                    ref_user_name = self._db_reader_get_string_value(r, 3, None)
                    timestamp = self._db_reader_get_int_value(r, 5, None)
                    source = self._db_reader_get_string_value(r, 6)
                    deleted = self._db_reader_get_int_value(r, 7, None)


                    comment = Common.MomentComment()
                    if sender_id not in [None, '']:
                        comment.Sender.Value = self._get_user_intro(account_id, sender_id, sender_name)
                    if ref_user_id not in [None, '']:
                        comment.Receiver.Value = self._get_user_intro(account_id, ref_user_id, ref_user_name)
                    comment.Content.Value = self._db_reader_get_string_value(r, 4)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            comment.TimeStamp.Value = ts
                    comment.SourceFile.Value = source
                    comment.Deleted = self._convert_deleted_status(deleted)
                    models.append(comment)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models

    def _get_location(self, latitude, longitude, elevation, address, timestamp, source, deleted):
        location = Locations.Location()
        location.Position.Value = Locations.Coordinate()
        if latitude:
            location.Position.Value.Latitude.Value = latitude
        if longitude:
            location.Position.Value.Longitude.Value = longitude
        if elevation:
            location.Position.Value.Elevation.Value = elevation
        if address:
            location.Position.Value.PositionAddress.Value = address
        if timestamp:
            ts = self._get_timestamp(timestamp)
            if ts:
                location.TimeStamp.Value = ts
        if source not in [None, '']:
            location.SourceFile.Value = source
        if deleted is not None:
            location.Deleted = self._convert_deleted_status(deleted)
        return location

    def _get_receipt(self, money, description, remark, status, create_time, expire_time, receive_info, source, deleted):
        receipt = Common.Receipt()
        if money:
            receipt.Money.Value = money
        if description:
            receipt.Description.Value = description
        if remark:
            receipt.Remarks.Value = remark
        if status:
            receipt.Status.Value = self._convert_receipt_status(status)
        if create_time:
            ts = self._get_timestamp(create_time)
            if ts:
                receipt.Timestamp.Value = ts
        if expire_time:
            ts = self._get_timestamp(expire_time)
            if ts:
                receipt.ExpireTime.Value = ts
        if source not in [None, '']:
            receipt.SourceFile.Value = source
        if deleted is not None:
            receipt.Deleted = self._convert_deleted_status(deleted)
        return receipt

    def _get_aareceipts(self, money, description, remark, status, create_time, expire_time, receive_info, source, deleted):
        receipt = Common.AAReceipts()
        if money:
            receipt.TotalMoney.Value = money
        if description:
            receipt.Description.Value = description
        if remark:
            receipt.Remarks.Value = remark
        #if status:
        #    receipt.Status.Value = self._convert_receipt_status(status)
        if create_time:
            ts = self._get_timestamp(create_time)
            if ts:
                receipt.Timestamp.Value = ts
        if expire_time:
            ts = self._get_timestamp(expire_time)
            if ts:
                receipt.ExpireTime.Value = ts
        if source not in [None, '']:
            receipt.SourceFile.Value = source
        if deleted is not None:
            receipt.Deleted = self._convert_deleted_status(deleted)
        return receipt

    def _get_link(self, url, title, desc, image_path, source, deleted):
        link = Common.MomentSharp()
        if url:
            link.Uri.Value = self._get_uri(url)
        if title:
            link.Title.Value = title
        if desc:
            link.Description.Value = desc
        if image_path:
            link.ImageUri.Value = self._get_uri(image_path)
        if source:
            link.SourceFile.Value = source
        if deleted is not None:
            link.Deleted = self._convert_deleted_status(deleted)
        return link

    @staticmethod
    def _convert_friend_type(friend_type):
        if friend_type == FRIEND_TYPE_FRIEND:
            return Common.FriendType.Friend
        elif friend_type == FRIEND_TYPE_GROUP_FRIEND:
            return Common.FriendType.GroupFriend
        elif friend_type == FRIEND_TYPE_FANS:
            return Common.FriendType.Fans
        elif friend_type == FRIEND_TYPE_FOLLOW:
            return Common.FriendType.Follow
        elif friend_type == FRIEND_TYPE_SPECAIL_FOLLOW:
            return Common.FriendType.SpecialFollow
        elif friend_type == FRIEND_TYPE_MUTUAL_FOLLOW:
            return Common.FriendType.MutualFollow
        elif friend_type == FRIEND_TYPE_RECENT:
            return Common.FriendType.Recent
        elif friend_type == FRIEND_TYPE_SUBSCRIBE:
            return Common.FriendType.Subscribe
        elif friend_type == FRIEND_TYPE_STRANGER:
            return Common.FriendType.Stranger
        elif friend_type == FRIEND_TYPE_SHOP:
            return Common.FriendType.Shop
        else:
            return Common.FriendType.None

    @staticmethod
    def _convert_sex_type(sex_type):
        if sex_type == GENDER_MALE:
            return Common.SexType.Men
        elif sex_type == GENDER_FEMALE:
            return Common.SexType.Women
        else:
            return Common.SexType.None

    @staticmethod
    def _convert_group_status(chatroom_type):
        if chatroom_type == CHATROOM_TYPE_TEMP:
            return Common.GroupStatus.Temp
        else:
            return Common.GroupStatus.Normal

    @staticmethod
    def _convert_chat_type(chat_type):
        if chat_type == CHAT_TYPE_FRIEND:
            return Common.ChatType.Friend
        elif chat_type == CHAT_TYPE_GROUP:
            return Common.ChatType.Group
        elif chat_type == CHAT_TYPE_SYSTEM:
            return Common.ChatType.System
        elif chat_type == CHAT_TYPE_OFFICIAL:
            return Common.ChatType.Official
        elif chat_type == CHAT_TYPE_SUBSCRIBE:
            return Common.ChatType.Subscribe
        elif chat_type == CHAT_TYPE_SHOP:
            return Common.ChatType.Shop
        else:
            return Common.ChatType.None

    @staticmethod
    def _convert_receipt_status(status):
        if status == RECEIPT_STATUS_RECEIVE:
            return Common.ReceiptStatus.Receive
        elif status == RECEIPT_STATUS_EXPIRE:
            return Common.ReceiptStatus.Expire
        else:
            return Common.ReceiptStatus.UnReceive

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_float_value(reader, index, default_value=0):
        return reader.GetFloat(index) if not reader.IsDBNull(index) else default_value