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
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite

import os
import sqlite3
import json
import uuid

VERSION_VALUE_DB = 4

GENDER_NONE = 0
GENDER_MALE = 1
GENDER_FEMALE = 2

FRIEND_TYPE_FRIEND = 1
FRIEND_TYPE_GROUP_FRIEND = 2
FRIEND_TYPE_FANS = 3
FRIEND_TYPE_FOLLOW = 4
FRIEND_TYPE_SPECAIL_FOLLOW = 5
FRIEND_TYPE_MUTUAL_FOLLOW = 6
FRIEND_TYPE_RECENT = 7
FRIEND_TYPE_SUBSCRIBE = 8
FRIEND_TYPE_STRANGER = 9

CHATROOM_TYPE_NORMAL = 1  # 普通群
CHATROOM_TYPE_TEMP = 2  # 临时群

CHAT_TYPE_FRIEND = 1  # 好友聊天
CHAT_TYPE_GROUP = 2  # 群聊天
CHAT_TYPE_SYSTEM = 3  # 系统消息
CHAT_TYPE_OFFICIAL = 4  # 公众号
CHAT_TYPE_SUBSCRIBE = 5  # 订阅号

MESSAGE_TYPE_SEND = 1
MESSAGE_TYPE_RECEIVE = 2

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
        birthday TEXT, 
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
        remark TEXT,
        photo TEXT, 
        type INT,
        telephone TEXT, 
        email TEXT, 
        gender INT, 
        age INT, 
        address TEXT, 
        birthday TEXT, 
        signature TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FRIEND = '''
    insert into friend(account_id, friend_id, nickname, remark, photo, type, telephone, email, gender, 
                       age, address, birthday, signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

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
        birthday TEXT, 
        signature TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_CHATROOM_MEMBER = '''
    insert into chatroom_member(account_id, chatroom_id, member_id, display_name, photo, telephone, 
                                email, gender, age, address, birthday, signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

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
        extra_id TEXT,
        status INT,
        talker_type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_MESSAGE = '''
    insert into message(account_id, talker_id, talker_name, sender_id, sender_name, is_sender, msg_id, type, content, 
                        media_path, send_time, extra_id, status, talker_type, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED = '''
    create table if not exists feed(
        account_id TEXT, 
        sender_id TEXT,
        type INT,
        content TEXT,
        media_path TEXT,
        urls TEXT,
        preview_urls TEXT,
        attachment_title TEXT,
        attachment_link TEXT,
        attachment_desc TEXT,
        send_time INT,
        likes TEXT,
        comments TEXT,
        location TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED = '''
    insert into feed(account_id, sender_id, type, content, media_path, urls, preview_urls, attachment_title, 
                     attachment_link, attachment_desc, send_time, likes, comments, location, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED_LIKE = '''
    create table if not exists feed_like(
        like_id TEXT primary key,
        sender_id TEXT,
        sender_name TEXT,
        create_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED_LIKE = '''
    insert into feed_like(like_id, sender_id, sender_name, create_time, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED_COMMENT = '''
    create table if not exists feed_comment(
        comment_id TEXT primary key,
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

SQL_CREATE_TABLE_LOCATION = '''
    create table if not exists location(
        location_id TEXT primary key,
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
        
SQL_CREATE_TABLE_DEAL = '''
    create table if not exists deal(
        deal_id TEXT primary key,
        type INT,
        money TEXT,
        description TEXT,
        remark TEXT,
        status INT,
        expire_time INT,
        receive_info TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_DEAL = '''
    insert into deal(deal_id, type, money, description, remark, status, expire_time, 
                     receive_info, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

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

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


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
                print('model_im db_create() remove %s error: %s' % (db_path, e))

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
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
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()

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

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

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
            pass

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
        self.birthday = None  # 生日[TEXT]
        self.signature = None  # 签名[TEXT]

    def get_values(self):
        return (self.account_id, self.nickname, self.username, self.password, self.photo, self.telephone, self.email, 
                self.gender, self.age, self.country, self.province, self.city, self.address, self.birthday, 
                self.signature) + super(Account, self).get_values()


class Friend(Column):
    def __init__(self):
        super(Friend, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.friend_id = None  # 好友ID[TEXT]
        self.nickname = None  # 昵称[TEXT]
        self.remark = None  # 备注[TEXT]
        self.photo = None  # 头像[TEXT]
        self.type = None  # 类型[INT]
        self.telephone = None  # 电话[TEXT]
        self.email = None  # 电子邮箱[TEXT]
        self.gender = GENDER_NONE  # 性别[INT]
        self.age = None  # 年龄[INT]
        self.address = None  # 地址[TEXT]
        self.birthday = None  # 生日[TEXT]
        self.signature = None  # 签名[TEXT]

    def get_values(self):
        return (self.account_id, self.friend_id, self.nickname, self.remark, self.photo, self.type, self.telephone, 
                self.email, self.gender, self.age, self.address, self.birthday, self.signature) + super(Friend, self).get_values()


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
        self.birthday = None  # 生日[TEXT]
        self.signature = None  # 签名[TEXT]

    def get_values(self):
        return (self.account_id, self.chatroom_id, self.member_id, self.display_name, self.photo, self.telephone, 
                self.email, self.gender, self.age, self.address, self.birthday, self.signature) + super(ChatroomMember, self).get_values()


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
        self.extra_id = None  # 扩展ID[TEXT] 地址类型指向location_id、交易类型指向deal_id
        self.status = None  # 消息状态[INT]，MESSAGE_STATUS
        self.talker_type = None  # 聊天类型[INT]，CHAT_TYPE

    def get_values(self):
        return (self.account_id, self.talker_id, self.talker_name, self.sender_id, self.sender_name, 
                self.is_sender, self.msg_id, self.type, self.content, self.media_path, self.send_time, 
                self.extra_id, self.status, self.talker_type) + super(Message, self).get_values()


class Feed(Column):
    def __init__(self):
        super(Feed, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.type = None  # 动态类型[INT]
        self.content = None  # 动态内容[TEXT]
        self.media_path = None  # 媒体文件地址[TEXT]
        self.urls = None  # 链接地址[json TEXT] json string ['url1', 'url2'...]
        self.preview_urls = None  # 预览地址[json TEXT] json string ['url1', 'url2'...]
        self.attachment_title = None  # 附件标题[TEXT]
        self.attachment_link = None  # 附件链接[TEXT]
        self.attachment_desc = None  # 附件描述[TEXT]
        self.send_time = None  # 发布时间[INT]
        self.likes = None  # 赞[TEXT] 逗号分隔like_id 例如：like_id,like_id,like_id,...
        self.comments = None  # 评论[TEXT] 逗号分隔comment_id 例如：comment_id,comment_id,comment_id,...
        self.location = None  # 地址ID[TEXT]
        
    def get_values(self):
        return (self.account_id, self.sender_id, self.type, self.content, self.media_path, self.urls, self.preview_urls, 
                self.attachment_title, self.attachment_link, self.attachment_desc, self.send_time, self.likes, self.comments, 
                self.location) + super(Feed, self).get_values()
    

class FeedLike(Column):
    def __init__(self):
        super(FeedLike, self).__init__()
        self.like_id = str(uuid.uuid1())  # 赞ID[TEXT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.sender_name = None  # 发布者昵称[TEXT]
        self.create_time = None  # 发布时间[INT]

    def get_values(self):
        return (self.like_id, self.sender_id, self.sender_name, self.create_time) + super(FeedLike, self).get_values()


class FeedComment(Column):
    def __init__(self):
        super(FeedComment, self).__init__()
        self.comment_id = str(uuid.uuid1())  # 评论ID[TEXT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.sender_name = None  # 发布者昵称[TEXT]
        self.ref_user_id = None  # 回复用户ID[TEXT]
        self.ref_user_name = None  # 回复用户昵称[TEXT]
        self.content = None  # 评论内容[TEXT]
        self.create_time = None  # 发布时间[INT]

    def get_values(self):
        return (self.comment_id, self.sender_id, self.sender_name, self.ref_user_id, self.ref_user_name, 
                self.content, self.create_time) + super(FeedComment, self).get_values()


class Location(Column):
    def __init__(self):
        super(Location, self).__init__()
        self.location_id = str(uuid.uuid1())  # 地址ID[TEXT]
        self.latitude = None  # 纬度[REAL]
        self.longitude = None  # 经度[REAL]
        self.elevation = None  # 海拔[REAL]
        self.address = None  # 地址名称[TEXT]
        self.timestamp = None  # 时间戳[TEXT]

    def get_values(self):
        return (self.location_id, self.latitude, self.longitude, self.elevation, self.address, 
                self.timestamp) + super(Location, self).get_values()


class Deal(Column):
    def __init__(self):
        super(Deal, self).__init__()
        self.deal_id = str(uuid.uuid1())  # 交易ID[TEXT]
        self.type = None  # 类型[INT]
        self.money = None  # 金额[TEXT]
        self.description = None  # 描述[TEXT]
        self.remark = None  # 备注[TEXT]
        self.status = None  # 状态[INT]
        self.expire_time = None  # 失效时间[INT]
        self.receive_info = None  # 收款信息[TEXT]

    def get_values(self):
        return (self.deal_id, self.type, self.money, self.description, self.remark, self.status,
                self.expire_time, self.receive_info) + super(Deal, self).get_values()


class Search(Column):
    def __init__(self):
        super(Search, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.key = None  # 搜索关键字[TEXT]
        self.create_time = None  # 搜索时间[INT]

    def get_values(self):
        return (self.account_id, self.key, self.create_time) + super(Search, self).get_values()


class GenerateModel(object):
    def __init__(self, cache_db):
        self.cache_db = cache_db
        self.friends = {}
        self.chatrooms = {}

    def get_models(self):
        models = []

        self.db = sqlite3.connect(self.cache_db)
        self.cursor = self.db.cursor()

        models.extend(self._get_account_models())
        models.extend(self._get_friend_models())
        models.extend(self._get_group_models())
        models.extend(self._get_chat_models())
        models.extend(self._get_feed_models())
        models.extend(self._get_search_models())

        self.cursor.close()
        self.db.close()
        return models

    def _get_account_models(self):
        models = []

        sql = '''select account_id, nickname, username, password, photo, telephone, email, gender, age, country, 
                        province, city, address, birthday, signature, source, deleted, repeated
                 from account'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            user = Common.User()
            account_id = None
            contact = {}
            if row[15] not in [None, '']:
                user.SourceFile.Value = row[15]
            if row[16]:
                user.Deleted = self._convert_deleted_status(row[16])
            if row[0]:
                user.ID.Value = row[0]
                account_id = row[0]
                contact['user_id'] = row[0]
            if row[1]:
                user.Name.Value = row[1]
                contact['nickname'] = row[1]
            if row[2]:
                user.Username.Value = row[2]
            if row[3]:
                user.Password.Value = row[3]
            if row[5]:
                user.PhoneNumber.Value= row[5]
            if row[4] and len(row[4]) > 0:
                user.PhotoUris.Add(self._get_uri(row[4]))
                contact['photo'] = row[4]
            if row[6]:
                user.Email.Value = row[6]
            if row[7]:
                user.Sex.Value = self._convert_sex_type(row[7])
            if row[8]:
                user.Age.Value = row[8]
            if row[13]:
                user.Birthday.Value = row[13]
            if row[14]:
                user.Signature.Value = row[14]
            address = Contacts.StreetAddress()
            if row[9]:
                address.Country.Value = row[9]
            if row[10]:
                address.Neighborhood.Value = row[10]
            if row[11]:
                address.City.Value = row[11]
            if row[12]:
                address.FullName.Value = row[12]
            user.Addresses.Add(address)
            models.append(user)

            if account_id is not None:
                self.friends[self._get_user_key(account_id, account_id)] = contact

            row = self.cursor.fetchone()

        return models 

    def _get_friend_models(self):
        models = []

        sql = '''select account_id, friend_id, nickname, remark, photo, type, telephone, email, gender, 
                        age, address, birthday, signature, source, deleted, repeated
                 from friend'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            friend = Common.Friend()
            account_id = None
            user_id = None
            contact = {}
            if row[13] not in [None, '']:
                friend.SourceFile.Value = row[13]
            if row[14]:
                friend.Deleted = self._convert_deleted_status(row[14])
            if row[0]:
                friend.OwnerUserID.Value = row[0]
                account_id = row[0]
            if row[1]:
                friend.ID.Value = row[1]
                user_id = row[1]
                contact['user_id'] = row[1]
            if row[2]:
                friend.NickName.Value = row[2]
                contact['nickname'] = row[2]
            if row[4] and len(row[4]) > 0:
                friend.PhotoUris.Add(self._get_uri(row[4]))
                contact['photo'] = row[4]
            if row[3]:
                friend.Remarks.Value = row[3]
            if row[5]:
                friend.FriendType.Value = self._convert_friend_type(row[5])
            if row[6]:
                friend.PhoneNumber.Value= row[6]
            if row[7]:
                friend.Email.Value= row[7]
            if row[8]:
                friend.Sex.Value = self._convert_sex_type(row[8])
            if row[9]:
                friend.Age.Value = row[9]
            if row[12]:
                friend.Signature.Value = row[12]
            address = Contacts.StreetAddress()
            if row[10]:
                address.FullName.Value = row[10]
            friend.Addresses.Add(address)
            models.append(friend)

            if account_id is not None and user_id is not None:
                self.friends[self._get_user_key(account_id, user_id)] = contact

            row = self.cursor.fetchone()

        return models 

    def _get_group_models(self):
        models = []

        sql = '''select account_id, chatroom_id, name, photo, type, notice, description, creator_id, 
                        owner_id, member_count, max_member_count, create_time, source, deleted, repeated
                 from chatroom'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            group = Common.Group()
            account_id = None
            user_id = None
            contact = {}
            if row[12] not in [None, '']:
                group.SourceFile.Value = row[12]
            if row[13]:
                group.Deleted = self._convert_deleted_status(row[13])
            if row[0]:
                group.OwnerUserID.Value = row[0]
                account_id = row[0]
            if row[1]:
                group.ID.Value = row[1]
                user_id = row[1]
                contact['user_id'] = row[1]
                group.Members.AddRange(self._get_chatroom_member_models(account_id, user_id))
            if row[2]:
                group.Name.Value = row[2]
                contact['nickname'] = row[2]
            if row[3] and len(row[3]) > 0:
                group.PhotoUris.Add(self._get_uri(row[3]))
                contact['photo'] = row[3]
            if row[4]:
                group.Status.Value = self._convert_group_status(row[4])
            if row[6]:
                group.Description.Value = row[6]
            if row[7]:
                group.Creator.Value = self._get_user_intro(account_id, row[7])
            if row[8]:
                group.Managers.Value = self._get_user_intro(account_id, row[8])
            if row[9]:
                group.MemberCount.Value = row[9]
            if row[10]:
                group.MemberMaxCount.Value = row[10]
            if row[11]:
                ts = self._get_timestamp(row[11])
                if ts:
                    group.JoinTime.Value = ts
            models.append(group)

            if account_id is not None and user_id is not None:
                self.chatrooms[self._get_user_key(account_id, user_id)] = contact

            row = self.cursor.fetchone()

        return models 

    def _get_chat_models(self):
        chats = {}

        sql = '''select account_id, talker_id, talker_name, sender_id, sender_name, is_sender, msg_id, type, 
                        content, media_path, send_time, extra_id, status, talker_type, source, deleted, repeated
                 from message'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            message = Common.Message()
            message.Content.Value = Common.MessageContent()
            account_id = None
            talker_id = None
            talker_name = None
            talker_type = row[13]

            if row[14] not in [None, '']:
                message.SourceFile.Value = row[14]
            if row[15]:
                message.Deleted = self._convert_deleted_status(row[15])
            if row[0]:
                message.OwnerUserID.Value = row[0]
                account_id = row[0]
            if row[1]:
                message.ID.Value = row[1]
                talker_id = row[1]
            if row[2]:
                talker_name = row[2]
            if row[3]:
                message.Sender.Value = self._get_user_intro(account_id, row[3], row[4], talker_type==CHAT_TYPE_GROUP)
                if row[3] == account_id:
                    message.Type.Value = Common.MessageType.Send
                else:
                    message.Type.Value = Common.MessageType.Receive
            if row[10]:
                ts = self._get_timestamp(row[10])
                if ts:
                    message.TimeStamp.Value = ts
                    message.SendTime.Value = ts

            msg_type = row[7]
            content = row[8]
            if content is None:
                content = ''
            media_path = row[9]

            message.Content.Value.Text.Value = content
            if msg_type == MESSAGE_CONTENT_TYPE_IMAGE:
                if media_path and len(media_path) > 0:
                    message.Content.Value.Image.Value = self._get_uri(media_path)
            elif msg_type == MESSAGE_CONTENT_TYPE_VOICE:
                if media_path and len(media_path) > 0:
                    message.Content.Value.Audio.Value = self._get_uri(media_path)
            elif msg_type == MESSAGE_CONTENT_TYPE_VIDEO:
                if media_path and len(media_path) > 0:
                    message.Content.Value.Video.Value = self._get_uri(media_path)
            elif msg_type == MESSAGE_CONTENT_TYPE_EMOJI:
                if media_path and len(media_path) > 0:
                    message.Content.Value.Gif.Value = self._get_uri(media_path)
            #elif msg_type == MESSAGE_CONTENT_TYPE_CONTACT_CARD:
            #    pass
            elif msg_type == MESSAGE_CONTENT_TYPE_LOCATION:
                if row[11]:
                    message.Content.Value.Location.Value = self._get_location(row[11])
            elif msg_type == MESSAGE_CONTENT_TYPE_RED_ENVELPOE:
                if row[11]:
                    message.Content.Value.RedEnvelope.Value = self._get_aareceipts(row[11])
                    message.Content.Value.RedEnvelope.Value.OwnerUserID.Value = message.OwnerUserID.Value
            elif msg_type == MESSAGE_CONTENT_TYPE_RECEIPT:
                if row[11]:
                    message.Content.Value.Receipt.Value = self._get_receipt(row[11])
                    message.Content.Value.Receipt.Value.OwnerUserID.Value = message.OwnerUserID.Value
            elif msg_type == MESSAGE_CONTENT_TYPE_AA_RECEIPT:
                if row[11]:
                    message.Content.Value.AAReceipts.Value = self._get_aareceipts(row[11])
                    message.Content.Value.AAReceipts.Value.OwnerUserID.Value = message.OwnerUserID.Value
            #elif msg_type == MESSAGE_CONTENT_TYPE_LINK:
            #    pass
            #elif msg_type == MESSAGE_CONTENT_TYPE_VOIP:
            #    pass
            #elif msg_type == MESSAGE_CONTENT_TYPE_SYSTEM:
            #    pass

            if account_id is not None and talker_id is not None:
                key = self._get_user_key(account_id, talker_id)
                if key in chats:
                    chat = chats[key]
                    chat.Messages.Add(message)
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
                    chats[key] = chat

            row = self.cursor.fetchone()
        return chats.values() 

    def _get_chatroom_member_models(self, account_id, chatroom_id):
        if account_id in [None, ''] or chatroom_id in [None, '']:
            return []
        models = []
        sql = '''select account_id, chatroom_id, member_id, display_name, photo, telephone, email, 
                        gender, age, address, birthday, signature, source, deleted, repeated
                 from chatroom_member
                 where account_id='{0}' and chatroom_id='{1}' '''.format(account_id, chatroom_id)
        cursor = self.db.cursor()
        row = None
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            if row[2]:
                model = self._get_user_intro(account_id, row[2])
                if row[3]:
                    model.Name.Value = row[3]
                if row[12] not in [None, '']:
                    model.SourceFile.Value = row[12]
                if row[13]:
                    model.Deleted = self._convert_deleted_status(row[13])
                models.append(model)
            row = cursor.fetchone()
        if cursor is not None:
            cursor.close()
        return models

    def _get_feed_models(self):
        models = []

        sql = '''select account_id, sender_id, type, content, media_path, urls, preview_urls, 
                        attachment_title, attachment_link, attachment_desc, send_time, likes, 
                        comments, location, source, deleted, repeated
                 from feed'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            moment = Common.Moment()
            moment.Content.Value = Common.MomentContent()
            account_id = None
            if row[14] not in [None, '']:
                moment.SourceFile.Value = row[14]
            if row[15]:
                moment.Deleted = self._convert_deleted_status(row[15])
            if row[0]:
                moment.OwnerUserID.Value = row[0]
                account_id = row[0]
            if row[1]:
                moment.ID.Value = row[1]
                moment.Sender.Value = self._get_user_intro(account_id, row[1])
            if row[3]:
                moment.Content.Value.Text.Value = row[3]
            if row[2]:
                moment.Type.Value = row[2]
            if row[5]:
                urls = json.loads(row[5])
                for url in urls:
                    if len(url) > 0:
                        moment.Uris.Add(url)
            #if row[6]:
            #    moment.PreviewUris.Add(row[6])
            if row[13]:
                moment.Location.Value = self._get_location(row[13])
            if row[10]:
                ts = self._get_timestamp(row[10])
                if ts:
                    moment.TimeStamp.Value = ts
            if row[11]:
                moment.Likes.AddRange(self._get_feed_likes(account_id, row[11]))
            if row[12]:
                moment.Comments.AddRange(self._get_feed_comments(account_id, row[12]))
            models.append(moment)

            row = self.cursor.fetchone()

        return models 

    def _get_search_models(self):
        models = []

        sql = '''select account_id, key, create_time, source, deleted, repeated
                 from search'''
        row = None
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
        except Exception as e:
            print(e)

        while row is not None:
            search = SearchedItem()
            if row[3] not in [None, '']:
                search.SourceFile.Value = row[3]
            if row[4]:
                search.Deleted = self._convert_deleted_status(row[4])
            if row[0]:
                search.OwnerUserID.Value = row[0]
            if row[1]:
                search.Value.Value = row[1]
            if row[2]:
                ts = self._get_timestamp(row[2])
                if ts:
                    search.TimeStamp.Value = ts
            models.append(search)

            row = self.cursor.fetchone()

        return models 

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
            return ConvertHelper.ToUri(path)

    def _get_feed_likes(self, account_id, likes):
        models = []
        like_ids = []
        try:
            like_ids = likes.split(',')
        except Exception as e:
            print(e)
        for like_id in like_ids:
            sql = '''select sender_id, sender_name, create_time, source, deleted, repeated
                     from feed_like
                     where like_id='{0}' '''.format(like_id)
            cursor = self.db.cursor()
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                print(e)

            while row is not None:
                like = Common.MomentLike()
                if row[0]:
                    like.User.Value = self._get_user_intro(account_id, row[0], row[1])
                if row[2]:
                    ts = self._get_timestamp(row[2])
                    if ts:
                        like.TimeStamp.Value = ts
                if row[3] not in [None, '']:
                    like.SourceFile.Value = row[3]
                if row[4]:
                    like.Deleted = self._convert_deleted_status(row[4])
                models.append(like)

                row = cursor.fetchone()

            if cursor is not None:
                cursor.close()
        return models

    def _get_feed_comments(self, account_id, comments):
        models = []
        comment_ids = None
        try:
            comment_ids = comments.split(',')
        except Exception as e:
            print(e)
        for comment_id in comment_ids:
            sql = '''select sender_id, sender_name, ref_user_id, ref_user_name, content, create_time, source, deleted, repeated
                     from feed_comment
                     where comment_id='{0}' '''.format(comment_id)
            cursor = self.db.cursor()
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                print(e)

            while row is not None:
                comment = Common.MomentComment()
                if row[0]:
                    comment.Sender.Value = self._get_user_intro(account_id, row[0], row[1])
                if row[2]:
                    comment.Receiver.Value = self._get_user_intro(account_id, row[2], row[3])
                if row[4]:
                    comment.Content.Value = row[4]
                if row[5]:
                    ts = self._get_timestamp(row[5])
                    if ts:
                        comment.TimeStamp.Value = ts
                if row[6] not in [None, '']:
                    comment.SourceFile.Value = row[6]
                if row[7]:
                    comment.Deleted = self._convert_deleted_status(row[7])
                models.append(comment)

                row = cursor.fetchone()

            if cursor is not None:
                cursor.close()
        return models

    def _get_location(self, location_id):
        location = Locations.Location()
        location.Position.Value = Locations.Coordinate()
        if location_id is not None:
            sql = '''select latitude, longitude, elevation, address, timestamp, source, deleted, repeated
                     from location where location_id='{0}' '''.format(location_id)
            cursor = self.db.cursor()
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                print(e)

            if row is not None:
                if row[0]:
                    location.Position.Value.Latitude.Value = row[0]
                if row[1]:
                    location.Position.Value.Longitude.Value = row[1]
                if row[2]:
                    location.Position.Value.Elevation.Value = row[2]
                if row[3]:
                    location.Position.Value.PositionAddress.Value = row[3]
                if row[4]:
                    ts = self._get_timestamp(row[4])
                    if ts:
                        location.TimeStamp.Value = ts
                if row[5] not in [None, '']:
                    location.SourceFile.Value = row[5]
                if row[6]:
                    location.Deleted = self._convert_deleted_status(row[6])

            if cursor is not None:
                cursor.close()
        return location

    def _get_receipt(self, deal_id):
        receipt = Common.Receipt()
        if deal_id is not None:
            sql = '''select type, money, description, remark, status, expire_time, 
                            receive_info, source, deleted, repeated
                     from deal where deal_id='{0}' '''.format(deal_id)
            cursor = self.db.cursor()
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                print(e)

            if row is not None:
                if row[1]:
                    receipt.Money.Value = row[1]
                if row[2]:
                    receipt.Description.Value = row[2]
                if row[3]:
                    receipt.Remarks.Value = row[3]
                if row[4]:
                    receipt.Status.Value = self._convert_receipt_status(row[4])
                if row[5]:
                    ts = self._get_timestamp(row[5])
                    if ts:
                        receipt.ExpireTime.Value = ts
                if row[7] not in [None, '']:
                    receipt.SourceFile.Value = row[7]
                if row[8]:
                    receipt.Deleted = self._convert_deleted_status(row[8])

            if cursor is not None:
                cursor.close()
        return receipt

    def _get_aareceipts(self, deal_id):
        receipt = Common.AAReceipts()
        if deal_id is not None:
            sql = '''select type, money, description, remark, status, expire_time, 
                            receive_info, source, deleted, repeated
                     from deal where deal_id='{0}' '''.format(deal_id)
            cursor = self.db.cursor()
            row = None
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
            except Exception as e:
                print(e)

            if row is not None:
                if row[1]:
                    receipt.TotalMoney.Value = row[1]
                if row[2]:
                    receipt.Description.Value = row[2]
                if row[3]:
                    receipt.Remarks.Value = row[3]
                #if row[4]:
                #    receipt.Status.Value = self._convert_receipt_status(row[4])
                if row[5]:
                    ts = self._get_timestamp(row[5])
                    if ts:
                        receipt.ExpireTime.Value = ts
                if row[7] not in [None, '']:
                    receipt.SourceFile.Value = row[7]
                if row[8]:
                    receipt.Deleted = self._convert_deleted_status(row[8])

            if cursor is not None:
                cursor.close()
        return receipt

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
