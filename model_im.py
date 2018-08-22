# -*- coding: utf-8 -*-
__author__ = "sumeng"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA.InfraLib.Utils import *

import os
import sqlite3
import json

VERSION_VALUE_DB = 1

GENDER_MALE = 0
GENDER_FEMALE = 1
GENDER_OTHER = 2

FRIEND_TYPE_FRIEND = 1
FRIEND_TYPE_GROUP_FRIEND = 2
FRIEND_TYPE_FANS = 3
FRIEND_TYPE_FOLLOW = 4
FRIEND_TYPE_SPECAIL_FOLLOW = 5
FRIEND_TYPE_MUTUAL_FOLLOW = 6
FRIEND_TYPE_RECENT = 7

CHAT_TYPE_FRIEND = 1
CHAT_TYPE_GROUP = 2
CHAT_TYPE_SYSTEM = 3

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
MESSAGE_CONTENT_TYPE_CHARTLET = 11
MESSAGE_CONTENT_TYPE_SYSTEM = 99  # 系统

MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT = 1
MESSAGE_STATUS_SENT = 2
MESSAGE_STATUS_UNREAD = 3
MESSAGE_STATUS_READ = 4

USER_TYPE_FRIEND = 0  # 好友
USER_TYPE_CHATROOM = 1  # 群

LABEL_DEFAULT = 0
LABEL_LIKED = 1
LABEL_DISLIKED = 2
LABEL_STAR = 3

PLATFORM_PC = 1
PLATFORM_MOBILE = 2

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
        gender TEXT, 
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
        gender TEXT, 
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
        gender TEXT, 
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
        location TEXT,
        status INT,
        talker_type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_MESSAGE = '''
    insert into message(account_id, talker_id, talker_name, sender_id, sender_name, is_sender, msg_id, type, content, 
                        media_path, send_time, location, status, talker_type, source, deleted, repeated) 
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

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


class IM(object):
    def __init__(self):
        self.db = None
        self.cursor = None

    def db_create(self, db_path):
        if os.path.exists(db_path):
            os.remove(db_path)

        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()

        self.db_create_table()

    def db_close(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def db_commit(self):
        if self.db is not None:
            self.db.commit()

    def db_create_table(self):
        if self.cursor is not None:
            self.cursor.execute(SQL_CREATE_TABLE_ACCOUNT)
            self.cursor.execute(SQL_CREATE_TABLE_FRIEND)
            self.cursor.execute(SQL_CREATE_TABLE_CHATROOM)
            self.cursor.execute(SQL_CREATE_TABLE_CHATROOM_MEMBER)
            self.cursor.execute(SQL_CREATE_TABLE_MESSAGE)
            self.cursor.execute(SQL_CREATE_TABLE_FEED)
            self.cursor.execute(SQL_CREATE_TABLE_FEED_LIKE)
            self.cursor.execute(SQL_CREATE_TABLE_FEED_COMMENT)
            self.cursor.execute(SQL_CREATE_TABLE_LOCATION)
            self.cursor.execute(SQL_CREATE_TABLE_VERSION)

    def db_insert_table_account(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_ACCOUNT, column.get_values())

    def db_insert_table_friend(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_FRIEND, column.get_values())

    def db_insert_table_chatroom(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_CHATROOM, column.get_values())

    def db_insert_table_chatroom_member(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_CHATROOM_MEMBER, column.get_values())

    def db_insert_table_message(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_MESSAGE, column.get_values())

    def db_insert_table_feed(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_FEED, column.get_values())

    def db_insert_table_feed_like(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_FEED_LIKE, column.get_values())

    def db_insert_table_feed_comment(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_FEED_COMMENT, column.get_values())

    def db_insert_table_location(self, column):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_LOCATION, column.get_values())

    def db_insert_table_version(self, key, version):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_VERSION, (key, version))

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
        self.gender = None  # 性别[INT]
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
        self.gender = None  # 性别[INT]
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
        self.gender = None  # 性别[TEXT]
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
        self.location = None  # 地址ID[TEXT]
        self.status = None  # 消息状态[INT]，MESSAGE_STATUS
        self.talker_type = None  # 聊天类型[INT]，USER_TYPE

    def get_values(self):
        return (self.account_id, self.talker_id, self.talker_name, self.sender_id, self.sender_name, 
                self.is_sender, self.msg_id, self.type, self.content, self.media_path, self.send_time, 
                self.location, self.status, self.talker_type) + super(Message, self).get_values()


class Feed(Column):
    def __init__(self):
        super(Feed, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.type = None  # 动态类型[INT]
        self.content = None  # 动态内容[TEXT]
        self.media_path = None  # 媒体文件地址[TEXT]
        self.urls = None  # 链接地址[TEXT] json string ['url1', 'url2'...]
        self.preview_urls = None  # 预览地址[TEXT] json string ['url1', 'url2'...]
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
        self.like_id = None  # 赞ID[TEXT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.sender_name = None  # 发布者昵称[TEXT]
        self.create_time = None  # 发布时间[INT]

    def get_values(self):
        return (self.like_id, self.sender_id, self.sender_name, self.create_time) + super(FeedLike, self).get_values()


class FeedComment(Column):
    def __init__(self):
        super(FeedComment, self).__init__()
        self.comment_id = None  # 评论ID[TEXT]
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
        self.location_id = None  # 地址ID[TEXT]
        self.latitude = None  # 纬度[REAL]
        self.longitude = None  # 经度[REAL]
        self.elevation = None  # 海拔[REAL]
        self.address = None  # 地址名称[TEXT]
        self.timestamp = None  # 时间戳[TEXT]

    def get_values(self):
        return (self.location_id, self.latitude, self.longitude, self.elevation, self.address, 
                self.timestamp) + super(Location, self).get_values()


class GenerateModel(object):
    def __init__(self, cache_db, mount_dir):
        self.cache_db = cache_db
        self.mount_dir = mount_dir
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
            if row[15]:
                user.Source.Value = row[15]
            # user.Delete = DeletedState.Intact if row[16] == 0 else DeletedState.Deleted
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
                user.Sex.Value = Common.SexType.Men if row[7] == 0 else Common.SexType.Women
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
                key = account_id + "#" + account_id
                self.friends[key] = contact

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
            if row[13]:
                friend.Source.Value = row[13]
            # friend.Delete = DeletedState.Intact if row[14] == 0 else DeletedState.Deleted
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
                friend.Sex.Value = Common.SexType.Men if row[8] == 0 else Common.SexType.Women
            if row[9]:
                friend.Age.Value = row[9]
            if row[12]:
                friend.Signature.Value = row[12]
            friend.FriendType.Value = Common.FriendType.Friend
            address = Contacts.StreetAddress()
            if row[10]:
                address.FullName = row[10]
            friend.Addresses.Add(address)
            models.append(friend)

            if account_id is not None and user_id is not None:
                key = account_id + "#" + user_id
                self.friends[key] = contact

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
            if row[12]:
                group.Source.Value = row[12]
            # group.Delete = DeletedState.Intact if row[13] == 0 else DeletedState.Deleted
            if row[0]:
                group.OwnerUserID.Value = row[0]
                account_id = row[0]
            if row[1]:
                group.ID.Value = row[1]
                user_id = row[1]
                contact['user_id'] = row[1]
            if row[2]:
                group.Name.Value = row[2]
                contact['nickname'] = row[2]
            if row[3] and len(row[3]) > 0:
                group.PhotoUris.Add(self._get_uri(row[3]))
                contact['photo'] = row[3]
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
                group.JoinTime.Value = self._get_timestamp(row[11])
            models.append(group)

            if account_id is not None and user_id is not None:
                key = account_id + "#" + user_id
                self.chatrooms[key] = contact

            row = self.cursor.fetchone()

        return models 

    def _get_chat_models(self):
        chats = {}

        sql = '''select account_id, talker_id, talker_name, sender_id, sender_name, is_sender, msg_id, type, 
                        content, media_path, send_time, location, status, talker_type, source, deleted, repeated
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

            if row[14]:
                message.Source.Value = row[14]
            # message.Delete = DeletedState.Intact if row[13] == 0 else DeletedState.Deleted
            if row[0]:
                message.OwnerUserID.Value = row[0]
                account_id = row[0]
            if row[1]:
                message.ID.Value = row[1]
                talker_id = row[1]
            if row[2]:
                talker_name = row[2]
            if row[3]:
                message.Sender.Value = self._get_user_intro(account_id, row[3], row[4], talker_type)
                if row[3] == account_id:
                    message.Type.Value = Common.MessageType.Send
                else:
                    message.Type.Value = Common.MessageType.Receive
            if row[11]:
                message.Content.Value.Location.Value = self._get_location(row[11])
            if row[10]:
                message.TimeStamp.Value = self._get_timestamp(row[10])

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
            #elif msg_type == MESSAGE_CONTENT_TYPE_LOCATION:
            #    pass
            #elif msg_type == MESSAGE_CONTENT_TYPE_LINK:
            #    pass
            #elif msg_type == MESSAGE_CONTENT_TYPE_VOIP:
            #    pass
            #elif msg_type == MESSAGE_CONTENT_TYPE_SYSTEM:
            #    pass

            if account_id is not None and talker_id is not None:
                key = account_id + "#" + talker_id
                if key in chats:
                    chat = chats[key]
                    chat.Messages.Add(message)
                else:
                    chat = Generic.Chat()
                    chat.Source.Value = message.Source.Value
                    chat.OwnerUserID.Value = account_id
                    chat.ChatId.Value = talker_id
                    if talker_type == USER_TYPE_FRIEND:
                        if talker_name is not None:
                            chat.ChatName.Value = talker_name
                        else:
                            chat.ChatName.Value = self.friends.get(key, {}).get('nickname', '')
                        chat.Participants.Add(self._get_user_intro(account_id, talker_id))
                        chat.Participants.Add(self._get_user_intro(account_id, account_id))
                    elif talker_type == USER_TYPE_CHATROOM:
                        if talker_name is not None:
                            chat.ChatName.Value = talker_name
                        else:
                            chat.ChatName.Value = self.chatrooms.get(key, {}).get('nickname', '')
                        chat.Participants.AddRange(self._get_chatroom_member_models(account_id, talker_id))
                    chat.Messages.Add(message)
                    chats[key] = chat

            row = self.cursor.fetchone()
        return chats.values() 

    def _get_chatroom_member_models(self, account_id, chatroom_id):
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
            if row[14]:
                moment.Source.Value = row[14]
            # moment.Delete = DeletedState.Intact if row[15] == 0 else DeletedState.Deleted
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
                moment.TimeStamp.Value = self._get_timestamp(row[10])
            if row[11]:
                moment.Likes.AddRange(self._get_feed_likes(account_id, row[11]))
            if row[12]:
                moment.Comments.AddRange(self._get_feed_comments(account_id, row[12]))
            models.append(moment)

            row = self.cursor.fetchone()

        return models 

    def _get_user_intro(self, account_id, user_id, user_name=None, user_type=USER_TYPE_FRIEND):
        user = Common.UserIntro()
        user.ID.Value = user_id

        if account_id is not None and user_id is not None:
            key = account_id + "#" + user_id
            contact = None
            if user_type == USER_TYPE_FRIEND:
                contact = self.friends.get(key)
            elif user_type == USER_TYPE_CHATROOM:
                contact = self.chatrooms.get(key)
            
            if contact is not None:
                user.Name.Value = contact.get('nickname', '')
                photo = contact.get('photo', '')
                if len(photo) > 0:
                    user.Photo.Value = self._get_uri(photo)

            if user_name is not None:
                user.Name.Value = user_name
        return user

    def _get_timestamp(self, timestamp):
        ts = TimeStamp.FromUnixTime(timestamp, False)
        if not ts.IsValidForSmartphone():
            ts = None
        return ts

    def _get_uri(self, path):
        if path.startswith('http') or len(path) == 0:
            return ConvertHelper.ToUri(path)
        else:
            return ConvertHelper.ToUri(self.mount_dir + path.replace('/', '\\'))

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
                    like.TimeStamp.Value = self._get_timestamp(row[2])
                if row[3]:
                    like.Source.Value = row[3]
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
                    comment.TimeStamp.Value = self._get_timestamp(row[5])
                if row[6]:
                    comment.Source.Value = row[6]
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
                    location.TimeStamp.Value = self._get_timestamp(row[4])
                if row[5]:
                    location.Source.Value = row[5]

            if cursor is not None:
                cursor.close()
        return location

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
        else:
            return Common.FriendType.None
