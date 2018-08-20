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

MESSAGE_CONTENT_TYPE_TEXT = 1
MESSAGE_CONTENT_TYPE_IMAGE = 2
MESSAGE_CONTENT_TYPE_VOICE = 3
MESSAGE_CONTENT_TYPE_VIDEO = 4
MESSAGE_CONTENT_TYPE_EMOJI = 5
MESSAGE_CONTENT_TYPE_CONTACT_CARD = 6
MESSAGE_CONTENT_TYPE_LOCATION = 7
MESSAGE_CONTENT_TYPE_LINK = 8
MESSAGE_CONTENT_TYPE_VOIP = 9
MESSAGE_CONTENT_TYPE_SYSTEM = 10

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
        sender_id TEXT,
        is_sender INT,
        msg_id TEXT, 
        type INT,
        content TEXT,
        media_path TEXT,
        send_time INT,
        location_lat REAL,
        location_lng REAL,
        location_name TEXT,
        status INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_MESSAGE = '''
    insert into message(account_id, talker_id, sender_id, is_sender, msg_id, type, content, media_path, 
                        send_time, location_lat, location_lng, location_name, status, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

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
        location_lat REAL,
        location_lng REAL,
        location_name TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED = '''
    insert into feed(account_id, sender_id, type, content, media_path, urls, preview_urls, attachment_title, 
                     attachment_link, attachment_desc, send_time, likes, comments, location_lat, location_lng, 
                     location_name, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

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
        self.sender_id = None  # 发送者ID[TEXT]
        self.is_sender = None  # 自己是否为发送发[INT]
        self.msg_id = None  # 消息ID[TEXT]
        self.type = None  # 消息类型[INT]
        self.content = None  # 内容[TEXT]
        self.media_path = None  # 媒体文件地址[TEXT]
        self.send_time = None  # 发送时间[INT]
        self.location_lat = None  # 经度[REAL]
        self.location_lng = None  # 纬度[REAL]
        self.location_name = None  # 地址名称[TEXT]
        self.status = None  # 消息状态[INT]

    def get_values(self):
        return (self.account_id, self.talker_id, self.sender_id, self.is_sender, self.msg_id, self.type, 
                self.content, self.media_path, self.send_time, self.location_lat, self.location_lng, 
                self.location_name, self.status) + super(Message, self).get_values()


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
        self.likes = None  # 赞的人和时间[TEXT] json string [{'username':username, 'nickname':nickname, 'createTime':createTime}]
        self.comments = None  # 评论的人、内容、时间[TEXT] json string [{'username':username, 'nickname':nickname, 'content':content, 'refUserName':refUserName, 'createTime':createTime}]
        self.location_lat = None  # 经度[REAL]
        self.location_lng = None  # 纬度[REAL]
        self.location_name = None  # 地址名称[TEXT]

    def get_values(self):
        return (self.account_id, self.sender_id, self.type, self.content, self.media_path, self.urls, self.preview_urls, 
                self.attachment_title, self.attachment_link, self.attachment_desc, self.send_time, self.likes, self.comments, 
                self.location_lat, self.location_lng, self.location_name) + super(Feed, self).get_values()
    

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
            if row[4]:
                user.PhotoUris.Add(ConvertHelper.ToUri(row[4]))
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
            if row[4]:
                friend.PhotoUris.Add(ConvertHelper.ToUri(row[4]))
                contact['photo'] = row[4]
            if row[3]:
                friend.Remarks.Value = row[3]
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
            if row[3]:
                group.PhotoUris.Add(ConvertHelper.ToUri(row[3]))
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
                group.JoinTime.Init(TimeStamp.FromUnixTime(row[11], False))
                if not group.JoinTime.Value.IsValidForSmartphone():
                    group.JoinTime.Value = None
            models.append(group)

            if account_id is not None and user_id is not None:
                key = account_id + "#" + user_id
                self.chatrooms[key] = contact

            row = self.cursor.fetchone()

        return models 

    def _get_chat_models(self):
        chats = {}

        sql = '''select account_id, talker_id, sender_id, is_sender, msg_id, type, content, media_path, 
                        send_time, location_lat, location_lng, location_name, status, source, deleted, repeated
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
            if row[13]:
                message.Source.Value = row[13]
            # message.Delete = DeletedState.Intact if row[14] == 0 else DeletedState.Deleted
            if row[0]:
                message.OwnerUserID.Value = row[0]
                account_id = row[0]
            if row[1]:
                message.ID.Value = row[1]
                talker_id = row[1]
            if row[2]:
                message.Sender.Value = self._get_user_intro(account_id, row[2])
                if row[2] == account_id:
                    message.Type.Value = Common.MessageType.Send
                else:
                    message.Type.Value = Common.MessageType.Receive
            if row[9] and row[10]:
                location = Locations.Location()
                location.Position.Value = Locations.Coordinate()
                location.Position.Value.Latitude.Value = row[9]
                location.Position.Value.Longitude.Value = row[10]
                if row[11]:
                    location.Position.Value.PositionAddress.Value = row[11]
                message.Content.Value.Location.Value = location
            #if row[8]:
            #    message.TimeStamp.Init(TimeStamp.FromUnixTime(row[8], False))
            #    if not message.TimeStamp.Value.IsValidForSmartphone():
            #        message.TimeStamp.Value = None

            msg_type = row[5]
            content = row[6]
            if content is None:
                content = ''
            media_path = row[7]
            if media_path is None:
                media_path = ''
            if msg_type == MESSAGE_CONTENT_TYPE_TEXT:
                message.Content.Value.Text.Value = content
            elif msg_type in [MESSAGE_CONTENT_TYPE_IMAGE, MESSAGE_CONTENT_TYPE_VOICE, MESSAGE_CONTENT_TYPE_VIDEO, MESSAGE_CONTENT_TYPE_EMOJI]:
                message.Content.Value.Image.Value = ConvertHelper.ToUri(media_path)
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
            else:
                message.Content.Value.Text.Value = content

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
                    if key in self.friends:
                        chat.ChatName.Value = self.friends[key].get('nickname', '')
                    elif key in self.chatrooms:
                        chat.ChatName.Value = self.chatrooms[key].get('nickname', '')
                    chat.Messages.Add(message)

                    if key in self.chatrooms:
                        chat.Participants.AddRange(self._get_chatroom_member_models(account_id, talker_id))
                    elif key in self.friends:
                        chat.Participants.Add(self._get_user_intro(account_id, talker_id))
                        chat.Participants.Add(self._get_user_intro(account_id, account_id))

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
        cursor.close()
        return models

    def _get_feed_models(self):
        models = []

        sql = '''select account_id, sender_id, type, content, media_path, urls, preview_urls, 
                        attachment_title, attachment_link, attachment_desc, send_time, likes, 
                        comments, location_lat, location_lng, location_name, source, deleted, repeated
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
            if row[16]:
                moment.Source.Value = row[16]
            # moment.Delete = DeletedState.Intact if row[13] == 0 else DeletedState.Deleted
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
                    moment.Uris.Add(url)
            #if row[6]:
            #    moment.PreviewUris.Add(row[6])
            #if row[13]:
            #    moment.Location.Value = self._get_location(row[13])
            #if row[10]:
            #    moment.TimeStamp.Init(TimeStamp.FromUnixTime(row[10]))
            #    if not moment.TimeStamp.Value.IsValidForSmartphone():
            #        moment.TimeStamp.Value = None
            if row[11]:
                moment.Likes.AddRange(self._get_feed_likes(account_id, row[11]))
            if row[12]:
                moment.Comments.AddRange(self._get_feed_comments(account_id, row[12]))
            models.append(moment)

            row = self.cursor.fetchone()

        return models 

    def _get_user_intro(self, account_id, user_id):
        user = Common.UserIntro()
        user.ID.Value = user_id

        if account_id is not None and user_id is not None:
            key = account_id + "#" + user_id
            contact = None
            if key in self.friends:
                contact = self.friends[key]
            elif key in self.chatrooms:
                contact = self.chatrooms[key]
            
            if contact is not None:
                user.Name.Value = contact.get('nickname', '')
                photo = contact.get('photo', '')
                if len(photo) > 0:
                    user.Photo.Value = ConvertHelper.ToUri(photo)
        return user

    def _get_feed_likes(self, account_id, likes_str):
        likes = []
        ls = None
        try:
            ls = json.loads(likes_str)
        except Exception as e:
            print(e)
        if ls is not None:
            for l in ls:
                like = Common.MomentLike()
                user = Common.UserIntro()
                if 'username' in l:
                    user.ID.Value = l['username']
                if 'nickname' in l:
                    user.Name.Value = l['nickname']
                like.User.Value = user
                #if 'createTime' in l:
                #    like.TimeStamp.Init(TimeStamp.FromUnixTime(l['createTime']))
                #    if not like.TimeStamp.Value.IsValidForSmartphone():
                #        like.TimeStamp.Value = None
                likes.append(like)
        return likes

    def _get_feed_comments(self, account_id, comments_str):
        comments = []
        cs = None
        try:
            cs = json.loads(comments_str)
        except Exception as e:
            print(e)
        if cs is not None:
            for c in cs:
                comment = Common.MomentComment()
                sender = Common.UserIntro()
                if 'username' in c:
                    sender.ID.Value = c['username']
                if 'nickname' in c:
                    sender.Name.Value = c['nickname']
                comment.Sender.Value = sender
                if 'refUserName' in c:
                    comment.Receiver.Value = self._get_user_intro(account_id, c['refUserName'])
                if 'content' in c:
                    comment.Content.Value = c['content']
                #if 'createTime' in c:
                #    comment.TimeStamp.Init(TimeStamp.FromUnixTime(c['createTime']))
                #    if not comment.TimeStamp.Value.IsValidForSmartphone():
                #        comment.TimeStamp.Value = None
                comments.append(comment)
        return comments

