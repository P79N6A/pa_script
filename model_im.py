# -*- coding: utf-8 -*-
__author__ = "sumeng"

import os
import sqlite3

DB_VERSION = 1

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

MSG_TYPE_TEXT = 1
MSG_TYPE_IMAGE = 3
MSG_TYPE_VOICE = 34
MSG_TYPE_CONTACT_CARD = 42
MSG_TYPE_VIDEO = 43
MSG_TYPE_VIDEO_2 = 62
MSG_TYPE_EMOJI = 47
MSG_TYPE_LOCATION = 48
MSG_TYPE_LINK = 49
MSG_TYPE_VOIP = 50
MSG_TYPE_VOIP_GROUP = 64
MSG_TYPE_SYSTEM = 10000
MSG_TYPE_SYSTEM_2 = 10002

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

SQL_CREATE_TABLE_ACCOUNT = '''
    create table if not exists account(
        account_id TEXT, 
        nickname TEXT,
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
    insert into account(account_id, nickname, password, photo, telephone, email, gender, age, country, province, city, address, birthday, signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

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
    insert into friend(account_id, friend_id, nickname, remark, photo, type, telephone, email, gender, age, address, birthday, signature, source, deleted, repeated) 
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
    insert into chatroom(account_id, chatroom_id, name, photo, type, notice, description, creator_id, owner_id, member_count, max_member_count, create_time, source, deleted, repeated) 
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
    insert into chatroom_member(account_id, chatroom_id, member_id, display_name, photo, telephone, email, gender, age, address, birthday, signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_MESSAGE = '''
    create table if not exists message(
        account_id TEXT, 
        talker_id TEXT,  
        sender_id TEXT,
        is_sender INT,
        msg_id TEXT, 
        type TEXT,
        content TEXT,
        media_path TEXT,
        send_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_MESSAGE = '''
    insert into message(account_id, talker_id, sender_id, is_sender, msg_id, type, content, media_path, send_time, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED = '''
    create table if not exists feed(
        account_id TEXT, 
        sender_id TEXT,
        type TEXT,
        content TEXT,
        media_path TEXT,
        url TEXT,
        preview_url TEXT,
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
    insert into feed(account_id, sender_id, type, content, media_path, url, preview_url, attachment_title, attachment_link, attachment_desc, send_time, likes, comments, location, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(version) values(?)'''


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

    def db_insert_table_version(self, version):
        if self.cursor is not None:
            self.cursor.execute(SQL_INSERT_TABLE_VERSION, (version, ))


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
        self.account_id = None
        self.nickname = None
        self.password = None
        self.photo = None
        self.telephone = None
        self.email = None
        self.gender = None
        self.age = None
        self.country = None
        self.province = None
        self.city = None
        self.address = None
        self.birthday = None
        self.signature = None

    def get_values(self):
        return (self.account_id, self.nickname, self.password, self.photo, self.telephone, self.email, self.gender, self.age, self.country, self.province, self.city, self.address, self.birthday, self.signature) + super(Account, self).get_values()


class Friend(Column):
    def __init__(self):
        super(Friend, self).__init__()
        self.account_id = None
        self.friend_id = None
        self.nickname = None
        self.remark = None
        self.photo = None
        self.type = None
        self.telephone = None
        self.email = None
        self.gender = None
        self.age = None
        self.address = None
        self.birthday = None
        self.signature = None

    def get_values(self):
        return (self.account_id, self.friend_id, self.nickname, self.remark, self.photo, self.type, self.telephone, self.email, self.gender, self.age, self.address, self.birthday, self.signature) + super(Friend, self).get_values()


class Chatroom(Column):
    def __init__(self):
        super(Chatroom, self).__init__()
        self.account_id = None
        self.chatroom_id = None
        self.name = None
        self.photo = None
        self.type = None
        self.notice = None
        self.description = None
        self.creator_id = None
        self.owner_id = None
        self.member_count = None
        self.max_member_count = None
        self.create_time = None

    def get_values(self):
        return (self.account_id, self.chatroom_id, self.name, self.photo, self.type, self.notice, self.description, self.creator_id, self.owner_id, self.member_count, self.max_member_count, self.create_time) + super(Chatroom, self).get_values()


class ChatroomMember(Column):
    def __init__(self):
        super(ChatroomMember, self).__init__()
        self.account_id = None
        self.chatroom_id = None
        self.member_id = None
        self.display_name = None
        self.photo = None
        self.telephone = None
        self.email = None
        self.gender = None
        self.age = None
        self.address = None
        self.birthday = None
        self.signature = None

    def get_values(self):
        return (self.account_id, self.chatroom_id, self.member_id, self.display_name, self.photo, self.telephone, self.email, self.gender, self.age, self.address, self.birthday, self.signature) + super(ChatroomMember, self).get_values()


class Message(Column):
    def __init__(self):
        super(Message, self).__init__()
        self.account_id = None
        self.talker_id = None
        self.sender_id = None
        self.is_sender = None
        self.msg_id = None
        self.type = None
        self.content = None
        self.media_path = None
        self.send_time = None

    def get_values(self):
        return (self.account_id, self.talker_id, self.sender_id, self.is_sender, self.msg_id, self.type, self.content, self.media_path, self.send_time) + super(Message, self).get_values()


class Feed(Column):
    def __init__(self):
        super(Feed, self).__init__()
        self.account_id = None
        self.sender_id = None
        self.type = None
        self.content = None
        self.media_path = None
        self.url = None
        self.preview_url = None
        self.attachment_title = None
        self.attachment_link = None
        self.attachment_desc = None
        self.send_time = None
        self.likes = None
        self.comments = None
        self.location = None

    def get_values(self):
        return (self.account_id, self.sender_id, self.type, self.content, self.media_path, self.url, self.preview_url, self.attachment_title, self.attachment_link, self.attachment_desc, self.send_time, self.likes, self.comments, self.location) + super(Feed, self).get_values()
