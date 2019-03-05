# -*- coding: utf-8 -*-
__author__ = "sumeng"

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('ResourcesExp')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from PA.InfraLib.Utils import *
import System.Data.SQLite as SQLite
from System import Convert

from PA.InfraLib.ModelsV2 import *
from PA.InfraLib.ModelsV2.IM import *

import os
import sqlite3
import time

from ResourcesExp import AppResources
from ScriptUtils import SemiXmlParser

VERSION_VALUE_DB = 8

GENDER_NONE = 0
GENDER_MALE = 1
GENDER_FEMALE = 2

FRIEND_TYPE_NONE = 0  # 陌生人
FRIEND_TYPE_FRIEND = 1  # 好友
FRIEND_TYPE_OFFICIAL = 2  # 公众号
FRIEND_TYPE_PROGRAM = 3  # 小程序
FRIEND_TYPE_BLOCKED = 4  # 黑名单

CHAT_TYPE_NONE = 0  # 未知聊天
CHAT_TYPE_FRIEND = 1  # 好友聊天
CHAT_TYPE_GROUP = 2  # 群聊天

MESSAGE_CONTENT_TYPE_TEXT = 1  # 文本
MESSAGE_CONTENT_TYPE_IMAGE = 2  # 图片
MESSAGE_CONTENT_TYPE_VOICE = 3  # 语音
MESSAGE_CONTENT_TYPE_VIDEO = 4  # 视频
MESSAGE_CONTENT_TYPE_CONTACT_CARD = 5  # 名片
MESSAGE_CONTENT_TYPE_LOCATION = 6  # 坐标
MESSAGE_CONTENT_TYPE_LINK = 7  # 链接
MESSAGE_CONTENT_TYPE_ATTACHMENT = 8  # 附件
MESSAGE_CONTENT_TYPE_RED_ENVELPOE = 9  # 红包
MESSAGE_CONTENT_TYPE_TRANSFER = 10  # 转账
MESSAGE_CONTENT_TYPE_SPLIT_BILL = 11  # 群收款
MESSAGE_CONTENT_TYPE_APPMESSAGE = 12
MESSAGE_CONTENT_TYPE_SEMI_XML = 13
MESSAGE_CONTENT_TYPE_LINK_SET = 14  # 链接集合
MESSAGE_CONTENT_TYPE_SYSTEM = 99  # 系统

# 收藏类型
FAV_TYPE_TEXT = 1  # 文本
FAV_TYPE_IMAGE = 2  # 图片
FAV_TYPE_VOICE = 3  # 语音
FAV_TYPE_VIDEO = 4  # 视频
FAV_TYPE_LINK = 5  # 链接
FAV_TYPE_LOCATION = 6  # 位置
FAV_TYPE_ATTACHMENT = 8  # 附件
FAV_TYPE_CHAT = 14  # 聊天记录
FAV_TYPE_VIDEO_2 = 16 # 视频
FAV_TYPE_ATTACHMENT_2 = 18  # 附件

LOCATION_TYPE_GPS = 1  # GPS坐标
LOCATION_TYPE_GPS_MC = 2  # GPS米制坐标
LOCATION_TYPE_GOOGLE = 3  # GCJ02坐标
LOCATION_TYPE_GOOGLE_MC = 4  # GCJ02米制坐标
LOCATION_TYPE_BAIDU = 5  # 百度经纬度坐标
LOCATION_TYPE_BAIDU_MC = 6  # 百度米制坐标
LOCATION_TYPE_MAPBAR = 7  # mapbar地图坐标
LOCATION_TYPE_MAP51 = 8  # 51地图坐标

DEAL_MODE_NONE = 0
DEAL_MODE_IDENTICAL = 1  # 人均
DEAL_MODE_SPECIFIED = 2  # 指定人

DEAL_STATUS_RED_ENVELOPE_NONE = 1  # 未知
DEAL_STATUS_RED_ENVELOPE_OPENED = 2  # 已开封
DEAL_STATUS_RED_ENVELOPE_UNOPENED = 3  # 未开封
DEAL_STATUS_TRANSFER_NONE = 4  # 未知
DEAL_STATUS_TRANSFER_UNRECEIVED = 5  # 未收
DEAL_STATUS_TRANSFER_RECEIVED = 6  # 已收
DEAL_STATUS_TRANSFER_BACK = 7  # 退回
DEAL_STATUS_SPLIT_BILL_NONE = 8  # 未知
DEAL_STATUS_SPLIT_BILL_NONEED = 9  # 无需付款
DEAL_STATUS_SPLIT_BILL_UNPAID = 10  # 未付款
DEAL_STATUS_SPLIT_BILL_PAID = 11  # 已付款
DEAL_STATUS_SPLIT_BILL_UNDONE = 12  # 未收齐
DEAL_STATUS_SPLIT_BILL_DONE = 13  # 已收齐

CONTACT_LABEL_TYPE_GROUP = 1  # 通讯录分组
CONTACT_LABEL_TYPE_EMERGENCY = 2  # 紧急联系人

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

SQL_CREATE_TABLE_ACCOUNT = '''
    create table if not exists account(
        account_id TEXT, 
        account_id_alias TEXT,
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
    insert into account(account_id, account_id_alias, nickname, username, password, photo, telephone, email, gender, age, 
                        country, province, city, address, birthday, signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FRIEND = '''
    create table if not exists friend(
        account_id TEXT, 
        friend_id TEXT, 
        friend_id_alias TEXT, 
        nickname TEXT, 
        remark TEXT,
        photo TEXT, 
        type INT,
        gender INT, 
        region TEXT, 
        signature TEXT,
        add_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FRIEND = '''
    insert into friend(account_id, friend_id, friend_id_alias, nickname, remark, photo, type, gender, region, signature, 
                       add_time, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_CHATROOM = '''
    create table if not exists chatroom(
        account_id TEXT, 
        chatroom_id TEXT, 
        name TEXT, 
        photo TEXT, 
        is_saved INT,
        notice TEXT,
        owner_id TEXT,
        create_time INT,
        join_time INT, 
        sp_id INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_CHATROOM = '''
    insert into chatroom(account_id, chatroom_id, name, photo, is_saved, notice, owner_id, create_time, join_time, 
                         sp_id, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        

SQL_CREATE_TABLE_CHATROOM_MEMBER = '''
    create table if not exists chatroom_member(
        account_id TEXT, 
        chatroom_id TEXT, 
        member_id TEXT, 
        display_name TEXT, 
        sp_id INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_CHATROOM_MEMBER = '''
    insert into chatroom_member(account_id, chatroom_id, member_id, display_name, sp_id, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_CHATROOM_MEMBER = '''
    create index idxChatroomMember on chatroom_member (account_id, chatroom_id, sp_id)
'''

SQL_CREATE_TABLE_MESSAGE = '''
    create table if not exists message(
        account_id TEXT, 
        talker_id TEXT,
        talker_type INT,
        sender_id TEXT,
        timestamp INT,
        msg_id TEXT, 
        type INT,
        content TEXT,
        media_path TEXT,
        media_thum_path TEXT,
        status INT,
        is_recall INT,
        location_latitude REAL,
        location_longitude REAL,
        location_elevation REAL,
        location_address TEXT,
        location_type INT,
        deal_money TEXT,
        deal_description TEXT,
        deal_remark TEXT,
        deal_status INT,
        deal_mode INT,
        deal_create_time INT,
        deal_expire_time INT,
        link_url TEXT,
        link_title TEXT,
        link_content TEXT,
        link_image TEXT,
        link_from TEXT,
        business_card_username TEXT,
        business_card_nickname TEXT,
        business_card_gender INT,
        business_card_photo TEXT,
        business_card_region TEXT,
        business_card_signature TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_MESSAGE = '''
    insert into message(account_id, talker_id, talker_type, sender_id, timestamp, msg_id, type, content, media_path, 
                        media_thum_path, status, is_recall, location_latitude, location_longitude, location_elevation, 
                        location_address, location_type, deal_money, deal_description, deal_remark, deal_status, deal_mode,
                        deal_create_time, deal_expire_time, link_url, link_title, link_content, link_image, link_from, 
                        business_card_username, business_card_nickname, business_card_gender, business_card_photo,
                        business_card_region, business_card_signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_MESSAGE_IDX = '''
    create table if not exists message_{:05d}(
        account_id TEXT, 
        talker_id TEXT,
        talker_type INT,
        sender_id TEXT,
        timestamp INT,
        msg_id TEXT, 
        type INT,
        content TEXT,
        media_path TEXT,
        media_thum_path TEXT,
        status INT,
        is_recall INT,
        location_latitude REAL,
        location_longitude REAL,
        location_elevation REAL,
        location_address TEXT,
        location_type INT,
        deal_money TEXT,
        deal_description TEXT,
        deal_remark TEXT,
        deal_status INT,
        deal_mode INT,
        deal_create_time INT,
        deal_expire_time INT,
        link_url TEXT,
        link_title TEXT,
        link_content TEXT,
        link_image TEXT,
        link_from TEXT,
        business_card_username TEXT,
        business_card_nickname TEXT,
        business_card_gender INT,
        business_card_photo TEXT,
        business_card_region TEXT,
        business_card_signature TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_MESSAGE_IDX = '''
    insert into message_{:05d}(account_id, talker_id, talker_type, sender_id, timestamp, msg_id, type, content, media_path, 
                               media_thum_path, status, is_recall, location_latitude, location_longitude, location_elevation, 
                               location_address, location_type, deal_money, deal_description, deal_remark, deal_status, deal_mode,
                               deal_create_time, deal_expire_time, link_url, link_title, link_content, link_image, link_from, 
                               business_card_username, business_card_nickname, business_card_gender, business_card_photo,
                               business_card_region, business_card_signature, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED = '''
    create table if not exists feed(
        account_id TEXT, 
        sender_id TEXT,
        content TEXT,
        image_path TEXT,
        video_path TEXT,
        timestamp INT,
        link_url TEXT,
        link_title TEXT,
        link_content TEXT,
        link_image TEXT,
        link_from TEXT,
        like_id INT,
        like_count INT,
        comment_id INT,
        comment_count INT,
        location_latitude REAL,
        location_longitude REAL,
        location_elevation REAL,
        location_address TEXT,
        location_type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED = '''
    insert into feed(account_id, sender_id, content, image_path, video_path, timestamp, link_url, 
                     link_title, link_content, link_image, link_from, like_id, like_count, comment_id, 
                     comment_count, location_latitude, location_longitude, location_elevation, 
                     location_address, location_type, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FEED_LIKE = '''
    create table if not exists feed_like(
        like_id INT,
        sender_id TEXT,
        sender_name TEXT,
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED_LIKE = '''
    insert into feed_like(like_id, sender_id, sender_name, timestamp, source, deleted, repeated) 
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
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FEED_COMMENT = '''
    insert into feed_comment(comment_id, sender_id, sender_name, ref_user_id, ref_user_name, content, timestamp, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_FEED_COMMENT = '''
    create index idxFeedComment on feed_comment (comment_id)
'''

SQL_CREATE_TABLE_SEARCH = '''
    create table if not exists search(
        account_id TEXT, 
        key TEXT,
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_SEARCH = '''
    insert into search(account_id, key, timestamp, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FAVORITE = '''
    create table if not exists favorite(
        account_id TEXT, 
        favorite_id INT,
        talker_id TEXT,
        talker_name TEXT,
        talker_type INT,
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FAVORITE = '''
    insert into favorite(account_id, favorite_id, talker_id, talker_name, talker_type, 
                         timestamp, source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_FAVORITE_ITEM = '''
    create table if not exists favorite_item(
        favorite_id INT, 
        type INT,
        sender_id TEXT,
        sender_name TEXT,
        content TEXT,
        media_path TEXT,
        timestamp INT,
        link_url TEXT,
        link_title TEXT,
        link_content TEXT,
        link_image TEXT,
        link_from TEXT,
        location_latitude REAL,
        location_longitude REAL,
        location_elevation REAL,
        location_address TEXT,
        location_type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_FAVORITE_ITEM = '''
    insert into favorite_item(favorite_id, type, sender_id, sender_name, content, media_path, timestamp, 
                              link_url, link_title, link_content, link_image, link_from, location_latitude, 
                              location_longitude, location_elevation, location_address, location_type, 
                              source, deleted, repeated) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_INDEX_ON_TABLE_FAVORITE_ITEM = '''
    create index idxFavoriteItem on favorite_item (favorite_id)
'''

SQL_CREATE_TABLE_LOGIN_DEVICE = '''
    create table if not exists login_device(
        account_id TEXT, 
        id TEXT,
        name TEXT,
        type TEXT,
        last_time INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_LOGIN_DEVICE = '''
    insert into login_device(account_id, id, name, type, last_time, source, deleted, repeated) values(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_CONTACT_LABEL = '''
    create table if not exists contact_label(
        account_id TEXT, 
        id TEXT,
        name TEXT,
        users TEXT,
        type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_CONTACT_LABEL = '''
    insert into contact_label(account_id, id, name, users, type, source, deleted, repeated) values(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_BANK_CARD = '''
    create table if not exists bank_card(
        account_id TEXT, 
        bank_name TEXT,
        card_type TEXT,
        card_number TEXT,
        phone_number TEXT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_BANK_CARD = '''
    insert into bank_card(account_id, bank_name, card_type, card_number, phone_number, source, deleted, repeated) values(?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_STORY = '''
    create table if not exists story(
        account_id TEXT, 
        sender_id TEXT,
        media_path TEXT,
        story_id INT,
        timestamp INT,
        location_latitude REAL,
        location_longitude REAL,
        location_elevation REAL,
        location_address TEXT,
        location_type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_STORY = '''
    insert into story(account_id, sender_id, media_path, story_id, timestamp, location_latitude, 
                      location_longitude, location_elevation, location_address, location_type, 
                      source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_STORY_COMMENT = '''
    create table if not exists story_comment(
        story_id INT,
        sender_id TEXT,
        content TEXT,
        timestamp INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0)'''

SQL_INSERT_TABLE_STORY_COMMENT = '''
    insert into story_comment(story_id, sender_id, content, timestamp, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?)'''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''


g_feed_like_id = 1
g_feed_comment_id = 1
g_favorite_id = 1
g_chatroom_sp_id = 1
g_story_id = 1

class IM(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None
        self.message_max_count = 10000  # 一张表最大消息数量
        self.message_count = 0  # 消息计数
        self.message_table_tail = 0  # 消息表尾数

    def db_create(self, db_path):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: LINE {}".format(traceback.format_exc()))
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
            #self.db_cmd.CommandText = SQL_CREATE_TABLE_MESSAGE
            #self.db_cmd.ExecuteNonQuery()
            self.db_create_message_table_with_tail()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FEED
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FEED_LIKE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FEED_COMMENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_SEARCH
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FAVORITE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_FAVORITE_ITEM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_LOGIN_DEVICE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_CONTACT_LABEL
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_BANK_CARD
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_STORY
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_STORY_COMMENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()

    def db_create_index(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_CHATROOM_MEMBER
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_FAVORITE_ITEM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_FEED_COMMENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_INDEX_ON_TABLE_FEED_LIKE
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

    #def db_insert_table_message(self, column):
    #    self.db_insert_table(SQL_INSERT_TABLE_MESSAGE, column.get_values())

    def db_insert_table_feed(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FEED, column.get_values())

    def db_insert_table_feed_like(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FEED_LIKE, column.get_values())

    def db_insert_table_feed_comment(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FEED_COMMENT, column.get_values())

    def db_insert_table_search(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_SEARCH, column.get_values())

    def db_insert_table_favorite(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FAVORITE, column.get_values())

    def db_insert_table_favorite_item(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_FAVORITE_ITEM, column.get_values())

    def db_insert_table_login_device(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_LOGIN_DEVICE, column.get_values())

    def db_insert_table_contact_label(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_CONTACT_LABEL, column.get_values())

    def db_insert_table_bank_card(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_BANK_CARD, column.get_values())

    def db_insert_table_story(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_STORY, column.get_values())

    def db_insert_table_story_comment(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_STORY_COMMENT, column.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    def db_increase_message_table_with_tail(self):
        self.message_table_tail += 1
        self.message_count = 0
        self.db_create_message_table_with_tail()

    def db_create_message_table_with_tail(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MESSAGE_IDX.format(self.message_table_tail)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_message_with_tail(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_MESSAGE_IDX.format(self.message_table_tail), column.get_values())
        self.message_count += 1
        if self.message_count >= self.message_max_count:
            self.db_increase_message_table_with_tail()

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
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: LINE {}".format(traceback.format_exc()))

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
        self.account_id_alias = None  # 账户ID别名[TEXT]
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
        return (self.account_id, self.account_id_alias, self.nickname, self.username, self.password, self.photo, self.telephone, 
                self.email, self.gender, self.age, self.country, self.province, self.city, self.address, self.birthday, 
                self.signature) + super(Account, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_account(self)


class Friend(Column):
    def __init__(self):
        super(Friend, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.friend_id = None  # 好友ID[TEXT]
        self.friend_id_alias = None  # 好友ID别名[TEXT]
        self.nickname = None  # 昵称[TEXT]
        self.remark = None  # 备注[TEXT]
        self.photo = None  # 头像[TEXT]
        self.type = FRIEND_TYPE_NONE  # 类型[INT] FRIEND_TYPE
        self.gender = GENDER_NONE  # 性别[INT]
        self.region = None  # 地区[TEXT]
        self.signature = None  # 签名[TEXT]
        self.add_time = None  # 添加时间[INT]

    def get_values(self):
        return (self.account_id, self.friend_id, self.friend_id_alias, self.nickname, self.remark, self.photo, self.type, 
                self.gender, self.region, self.signature, self.add_time) + super(Friend, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_friend(self)


class Chatroom(Column):
    def __init__(self):
        super(Chatroom, self).__init__()
        global g_chatroom_sp_id
        self.account_id = None  # 账号ID[TEXT]
        self.chatroom_id = None  # 群ID[TEXT]
        self.name = None  # 群名称[TEXT]
        self.photo = None  # 群头像[TEXT]
        self.is_saved = 0  # 群是否保存到通讯录[INT]
        self.notice = None  # 群声明[TEXT]
        self.owner_id = None  # 群主[TEXT]
        self.create_time = None  # 创建时间[INT]
        self.join_time = None  # 加入时间[INT]
        self.sp_id = g_chatroom_sp_id  # 群识别码(用于区别恢复数据、相同群ID)[INT]
        g_chatroom_sp_id += 1

    def get_values(self):
        return (self.account_id, self.chatroom_id, self.name, self.photo, self.is_saved, self.notice, self.owner_id, 
                self.create_time, self.join_time, self.sp_id) + super(Chatroom, self).get_values()

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
        self.sp_id = None  # 群识别码(用于区别恢复数据、相同群ID)[INT]

    def get_values(self):
        return (self.account_id, self.chatroom_id, self.member_id, self.display_name, self.sp_id) + super(ChatroomMember, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_chatroom_member(self)


class Message(Column):
    def __init__(self):
        super(Message, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.talker_id = None  # 会话ID[TEXT]
        self.talker_type = None  # 聊天类型[INT]  CHAT_TYPE
        self.sender_id = None  # 发送者ID[TEXT]
        self.timestamp = None  # 发送时间[INT]
        self.msg_id = None  #  消息ID[TEXT]
        self.type = MESSAGE_CONTENT_TYPE_TEXT  #  消息类型[INT]，MESSAGE_CONTENT_TYPE
        self.content = None  #  消息内容[TEXT]
        self.media_path = None  #  媒体文件地址[TEXT]
        self.media_thum_path = None  #  媒体文件缩略图地址[TEXT]
        self.status = None  #  消息状态[INT]  MESSAGE_STATUS
        self.is_recall = False  # 是否撤回消息[INT]
        self.location_latitude = 0  # 地址纬度[REAL]
        self.location_longitude = 0  # 地址经度[REAL]
        self.location_elevation = 0  # 地址海拔[REAL]
        self.location_address = None  # 地址名称[TEXT]
        self.location_type = LOCATION_TYPE_GPS  # 地址类型[INT]，LOCATION_TYPE
        self.deal_money = None  # 交易金额[TEXT]
        self.deal_description = None  # 交易描述[TEXT]
        self.deal_remark = None  # 交易备注[TEXT]
        self.deal_status = None  # 交易状态[INT]
        self.deal_mode = None  # 交易模式[INT]
        self.deal_create_time = None  # 交易创建时间[INT]
        self.deal_expire_time = None  # 交易过期时间[INT]
        self.link_url = None  # 链接地址[TEXT]
        self.link_title = None  # 链接标题[TEXT]
        self.link_content = None  # 链接内容[TEXT]
        self.link_image = None  # 链接图片[TEXT]
        self.link_from = None  # 链接来源[TEXT]
        self.business_card_username = None  # 名片ID[TEXT]
        self.business_card_nickname = None  # 名片昵称[TEXT]
        self.business_card_gender = GENDER_NONE  # 名片性别[INT]
        self.business_card_photo = None  # 名片头像[TEXT]
        self.business_card_region = None  # 名片地区[TEXT]
        self.business_card_signature = None   # 名片签名[TEXT]

    def get_values(self):
        return (self.account_id, self.talker_id, self.talker_type, self.sender_id, self.timestamp, self.msg_id, self.type, 
                self.content, self.media_path, self.media_thum_path, self.status, self.is_recall, self.location_latitude, 
                self.location_longitude, self.location_elevation, self.location_address, self.location_type, self.deal_money, 
                self.deal_description, self.deal_remark, self.deal_status, self.deal_mode, self.deal_create_time, 
                self.deal_expire_time, self.link_url, self.link_title, self.link_content, self.link_image, self.link_from, 
                self.business_card_username, self.business_card_nickname, self.business_card_gender, self.business_card_photo, 
                self.business_card_region, self.business_card_signature) + super(Message, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_message_with_tail(self)


class Feed(Column):
    def __init__(self):
        global g_feed_comment_id, g_feed_like_id
        super(Feed, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.content = None  # 文本[TEXT]
        self.image_path = ''  # 图片地址[TEXT]  多个文件以逗号分隔  image_path1,image_path2,...
        self.video_path = ''  # 视频地址[TEXT]  多个文件以逗号分隔  video_path1,video_path2,...
        self.timestamp = None  # 发布时间[INT]
        self.link_url = None  # 链接地址[TEXT]
        self.link_title = None  # 链接标题[TEXT]
        self.link_content = None  # 链接内容[TEXT]
        self.link_image = None  # 链接图片[TEXT]
        self.link_from = None  # 链接来源[TEXT]
        self.like_id = g_feed_like_id  # 赞ID[INT]
        g_feed_like_id += 1
        self.like_count = 0  # 赞数量[INT]
        self.comment_id = g_feed_comment_id  # 评论ID[INT]
        g_feed_comment_id += 1
        self.comment_count = 0  # 评论数量[INT]
        self.location_latitude = 0  # 地址纬度[REAL]
        self.location_longitude = 0  # 地址经度[REAL]
        self.location_elevation = 0  # 地址海拔[REAL]
        self.location_address = None  # 地址名称[TEXT]
        self.location_type = LOCATION_TYPE_GPS  # 地址类型[INT]，LOCATION_TYPE

        self.likes = []
        self.comments = []
        
    def get_values(self):
        return (self.account_id, self.sender_id, self.content, self.image_path, self.video_path, self.timestamp, 
                self.link_url, self.link_title, self.link_content, self.link_image, self.link_from, self.like_id, 
                self.like_count, self.comment_id, self.comment_count, self.location_latitude, self.location_longitude, 
                self.location_elevation, self.location_address, self.location_type) + super(Feed, self).get_values()
    
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

    def insert_db(self, im):
        if isinstance(im, IM):
            for like in self.likes:
                like.insert_db(im)
            for comment in self.comments:
                comment.insert_db(im)
            im.db_insert_table_feed(self)


class FeedLike(Column):
    def __init__(self):
        super(FeedLike, self).__init__()
        self.like_id = None  # 赞ID[INT]
        self.sender_id = None  # 发布者ID[TEXT]
        self.sender_name = None  # 发布者昵称[TEXT]
        self.timestamp = None  # 发布时间[INT]

    def get_values(self):
        return (self.like_id, self.sender_id, self.sender_name, self.timestamp) + super(FeedLike, self).get_values()

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
        self.timestamp = None  # 发布时间[INT]

    def get_values(self):
        return (self.comment_id, self.sender_id, self.sender_name, self.ref_user_id, self.ref_user_name, 
                self.content, self.timestamp) + super(FeedComment, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_feed_comment(self)


class Search(Column):
    def __init__(self):
        super(Search, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.key = None  # 搜索关键字[TEXT]
        self.timestamp = None  # 搜索时间[INT]

    def get_values(self):
        return (self.account_id, self.key, self.timestamp) + super(Search, self).get_values()

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
        self.talker_id = None  # 会话ID[TEXT]
        self.talker_name = None  # 会话昵称[TEXT]
        self.talker_type = None  # 聊天类型[INT] CHAT_TYPE
        self.timestamp = None  # 时间戳[INT]

        self.items = []

    def get_values(self):
        return (self.account_id, self.favorite_id, self.talker_id, self.talker_name, self.talker_type,
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
        self.sender_id = None  # 发送者[TEXT]
        self.sender_name = None  # 发送者昵称[TEXT]
        self.content = None  # 内容[TEXT]
        self.media_path = None  # 文件路径[TEXT]
        self.timestamp = None  # 时间戳[TEXT]
        self.link_url = None  # 链接地址[TEXT]
        self.link_title = None  # 链接标题[TEXT]
        self.link_content = None  # 链接内容[TEXT]
        self.link_image = None  # 链接图片[TEXT]
        self.link_from = None  # 链接来源[TEXT]
        self.location_latitude = 0  # 地址纬度[REAL]
        self.location_longitude = 0  # 地址经度[REAL]
        self.location_elevation = 0  # 地址海拔[REAL]
        self.location_address = None  # 地址名称[TEXT]
        self.location_type = LOCATION_TYPE_GPS  # 地址类型[INT]，LOCATION_TYPE

    def get_values(self):
        return (self.favorite_id, self.type, self.sender_id, self.sender_name, self.content, self.media_path, 
                self.timestamp, self.link_url, self.link_title, self.link_content, self.link_image, self.link_from, 
                self.location_latitude, self.location_longitude, self.location_elevation, self.location_address, 
                self.location_type) + super(FavoriteItem, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_favorite_item(self)


class LoginDevice(Column):
    def __init__(self):
        super(LoginDevice, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.id = None  # [TEXT]
        self.name = None  # [TEXT]
        self.type = None  # [TEXT]
        self.last_time = None  # [INT]

    def get_values(self):
        return (self.account_id, self.id, self.name, self.type, self.last_time) + super(LoginDevice, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_login_device(self)


class ContactLabel(Column):
    def __init__(self):
        super(ContactLabel, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.id = None  # [TEXT]
        self.name = None  # [TEXT]
        self.users = ''  # [TEXT]
        self.type = 0  # [INT]  CONTACT_LABEL_TYPE

    def get_values(self):
        return (self.account_id, self.id, self.name, self.users, self.type) + super(ContactLabel, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_contact_label(self)


class BankCard(Column):
    def __init__(self):
        super(BankCard, self).__init__()
        self.account_id = None  # 账号ID[TEXT]
        self.bank_name = None  # 银行名称[TEXT]
        self.card_type = None  # 卡片类型[TEXT]
        self.card_number = None  # 卡片尾号[TEXT]
        self.phone_number = None  # 手机号[TEXT]

    def get_values(self):
        return (self.account_id, self.bank_name, self.card_type, self.card_number, self.phone_number) + super(BankCard, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_bank_card(self)


class Story(Column):
    def __init__(self):
        super(Story, self).__init__()
        global g_story_id
        self.account_id = None  # 账号ID[TEXT]
        self.sender_id = None  # 发送者ID[TEXT]
        self.media_path = None  # 文件路径[TEXT]
        self.story_id = g_story_id  # story id[INT]
        g_story_id += 1
        self.timestamp = None  # 时间戳[INT]
        self.location_latitude = 0  # 地址纬度[REAL]
        self.location_longitude = 0  # 地址经度[REAL]
        self.location_elevation = 0  # 地址海拔[REAL]
        self.location_address = None  # 地址名称[TEXT]
        self.location_type = LOCATION_TYPE_GPS  # 地址类型[INT]，LOCATION_TYPE

        self.comments = []

    def get_values(self):
        return (self.account_id, self.sender_id, self.media_path, self.story_id, self.timestamp, self.location_latitude, 
                self.location_longitude, self.location_elevation, self.location_address, self.location_type) + super(Story, self).get_values()

    def create_comment(self):
        comment = StoryComment()
        comment.story_id = self.story_id
        comment.deleted = self.deleted
        comment.source = self.source
        self.comments.append(comment)
        return comment

    def insert_db(self, im):
        if isinstance(im, IM):
            for comment in self.comments:
                comment.insert_db(im)
            im.db_insert_table_story(self)


class StoryComment(Column):
    def __init__(self):
        super(StoryComment, self).__init__()
        self.story_id = 0  # ID[INT]
        self.sender_id = None  # 发送者[TEXT]
        self.content = None  # 内容[TEXT]
        self.timestamp = None  # 时间戳[TEXT]

    def get_values(self):
        return (self.story_id, self.sender_id, self.content, self.timestamp) + super(StoryComment, self).get_values()

    def insert_db(self, im):
        if isinstance(im, IM):
            im.db_insert_table_story_comment(self)


class GenerateModel(object):
    def __init__(self, cache_db, build, ar):
        self.cache_db = cache_db
        self.build = build
        self.account_models = {}
        self.friend_models = {}
        self.chatroom_models = {}
        self.models = []
        self.media_models = []
        self.ar = ar
        self.progresses = {}

    def add_model(self, model):
        if model is not None:
            self.models.append(model)
            if len(self.models) >= 1000:
                self.push_models()

    def push_models(self):
        if len(self.models) > 0:
            pr = ParserResults()
            pr.Categories = DescripCategories.Wechat
            pr.Models.AddRange(self.models)
            pr.Build(self.build)
            ds.Add(pr)
            self.models = []

    def set_progress(self, value, account_id=None):
        v = value
        if v > 100:
            v = 100
        elif v < 0:
            v = 0
        if account_id is not None:
            if account_id in self.progresses and v != self.progresses.get(account_id).Value:
                self.progresses.get(account_id).Value = v
        else:
            for pg in self.progresses.values():
                if v != pg.Value:
                    pg.Value = v

    def get_models(self):
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cache_db))
        self.db.Open()

        self._get_account_models()
        self.set_progress(2)
        self._get_login_device_models()
        self.set_progress(3)
        self._get_bank_card_models()
        self.set_progress(5)
        self._get_friend_models()
        self.set_progress(15)
        self._get_group_models()
        self.set_progress(25)
        self._get_contact_label_models()
        self.set_progress(26)
        self._get_feed_models()
        self.set_progress(45)
        self._get_search_models()
        self.set_progress(50)
        self._get_favorite_models()
        self.set_progress(55)
        self._get_story_models()
        self.set_progress(60)
        self._get_message_models(60, 100)
        self.set_progress(100)
        for pg in self.progresses.values():
            pg.Finish(True)

        self.db.Close()

    def _get_account_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('account'):
            return []

        sql = '''select account_id, account_id_alias, nickname, username, password, photo, telephone, email, gender, age, country, 
                        province, city, address, birthday, signature, source, deleted, repeated
                 from account'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 16)
                    deleted = self._db_reader_get_int_value(r, 17, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    account_id_alias = self._db_reader_get_string_value(r, 1)
                    nickname = self._db_reader_get_string_value(r, 2)
                    username = self._db_reader_get_string_value(r, 3)
                    password = self._db_reader_get_string_value(r, 4)
                    photo = self._db_reader_get_string_value(r, 5, None)
                    telephone = self._db_reader_get_string_value(r, 6)
                    email = self._db_reader_get_string_value(r, 7)
                    gender = self._db_reader_get_int_value(r, 8)
                    country = self._db_reader_get_string_value(r, 10)
                    signature = self._db_reader_get_string_value(r, 15)

                    if account_id in [None, '']:
                        continue
                    
                    model = WeChat.UserAccount()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.Account = account_id
                    model.CustomAccount = account_id_alias
                    model.NickName = nickname
                    model.HeadPortraitPath = photo
                    model.Gender = self._convert_gender(gender)
                    model.Region = country
                    model.Signature = signature
                    model.PhoneNumber = telephone
                    model.Email = email
                    self.add_model(model)

                    if deleted == 0 or account_id not in self.account_models:
                        self.account_models[account_id] = model
                        self.progresses[account_id] = progress['APP', self.build]['ACCOUNT', account_id, model]
                        self.progresses[account_id].Start()

                    if deleted == 0 or self._get_user_key(account_id, account_id) not in self.friend_models:
                        friend = WeChat.Friend()
                        friend.SourceFile = source
                        friend.Deleted = self._convert_deleted_status(deleted)
                        friend.AppUserAccount = self.account_models.get(account_id)
                        friend.Account = account_id
                        friend.NickName = nickname
                        friend.HeadPortraitPath = photo
                        friend.Gender = self._convert_gender(gender)
                        friend.Region = country
                        friend.Signature = signature
                        friend.Type = WeChat.FriendType.Friend
                        self.friend_models[self._get_user_key(account_id, account_id)] = friend
                        self.add_model(friend)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_login_device_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('login_device'):
            return []

        sql = '''select account_id, id, name, type, last_time, source, deleted, repeated
                 from login_device'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 5)
                    deleted = self._db_reader_get_int_value(r, 6, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    id = self._db_reader_get_string_value(r, 1)
                    name = self._db_reader_get_string_value(r, 2)
                    device_type = self._db_reader_get_string_value(r, 3)
                    last_time = self._db_reader_get_int_value(r, 4, None)

                    model = ModelsV2.IM.LoginDevice()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    model.Id = id
                    model.Name = name
                    model.Type = device_type
                    model.LastLoginTime = self._get_timestamp(last_time)
                    self.add_model(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_bank_card_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('bank_card'):
            return []

        sql = '''select account_id, bank_name, card_type, card_number, phone_number, source, deleted, repeated
                 from bank_card'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 5)
                    deleted = self._db_reader_get_int_value(r, 6, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    bank_name = self._db_reader_get_string_value(r, 1)
                    card_type = self._db_reader_get_string_value(r, 2)
                    card_number = self._db_reader_get_string_value(r, 3)
                    phone_number = self._db_reader_get_string_value(r, 4)

                    model = Base.BankCard()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    model.BankName = bank_name
                    model.CardType = card_type
                    model.CardNumber = card_number
                    model.PhoneNumber = phone_number
                    self.add_model(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_friend_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('friend'):
            return []

        sql = '''select account_id, friend_id, friend_id_alias, nickname, remark, photo, type, gender, region, signature, 
                        add_time, source, deleted, repeated
                 from friend '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 11)
                    deleted = self._db_reader_get_int_value(r, 12, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    user_id = self._db_reader_get_string_value(r, 1)
                    user_id_alias = self._db_reader_get_string_value(r, 2)
                    nickname = self._db_reader_get_string_value(r, 3)
                    remark = self._db_reader_get_string_value(r, 4)
                    photo = self._db_reader_get_string_value(r, 5)
                    user_type = self._db_reader_get_int_value(r, 6)
                    gender = self._db_reader_get_int_value(r, 7)
                    region = self._db_reader_get_string_value(r, 8)
                    signature = self._db_reader_get_string_value(r, 9)
                    add_time = self._db_reader_get_int_value(r, 10)
                    
                    if account_id in [None, ''] or user_id in [None, '']:
                        continue

                    model = WeChat.Friend()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    model.Account = user_id
                    model.CustomAccount = user_id_alias
                    model.NickName = nickname
                    model.HeadPortraitPath = photo
                    model.Gender = self._convert_gender(gender)
                    model.Region = region
                    model.Signature = signature
                    model.RemarkName = remark
                    model.Type = self._convert_friend_type(user_type)
                    self.add_model(model)

                    if deleted == 0 or self._get_user_key(account_id, user_id) not in self.friend_models:
                        self.friend_models[self._get_user_key(account_id, user_id)] = model
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_group_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('chatroom'):
            return []

        sql = '''select account_id, chatroom_id, name, photo, is_saved, notice, owner_id, create_time, join_time, 
                        sp_id, source, deleted, repeated
                 from chatroom'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 10)
                    deleted = self._db_reader_get_int_value(r, 11, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    user_id = self._db_reader_get_string_value(r, 1)
                    nickname = self._db_reader_get_string_value(r, 2)
                    photo = self._db_reader_get_string_value(r, 3, None)
                    is_saved = self._db_reader_get_int_value(r, 4)
                    notice = self._db_reader_get_string_value(r, 5)
                    owner_id = self._db_reader_get_string_value(r, 6)
                    create_time = self._db_reader_get_int_value(r, 7)
                    join_time = self._db_reader_get_int_value(r, 8)
                    sp_id = self._db_reader_get_int_value(r, 9)

                    if account_id in [None, ''] or user_id in [None, '']:
                        continue

                    model = WeChat.Group()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    model.Account = user_id
                    model.NickName = nickname
                    model.HeadPortraitPath = photo
                    model.Notice = notice
                    model.IsSave = is_saved != 0
                    member_models, owner_model = self._get_chatroom_member_models(account_id, user_id, sp_id, deleted, owner_id)
                    model.GroupOwner = owner_model
                    model.Members.AddRange(member_models)
                    model.JoinTime = self._get_timestamp(join_time)
                    self.add_model(model)

                    if deleted == 0 or self._get_user_key(account_id, user_id) not in self.chatroom_models:
                        self.chatroom_models[self._get_user_key(account_id, user_id)] = model
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_message_models(self, progress_start, progress_end):
        if canceller.IsCancellationRequested:
            return []
        tables = self._db_get_message_tables()
        for i, table in enumerate(tables):
            sql = '''select account_id, talker_id, talker_type, sender_id, timestamp, msg_id, type, content, media_path, 
                            media_thum_path, status, is_recall, location_latitude, location_longitude, location_elevation, location_address, 
                            location_type, deal_money, deal_description, deal_remark, deal_status, deal_mode, deal_create_time, 
                            deal_expire_time, link_url, link_title, link_content, link_image, link_from, business_card_username, 
                            business_card_nickname, business_card_gender, business_card_photo, business_card_region, business_card_signature, 
                            source, deleted, repeated
                     from {} '''.format(table)
            try:
                cmd = self.db.CreateCommand()
                cmd.CommandText = sql
                r = cmd.ExecuteReader()
                while r.Read():
                    if canceller.IsCancellationRequested:
                        break
                    deleted = 0
                    try:
                        source = self._db_reader_get_string_value(r, 35)
                        deleted = self._db_reader_get_int_value(r, 36, None)
                        account_id = self._db_reader_get_string_value(r, 0)
                        talker_id = self._db_reader_get_string_value(r, 1)
                        talker_type = self._db_reader_get_int_value(r, 2)
                        sender_id = self._db_reader_get_string_value(r, 3)
                        timestamp = self._db_reader_get_int_value(r, 4, None)
                        msg_type = self._db_reader_get_int_value(r, 6)
                        content = self._db_reader_get_string_value(r, 7)
                        media_path = self._db_reader_get_string_value(r, 8)
                        media_thum_path = self._db_reader_get_string_value(r, 9)
                        is_recall = self._db_reader_get_int_value(r, 11)
                    
                        location_latitude = self._db_reader_get_float_value(r, 12)
                        location_longitude = self._db_reader_get_float_value(r, 13)
                        location_elevation = self._db_reader_get_float_value(r, 14)
                        location_address = self._db_reader_get_string_value(r, 15)
                        location_type = self._db_reader_get_int_value(r, 16)
                    
                        deal_money = self._db_reader_get_string_value(r, 17)
                        deal_description = self._db_reader_get_string_value(r, 18)
                        deal_remark = self._db_reader_get_string_value(r, 19)
                        deal_status = self._db_reader_get_int_value(r, 20)
                        deal_mode = self._db_reader_get_int_value(r, 21)
                        deal_create_time = self._db_reader_get_int_value(r, 22)
                        deal_expire_time = self._db_reader_get_int_value(r, 23)

                        link_url = self._db_reader_get_string_value(r, 24)
                        link_title = self._db_reader_get_string_value(r, 25)
                        link_content = self._db_reader_get_string_value(r, 26)
                        link_image = self._db_reader_get_string_value(r, 27)
                        link_from = self._db_reader_get_string_value(r, 28)

                        business_card_username = self._db_reader_get_string_value(r, 29)
                        business_card_nickname = self._db_reader_get_string_value(r, 30)
                        business_card_gender = self._db_reader_get_int_value(r, 31)
                        business_card_photo = self._db_reader_get_string_value(r, 32)
                        business_card_region = self._db_reader_get_string_value(r, 33)
                        business_card_signature = self._db_reader_get_string_value(r, 34)

                        if talker_id.endswith("@chatroom"):
                            model = WeChat.GroupMessage()
                            model.Group = self.chatroom_models.get(self._get_user_key(account_id, talker_id))
                        else:
                            model = WeChat.FriendMessage()
                            model.Friend = self.friend_models.get(self._get_user_key(account_id, talker_id))
                        model.SourceFile = source
                        model.Deleted = self._convert_deleted_status(deleted)
                        model.AppUserAccount = self.account_models.get(account_id)
                        model.Sender = self.friend_models.get(self._get_user_key(account_id, sender_id))
                        #model.SourceData = content
                        model.CreateTime = self._get_timestamp(timestamp)
                        model.IsRecall = is_recall != 0
                        if msg_type == MESSAGE_CONTENT_TYPE_SYSTEM:
                            model.Way = CommonEnum.MessageWay.System
                        elif sender_id == account_id:
                            model.Way = CommonEnum.MessageWay.Send
                        else:
                            model.Way = CommonEnum.MessageWay.Receive
                    
                        if msg_type == MESSAGE_CONTENT_TYPE_IMAGE:
                            model.Content = Base.Content.ImageContent(model)
                            media_model = Base.MediaFile.ImageFile(model)
                            media_model.Path = media_path
                            model.Content.Value = media_model
                            if is_valid_media_model_path(media_path):
                                self.ar.save_media_model(media_model)
                        elif msg_type == MESSAGE_CONTENT_TYPE_VOICE:
                            model.Content = Base.Content.VoiceContent(model)
                            media_model = Base.MediaFile.AudioFile(model)
                            media_model.Path = media_path
                            model.Content.Value = media_model
                            if is_valid_media_model_path(media_path):
                                self.ar.save_media_model(media_model)
                        elif msg_type == MESSAGE_CONTENT_TYPE_VIDEO:
                            model.Content = Base.Content.VideoContent(model)
                            if media_path not in [None, '']:
                                media_model = Base.MediaFile.VideoFile(model)
                                media_model.Path = media_path
                                model.Content.Value = media_model
                                if is_valid_media_model_path(media_path):
                                    self.ar.save_media_model(media_model)
                            elif is_valid_media_model_path(media_thum_path):
                                media_model = Base.MediaFile.VideoThumbnailFile(model)
                                media_model.Deleted = self._convert_deleted_status(1)
                                media_model.Path = media_path
                                model.Content.Value = media_model
                                self.ar.save_media_model(media_model)
                            else:
                                media_model = Base.MediaFile.VideoFile(model)
                                media_model.Path = media_path
                                model.Content.Value = media_model
                        elif msg_type == MESSAGE_CONTENT_TYPE_CONTACT_CARD:
                            model.Content = Base.Content.BusinessCardContent(model)
                            model.Content.Value = WeChat.BusinessCard()
                            #model.Content.Value.AppUserAccount = 
                            model.Content.Value.UserID = business_card_username
                            model.Content.Value.NickName = business_card_nickname
                            model.Content.Value.Gender = self._convert_gender(business_card_gender)
                            model.Content.Value.Region = business_card_region
                            model.Content.Value.Signature = business_card_signature
                        elif msg_type == MESSAGE_CONTENT_TYPE_LOCATION:
                            model.Content = Base.Content.LocationContent(model)
                            model.Content.Value = Base.Location()
                            model.Content.Value.SourceType = LocationSourceType.App
                            model.Content.Value.Time = model.CreateTime
                            model.Content.Value.AddressName = location_address
                            model.Content.Value.Coordinate = Base.Coordinate(location_longitude, location_latitude, self._convert_location_type(location_type))
                            self.add_model(model.Content.Value)
                        elif msg_type == MESSAGE_CONTENT_TYPE_LINK:
                            model.Content = Base.Content.LinkContent(model)
                            model.Content.Value = Base.Link()
                            model.Content.Value.Title = link_title
                            model.Content.Value.Description = link_content
                            model.Content.Value.Url = link_url
                            model.Content.Value.ImagePath = link_image
                        elif msg_type == MESSAGE_CONTENT_TYPE_RED_ENVELPOE:
                            model.Content = Base.Content.RedEnvelopeContent(model)
                            model.Content.Value = WeChat.RedEnvelope()
                            model.Content.Value.Expiration = self._get_timestamp(deal_expire_time)
                            model.Content.Value.Title = deal_description
                            model.Content.Value.Remark = deal_remark
                            model.Content.Value.Status = self._convert_deal_status(deal_status)
                        elif msg_type == MESSAGE_CONTENT_TYPE_ATTACHMENT:
                            model.Content = Base.Content.AttachmentContent(model)
                            model.Content.Value = Base.Attachment()
                            model.Content.Value.FileName = link_title
                            model.Content.Value.Path = link_url
                        elif msg_type == MESSAGE_CONTENT_TYPE_SPLIT_BILL:
                            model.Content = Base.Content.SplitBillContent(model)
                            model.Content.Value = WeChat.SplitBill()
                            model.Content.Value.Expiration = self._get_timestamp(deal_expire_time)
                            model.Content.Value.Title = deal_description
                            model.Content.Value.Remark = deal_remark
                            model.Content.Value.Mode = self._convert_deal_mode(deal_mode)
                            model.Content.Value.Status = self._convert_deal_status(deal_status)
                        elif msg_type == MESSAGE_CONTENT_TYPE_TRANSFER:
                            model.Content = Base.Content.TransferContent(model)
                            model.Content.Value = WeChat.Transfer()
                            model.Content.Value.Expiration = self._get_timestamp(deal_expire_time)
                            model.Content.Value.Title = deal_description
                            model.Content.Value.Remark = deal_remark
                            model.Content.Value.MoneyOfString = deal_money
                            model.Content.Value.Status = self._convert_deal_status(deal_status)
                        elif msg_type == MESSAGE_CONTENT_TYPE_APPMESSAGE:
                            model.Content = Base.Content.TemplateContent(model)
                            try:
                                title, content, url = content.split('#*#', 2)
                            except Exception as e:
                                #print('debug', e)
                                title = url = ''
                                content = content
                            model.Content.Title = title
                            model.Content.Content = content
                            model.Content.InfoUrl = url
                            model.Content.SendTime = self._get_timestamp(timestamp)
                        elif msg_type == MESSAGE_CONTENT_TYPE_SEMI_XML:
                            model.Content = Base.Content.LinkSetContent(model)
                            parser = SemiXmlParser()
                            parser.parse(content.encode('utf-8'))
                            items = parser.export_items()
                            for item in items:
                                link = Base.Link()
                                link.Title = getattr(item.get('title'), 'value', None)
                                link.Description = getattr(item.get('digest'), 'value', None)
                                link.Url = getattr(item.get('url'), 'value', None)
                                link.ImagePath = getattr(item.get('cover'), 'value', None)
                                model.Content.Values.Add(link)
                        elif msg_type == MESSAGE_CONTENT_TYPE_LINK_SET:
                            model.Content = Base.Content.LinkSetContent(model)
                            items = []
                            try:
                                items = json.loads(content)
                            except Exception as e:
                                pass
                            for item in items:
                                link = Base.Link()
                                link.Title = item.get('title')
                                link.Description = item.get('description')
                                link.Url = item.get('url')
                                link.ImagePath = item.get('image')
                                model.Content.Values.Add(link)
                        else:
                            model.Content = Base.Content.TextContent(model)
                            model.Content.Value = content
                        self.add_model(model)
                    except Exception as e:
                        if deleted == 0:
                            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            except Exception as e:
                TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.set_progress(progress_start + i * 100 / len(tables) * (progress_end - progress_start) / 100)
        self.push_models()

    def _get_chatroom_member_models(self, account_id, chatroom_id, sp_id, deleted, owner_id):
        if account_id in [None, ''] or chatroom_id in [None, '']:
            return [], None
        models = []
        owner_model = None
        if sp_id not in [None, 0]:
            sql = '''select member_id, display_name
                     from chatroom_member
                     where account_id='{0}' and chatroom_id='{1}' and sp_id='{2}' '''.format(account_id, chatroom_id, sp_id)
        else:
            sql = '''select member_id, display_name
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
                    member_id = self._db_reader_get_string_value(r, 0)
                    display_name = self._db_reader_get_string_value(r, 1)
                    if member_id not in [None, '']:
                        model = GroupMember()
                        model.User = self.friend_models.get(self._get_user_key(account_id, member_id))
                        if model.User is not None:
                            model.SourceFile = model.User.SourceFile
                            model.Deleted = model.User.Deleted
                        model.NickName = display_name
                        models.append(model)
                        if member_id == owner_id:
                            owner_model = model
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        except Exception as e:
            if deleted == 0:
                TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        return models, owner_model

    def _get_feed_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('feed'):
            return []
        models = []

        sql = '''select account_id, sender_id, content, image_path, video_path, timestamp, link_url, 
                        link_title, link_content, link_image, link_from, like_id, like_count, comment_id, 
                        comment_count, location_latitude, location_longitude, location_elevation, 
                        location_address, location_type, source, deleted, repeated
                 from feed '''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 20)
                    deleted = self._db_reader_get_int_value(r, 21, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    sender_id = self._db_reader_get_string_value(r, 1)
                    content = self._db_reader_get_string_value(r, 2)
                    image_path = self._db_reader_get_string_value(r, 3)
                    video_path = self._db_reader_get_string_value(r, 4)
                    timestamp = self._db_reader_get_int_value(r, 5, None)
                    link_url = self._db_reader_get_string_value(r, 6)
                    link_title = self._db_reader_get_string_value(r, 7)
                    link_content = self._db_reader_get_string_value(r, 8)
                    link_image = self._db_reader_get_string_value(r, 9)
                    link_from = self._db_reader_get_string_value(r, 10)
                    like_id = self._db_reader_get_int_value(r, 11)
                    like_count = self._db_reader_get_int_value(r, 12)
                    comment_id = self._db_reader_get_int_value(r, 13)
                    comment_count = self._db_reader_get_int_value(r, 14)
                    location_latitude = self._db_reader_get_float_value(r, 15)
                    location_longitude = self._db_reader_get_float_value(r, 16)
                    location_elevation = self._db_reader_get_float_value(r, 10)
                    location_address = self._db_reader_get_string_value(r, 18)
                    location_type = self._db_reader_get_int_value(r, 19)
                    
                    model = WeChat.Dynamic()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    #model.SourceData
                    model.Sender = self.friend_models.get(self._get_user_key(account_id, sender_id))
                    model.CreateTime = self._get_timestamp(timestamp)
                    if content not in [None, '']:
                        text_content = Base.Content.TextContent(model)
                        text_content.Value = content
                        model.Contents.Add(text_content)
                    if image_path not in [None, '']:
                        images_content = Base.Content.ImagesContent(model)
                        images = image_path.split(',')
                        for image in images:
                            if image not in [None, '']:
                                media_model = Base.MediaFile.ImageFile()
                                media_model.Path = image
                                images_content.Values.Add(media_model)
                                if is_valid_media_model_path(image):
                                    self.ar.save_media_model(media_model)
                        model.Contents.Add(images_content)
                    if video_path not in [None, '']:
                        videos = video_path.split(',')
                        for video in videos:
                            if video not in [None, '']:
                                video_content = Base.Content.VideoContent(model)
                                media_model = Base.MediaFile.VideoFile()
                                media_model.Path = video
                                video_content.Value = media_model
                                model.Contents.Add(video_content)
                                if is_valid_media_model_path(video):
                                    self.ar.save_media_model(media_model)
                    if link_url not in [None, '']:
                        l_content = Base.Content.LinkContent(model)
                        l_content.Value = Base.Link()
                        l_content.Value.Title = link_title
                        l_content.Value.Description = link_content
                        l_content.Value.Url = link_url
                        l_content.Value.ImagePath = link_image
                        model.Contents.Add(l_content)
                    if location_latitude != 0 or location_longitude != 0:
                        location_content = Base.Content.LocationContent(model)
                        location_content.Value = Base.Location()
                        location_content.Value.SourceType = LocationSourceType.App
                        location_content.Value.Time = model.CreateTime
                        location_content.Value.AddressName = location_address
                        location_content.Value.Coordinate = Base.Coordinate(location_longitude, location_latitude, self._convert_location_type(location_type))
                        model.Contents.Add(location_content)
                        self.add_model(location_content)
                    if like_id not in [None, 0]:
                        model.Likers.AddRange(self._get_feed_likes(model, like_id, deleted))
                    if comment_id not in [None, 0]:
                        model.Comments.AddRange(self._get_feed_comments(model, comment_id, deleted))
                    self.add_model(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_search_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('search'):
            return []
        models = []

        sql = '''select account_id, key, timestamp, source, deleted, repeated
                 from search'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 3)
                    deleted = self._db_reader_get_int_value(r, 4, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    key = self._db_reader_get_string_value(r, 1)
                    timestamp = self._db_reader_get_int_value(r, 2, None)

                    model = SearchRecord()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    model.Keyword = key
                    model.CreateTime = self._get_timestamp(timestamp)
                    self.add_model(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_favorite_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('favorite'):
            return []
        models = []

        sql = '''select account_id, favorite_id, talker_id, talker_name, talker_type, 
                        timestamp, source, deleted, repeated
                 from favorite'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 6)
                    deleted = self._db_reader_get_int_value(r, 7, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    favorite_id = self._db_reader_get_int_value(r, 1)
                    talker_id = self._db_reader_get_string_value(r, 2)
                    talker_name = self._db_reader_get_string_value(r, 3, None)
                    talker_type = self._db_reader_get_int_value(r, 4)
                    timestamp = self._db_reader_get_int_value(r, 5, None)

                    model = Base.Favorites()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    model.CreateTime = self._get_timestamp(timestamp)
                    if favorite_id not in [None, 0]:
                        model.Contents.AddRange(self._get_favorite_item_models(account_id, favorite_id, deleted))
                    self.add_model(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_favorite_item_models(self, account_id, favorite_id, favorite_deleted):
        if canceller.IsCancellationRequested:
            return []
        models = []

        sql = '''select favorite_id, type, sender_id, sender_name, content, media_path, timestamp, 
                        link_url, link_title, link_content, link_image, link_from, location_latitude, 
                        location_longitude, location_elevation, location_address, location_type, 
                        source, deleted, repeated
                 from favorite_item
                 where favorite_id = {} '''.format(favorite_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 17)
                    deleted = self._db_reader_get_int_value(r, 18, None)
                    fav_type = self._db_reader_get_int_value(r, 1)
                    sender_id = self._db_reader_get_string_value(r, 2)
                    sender_name = self._db_reader_get_string_value(r, 3, None)
                    content = self._db_reader_get_string_value(r, 4)
                    media_path = self._db_reader_get_string_value(r, 5)
                    timestamp = self._db_reader_get_int_value(r, 6, None)
                    
                    link_url = self._db_reader_get_string_value(r, 7)
                    link_title = self._db_reader_get_string_value(r, 8)
                    link_content = self._db_reader_get_string_value(r, 9)
                    link_image = self._db_reader_get_string_value(r, 10)
                    link_from = self._db_reader_get_string_value(r, 11)

                    location_latitude = self._db_reader_get_float_value(r, 12)
                    location_longitude = self._db_reader_get_float_value(r, 13)
                    location_elevation = self._db_reader_get_float_value(r, 14)
                    location_address = self._db_reader_get_string_value(r, 15)
                    location_type = self._db_reader_get_int_value(r, 16)
                    
                    model = Base.FavoritesContent()
                    model.Sender = self.friend_models.get(self._get_user_key(account_id, sender_id))
                    if fav_type == FAV_TYPE_IMAGE:
                        model.Content = Base.Content.ImageContent(model)
                        media_model = Base.MediaFile.ImageFile()
                        media_model.Path = media_path
                        model.Content.Value = media_model
                        if is_valid_media_model_path(media_path):
                            self.ar.save_media_model(media_model)
                    elif fav_type == FAV_TYPE_VOICE:
                        model.Content = Base.Content.VoiceContent(model)
                        media_model = Base.MediaFile.AudioFile()
                        media_model.Path = media_path
                        model.Content.Value = media_model
                        if is_valid_media_model_path(media_path):
                            self.ar.save_media_model(media_model)
                    elif fav_type == FAV_TYPE_VIDEO:
                        model.Content = Base.Content.VideoContent(model)
                        media_model = Base.MediaFile.VideoFile()
                        media_model.Path = media_path
                        model.Content.Value = media_model
                        if is_valid_media_model_path(media_path):
                            self.ar.save_media_model(media_model)
                    elif fav_type == FAV_TYPE_LINK:
                        model.Content = Base.Content.LinkContent(model)
                        model.Content.Value = Base.Link()
                        model.Content.Value.Title = link_title
                        model.Content.Value.Description = link_content
                        model.Content.Value.Url = link_url
                        model.Content.Value.ImagePath = link_image
                    elif fav_type == FAV_TYPE_LOCATION:
                        model.Content = Base.Content.LocationContent(model)
                        model.Content.Value = Base.Location()
                        model.Content.Value.SourceType = LocationSourceType.App
                        model.Content.Value.Time = self._get_timestamp(timestamp)
                        model.Content.Value.AddressName = location_address
                        model.Content.Value.Coordinate = Base.Coordinate(location_longitude, location_latitude, self._convert_location_type(location_type))
                        #self.add_model(model.Content.Value)
                    elif fav_type == [FAV_TYPE_ATTACHMENT, FAV_TYPE_ATTACHMENT_2]:
                        model.Content = Base.Content.AttachmentContent(model)
                        model.Content.Value = Base.Attachment()
                        model.Content.Value.FileName = content
                        model.Content.Value.Path = media_path
                    else:
                        model.Content = Base.Content.TextContent(model)
                        model.Content.Value = content
                    models.append(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        except Exception as e:
            if favorite_deleted == 0:
                TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        return models

    def _get_contact_label_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('contact_label'):
            return []

        sql = '''select account_id, id, name, users, type, source, deleted, repeated
                 from contact_label'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 5)
                    deleted = self._db_reader_get_int_value(r, 6, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    name = self._db_reader_get_string_value(r, 2)
                    users = (self._db_reader_get_string_value(r, 3)).split(',')
                    cl_type = self._db_reader_get_int_value(r, 4)

                    if cl_type == CONTACT_LABEL_TYPE_GROUP:
                        model = FriendGroup()
                        model.SourceFile = source
                        model.Deleted = self._convert_deleted_status(deleted)
                        model.AppUserAccount = self.account_models.get(account_id)
                        model.Name = name
                        for user_id in users:
                            friend = self.friend_models.get(self._get_user_key(account_id, user_id))
                            if friend is not None:
                                model.Friends.Add(friend)
                        self.add_model(model)
                    elif cl_type == CONTACT_LABEL_TYPE_EMERGENCY:
                        model = Base.EmergencyContacts()
                        model.SourceFile = source
                        model.Deleted = self._convert_deleted_status(deleted)
                        model.AppUserAccount = self.account_models.get(account_id)
                        for user_id in users:
                            friend = self.friend_models.get(self._get_user_key(account_id, user_id))
                            if friend is not None:
                                model.Friends.Add(friend)
                        self.add_model(model)

                    
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_emergency_contact_models(self):
        pass

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

    def _db_get_message_tables(self):
        tables = []
        try:
            sql = r"select name from sqlite_master where type='table' and name like 'message%' "
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                tables.append(self._db_reader_get_string_value(r, 0))
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        return tables

    def _get_user_key(self, account_id, user_id):
        if account_id is None or user_id is None:
            return ''
        else:
            return account_id + "#*#" + user_id

    def _get_feed_likes(self, feed_model, like_id, feed_deleted):
        models = []
        sql = '''select sender_id, sender_name, timestamp, source, deleted, repeated
                 from feed_like
                 where like_id='{0}' '''.format(like_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            deleted = 0
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    sender_id = self._db_reader_get_string_value(r, 0)
                    sender_name = self._db_reader_get_string_value(r, 1, None)
                    timestamp = self._db_reader_get_int_value(r, 2, None)
                    source = self._db_reader_get_string_value(r, 3)
                    deleted = self._db_reader_get_int_value(r, 4, None)
                    
                    model = Base.Like(feed_model)
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.Sender = self.friend_models.get(self._get_user_key(feed_model.AppUserAccount.Account, sender_id))
                    model.CreateTime = self._get_timestamp(timestamp)
                    models.append(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        except Exception as e:
            if feed_deleted == 0:
                TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        return models

    def _get_feed_comments(self, feed_model, comment_id, feed_deleted):
        models = []
        sql = '''select sender_id, sender_name, ref_user_id, ref_user_name, content, timestamp, source, deleted, repeated
                 from feed_comment
                 where comment_id='{0}' '''.format(comment_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            deleted = 0
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    source = self._db_reader_get_string_value(r, 6)
                    deleted = self._db_reader_get_int_value(r, 7, None)
                    sender_id = self._db_reader_get_string_value(r, 0)
                    sender_name = self._db_reader_get_string_value(r, 1, None)
                    ref_user_id = self._db_reader_get_string_value(r, 2, None)
                    ref_user_name = self._db_reader_get_string_value(r, 3, None)
                    content = self._db_reader_get_string_value(r, 4)
                    timestamp = self._db_reader_get_int_value(r, 5, None)
                    
                    model = Base.Comment(feed_model)
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.From = self.friend_models.get(self._get_user_key(feed_model.AppUserAccount.Account, sender_id))
                    if ref_user_id not in [None, '']:
                        model.To = self.friend_models.get(self._get_user_key(feed_model.AppUserAccount.Account, ref_user_id))
                    model.Content = content
                    model.CreateTime = self._get_timestamp(timestamp)
                    models.append(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        except Exception as e:
            if feed_deleted == 0:
                TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        return models

    def _get_story_models(self):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('story'):
            return []
        models = []

        sql = '''select account_id, sender_id, media_path, story_id, timestamp, location_latitude, 
                        location_longitude, location_elevation, location_address, location_type, 
                        source, deleted, repeated
                 from story'''
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 10)
                    deleted = self._db_reader_get_int_value(r, 11, None)
                    account_id = self._db_reader_get_string_value(r, 0)
                    sender_id = self._db_reader_get_string_value(r, 1)
                    media_path = self._db_reader_get_string_value(r, 2)
                    story_id = self._db_reader_get_int_value(r, 3)
                    timestamp = self._db_reader_get_int_value(r, 4, None)

                    location_latitude = self._db_reader_get_float_value(r, 5)
                    location_longitude = self._db_reader_get_float_value(r, 6)
                    location_elevation = self._db_reader_get_float_value(r, 7)
                    location_address = self._db_reader_get_string_value(r, 8)
                    location_type = self._db_reader_get_int_value(r, 9)

                    model = WeChat.Story()
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.AppUserAccount = self.account_models.get(account_id)
                    model.CreateTime = self._get_timestamp(timestamp)
                    model.Sender = self.friend_models.get(self._get_user_key(account_id, sender_id))
                    model.CreateTime = self._get_timestamp(timestamp)
                    if media_path not in [None, '']:
                        video_content = Base.Content.VideoContent(model)
                        media_model = Base.MediaFile.VideoFile()
                        media_model.Path = media_path
                        video_content.Value = media_model
                        model.Contents.Add(video_content)
                        if is_valid_media_model_path(media_path):
                            self.ar.save_media_model(media_model)
                    if location_latitude != 0 or location_longitude != 0:
                        location_content = Base.Content.LocationContent(model)
                        location_content.Value = Base.Location()
                        location_content.Value.SourceType = LocationSourceType.App
                        location_content.Value.Time = model.CreateTime
                        location_content.Value.AddressName = location_address
                        location_content.Value.Coordinate = Base.Coordinate(location_longitude, location_latitude, self._convert_location_type(location_type))
                        model.Contents.Add(location_content)
                        self.add_model(location_content)
                    if story_id not in [None, 0]:
                        model.Comments.AddRange(self._get_story_comment_models(model, story_id, deleted))
                    self.add_model(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
            self.push_models()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))

    def _get_story_comment_models(self, story_model, story_id, story_deleted):
        if canceller.IsCancellationRequested:
            return []
        if not self._db_has_table('story_comment'):
            return []
        models = []

        sql = '''select sender_id, content, timestamp, source, deleted, repeated
                 from story_comment
                 where story_id = {} '''.format(story_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                deleted = 0
                try:
                    source = self._db_reader_get_string_value(r, 3)
                    deleted = self._db_reader_get_int_value(r, 4, None)
                    sender_id = self._db_reader_get_string_value(r, 0)
                    content = self._db_reader_get_string_value(r, 1)
                    timestamp = self._db_reader_get_int_value(r, 2, None)
                    
                    model = Base.Comment(story_model)
                    model.SourceFile = source
                    model.Deleted = self._convert_deleted_status(deleted)
                    model.From = self.friend_models.get(self._get_user_key(story_model.AppUserAccount.Account, sender_id))
                    model.Content = content
                    model.CreateTime = self._get_timestamp(timestamp)
                    models.append(model)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        except Exception as e:
            if story_deleted == 0:
                TraceService.Trace(TraceLevel.Error, "model_wechat.py Error: db:{} LINE {}".format(self.cache_db, traceback.format_exc()))
        return models

    @staticmethod
    def _get_timestamp(timestamp):
        if timestamp in [None, 0]:
            return None
        try:
            ts = TimeStamp.FromUnixTime(timestamp, False)
            if not ts.IsValidForSmartphone():
                ts = None
            return ts
        except Exception as e:
            return None

    @staticmethod
    def _convert_friend_type(friend_type):
        if friend_type == FRIEND_TYPE_FRIEND:
            return WeChat.FriendType.Friend
        elif friend_type == FRIEND_TYPE_OFFICIAL:
            return WeChat.FriendType.Official
        elif friend_type == FRIEND_TYPE_PROGRAM:
            return WeChat.FriendType.Program
        elif friend_type == FRIEND_TYPE_BLOCKED:
            return WeChat.FriendType.BlackList
        else:
            return WeChat.FriendType.None

    @staticmethod
    def _convert_gender(gender):
        if gender == GENDER_MALE:
            return CommonEnum.Gender.Man
        elif gender == GENDER_FEMALE:
            return CommonEnum.Gender.Woman
        else:
            return CommonEnum.Gender.None

    @staticmethod
    def _convert_deal_mode(mode):
        if mode == DEAL_MODE_IDENTICAL:
            return CommonEnum.SplitBillMode.Identical
        elif mode == DEAL_MODE_SPECIFIED:
            return CommonEnum.SplitBillMode.Specified
        else:
            return CommonEnum.SplitBillMode.None

    @staticmethod
    def _convert_deal_status(status):
        if status == DEAL_STATUS_RED_ENVELOPE_NONE:
            return CommonEnum.RedEnvelopeStatus.None
        elif status == DEAL_STATUS_RED_ENVELOPE_OPENED:
            return CommonEnum.RedEnvelopeStatus.Opened
        elif status == DEAL_STATUS_RED_ENVELOPE_UNOPENED:
            return CommonEnum.RedEnvelopeStatus.UnOpened
        elif status == DEAL_STATUS_TRANSFER_NONE:
            return CommonEnum.TransferStatus.None
        elif status == DEAL_STATUS_TRANSFER_UNRECEIVED:
            return CommonEnum.TransferStatus.UnReceived
        elif status == DEAL_STATUS_TRANSFER_RECEIVED:
            return CommonEnum.TransferStatus.Received
        elif status == DEAL_STATUS_TRANSFER_BACK:
            return CommonEnum.TransferStatus.Back
        elif status == DEAL_STATUS_SPLIT_BILL_NONE:
            return CommonEnum.SplitBillStatus.None
        elif status == DEAL_STATUS_SPLIT_BILL_NONEED:
            return CommonEnum.SplitBillStatus.NoNeed
        elif status == DEAL_STATUS_SPLIT_BILL_UNPAID:
            return CommonEnum.SplitBillStatus.UnPaid
        elif status == DEAL_STATUS_SPLIT_BILL_PAID:
            return CommonEnum.SplitBillStatus.Paid
        elif status == DEAL_STATUS_SPLIT_BILL_UNDONE:
            return CommonEnum.SplitBillStatus.UnDone
        elif status == DEAL_STATUS_SPLIT_BILL_DONE:
            return CommonEnum.SplitBillStatus.Done
        else:
            return CommonEnum.RedEnvelopeStatus.None

    @staticmethod
    def _convert_location_type(location_type):
        if location_type == LOCATION_TYPE_GPS_MC:
            return CommonEnum.CoordinateType.GPSmc
        elif location_type == LOCATION_TYPE_GOOGLE:
            return CommonEnum.CoordinateType.Google
        elif location_type == LOCATION_TYPE_GOOGLE_MC:
            return CommonEnum.CoordinateType.Googlemc
        elif location_type == LOCATION_TYPE_BAIDU:
            return CommonEnum.CoordinateType.Baidu
        elif location_type == LOCATION_TYPE_BAIDU_MC:
            return CommonEnum.CoordinateType.Baidumc
        elif location_type == LOCATION_TYPE_MAPBAR:
            return CommonEnum.CoordinateType.MapBar
        elif location_type == LOCATION_TYPE_MAP51:
            return CommonEnum.CoordinateType.Map51
        else:
            return CommonEnum.CoordinateType.GPS

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


def is_valid_media_model_path(path):
    return path not in [None, ''] and not path.startswith('http')
