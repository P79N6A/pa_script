#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('Mono.HttpUtility')
try:
    clr.AddReference('model_im')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *

import Mono
import os
import sqlite3
import json
import model_im
import uuid
import time
import gc
import re

# app数据库版本
VERSION_APP_VALUE = 1

def analyze_youxin(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = YouXinParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('有信')
    gc.collect()
    return pr

def execute(node,extracteDeleted):
    return analyze_renren(node, extracteDeleted, False)

class YouXinParser(model_im.IM):
    def __init__(self, node, extract_deleted, extract_source):
        super(YouXinParser, self).__init__()
        self.extract_deleted = False
        self.extract_source = extract_source
        self.root = node
        self.app_name = 'YouXin'
        self.cache_path =ds.OpenCachePath('YouXin')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

        nameValues.SafeAddValue('1030087', self.cache_db)

    def parse(self):
        if self.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.db_create(self.cache_db)
            user_list = self.get_user_list()
            for user in user_list:
                self.contacts = {}
                self.user = user
                self.parse_user()
                self.user = None
                self.contacts = None
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
        models = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db).get_models()
        return models

    def get_user_list(self):
        user_list = []
        node = self.root.GetByPath('/databases')
        if node is not None:
            for file in os.listdir(node.PathWithMountPoint):
                str = re.search('\\d+', file, re.M | re.I)
                if str is not None:
                    user = str.group(0)
                    if user in user_list:
                        continue
                    user_list.append(user)
        return user_list

    def parse_user(self):
        self.get_user()
        self.get_contacts()
        self.get_chats()
    
    def get_user(self):
        if self.user is None:
            return False

        account = model_im.Account()
        account.source = self.app_name
        account.account_id = self.user

        dbPath = self.root.GetByPath('/databases/' + 'youxin_db_' + self.user)
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            self.db_insert_table_account(account)
            self.db_commit()
            return True

        if 'MY_NAME_CARD' in db.Tables:
            ts = SQLiteParser.TableSignature('MY_NAME_CARD')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'UID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                account.username = rec['NAME'].Value
                account.gender = 2 if rec['GENDER'].Value == '女' else 1
                account.telephone = rec['MOBILE_NUMBER'].Value
                account.email = rec['EMAIL'].Value
                account.birthday = rec['BIRTHDAY'].Value
                account.photo = rec['PHOTO_LOCATION'].Value

        self.db_insert_table_account(account)
        self.db_commit()
        return True

    def get_contacts(self):
        if self.user is None:
            return False

        dbPath = self.root.GetByPath('/databases/' + 'youxin_db_' + self.user)
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 'contact' in db.Tables:
            ts = SQLiteParser.TableSignature('contact')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'UID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    self.db_close()
                    return
                friend = model_im.Friend()
                self.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = self.app_name
                friend.account_id = self.user
                friend.friend_id = rec['UID'].Value
                friend.nickname = rec['NAME'].Value
                friend.telephone = rec['NUMBER'].Value
                friend.photo = rec['HEAD_URL'].Value
                friend.address = rec['LOCATION'].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                if IsDBNull(friend.photo):
                    friend.photo = None
                if friend.friend_id != '':
                    self.contacts[friend.friend_id] = friend
                self.db_insert_table_friend(friend)

        if 'PROFILE_TABLE' in db.Tables:
            ts = SQLiteParser.TableSignature('PROFILE_TABLE')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'UID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    self.db_close()
                    return
                id = rec['UID'].Value
                if id == self.user or id in self.contacts.keys():
                    continue
                friend = model_im.Friend()
                self.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = self.app_name
                friend.account_id = self.user
                friend.friend_id = id
                friend.nickname = rec['NAME'].Value
                friend.gender = 2 if rec['SEX'].Value == '女' else 1
                friend.signature = rec['SIGNATURE'].Value
                friend.email = rec['BIRTHDAY'].Value
                friend.province = rec['PROVINCE'].Value
                friend.city = rec['CITY'].Value
                friend.telephone = rec['MOBILE_NUMBER'].Value
                friend.photo = rec['PICTURE'].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                if IsDBNull(friend.photo):
                    friend.photo = None
                self.contacts[id] = friend
                self.db_insert_table_friend(friend)
        self.db_commit()
        return True

    def get_chats(self):
        if self.user is None:
            return False

        dbPath = self.root.GetByPath('/databases/yx_new_messages')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        table = 'messages' + self.user
        if table in db.Tables:
            ts = SQLiteParser.TableSignature(table)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'uid', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for id in self.contacts.keys():
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if canceller.IsCancellationRequested:
                        self.db_close()
                        return
                    if id != rec['uid'].Value:
                        continue

                    friend = self.contacts.get(id)

                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = self.app_name
                    message.account_id = self.user
                    message.talker_id = id
                    message.talker_name = friend.nickname
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.sender_id = message.talker_id
                    message.sender_name = message.talker_name
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['type'].Value else model_im.MESSAGE_TYPE_RECEIVE
                    message.msg_id = str(uuid.uuid1()).replace('-', '')
                    message.type = self.parse_message_type(rec['extra_mime'].Value)
                    message.content = rec['body'].Value
                    message.media_path = self.get_media_path(message.content, message.type)
                    message.send_time = rec['date'].Value
                    if message.is_sender == model_im.MESSAGE_TYPE_SEND:
                        message.status = model_im.MESSAGE_STATUS_SENT if rec['status'].Value == 1 else model_im.MESSAGE_STATUS_UNSENT
                    if message.is_sender == model_im.MESSAGE_TYPE_RECEIVE:
                        message.status = model_im.MESSAGE_STATUS_READ if rec['status'].Value == 1 else model_im.MESSAGE_STATUS_UNREAD
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        self.get_location(message.content, message.deleted, message.repeated, message.send_time)
                    self.db_insert_table_message(message)
        self.db_commit()
        return True

    def parse_message_type(self, type):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if type == 2:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if type == 3:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        if type == 4:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        if type == 5:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        return msgtype

    def get_media_path(self, content, type):
        if type != model_im.MESSAGE_CONTENT_TYPE_TEXT and type != model_im.MESSAGE_CONTENT_TYPE_LOCATION:
            return content
        return ''

    def get_location(self, content, deleted, repeated, time):
        try:
            obj = json.loads(content)
        except:
            traceback.print_exc()

        location = model_im.Location()
        location.deleted = deleted
        location.repeated = repeated
        location.account_id = self.user
        location.address = obj['description']
        location.latitude = obj['latitude']
        location.longitude = obj['longitude']
        self.db_insert_table_location(location)
        self.db_commit()
        return True
