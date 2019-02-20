#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('Mono.HttpUtility')
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
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
import bcp_im
import uuid
import time
import gc
import re
import time

# app数据库版本
VERSION_APP_VALUE = 2

def analyze_youxin(root, extract_deleted, extract_source):
    if root.AbsolutePath == '/data/media/0/Android/data/com.yx':
        return

    pr = ParserResults()

    models = YouXinParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('有信')
    gc.collect()
    return pr

def execute(node,extracteDeleted):
    return analyze_renren(node, extracteDeleted, False)

class YouXinParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.extract_deleted = False
        self.extract_source = extract_source
        self.root = node.Parent
        self.im = model_im.IM()
        self.cache_path =ds.OpenCachePath('YouXin')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

        nameValues.SafeAddValue(bcp_im.CONTACT_ACCOUNT_TYPE_IM_YOUXIN, self.cache_db)

    def parse(self):
        if self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.im.db_create(self.cache_db)
            user_list = self.get_user_list()
            for user in user_list:
                self.contacts = {}
                self.user = user
                self.parse_user()
                self.user = None
                self.contacts = None
            self.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.im.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.im.db_commit()
            self.im.db_close()
        models = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db).get_models()
        return models

    def get_user_list(self):
        user_list = []
        node = self.root.GetByPath('/databases')
        print(self.root.PathWithMountPoint)
        print(node.PathWithMountPoint)
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
            return

        account = model_im.Account()
        account.account_id = self.user

        dbPath = self.root.GetByPath('/databases/' + 'youxin_db_' + self.user)
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            self.im.db_insert_table_account(account)
            self.im.db_commit()
            return

        account.source = dbPath.AbsolutePath
        if 'MY_NAME_CARD' in db.Tables:
            ts = SQLiteParser.TableSignature('MY_NAME_CARD')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'UID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                account.username = self._db_record_get_string_value(rec, 'NAME')
                self.username = account.username
                account.gender = 2 if self._db_record_get_string_value(rec, 'GENDER') == '女' else 1
                account.telephone = self._db_record_get_string_value(rec, 'MOBILE_NUMBER')
                account.email = self._db_record_get_string_value(rec, 'EMAIL')
                birthday = self._db_record_get_string_value(rec, 'BIRTHDAY')
                ts = time.strptime(birthday, '%Y-%m-%d')
                birthday = int(time.mktime(ts))
                account.birthday = birthday
                account.photo = self._db_record_get_string_value(rec, 'PHOTO_LOCATION')

        self.im.db_insert_table_account(account)
        self.im.db_commit()

    def get_contacts(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath('/databases/' + 'youxin_db_' + self.user)
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'PROFILE_TABLE' in db.Tables:
            ts = SQLiteParser.TableSignature('PROFILE_TABLE')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'UID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                try:
                    id = self._db_record_get_string_value(rec, 'UID')
                    if id == self.user or id in self.contacts.keys():
                        continue
                    friend = model_im.Friend()
                    self.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.source = dbPath.AbsolutePath
                    friend.account_id = self.user
                    friend.friend_id = id
                    friend.nickname = self._db_record_get_string_value(rec, 'NAME')
                    friend.gender = 2 if self._db_record_get_string_value(rec, 'SEX') == '女' else 1
                    friend.signature = self._db_record_get_string_value(rec, 'SIGNATURE')
                    friend.email = self._db_record_get_string_value(rec, 'EMAIL')
                    birthday = self._db_record_get_string_value(rec, 'BIRTHDAY')
                    ts = time.strptime(birthday, '%Y-%m-%d')
                    birthday = int(time.mktime(ts))
                    friend.birthday = birthday
                    friend.province = self._db_record_get_string_value(rec, 'PROVINCE')
                    friend.city = self._db_record_get_string_value(rec, 'CITY')
                    friend.telephone = self._db_record_get_string_value(rec, 'MOBILE_NUMBER')
                    friend.photo = self._db_record_get_string_value(rec, 'PICTURE')
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    if IsDBNull(friend.photo):
                        friend.photo = None
                    self.contacts[id] = friend
                    self.im.db_insert_table_friend(friend)
                except:
                    pass

        if 'contact' in db.Tables:
            ts = SQLiteParser.TableSignature('contact')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'UID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                try:
                    id = self._db_record_get_string_value(rec, 'UID')
                    if id == '':
                        id = self._db_record_get_string_value(rec, 'NUMBER')

                    if id in self.contacts.keys():
                        continue

                    friend = model_im.Friend()
                    self.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.source = dbPath.AbsolutePath
                    friend.account_id = self.user
                    friend.friend_id = self._db_record_get_string_value(rec, 'UID')
                    friend.nickname = self._db_record_get_string_value(rec, 'NAME')
                    friend.telephone = self._db_record_get_string_value(rec, 'NUMBER')
                    friend.photo = self._db_record_get_string_value(rec, 'HEAD_URL')
                    friend.address = self._db_record_get_string_value(rec, 'LOCATION')
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    if IsDBNull(friend.photo):
                        friend.photo = None
                    self.contacts[id] = friend
                    self.im.db_insert_table_friend(friend)
                except:
                    pass
        self.im.db_commit()

    def get_chats(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath('/databases/yx_new_messages')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        table = 'messages' + self.user
        if table in db.Tables:
            ts = SQLiteParser.TableSignature(table)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'uid', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for id in self.contacts.keys():
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if canceller.IsCancellationRequested:
                        return
                    try:
                        if id != self._db_record_get_string_value(rec, 'uid'):
                            continue

                        friend = self.contacts.get(id)

                        message = model_im.Message()
                        message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        message.source = dbPath.AbsolutePath
                        message.account_id = self.user
                        message.talker_id = id
                        message.talker_name = friend.nickname
                        message.talker_type = model_im.CHAT_TYPE_FRIEND
                        message.is_sender = model_im.MESSAGE_TYPE_SEND if self._db_record_get_int_value(rec, 'type') else model_im.MESSAGE_TYPE_RECEIVE
                        message.sender_id = self.user if message.is_sender == model_im.MESSAGE_TYPE_SEND else id
                        message.sender_name = friend.nickname if message.sender_id == id else self.username
                        message.msg_id = str(uuid.uuid1()).replace('-', '')
                        message.type = self.parse_message_type(self._db_record_get_int_value(rec, 'extra_mime'))
                        message.content = self._db_record_get_string_value(rec, 'body')
                        try:
                            if type != model_im.MESSAGE_CONTENT_TYPE_TEXT and type != model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                                media_nodes = self.root.FileSystem.Search(os.path.basename(message.content) + '$')
                                if len(list(media_nodes)) != 0:
                                    for node in media_nodes:
                                        message.content = None
                                        message.media_path = node.AbsolutePath
                                        if re.findall('image', message.media_path):
                                            message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                                        elif re.findall('audio', message.media_path):
                                            message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                                        break
                                else:
                                    message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        except:
                            pass
                        message.send_time = int(self._db_record_get_string_value(rec, 'date')[0:10:])
                        if message.is_sender == model_im.MESSAGE_TYPE_SEND:
                            message.status = model_im.MESSAGE_STATUS_SENT if self._db_record_get_int_value(rec, 'status') == 1 else model_im.MESSAGE_STATUS_UNSENT
                        if message.is_sender == model_im.MESSAGE_TYPE_RECEIVE:
                            message.status = model_im.MESSAGE_STATUS_READ if self._db_record_get_int_value(rec, 'status') == 1 else model_im.MESSAGE_STATUS_UNREAD
                        if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                            message.location_obj = message.create_location()
                            message.location_id = self.get_location(message.location_obj, message.content, message.send_time)
                        self.im.db_insert_table_message(message)
                    except:
                        pass
        self.im.db_commit()

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

    def get_location(self, location, content, time):
        try:
            obj = json.loads(content)
        except:
            traceback.print_exc()
        location.account_id = self.user
        location.address = obj['description']
        location.latitude = obj['latitude']
        location.longitude = obj['longitude']
        location.timestamp = time
        self.im.db_insert_table_location(location)
        self.im.db_commit()
        return location.location_id

    @staticmethod
    def _db_record_get_string_value(record, column, default_value=''):
        if not record[column].IsDBNull:
            try:
                value = str(record[column].Value)
                #if record.Deleted != DeletedState.Intact:
                #    value = filter(lambda x: x in string.printable, value)
                return value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_int_value(record, column, default_value=0):
        if not record[column].IsDBNull:
            try:
                return int(record[column].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_blob_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytes(value)
            except Exception as e:
                return default_value
        return default_value