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

class YouXinParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.extract_deleted = False
        self.extract_source = extract_source
        self.root = node
        self.app_name = 'YouXin'
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
        node = self.root.GetByPath('/Documents')
        if node is not None:
            for file in os.listdir(node.PathWithMountPoint):
                if file.isdigit():
                    user_list.append(file)
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
         
        dbPath = self.root.GetByPath('/Documents/' + self.user + '/StrangePersonInfo.Sqlite3')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 'StrangePhonePersonInfo' in db.Tables:
            ts = SQLiteParser.TableSignature('tatnlinelistusers')
            SQLiteParser.Tools.AddSignatureToTable(ts, "[uid]", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if rec['[uid]'].Value != self.user:
                    continue

                obj = json.loads(rec['[info]'].Value)
                account.username = obj['name']
                if rec['[type]'].Value == '2':
                    account.photo = obj['picture']
                    account.signature = obj['signature']
                account.gender = model_im.GENDER_MALE if obj['sex'] == '男' else model_im.GENDER_FEMALE
                account.birthday = obj['birthday']
                break

        self.im.db_insert_table_account(account)
        self.im.db_commit()
        return True

    def get_contacts(self):
        if self.user is None:
            return False

        dbPath = self.root.GetByPath('/Documents/' + self.user + '/uxin_users.cache')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 't_uxin_user' in db.Tables:
            ts = SQLiteParser.TableSignature('t_uxin_user')
            SQLiteParser.Tools.AddSignatureToTable(ts, "[uid]", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = self.app_name
                friend.account_id = self.user
                friend.friend_id = rec['[uid]'].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                friend.nickname = rec['[name]'].Value
                friend.photo = rec['[small_head_image_url]'].Value
                friend.signature = rec['[signature]'].Value
                friend.gender = model_im.GENDER_MALE if rec['[sex]'].Value == '男' else model_im.GENDER_FEMALE
                friend.birthday = rec['[birthday]'].Value
                self.contacts[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)

        dbPath = self.root.GetByPath('/Documents/' + self.user + '/StrangePersonInfo.Sqlite3')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 'StrangePhonePersonInfo' in db.Tables:
            ts = SQLiteParser.TableSignature('tatnlinelistusers')
            SQLiteParser.Tools.AddSignatureToTable(ts, "[uid]", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                id = rec['[uid]'].Value
                if id in self.contacts.keys():
                    continue
                obj = json.loads(rec['[info]'].Value)
                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = self.app_name
                friend.account_id = self.user
                friend.friend_id = id
                friend.username = obj['name']
                if rec['[type]'].Value == '2':
                    friend.photo = obj['picture']
                    friend.signature = obj['signature']
                friend.gender = model_im.GENDER_MALE if obj['sex'] == '男' else model_im.GENDER_FEMALE
                friend.birthday = obj['birthday']
                friend.type = model_im.FRIEND_TYPE_STRANGER
                self.contacts[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)
        self.im.db_commit()
        return True

    def get_chats(self):
        if self.user is None:
            return False

        dbPath = self.root.GetByPath('/Documents/' + self.user + '/IMDataNew.Sqlite3')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 'NewIMMessageInfo' in db.Tables:
            for contact_id in self.contacts.keys():
                ts = SQLiteParser.TableSignature('NewIMMessageInfo')
                SQLiteParser.Tools.AddSignatureToTable(ts, "[uid]", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if canceller.IsCancellationRequested:
                        return
                    if str(contact_id) != rec['[uid]'].Value:
                        continue
                    contact = self.contacts.get(contact_id)
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = self.app_name
                    message.account_id = self.user
                    message.msg_id = str(uuid.uuid1()).replace('-', '')
                    message.talker_id = contact_id
                    message.talker_name = contact.nickname
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['[msgtype]'].Value else model_im.MESSAGE_TYPE_RECEIVE
                    message.sender_id = contact_id
                    message.sender_name = contact.nickname
                    message.type = self.parse_message_type(rec['[msgcontype]'].Value)
                    message.content = self.decode_url_message(rec['[msgcontent]'].Value)
                    message.send_time = int(time.mktime(time.strptime(rec['[msgtime]'].Value, '%Y-%m-%d %H:%M:%S')))
                    message.media_path = self.get_media_path(message.type, message.content, contact_id)
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.extra_id = self.get_location(message.content, message.send_time, message.deleted, message.repeated)
                    self.im.db_insert_table_message(message)

        dbPath = self.root.GetByPath('/Documents/' + self.user + '/callHistoryRecord.Sqlite3')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 'call' in db.Tables:
            for contact_id in self.contacts.keys():
                ts = SQLiteParser.TableSignature('call')
                SQLiteParser.Tools.AddSignatureToTable(ts, "[uid]", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if canceller.IsCancellationRequested:
                        return
                    if contact_id != rec['[uid]'].Value:
                        continue
                    contact = self.contacts.get(contact_id)
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = self.app_name
                    message.account_id = self.user
                    message.msg_id = str(uuid.uuid1()).replace('-', '')
                    message.talker_id = contact_id
                    message.talker_name = contact.nickname
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['[calltype]'].Value == 0 else model_im.MESSAGE_TYPE_RECEIVE
                    message.sender_id = contact_id
                    message.sender_name = contact.nickname
                    message.type = model_im.MESSAGE_CONTENT_TYPE_VOIP
                    if IsDBNull(rec['[telephone]'].Value):
                        if(rec['[calltime]'].Value == '00:00'):
                            message.content = "[视频]已取消"
                        else:
                            message.content = "[视频]通话时长" + rec['[calltime]'].Value
                    else:
                        if(rec['[calltime]'].Value == '00:00'):
                            message.content = "[语音]已取消"
                        else:
                            message.content = "[语音]通话时长" + rec['[calltime]'].Value
                    self.im.db_insert_table_message(message)
        self.im.db_commit()
        return True

    def get_location(self, content, time, deleted, repeated):
        obj = json.loads(content)
        location = model_im.Location()
        location.source = self.app_name
        location.deleted = deleted
        location.repeated = repeated
        location.location_id = str(uuid.uuid1()).replace('-', '')
        location.address = obj['location']['description']
        location.latitude = obj['location']['latitude']
        location.longitude = obj['location']['longitude']
        location.timestamp = time
        self.im.db_insert_table_location(location)
        self.im.db_commit()
        return location.location_id

    def get_media_path(self, type, content, contact_id):
        media_path = ''
        
        if type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
            obj = json.loads(content)
            node = self.root.GetByPath('/Documents/' + self.user + '/IMMedia/' + str(contact_id))
            if node is not None:
                media_path = os.path.join(node.AbsolutePath, obj['[UXinIMPicName]']).replace('.thumb', '')
        if type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
            node = self.root.GetByPath('/Documents/' + self.user + '/IMMedia/' + str(contact_id))
            if node is not None:
                media_path = os.path.join(node.AbsolutePath, content)
        return media_path

    def parse_message_type(self, type):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if type == 2:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if type == 3:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        if type == 5:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        return msgtype

    def decode_url_message(self, content):
        json_string = ''
        try:
            json_string = Mono.Web.HttpUtility.UrlDecode(content)
        except Exception as e:
            traceback.print_exc()
        return json_string
    