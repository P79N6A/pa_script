#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('Mono.HttpUtility')
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

# app数据库版本
VERSION_APP_VALUE = 1

def analyze_youxin(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = YouXinParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))

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
        self.mount_dir = node.FileSystem.MountPoint
        self.cache_path =ds.OpenCachePath('YouXin')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

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
        models = model_im.GenerateModel(self.cache_db, self.mount_dir).get_models()
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

        dbPath = self.root.GetByPath('/Documents/' + self.user + '/uxin_users.cache')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 't_uxin_user' in db.Tables:
            ts = SQLiteParser.TableSignature('t_uxin_user')
            SQLiteParser.Tools.AddSignatureToTable(ts, "[uid]", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                account = model_im.Account()
                account.source = self.app_name
                account.account_id = self.user
                if rec['[uid]'].Value != self.user:
                    continue
                account.username = rec['[name]'].Value
                account.photo = rec['[small_head_image_url]'].Value
                account.signature = rec['[signature]'].Value
                account.gender = 1 if rec['[sex]'].Value == '男' else 2
                account.birthday = rec['[birthday]'].Value
            self.db_insert_table_account(account)
            self.db_commit()
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
                contact = {'deleted' : rec.Deleted, 'repeated' : 0}
                contactid = rec['[uid]'].Value
                if contactid in self.contacts:
                    continue
                else:
                    self.contacts[contactid] = contact

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.repeated = contact.get('repeated', 0)
                friend.source = self.app_name
                friend.account_id = self.user
                friend.friend_id = contactid
                friend.type = model_im.FRIEND_TYPE_FRIEND
                friend.nickname = rec['[name]'].Value
                self.contacts[contactid]['name'] = friend.nickname
                friend.photo = rec['[small_head_image_url]'].Value
                friend.signature = rec['[signature]'].Value
                friend.gender = 1 if rec['[sex]'].Value == '男' else 2
                friend.birthday = rec['[birthday]'].Value
                self.db_insert_table_friend(friend)
            self.db_commit()
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
                    if str(contact_id) != rec['[uid]'].Value:
                        continue
                    contact = self.contacts.get(contact_id)
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.repeated = contact.get('repeated', 0)
                    message.source = self.app_name

                    message.account_id = self.user
                    message.msg_id = str(uuid.uuid1()).replace('-', '')
                    message.talker_id = contact_id
                    message.talker_name = contact.get('name', 0)
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['[msgtype]'].Value else model_im.MESSAGE_TYPE_RECEIVE
                    message.sender_id = contact_id
                    message.sender_name = contact.get('name', 0)
                    message.type = self.parse_message_type(rec['[msgcontype]'].Value)
                    message.content = self.decode_url_message(rec['[msgcontent]'].Value)
                    message.send_time = int(time.mktime(time.strptime(rec['[msgtime]'].Value, '%Y-%m-%d %H:%M:%S')))
                    message.media_path = self.get_media_path(message.type, message.content, contact_id)
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.location = self.get_location(message.content, message.send_time, message.deleted, message.repeated)
                    self.db_insert_table_message(message)
        self.db_commit()
        return True

    def get_location(self, content, time, deleted, repeated):
        obj = json.loads(content)
        location = model_im.Location()
        location.source = self.app_name
        location.deleted = deleted
        location.repeated = repeated
        location.address = obj['location']['description']
        location.latitude = obj['location']['latitude']
        location.longitude = obj['location']['longitude']
        location.timestamp = time
        self.db_insert_table_location(location)
        self.db_commit()
        return location.address

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
 