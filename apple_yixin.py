#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
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

import os
import sqlite3
import json
import model_im
import gc

# app数据库版本
VERSION_APP_VALUE = 1

def analyze_yixin(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = YiXinParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('易信')
    gc.collect()
    return pr
    
def execute(node,extracteDeleted):
    return analyze_renren(node, extracteDeleted, False)

class YiXinParser(model_im.IM):
    def __init__(self, node, extracted_deleted, extract_source):
        super(YiXinParser, self).__init__()
        self.extract_deleted = False
        self.extract_source = extract_source
        self.root = node
        self.app_name = 'YiXin'
        self.cache_path = ds.OpenCachePath('YiXin')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

        nameValues.SafeAddValue('1030047', self.cache_db)

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
            # 数据库填充完毕，请将中间数据库版本和app数据库版本插入数据库，用来检测app是否需要重新解析
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
        self.db_insert_table_account(account)
        self.db_commit()
        return True

    def get_contacts(self):
        if self.user is None:
            return False
        
        dbPath = self.root.GetByPath('/Documents/' + self.user + '/msg2.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 'msglog' in db.Tables:
            ts = SQLiteParser.TableSignature('msglog')
            SQLiteParser.Tools.AddSignatureToTable(ts, "id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    self.db_close()
                    return
                contact = {'deleted' : rec.Deleted, 'repeated' : 0}
                contactid = rec['id'].Value
                type = rec['msg_type'].Value
                if contactid in self.contacts:
                    continue
                else:
                    self.contacts[contactid] = contact

                if type == 1:
                    friend = model_im.Friend()
                    friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.repeated = contact.get('repeated', 0)
                    friend.source = self.app_name

                    friend.account_id = self.user
                    friend.friend_id = contactid
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    self.db_insert_table_friend(friend)

                if type == 2:
                    chatroom = model_im.Chatroom()
                    chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    chatroom.repeated = contact.get('repeated', 0)
                    chatroom.source = self.app_name

                    chatroom.account_id = self.user
                    chatroom.chatroom_id = contactid
                    self.db_insert_table_chatroom(chatroom)

                if type == 6:
                    friend = model_im.Friend()
                    friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.repeated = contact.get('repeated', 0)
                    friend.source = self.app_name

                    friend.account_id = self.user
                    friend.friend_id = contactid
                    friend.type = model_im.FRIEND_TYPE_FOLLOW
                    self.db_insert_table_friend(friend)
            self.db_commit()
        return True

    def get_chats(self):
        if self.user is None:
            return False
        
        for contact_id in self.contacts.keys():
            dbPath = self.root.GetByPath('/Documents/' + self.user + '/msg2.db')
            db = SQLiteParser.Database.FromNode(dbPath)
            if not db:
                return False
        
            if 'msglog' in db.Tables:
                ts = SQLiteParser.TableSignature('msglog')
                SQLiteParser.Tools.AddSignatureToTable(ts, "id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if canceller.IsCancellationRequested:
                        self.db_close()
                        return
                    if contact_id != rec['id'].Value:
                        continue
                    contact = self.contacts.get(contact_id)
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.repeated = contact.get('repeated', 0)
                    message.source = self.app_name

                    message.msg_id = rec['uuid'].Value.replace('-', '')
                    message.account_id = self.user
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['msg_from_id'].Value == message.account_id else model_im.MESSAGE_TYPE_RECEIVE
                    message.talker_id = rec['id'].Value if message.is_sender else rec['msg_from_id'].Value
                    message.send_id = rec['msg_from_id'].Value if message.is_sender == model_im.MESSAGE_TYPE_SEND else rec['id'].Value
                    type = rec['msg_content_type'].Value
                    message.type = self.get_message_type(type)
                    message.send_time = rec['msg_time'].Value
                    message.content = rec['msg_body'].Value
                    contact_type = rec['msg_type']
                    if contact_type == 1:
                        message.talker_type = model_im.CHAT_TYPE_FRIEND
                    if contact_type == 2:
                        message.talker_type = model_im.CHAT_TYPE_GROUP
                    if contact_type == 6:
                        message.talker_type = model_im.CHAT_TYPE_OFFICIAL
                    message.media_path = self.parse_message_content(message.content, message.type)
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.location = self.get_location(message.content, message.send_time, message.deleted, message.repeated)
                    self.db_insert_table_message(message)
        self.db_commit()
        return True

    def get_location(self, content, time, deleted, repeated):
        object = json.loads(content)
        location = model_im.Location()
        location.source = self.app_name
        location.deleted = deleted
        location.repeated = repeated
        location.latitude = object['location'].split(',')[0]
        location.longitude = object['location'].split(',')[1]
        location.address = object['description']
        location.timestamp = time
        
        self.db_insert_table_location(location)
        self.db_commit()
        return location.address

    def get_message_type(self, type):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if type == 1:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if type == 2:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        if type == 3:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        if type == 4:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        #if type == 7:
        #    msgtype = model_im.MESSAGE_CONTENT_TYPE_CHARTLET
        return msgtype
    
    def parse_message_content(self, content, type):
        media_path = ""
        try:
            object = json.loads(content)
            if type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                node = self.root.GetByPath('/Documents/' + self.user + '/video')
                if node is not None:
                    media_path = os.path.join(node.AbsolutePath, object['filename'])
            if type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
                node = self.root.GetByPath('/Documents/' + self.user + '/audio')
                if node is not None:
                    media_path = os.path.join(node.AbsolutePath, object['filename'])
            if type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                node = self.root.GetByPath('/Documents/' + self.user + '/image')
                if node is not None:
                    media_path = os.path.join(node.AbsolutePath, object['filename'])
            #if type == model_im.MESSAGE_CONTENT_TYPE_CHARTLET:
            #    node = self.root.GetByPath('/Documents/' + self.user + '/chartlet')
            #    if node is not None:
            #        media_path = os.path.join(node.AbsolutePath, object['filename'])
        except:
            pass
        return media_path




