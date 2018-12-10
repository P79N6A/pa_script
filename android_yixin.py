#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
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

import os
import sqlite3
import json
import model_im
import bcp_im
import gc

DEBUG = True
DEBUG = False

# app 数据库版本
VERSION_APP_VALUE = 1

def analyze_yixin(root, extract_deleted, extract_source):
    if root.AbsolutePath == '/data/media/0/Android/data/im.yixin':
        return

    pr = ParserResults()

    models = YiXinParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))

    pr.Build('易信')
    gc.collect()
    return pr

def execute(node,extracteDeleted):
    return analyze_yixin(node, extracteDeleted, False)

class YiXinParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.extract_deleted = False
        self.extract_source = extract_source
        self.root = node
        self.im = model_im.IM()
        self.cache_path = ds.OpenCachePath('YiXin')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

        nameValues.SafeAddValue(bcp_im.CONTACT_ACCOUNT_TYPE_IM_YIXIN, self.cache_db)

    def parse(self):

        if DEBUG or self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.im.db_create(self.cache_db)
            user_list = self.get_user_list()
            for user in user_list:
                self.friends = {}
                self.chatrooms = {}
                self.user = user
                self.parse_user()
                self.user = None
                self.friends = None
                self.chatrooms = None
            self.im.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.im.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.im.db_commit()
            self.im.db_close()
        models  = self.get_models_from_cache_db()
        return models

    def get_models_from_cache_db(self):
        models = model_im.GenerateModel(self.cache_db).get_models()
        return models

    def get_user_list(self):
        user_list = []
        for file in os.listdir(self.root.PathWithMountPoint):
            if file.isdigit():
                user_list.append(file)
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

        dbPath = self.root.GetByPath(self.user + '/main.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            self.im.db_insert_table_account(account)
            self.im.db_commit()
            return

        account.source = dbPath.AbsolutePath
        if 'yixin_contact' in db.Tables:
            ts = SQLiteParser.TableSignature('yixin_contact')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if account.account_id != rec['uid'].Value:
                    continue
                account.username = rec['nickname'].Value
                self.username = account.username
                account.gender = 2 if rec['gender'].Value == 0 else 1
                account.email = rec['email'].Value
                account.signature = rec['signature'].Value
                account.address = rec['address']
        self.im.db_insert_table_account(account)
        self.im.db_commit()

    def get_contacts(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath(self.user + '/main.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'yixin_contact' in db.Tables:
            ts = SQLiteParser.TableSignature('yixin_contact')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                id = rec['uid'].Value
                if id in self.friends:
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = 'YiXin'
                friend.account_id = self.user
                friend.friend_id = id
                friend.nickname = rec['nickname'].Value
                friend.photo = rec['photourl'].Value
                friend.gender = 2 if rec['gender'].Value == 0 else 1
                friend.signature = rec['signature'].Value
                friend.email = rec['email'].Value
                friend.address = rec['address'].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                self.friends[id] = friend
                if 'buddylist' in db.Tables:
                    ts = SQLiteParser.TableSignature('buddylist')
                    SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        if friend.friend_id != rec['uid']:
                            continue
                        friend.remark = rec['alias'].Value
                self.im.db_insert_table_friend(friend)
                
        if 'tinfo' in db.Tables:
            ts = SQLiteParser.TableSignature('tinfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                id = rec['tid'].Value
                if id in self.chatrooms:
                    continue

                chatroom = model_im.Chatroom()
                chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                chatroom.source = 'YiXin'
                chatroom.account_id = self.user
                chatroom.chatroom_id = id
                chatroom.name = rec['defaultname'].Value
                chatroom.creator_id = rec['creator'].Value
                chatroom.photo = rec['photo'].Value
                chatroom.member_count = rec['membercount'].Value
                chatroom.type = model_im.CHATROOM_TYPE_NORMAL
                self.chatrooms[id] = chatroom
                self.im.db_insert_table_chatroom(chatroom)
        
                if 'tuser' in db.Tables:
                    ts = SQLiteParser.TableSignature('tuser')
                    SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        if canceller.IsCancellationRequested:
                            return
                        room_id = rec['tid'].Value
                        if chatroom.chatroom_id != room_id:
                            continue

                        chatroom_member = model_im.ChatroomMember()
                        chatroom_member.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        chatroom_member.source = 'YiXin'
                        chatroom_member.account_id = self.user
                        chatroom_member.chatroom_id = room_id
                        chatroom_member.member_id = rec['uid'].Value
                        friend = self.friends.get(chatroom_member.member_id)
                        if friend is not None:
                            chatroom_member.display_name = friend.nickname
                            chatroom_member.email = friend.email
                            chatroom_member.gender = friend.gender
                            chatroom_member.address = friend.address
                            chatroom_member.signature = friend.signature
                            chatroom_member.photo = friend.photo
                        self.im.db_insert_table_chatroom_member(chatroom_member)

        if 'painfo' in db.Tables:
            ts = SQLiteParser.TableSignature('painfo')
            SQLiteParser.Tools.AddSignatureToTable(ts, "uid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                id = rec['uid'].Value
                if id in self.friends:
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = 'YiXin'
                friend.account_id = self.user
                friend.friend_id = id
                friend.nickname = rec['nickname'].Value
                friend.photo = rec['photourl'].Value
                friend.gender = rec['gender'].Value
                friend.signature = rec['signature'].Value
                friend.type = model_im.FRIEND_TYPE_FOLLOW
                self.friends[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)
        self.im.db_commit()
                        
    def get_chats(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath(self.user + '/msg.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        for id in self.friends.keys() or self.chatrooms.keys():
            if 'msghistory' in db.Tables:
                ts = SQLiteParser.TableSignature('msghistory')
                SQLiteParser.Tools.AddSignatureToTable(ts, "tid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if canceller.IsCancellationRequested:
                        return
                    if id != rec['id'].Value:
                        continue

                    friend = self.friends.get(id)
                    
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = dbPath.AbsolutePath
                    message.account_id = self.user
                    message.talker_id = id
                    message.talker_type = model_im.CHAT_TYPE_FRIEND if id in self.friends.keys() else model_im.CHAT_TYPE_GROUP
                    message.talker_name = friend.nickname
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['fromid'].Value == self.user else model_im.MESSAGE_TYPE_RECEIVE
                    message.sender_id = rec['fromid'].Value
                    message.sender_name = self.username if message.is_sender == model_im.MESSAGE_TYPE_SEND else message.talker_name 
                    message.msg_id = rec['msgid'].Value
                    message.type = self.parse_message_type(rec['msgtype'].Value)
                    message.send_time = rec['time'].Value
                    message.content = rec['content'].Value
                    media_path = self.get_media_path(rec['attachstr'].Value, message.type)
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.location_obj = message.create_location()
                        message.location_id = self.get_location(message.location_obj, message.content, rec['attachstr'].Value, message.send_time)
                    self.im.db_insert_table_message(message)
        self.im.db_commit()

    def parse_message_type(self, type):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if type == 1:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if type == 2:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        if type == 3:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        if type == 4:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        return msgtype

    def get_media_path(self, attachstr, type):
        if attachstr == '':
            return None

        media_path = ''
        try:
            obj = json.loads(attachstr)
        except:
            traceback.print_exc()
        if type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
            node = self.root.GetByPath('../../media/0/Yixin/audio/')
            if node is not None:
                key = obj['key']
                filepath = key[0:2] + '/' + key[2:4] + '/' + key + '.aac'
                media_path = os.path.join(node.AbsolutePath, filepath)
        if type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
            node = self.root.GetByPath('../../media/0/Yixin/image/')
            if node is not None:
                key = obj['key']
                filepath = key[0:2] + '/' + key[2:4] + '/' + key + '.jpg'
                media_path = os.path.join(node.AbsolutePath, filepath)
        if type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
            node = self.root.GetByPath('../../media/0/Yixin/video/')
            if node is not None:
                key = obj['key']
                filepath = key[0:1] + '/' + key[2:3] + '/' + key + '.mp4'
                media_path = os.path.join(node.AbsolutePath, filepath)
        return media_path

    def get_location(self, location, content, attachstr, time):
        location.account_id = self.user
        location.timestamp = time
        location.latitude = content.split(',')[0]
        location.longitude = content.split(',')[1]
        try:
            obj = json.loads(attachstr)
            location.address = obj['desc']
        except:
            traceback.print_exc()
        self.im.db_insert_table_location(location)
        self.im.db_commit()
        return location.location_id
