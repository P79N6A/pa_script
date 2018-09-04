#coding=utf-8

import PA_runtime
import clr 
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
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
import uuid
import re
import hashlib

# app 数据库版本
VERSION_APP_VALUE = 1

CONTACT_TYPE_FRIEND = 1
CONTACT_TYPE_GROUP = 2

def analyze_renren(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = RenRenParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))
    return pr

class RenRenParser(model_im.IM):
    def __init__(self, node, extracted_deleted, extract_source):
        super(RenRenParser, self).__init__()
        self.extract_deleted = False
        self.extract_source = extract_source
        self.root = node 
        self.app_name = 'RenRen'
        self.mount_dir = node.FileSystem.MountPoint
        self.cache_path = ds.OpenCachePath('RenRen')
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
        models  = self.get_models_from_cache_db()
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

        node = self.root.GetByPath('/Documents/' + self.user + '/userCaches/userProfileDict')
        if node is None:
            return

        root = None
        try:
            root = BPReader.GetTree(node)
        except:
            return

        account = model_im.Account()
        account.source = self.app_name
        account.account_id = self.user
        account.username = self.bpreader_node_get_value(root, 'user_name', '')
        account.photo = self.bpreader_node_get_value(root, 'head_url', '')
        self.db_insert_table_account(account)
        self.db_commit()
        return True

    def get_contacts(self):
        if self.user is None:
            return True

        subDbPath = self.root.GetByPath('/Documents/DB/' + self.user + '/subscribed.sqlite')
        subDb = SQLiteParser.Database.FromNode(subDbPath)
        if subDb is None:
            return False

        if 'r_h_c_public_account_object' in subDb.Tables:
            ts = SQLiteParser.TableSignature('r_h_c_public_account_object')
            SQLiteParser.Tools.AddSignatureToTable(ts, "account_i_d", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in subDb.ReadTableRecords(ts, self.extract_deleted):
                contact = {'deleted' : rec.Deleted, 'repeated' : 0}
                contactid = str(rec['account_i_d'].Value)
                if contactid in self.contacts:
                    if rec.Deleted != DeletedState.Intact:
                        self.contacts[contactid] = contact
                    else:
                        contact['repeated'] = 1
                else:
                    self.contacts[contactid] = contact

                self.contacts[contactid]['type'] = CONTACT_TYPE_FRIEND

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.repeated = contact.get('repeated', 0)
                friend.source = self.app_name
                friend.account_id = self.user
                friend.friend_id = contactid
                friend.nickname = rec['account_name'].Value
                friend.photo = rec['account_head_u_r_l'].Value
                friend.type = model_im.FRIEND_TYPE_FOLLOW
                self.db_insert_table_friend(friend)
                
        infoDbPath = self.root.GetByPath('/Documents/DB/' + self.user + '/info.sqlite')
        infoDb = SQLiteParser.Database.FromNode(infoDbPath)
        if infoDb is None:
            return False

        if 'r_s_chat_room_persistence_object' in infoDb.Tables:
            ts_1 = SQLiteParser.TableSignature('r_s_chat_room_persistence_object')
            SQLiteParser.Tools.AddSignatureToTable(ts_1, "room_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in infoDb.ReadTableRecords(ts_1, self.extract_deleted):
                contact = {'deleted' : rec.Deleted, 'repeated' : 0}
                contactid = str(rec['room_id'].Value)
                if contactid in self.contacts:
                    continue
                else:
                    self.contacts[contactid] = contact

                self.contacts[contactid]['type'] = CONTACT_TYPE_GROUP

                chatroom = model_im.Chatroom()
                chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                chatroom.repeated = contact.get('repeated', 0)
                chatroom.source = self.app_name
                chatroom.account_id = self.user
                chatroom.chatroom_id = contactid
                chatroom.name = rec['room_name'].Value
                chatroom.photo = rec['head_url'].Value
                if IsDBNull(chatroom.photo):
                    chatroom.photo = None
                self.db_insert_table_chatroom(chatroom)
                    
                chatroom_members = {}
                if 'r_s_chat_member_persistence_object' in infoDb.Tables:
                    ts_2 = SQLiteParser.TableSignature('r_s_chat_member_persistence_object')
                    SQLiteParser.Tools.AddSignatureToTable(ts_2, "user_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in infoDb.ReadTableRecords(ts_2, self.extract_deleted):
                        room_id = str(rec['room_id'].Value)
                        if room_id != chatroom.chatroom_id:
                            continue

                        member = {'deleted' : rec.Deleted, 'repeated' : 0}
                        member_id = rec['user_id'].Value
                        if member_id in chatroom_members:
                            continue
                        else:
                            chatroom_members[member_id] = member

                        chatroom_member = model_im.ChatroomMember()
                        chatroom_member.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        chatroom.repeated = member.get('repeated', 0)
                        chatroom_member.source = self.app_name
                        chatroom_member.account_id = self.user
                        chatroom_member.chatroom_id = room_id
                        chatroom_member.member_id = member_id
                        chatroom_member.display_name = rec['name'].Value
                        chatroom_member.photo = rec['head_url'].Value
                        self.db_insert_table_chatroom_member(chatroom_member)

            chatDbPath = self.root.GetByPath('/Documents/DB/' + self.user + '/chat.sqlite')
            chatDb = SQLiteParser.Database.FromNode(chatDbPath)
            if chatDb is None:
                return False

            if 'r_s_chat_session' in chatDb.Tables:
                ts_1 = SQLiteParser.TableSignature('r_s_chat_session')
                SQLiteParser.Tools.AddSignatureToTable(ts_1, "target_user_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in chatDb.ReadTableRecords(ts_1, self.extract_deleted):
                    contact = {'deleted' : rec.Deleted, 'repeated' : 0}
                    contactid = str(rec['target_user_id'].Value)
                    if contactid in self.contacts:
                        continue
                    else:
                        self.contacts[contactid] = contact

                    self.contacts[contactid]['type'] = CONTACT_TYPE_FRIEND

                    friend = model_im.Friend()
                    friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.repeated = contact.get('repeated', 0)
                    friend.source = self.app_name
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    friend.account_id = self.user
                    friend.friend_id = contactid
                    friend.nickname = rec['target_user_name'].Value

                    if 'r_n_chat_target_info' in infoDb.Tables:
                        ts_2 = SQLiteParser.TableSignature('r_n_chat_target_info')
                        SQLiteParser.Tools.AddSignatureToTable(ts_2, "target_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                        for rec in infoDb.ReadTableRecords(ts_2, self.extract_deleted):
                            id = str(rec['target_id'].Value)
                            if id != friend.friend_id:
                                continue
                            friend.photo = rec['target_head_url'].Value
                            if IsDBNull(friend.photo):
                                friend.photo = None
                            self.db_insert_table_friend(friend)

        self.db_commit()
        return True

    def get_chats(self):
        if self.user is None:
            return True

        dbPath = self.root.GetByPath('/Documents/DB/' + self.user + '/chat.sqlite')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return False

        if 'r_s_chat_message_persistence_object' in db.Tables:
            for contactid in self.contacts.keys():
                ts = SQLiteParser.TableSignature('r_s_chat_message_persistence_object')
                SQLiteParser.Tools.AddSignatureToTable(ts, "msg_key", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if contactid != str(rec['from_user_id'].Value):
                        if contactid != str(rec['to_user_id'].Value):
                            continue
                    
                    contact = self.contacts.get(contactid)

                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = self.app_name
                    message.account_id = self.user
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['from_user_id'].Value == self.user else model_im.MESSAGE_TYPE_RECEIVE
                    message.talker_id = contactid
                    if contact.get('type', 0) == CONTACT_TYPE_FRIEND:
                        message.talker_type = model_im.CHAT_TYPE_GROUP
                    if contact.get('type', 0) == CONTACT_TYPE_GROUP:
                        message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.talker_name = rec['fname'].Value
                    message.sender_id = message.talker_id
                    message.sender_name = message.talker_name
                    message.type = self.parse_message_type(rec['class_type'].Value, rec['child_node_string'].Value)
                    message.content = rec['summary'].Value
                    message.send_time = rec['time_stamp'].Value
                    message.media_path = self.get_media_path(contactid, message.is_sender, rec['child_node_string'].Value, rec['elements'].Value, 
                                                             message.type, message.send_time, message.deleted, message.repeated)
                    message.msg_id = str(uuid.uuid1()).replace('-', '')
                    self.db_insert_table_message(message)
            self.db_commit()
            return True

    def get_media_path(self, contactid, is_sender, xml_string, media_content, type, time, deleted, repeated):
            media_path = ''
            if type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                src     = ''
                thumb   = ''
                origninal = ''
                img = re.search('headsrc=\"(.*?)\"', xml_string, re.M | re.I)
                if img is not None:
                    src = img.group(1).split('/')[6]
                    thumb = hashlib.md5(img.group(1)).hexdigest()
                img = re.search('headsrc=\'(.*?)\'', xml_string, re.M | re.I)
                if img is not None:
                    src = img.group(1).split('/')[6]
                    thumb = hashlib.md5(img.group(1)).hexdigest()
                img = re.search('originalsrc=\"(.*?)\"', xml_string, re.M | re.I)
                if img is not None:
                    src = img.group(1).split('/')[6]
                    origninal = hashlib.md5(img.group(1)).hexdigest()
                img = re.search('originalsrc=\'(.*?)\'', xml_string, re.M | re.I)
                if img is not None:
                    src = img.group(1).split('/')[6]
                    origninal = hashlib.md5(img.group(1)).hexdigest()

                return self.is_image_file_exist(src, thumb, origninal)

            if type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
                root = None
                try:
                    memoryRange = MemoryRange.FromBytes(media_content)
                    root = BPReader.GetTree(memoryRange)
                except:
                    traceback.print_exc()
                   
                obj = root.Value[0].Children['audioUrl'].Value
                if obj is not None:
                    node = self.root.GetByPath('/Documents/chatfile/' + self.user + '/' + contactid)
                    if node is not None:
                        file = 'I' if is_sender == model_im.MESSAGE_TYPE_SEND else ''
                        file += root.Value[0].Children['elementId'].Value + '.spx'
                        media_path = os.path.join(node.AbsolutePath, file)
            if type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                root = None
                try:
                    memoryRange = MemoryRange.FromBytes(media_content)
                    root = BPReader.GetTree(memoryRange)
                except:
                    traceback.print_exc()
                   
                obj = root.Value[0].Children['localvideoUrl'].Value
                if obj is not None:
                    node = self.root.GetByPath('/Documents/chatfile/' + self.user + '/' + contactid)
                    if node is not None:
                        file = 'I' if is_sender == model_im.MESSAGE_TYPE_SEND else ''
                        file += root.Value[0].Children['elementId'].Value + '.mp4'
                        media_path = os.path.join(node.AbsolutePath, file)
            return media_path

    def is_image_file_exist(self, src, thumb, origninal):
        if thumb != '':
            media_path = self.is_file_exist(thumb)
            if media_path is not None:
                return media_path
        if origninal != '':
            media_path = self.is_file_exist(origninal)
            if media_path is not None:
                return media_path
            media_path = self.root.GetByPath(src)
        if src != '':
            if media_path is not None:
                return media_path.PathWithMountPoint
        return ''

    def is_file_exist(self, filename):
        dir = '/Library/Caches/default/com.hackemist.SDWebImageCache.default/'
        file = self.root.GetByPath(dir + filename + '.jpg')
        if file is not None:
            return file.AbsolutePath
        file = self.root.GetByPath(dir + filename + '.png')
        if file is not None:
            return file.AbsolutePath
        file = self.root.GetByPath(dir + filename + '.gif')
        if file is not None:
            return file.AbsolutePath
        file = self.root.GetByPath(dir + filename)
        if file is not None:
            return file.AbsolutePath

    def parse_message_type(self, type, xml_string):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if type == 'video':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        if type == 'dialog':
            group = re.search('<img (.*?)', xml_string, re.M | re.I)
            if group is not None:
                msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
            group = re.search('<voice (.*?)', xml_string, re.M | re.I)
            if group is not None:
                msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        if type == 'big_emotion':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_EMOJI
        if type == 'secret':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if type == 'name_card':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
        return msgtype

    @staticmethod
    def bpreader_node_get_value(node, key, default_value = None):
        if key in node.Children and node.Children[key] is not None:
            return node.Children[key].Value
        return default_value
