#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
try:
    clr.AddReference('model_im')
    clr.AddReference('model_eb')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *
from System.Security.Cryptography import *

import os
import sqlite3
import json
import gc
import model_im
import model_eb
import re
import hashlib
import base64
import uuid

# app 数据库版本
VERSION_APP_VALUE = 1

def analyze_alipay(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = AlipayParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))

    pr.Build('支付宝')
    gc.collect()
    return pr

def execute(node, extracteDeleted):
    return analyze_alipay(node, extracteDeleted, False)

class AlipayParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source = extract_source
        self.cache = ds.OpenCachePath('Alipay')
        self.contacts_models = []
        self.eb = model_eb.EB(self.cache + '/Alipay', VERSION_APP_VALUE, 'Alipay')
        self.im = self.eb.im
        self.need_parse = self.eb.need_parse
        nameValues.SafeAddValue('1290007', self.cache + '/Alipay')

    def parse(self):
        user_list = self.get_user_list()
        if self.need_parse:
            self.eb.db_create()
            for user in user_list:
                self.contacts = {}
                self.msg_deals = []
                self.user = user
                self.parse_user()
                self.user = None
                self.contacts = None
                self.msg_deals = None
            self.eb.db_insert_table_version(model_eb.EB_VERSION_KEY, model_eb.EB_VERSION_VALUE)
            self.eb.db_insert_table_version(model_eb.EB_APP_VERSION_KEY, VERSION_APP_VALUE)
            self.eb.db_commit()
            self.eb.sync_im_version()
            self.im.db_close()
            self.eb.db_close()
        models = self.get_models_from_cache_db()
        for user in user_list:
            self.user = user
            self.get_mobile_contacts()
        for model in self.contacts_models:
            models.append(model)
        return models

    def get_models_from_cache_db(self):
        models = model_eb.GenerateModel(self.cache + '/Alipay').get_models()
        return models

    def get_user_list(self):
        user_list = []
        node = self.root.GetByPath('../../../Documents/Contact/')
        if node is None:
            return user_list
        for file in os.listdir(node.PathWithMountPoint):
            str = re.search('-shm', file, re.M | re.I)
            if str is not None:
                continue
            str = re.search('-wal', file, re.M | re.I)
            if str is not None:
                continue

            tmp = file[0:16]
                
            dbPath = self.root.GetByPath('../../../Documents/Contact/' + file)
            db = SQLiteParser.Database.FromNode(dbPath)
            if db is None:
                continue

            if 'contact_account_list' in db.Tables:
                ts = SQLiteParser.TableSignature('contact_account_list')
                SQLiteParser.Tools.AddSignatureToTable(ts, "userID", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    user_id = rec['userID'].Value
                    val = user_id.encode('utf8')
                    if self.md5_encode(user_id.encode('utf8'))[8:24] == tmp:
                        if user_id not in user_list:
                            user_list.append(user_id)
                            break
        return user_list

    def parse_user(self):
        self.get_user()
        self.get_contacts()
        self.get_searchs()
        self.get_chats()
        self.get_deals()

    def get_user(self):
        if self.user is None:
            return

        account = model_im.Account()
        account.account_id = self.user
        
        dbPath = self.root.GetByPath('../../../Documents/Contact/' + self.md5_encode(self.user)[8:24] + '.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            self.im.db_insert_table_account(account)
            self.im.db_commit()
            return
        
        account.source = dbPath.AbsolutePath
        if 'contact_account_list' in db.Tables:
            ts = SQLiteParser.TableSignature('contact_account_list')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'userID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if self.user != rec['userID'].Value:
                    continue

                tmp = self.aes_decode(rec['fullName'].Value)
                if tmp is not None:
                    memoryRange = MemoryRange.FromBytes(tmp)
                    obj = BPReader.GetTree(memoryRange)
                    if obj is not None:
                        account.username = obj.Value
                account.nickname = rec['nickName'].Value
                exposedAccount = rec['exposedAlipayAccount'].Value
                if not IsDBNull(exposedAccount):
                    info = Text.ASCIIEncoding.ASCII.GetString(exposedAccount)
                    g = re.match('.cn', info, re.M | re.I)
                    if g is not None:
                        account.email = g.group(0)
                    g = re.match('.com', info, re.M | re.I)
                    if g is not None:
                        account.email = g.group(0)
                if IsDBNull(account.email):
                    tmp = self.aes_decode(rec['account'].Value)
                    if tmp is not None:
                        memoryRange = MemoryRange.FromBytes(tmp)
                        obj = BPReader.GetTree(memoryRange)
                        if obj is not None:
                            str = obj.Value
                            g = re.match('.cn', str, re.M | re.I)
                            if g is not None:
                                account.email = g.group(0)
                            g = re.match('.com', str, re.M | re.I)
                            if g is not None:
                                account.email = g.group(0)
                account.photo = rec['headUrl'].Value
                account.gender = model_im.GENDER_MALE if rec['gender'].Value == 'm' else model_im.GENDER_FEMALE
                account.signature = rec['signature'].Value
                account.telephone = rec['phoneString'].Value
                if IsDBNull(account.telephone):
                    tmp = self.aes_decode(rec['account'].Value)
                    if tmp is not None:
                        memoryRange = MemoryRange.FromBytes(tmp)
                        obj = BPReader.GetTree(memoryRange)
                        if obj is not None:
                            str = obj.Value
                            g = re.match('\d{3}\D{6}\d{2}', str, re.M | re.I)
                            if g is not None:
                                account.telephone = g.group(0)
                account.address = rec['showArea'].Value
                account.age = rec['age'].Value
                self.name = account.username
                self.im.db_insert_table_account(account)
        self.im.db_commit()

    def get_contacts(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath('../../../Documents/Contact/' + self.md5_encode(self.user)[8:24] + '.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'contact_account_list' in db.Tables:
            ts = SQLiteParser.TableSignature('contact_account_list')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'userID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return

                if rec['userID'].Value == self.user:
                    continue
                
                if rec['noAliPay'].Value == '1':
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = dbPath.AbsolutePath
                friend.account_id = self.user
                friend.friend_id = rec['userID'].Value
                friend.nickname = rec['nickName'].Value
                tmp = self.aes_decode(rec['fullName'].Value)
                if tmp is not None:
                    memoryRange = MemoryRange.FromBytes(tmp)
                    obj = BPReader.GetTree(memoryRange)
                    if obj is not None:
                        friend.remark = obj.Value
                exposedAccount = rec['exposedAlipayAccount'].Value
                if not IsDBNull(exposedAccount):
                    str = Text.ASCIIEncoding.ASCII.GetString(exposedAccount)
                    g = re.match('.cn', str, re.M | re.I)
                    if g is not None:
                        friend.email = g.group(0)
                    g = re.match('.com', str, re.M | re.I)
                    if g is not None:
                        friend.email = g.group(0)
                if IsDBNull(friend.email):
                    tmp = self.aes_decode(rec['account'].Value)
                    if tmp is not None:
                        memoryRange = MemoryRange.FromBytes(tmp)
                        obj = BPReader.GetTree(memoryRange)
                        if obj is not None:
                            str = obj.Value
                            g = re.match('.cn', str, re.M | re.I)
                            if g is not None:
                                friend.email = g.group(0)
                            g = re.match('.com', str, re.M | re.I)
                            if g is not None:
                                friend.email = g.group(0)
                friend.photo = rec['headUrl'].Value
                friend.gender = model_im.GENDER_MALE if rec['gender'].Value == 'm' else model_im.GENDER_FEMALE
                friend.age = rec['age'].Value
                friend.signature = rec['signature'].Value
                friend.address = rec['area'].Value
                friend.telephone = rec['phoneString'].Value
                if IsDBNull(friend.telephone):
                    tmp = self.aes_decode(rec['account'].Value)
                    if tmp is not None:
                        memoryRange = MemoryRange.FromBytes(tmp)
                        obj = BPReader.GetTree(memoryRange)
                        if obj is not None:
                            str = obj.Value
                            g = re.match('\d{3}\D{6}\d{2}', str, re.M | re.I)
                            if g is not None:
                                friend.telephone = g.group(0)
                friend.type = model_im.FRIEND_TYPE_FRIEND
                if friend.friend_id not in self.contacts.keys():
                    self.contacts[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)

        if 'contact_recent_list' in db.Tables:
            ts = SQLiteParser.TableSignature('contact_recent_list')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'userID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if rec['userType'].Value != '107' and rec['userType'].Value != '11':
                    continue
                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = dbPath.AbsolutePath
                friend.account_id = self.user
                g = re.match('\d{16}', rec['userID'].Value, re.M | re.I)
                if g is not None:
                    friend.friend_id = g.group(0)
                memoryRange = MemoryRange.FromBytes(self.aes_decode(rec['displayName'].Value))
                obj = BPReader.GetTree(memoryRange)
                friend.nickname = obj.Value
                friend.photo = rec['headUrl'].Value
                friend.create_time = rec['createTime'].Value
                friend.type = model_im.FRIEND_TYPE_FOLLOW
                if friend.friend_id not in self.contacts.keys():
                    self.contacts[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)

        if 'ap_group_list' in db.Tables:
            ts = SQLiteParser.TableSignature('ap_group_list')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'groupId', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                chatroom = model_im.Chatroom()
                chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                chatroom.source = dbPath.AbsolutePath
                chatroom.account_id = self.user
                chatroom.chatroom_id = rec['groupId'].Value
                chatroom.name = rec['currentDisplayGroupName'].Value
                chatroom.photo = rec['groupImageUrl'].Value
                chatroom.max_member_count = rec['threshold'].Value
                chatroom.create_time = rec['gmtCreate'].Value
                chatroom.owner_id = rec['masterUserId'].Value
                chatroom.type = model_im.CHATROOM_TYPE_NORMAL
                member_id_list = rec['memberUserIdsDesc'].Value.replace('[', '').replace(']', '').replace('\"', '').split(',')
                chatroom.member_count = len(member_id_list)
                if chatroom.chatroom_id not in self.contacts.keys():
                    self.contacts[chatroom.chatroom_id] = chatroom
                self.im.db_insert_table_chatroom(chatroom)

                for member_id in member_id_list:
                    if canceller.IsCancellationRequested:
                        return
                    chatroom_member = model_im.ChatroomMember()
                    chatroom_member.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    chatroom_member.source = dbPath.AbsolutePath
                    chatroom_member.account_id = self.user
                    chatroom_member.chatroom_id = chatroom.chatroom_id
                    chatroom_member.member_id = member_id
                    friend = self.contacts.get(member_id)
                    if friend is not None:
                        chatroom_member.display_name = friend.nickname
                        chatroom_member.photo = friend.photo
                        chatroom_member.gender = friend.gender
                        chatroom_member.telephone = friend.telephone
                        chatroom_member.age = friend.age
                        chatroom_member.address = friend.address
                        chatroom_member.birthday = friend.birthday
                        chatroom_member.signature = friend.signature
                    self.im.db_insert_table_chatroom_member(chatroom_member)
        self.im.db_commit()

    def get_searchs(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath('../../../Documents/APGlobalSearch/' + self.md5_encode(self.user)[8:24] + '.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'global_search_recent_data' in db.Tables:
            ts = SQLiteParser.TableSignature('global_search_recent_data')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'keyword', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                search = model_im.Search()
                search.deleted = DeletedState.Intact
                search.source = dbPath.AbsolutePath
                search.account_id = self.user
                search.key = rec['keyword'].Value
                search.create_time = rec['timeStamp'].Value
                self.im.db_insert_table_search(search)
        self.im.db_commit()

    def get_mobile_contacts(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath('../../../Documents/MobileContact/' + self.md5_encode(self.user)[8:24] + '.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'contact_uploadAddress_list' in db.Tables:
            ts = SQLiteParser.TableSignature('contact_uploadAddress_list')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'indexKey', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                contact = Contacts.Contact()
                contact.Deleted = DeletedState.Intact
                contact.Source.Value = dbPath.AbsolutePath
                contact.OwnerUserID.Value = self.user
                contact.PhoneNumber.Value = rec['phoneNumber'].Value
                contact.ID.Value = rec['indexKey'].Value
                contact.FullName.Value = rec['phoneBookName'].Value
                self.contacts_models.append(contact)

    def get_chats(self):
        if self.user is None:
            return

        dbPath = self.root.GetByPath('../../../Documents/Contact/' + self.md5_encode(self.user)[8:24] + '.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return
        
        recentContact = {}
        if 'contact_recent_list' in db.Tables:
            ts = SQLiteParser.TableSignature('contact_recent_list')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'userId', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                type = rec['userType'].Value
                if type != '1' and type != '2' and type != '107' and type != '11':
                    continue
                id = None
                if type == '107' or type == '11':
                    g = re.match('\d{16}', rec['userID'].Value, re.M | re.I)
                    if g is not None:
                        id = g.group(0)
                else:
                    id = rec['userID'].Value
                contact = { 'userId' : id,   'userType' : type}
                if type != '107' and type != '11':
                    contact['chatTable'] = "chat_" + self.md5_encode(id + '_' + type)[8:24]
                recentContact[id] = contact
        
        for id in recentContact.keys():
            if canceller.IsCancellationRequested:
                return

            contact = self.contacts.get(id)

            if recentContact[id]['userType'] == '107' or recentContact[id]['userType'] == '11':
                dbPath = self.root.GetByPath('../../../Documents/Life/' + self.md5_encode(self.user)[8:24] + '.db')
                db = SQLiteParser.Database.FromNode(dbPath)
                if db is None:
                    return

                if 'LFHomeCardDB' in db.Tables:
                    ts = SQLiteParser.TableSignature('LFHomeCardDB')
                    SQLiteParser.Tools.AddSignatureToTable(ts, 'publicId', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        if rec['publicId'].Value != id:
                            continue
                        message = model_im.Message()
                        message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        message.source = dbPath.AbsolutePath
                        message.msg_id = str(uuid.uuid1()).replace('-', '')
                        message.account_id = self.user
                        message.talker_id = contact.friend_id
                        message.talker_name = contact.nickname
                        message.talker_type = model_im.CHAT_TYPE_OFFICIAL
                        message.sender_id = contact.friend_id
                        message.sender_name = contact.nickname
                        message.type = model_im.MESSAGE_CONTENT_TYPE_LINK
                        message.is_sender = model_im.MESSAGE_TYPE_SEND
                        message.content = '[标题]:' + rec['mSum'].Value
                        message.send_time = rec['msgTime'].Value
                        self.im.db_insert_table_message(message)
            elif recentContact[id]['userType'] == '1' or recentContact[id]['userType'] == '2':
                dbPath = self.root.GetByPath('../../../Documents/Chat/' + self.md5_encode(self.user)[8:24] + '.db')
                db = SQLiteParser.Database.FromNode(dbPath)
                if db is None:
                    return

                if recentContact[id]['chatTable'] in db.Tables:
                    ts = SQLiteParser.TableSignature(recentContact[id]['chatTable'])
                    SQLiteParser.Tools.AddSignatureToTable(ts, 'msgID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        message = model_im.Message()
                        message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        message.source = dbPath.AbsolutePath
                        message.msg_id = str(uuid.uuid1()).replace('-', '')
                        message.account_id = self.user
                        if recentContact[id]['userType'] == '1':
                            message.talker_id = contact.friend_id
                            message.talker_name = contact.nickname
                            message.talker_type = model_im.CHAT_TYPE_FRIEND
                            message.sender_id = rec['fromUId'].Value
                            message.sender_name = message.talker_name
                        if recentContact[id]['userType'] == '2':
                            message.talker_id = contact.chatroom_id
                            message.talker_name = contact.name
                            message.talker_type = model_im.CHAT_TYPE_GROUP
                            fromUId = rec['fromUId'].Value
                            toUId = rec['toUId'].Value
                            message.sender_id = fromUId
                            if self.contacts.get(message.sender_id) is not None:
                                message.sender_name = self.contacts.get(message.sender_id).nickname
                            if message.sender_id == self.user:
                                message.sender_name = self.name
                        message.is_sender = model_im.MESSAGE_TYPE_SEND if message.sender_id == self.user else model_im.MESSAGE_TYPE_RECEIVE
                        message.send_time = rec['createTime'].Value
                        message.type = self.parse_message_type(rec['templateCode'].Value)
                        message.content = self.get_message_content(rec['bizMemo'].Value)
                        data = rec['templateData'].Value
                        if message.type == model_im.MESSAGE_CONTENT_TYPE_VOICE or \
                           message.type == model_im.MESSAGE_CONTENT_TYPE_IMAGE or \
                           message.type == model_im.MESSAGE_CONTENT_TYPE_VIDEO or \
                           message.type == model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD:
                            message.media_path = self.get_media_path(data, message.type)
                        if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                            message.location_obj = message.create_location()
                            message.location_id = self.get_location(message.location_obj, data, rec['link'].Value, message.send_time)
                        if message.type == model_im.MESSAGE_CONTENT_TYPE_RED_ENVELPOE or \
                           message.type == model_im.MESSAGE_CONTENT_TYPE_RECEIPT or \
                           message.type == model_im.MESSAGE_CONTENT_TYPE_AA_RECEIPT:
                            message.deal_obj = message.create_deal()
                            message.deal_id = self.get_deal(message.deal_obj, data, message.type, message.send_time)
                        self.im.db_insert_table_message(message)
        self.im.db_commit()

    def get_deal(self, deal, content, type, time):
        json_obj = None
        try:
            memoryRange = MemoryRange.FromBytes(self.aes_decode(content))
            plist_obj = BPReader.GetTree(memoryRange)
            json_obj = json.loads(plist_obj.Value)
        except:
            traceback.print_exc()

        deal.create_time = int(time)
        if type == model_im.MESSAGE_CONTENT_TYPE_AA_RECEIPT:
            deal.type = model_im.DEAL_TYPE_AA_RECEIPT
            g = re.search('\d+\.\d+(.*?)', json_obj['topTitle'], re.M | re.I)
            if g is not None:
                deal.money = g.group(0).replace(',', '')
            deal.description = json_obj['topTitle']
            deal.remark = json_obj['appName']
        if type == model_im.MESSAGE_CONTENT_TYPE_RED_ENVELPOE:
            deal.type = model_im.DEAL_TYPE_RED_ENVELPOE
            g = re.search('\d+\.\d+(.*?)', json_obj['m'], re.M | re.I)
            if g is not None:
                deal.money = g.group(0).replace(',', '')
            deal.description = json_obj['m']
            deal.remark = json_obj['appName']
        if type == model_im.MESSAGE_CONTENT_TYPE_RECEIPT:
            deal.type = model_im.DEAL_TYPE_RECEIPT
            g = re.search('\d+\.\d+(.*?)', json_obj['title'], re.M | re.I)
            if g is not None:
                deal.money = g.group(0).replace(',', '')
            deal.description = json_obj['m']
            deal.remark = json_obj['appName']
        self.msg_deals.append(deal)
        self.im.db_insert_table_deal(deal)
        self.im.db_commit()
        return deal.deal_id
    
    def get_message_content(self, biz):
        biz = self.aes_decode(biz)
        if biz is None:
            return None
        memoryRange = MemoryRange.FromBytes(biz)
        obj = BPReader.GetTree(memoryRange)
        if obj is not None:
            return obj.Value
        return None

    def get_media_path(self, content, type):
        media_path = None
        try:
            memoryRange = MemoryRange.FromBytes(self.aes_decode(content))
            plist_obj = BPReader.GetTree(memoryRange)
            json_obj = json.loads(plist_obj.Value)
            if type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
                key = json_obj['v']
                media_path = self.get_local_cache(key)
            if type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                key = json_obj['i']
                media_path = self.get_local_cache(key)
            if type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                key = json_obj['video'].split('|')[0]
                media_path = self.get_local_cache(key)
            if type == model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD:
                media_path = json_obj['n'] + ':' + json_obj['id']
        except:
            traceback.print_exc()
        return media_path

    def get_local_cache(self, key):
        if key is None:
            return None

        dbPath = self.root.GetByPath('../../com.alipay.multimedia/apmcache.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return None

        if 'apmcache' in db.Tables:
            ts = SQLiteParser.TableSignature('apmcache')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'filename', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if rec['key'].Value == key or rec['alias_key'].Value == key:
                    node = self.root.GetByPath('../../com.alipay.multimedia/data/')
                    if node is not None:
                        return os.path.join(node.AbsolutePath, rec['filename'].Value)
        return None

    def get_location(self, location, content, link, time):
        json_obj = None
        try:
            memoryRange = MemoryRange.FromBytes(self.aes_decode(content))
            plist_obj = BPReader.GetTree(memoryRange)
            json_obj = json.loads(plist_obj.Value)
        except:
            traceback.print_exc()
        location.address = json_obj['d']
        g = re.search('&lon=(\d+\.\d+)', link, re.M | re.I)
        if g is not None:
            location.longitude = g.group(1)
        g = re.search('&lat=(\d+\.\d+)', link, re.M | re.I)
        if g is not None:
            location.latitude = g.group(1)
        location.timestamp = time
        self.im.db_insert_table_location(location)
        self.im.db_commit()
        return location.location_id

    def parse_message_type(self, type):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if type == '12':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        if type == '13':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_EMOJI
        if type == '14':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if type == '16':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        if type == '8003':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        if type == '19' or type == '20':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        if type == '109' or type == '209':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_RECEIPT
        if type == '107':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_RED_ENVELPOE
        if type == '17':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
        if type == '105' or msgtype == '125':
            msgtype = model_im.MESSAGE_CONTENT_TYPE_AA_RECEIPT
        return msgtype

    def get_deals(self):
        for deal in self.msg_deals:
            trade = model_eb.EBDeal()
            trade.set_value_with_idx(trade.account_id, self.user)
            trade.set_value_with_idx(trade.money, deal.money.replace(',', '') if deal.money is not None else None)
            trade.set_value_with_idx(trade.deal_type, model_eb.EBDEAL_TYPE_OTHER)
            trade.set_value_with_idx(trade.status, deal.status)
            trade.set_value_with_idx(trade.begin_time, deal.create_time)
            trade.set_value_with_idx(trade.end_time, deal.expire_time)
            trade.set_value_with_idx(trade.deleted, deal.deleted)
            trade.set_value_with_idx(trade.desc, deal.description)
            trade.set_value_with_idx(trade.source_file, deal.source)
            self.eb.db_insert_table_deal(trade.get_value())
        self.eb.db_commit()

        if self.user is None:
            return

        dbPath = self.root.GetByPath('../../../Documents/Preferences/' + self.md5_encode(self.user)[8:24] + '.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return None

        if '__DEFAULTS__' in db.Tables:
            ts = SQLiteParser.TableSignature('__DEFAULTS__')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'key', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if rec['key'].Value != 'BILL_LIST_CACHE_NEW_20000003':
                    continue

                try:
                    memoryRange = MemoryRange.FromBytes(rec['data'].Value)
                    obj = BPReader.GetTree(memoryRange)
                    list = obj.Children['list']
                    for month_obj in list:
                        recordList = month_obj.Children['recordList']
                        for record in recordList:
                            trade = model_eb.EBDeal()
                            trade.set_value_with_idx(trade.deleted, DeletedState.Intact)
                            trade.set_value_with_idx(trade.source_file, dbPath.AbsolutePath)
                            trade.set_value_with_idx(trade.account_id, self.user)
                            trade.set_value_with_idx(trade.deal_type, model_eb.EBDEAL_TYPE_OTHER)
                            trade.set_value_with_idx(trade.money, record['money'].Value.replace(',', ''))
                            trade.set_value_with_idx(trade.desc, record['title'].Value)
                            trade.set_value_with_idx(trade.begin_time, record['gmtCreate'].Value)
                            self.eb.db_insert_table_deal(trade.get_value())
                    self.eb.db_commit()
                except:
                    traceback.print_exc()

    @staticmethod
    def aes_decode(text):
        if IsDBNull(text):
            return None
        rm = RijndaelManaged()
        key = bytes([0x09,0x90,0xE1,0x06,0x7A,0x30,0x23,0xD2,0x79,0x0C,0x51,0x4C,0x74,0x78,0x2F,0x94,0x19,0x6A,0x49,0x51,0x4C,0xC4,0x78,0x3A,0x9A,0x2E,0x33,0x68,0x25,0x49,0x51,0xF7])
        rm.Key = Convert.FromBase64String(base64.b64encode(key))
        rm.BlockSize = 128
        rm.Mode = CipherMode.CBC
        rm.Padding = PaddingMode.PKCS7
        rm.IV = Convert.FromBase64String(base64.b64encode(bytes([0] * 16)))
        tr = rm.CreateDecryptor(rm.Key, rm.IV)
        return tr.TransformFinalBlock(text, 0, len(text))

    @staticmethod
    def md5_encode(text):
        if IsDBNull(text):
            return None
        return hashlib.md5(text).hexdigest()
