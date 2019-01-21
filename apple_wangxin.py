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
from PA_runtime  import *
from PA.InfraLib.Extensions import PlistHelper

import gc
import bcp_im
import model_im
import uuid
import json

VERSION_APP_VALUE = 1

class WangxinParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source =  extract_source
        self.im = model_im.IM()
        self.cache_path = ds.OpenCachePath('WangXin')
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.cache_db = os.path.join(self.cache_path, 'cache.db')

        nameValues.SafeAddValue(bcp_im.CONTACT_ACCOUNT_TYPE_IM_WANGXIN, self.cache_db)

    def parse(self):
        if self.im.need_parse(self.cache_db, VERSION_APP_VALUE):
            self.im.db_create(self.cache_db)
            user_list = self.get_user_list()
            for user in user_list.keys():
                self.user = user
                self.dbNode = user_list[user]
                self.friends = {}
                self.chatrooms = {}
                self.chatroom_members = {}
                self.parse_user()
                self.user = None
                self.dbNode = None
                self.friends = None
                self.chatrooms = None
                self.chatroom_members = None
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
        user_list = {}
        dir = '../../Caches/YWDB/'
        dirNode = self.root.GetByPath(dir)
        if dirNode is None:
            dir = '../../../Documents/YWDB/'
            dirNode = self.root.GetByPath(dir)
            if dirNode is None:
                return user_list

        for file in os.listdir(dirNode.PathWithMountPoint):
            node = self.root.GetByPath(dir + file + '/message.db')
            if node is not None:
                db = SQLiteParser.Database.FromNode(node)
                if db is None:
                    return user_list
                if 'ZUSERINFO' in db.Tables:
                    ts = SQLiteParser.TableSignature('ZUSERINFO')
                    SQLiteParser.Tools.AddSignatureToTable(ts, 'ZUSER_ID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        account = model_im.Account()
                        account.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        account.source = node.AbsolutePath
                        account.account_id = UnicodeEncoding.UTF8.GetString(rec['ZUSER_ID'].Value) if not IsDBNull(rec['ZUSER_ID'].Value) else None
                        account.nickname = UnicodeEncoding.UTF8.GetString(rec['ZTBNICK'].Value) if not IsDBNull(rec['ZTBNICK'].Value) else None
                        account.username = UnicodeEncoding.UTF8.GetString(rec['ZNAME'].Value) if not IsDBNull(rec['ZNAME'].Value) else None
                        if account.username is None:
                            account.username = account.nickname
                        self.username = account.username
                        account.photo = rec['ZAVATAR'].Value
                        if rec['ZGENDER'].Value == '男':
                            account.gender = model_im.GENDER_MALE 
                        elif rec['ZGENDER'].Value == '女':
                            account.gender = model_im.GENDER_FEMALE
                        else:
                            account.gender = model_im.GENDER_NONE
                        account.province = rec['ZPROVINCE'].Value
                        account.city = rec['ZCITY'].Value
                        self.im.db_insert_table_account(account)
                        user_list[account.account_id] = node
            self.im.db_commit()
        return user_list

    def parse_user(self):
        self.get_friends()
        self.get_chats()

    def get_friends(self):
        if self.user is None:
            return

        db = SQLiteParser.Database.FromNode(self.dbNode)
        if db is None:
            return

        if 'ZPUBACCOUNT' in db.Tables:
            ts = SQLiteParser.TableSignature('ZPUBACCOUNT')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZSNSID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = self.dbNode.AbsolutePath
                friend.account_id = self.user
                friend.friend_id = rec['ZWWID'].Value
                friend.nickname = rec['ZNICK'].Value
                friend.remark = rec['ZDESC'].Value
                friend.photo = rec['ZAVATAR'].Value
                friend.type = model_im.FRIEND_TYPE_SUBSCRIBE
                self.friends[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)
            self.im.db_commit()

        if 'ZWWPERSON' in db.Tables:
            ts = SQLiteParser.TableSignature('ZWWPERSON')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZTB_ID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                id = UnicodeEncoding.UTF8.GetString(rec['ZTB_ID'].Value) if not IsDBNull(rec['ZTB_ID'].Value) else None
                if id in self.friends.keys():
                    continue
                if rec['ZISFRIEND'].Value != 1:
                    continue

                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = self.dbNode.AbsolutePath
                friend.account_id = self.user
                friend.friend_id = id
                friend.nickname = rec['ZDISPLAYNAME'].Value
                friend.remark = rec['ZFULL_NAME'].Value
                friend.photo = rec['ZAVATAR'].Value
                friend.type = model_im.FRIEND_TYPE_FRIEND
                if rec['ZGENDER'].Value == '男':
                    friend.gender = model_im.GENDER_MALE 
                elif rec['ZGENDER'].Value == '女':
                    friend.gender = model_im.GENDER_FEMALE
                else:
                    friend.gender = model_im.GENDER_NONE
                friend.province = rec['ZPROVINCE'].Value
                friend.city = rec['ZCITY'].Value
                friend.email = rec['ZEMAIL'].Value
                if friend.friend_id not in self.friends.keys():
                    self.friends[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)
            self.im.db_commit()

        if 'ZWXSHOP' in db.Tables:
            ts = SQLiteParser.TableSignature('ZWXSHOP')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZSHOPID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                friend = model_im.Friend()
                friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                friend.source = self.dbNode.AbsolutePath
                friend.account_id = self.user
                friend.friend_id = rec['ZSHOPID'].Value
                friend.nickname = rec['ZNICK'].Value
                friend.photo = rec['ZPICURL'].Value
                friend.remark = rec['ZTITLE'].Value
                friend.type = model_im.FRIEND_TYPE_SHOP
                friend.province = rec['ZPROVINCE'].Value
                friend.city = rec['ZCITY'].Value
                self.friends[friend.friend_id] = friend
                self.im.db_insert_table_friend(friend)
            self.im.db_commit()

        if 'ZWWTRIBELIST' in db.Tables:
            ts = SQLiteParser.TableSignature('ZWWTRIBELIST')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZTRIBEID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                chatroom = model_im.Chatroom()
                chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                chatroom.source = self.dbNode.AbsolutePath
                chatroom.account_id = self.user
                chatroom.chatroom_id = rec['ZTRIBEID'].Value
                chatroom.name = rec['ZDISPLAYNAME'].Value
                chatroom.photo = rec['ZAVATAR'].Value
                chatroom.create_time = rec['ZPROFILELATESTUPDATE'].Value
                chatroom.member_count = rec['ZMEMBERCOUNT'].Value
                chatroom.type = model_im.CHATROOM_TYPE_NORMAL
                self.im.db_insert_table_chatroom(chatroom)
                self.chatrooms[chatroom.chatroom_id] = chatroom

                members = []
                if 'ZWWTRIBEMEMBER' in db.Tables:
                    ts = SQLiteParser.TableSignature('ZWWTRIBEMEMBER')
                    SQLiteParser.Tools.AddSignatureToTable(ts, 'ZUID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(ts, self.extract_deleted):
                        if rec['ZTRIBEID'].Value != chatroom.chatroom_id:
                            continue

                        chatroom_member = model_im.ChatroomMember()
                        chatroom_member.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                        chatroom_member.source = self.dbNode.AbsolutePath
                        chatroom_member.account_id = self.user
                        chatroom_member.chatroom_id = chatroom.chatroom_id
                        chatroom_member.member_id = rec['ZUID'].Value
                        chatroom_member.display_name = rec['ZDISPLAYNAME'].Value
                        friend = self.friends.get(chatroom_member.member_id, None)
                        if friend is not None:
                            chatroom_member.photo = friend.photo
                            chatroom_member.gender = friend.gender
                            chatroom_member.email = friend.email
                        members.append(chatroom_member)
                        self.im.db_insert_table_chatroom_member(chatroom_member)
                self.chatroom_members[chatroom.chatroom_id] = members
            self.im.db_commit()

    def get_chats(self):
        if self.user is None:
            return

        db = SQLiteParser.Database.FromNode(self.dbNode)
        if db is None:
            return
        
        for id in self.friends.keys():
            if 'ZWWMESSAGE' in db.Tables:
                ts = SQLiteParser.TableSignature('ZWWMESSAGE')
                SQLiteParser.Tools.AddSignatureToTable(ts, 'ZSENDERID', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    sender_id = UnicodeEncoding.UTF8.GetString(rec['ZSENDERID'].Value) if not IsDBNull(rec['ZSENDERID'].Value) else None
                    receive_id = UnicodeEncoding.UTF8.GetString(rec['ZRECEIVERID'].Value) if not IsDBNull(rec['ZRECEIVERID'].Value) else None
                    if sender_id != id and receive_id != id:
                        continue
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = self.dbNode.AbsolutePath
                    message.msg_id = str(uuid.uuid1()).replace('-', '')
                    message.account_id = self.user
                    message.talker_id = id
                    message.sender_id = sender_id
                    friend = self.friends.get(id, None)
                    if friend is not None:
                        message.sender_name = self.username if sender_id == self.user else friend.nickname
                        message.talker_name = friend.nickname
                        if friend.type == model_im.FRIEND_TYPE_FRIEND:
                            message.talker_type = model_im.CHAT_TYPE_FRIEND
                        elif friend.type == model_im.FRIEND_TYPE_SUBSCRIBE:
                            message.talker_type = model_im.CHAT_TYPE_OFFICIAL
                        elif friend.type == model_im.FRIEND_TYPE_SHOP:
                            message.talker_type = model_im.CHAT_TYPE_SHOP
                    message.is_sender = message.sender_id == self.user
    
                    message.type = self.parse_message_type(rec['ZTYPE'].Value)
                    message.content =  UnicodeEncoding.UTF8.GetString(rec['ZCONTENT'].Value) if not IsDBNull(rec['ZCONTENT'].Value) else None
                    message.send_time = int(float(self.macTime_to_unixTime(rec['ZTIME'].Value)))
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                        try:
                            obj = json.loads(message.content)
                            message.content = obj['resource']
                        except:
                            traceback.print_exc()
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:  
                        message.location_obj = message.create_location()
                        message.location_id = self.get_location(message.location_obj, message.talker_type, message.send_time, rec['ZLOCATION'].Value)
                    self.im.db_insert_table_message(message)
                self.im.db_commit()
                
        for id in self.chatrooms.keys():
            if 'ZWWTRIBEMESSAGE' in db.Tables:
                ts = SQLiteParser.TableSignature('ZWWTRIBEMESSAGE')
                SQLiteParser.Tools.AddSignatureToTable(ts, 'ZWWTRIBEMESSAGE', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableRecords(ts, self.extract_deleted):
                    if str(bytes(rec['ZTRIBEID'].Value)) != id:
                        continue
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.source = self.dbNode.AbsolutePath
                    message.msg_id = str(uuid.uuid1()).replace('-', '')
                    message.account_id = self.user
                    chatroom = self.chatrooms.get(id)
                    message.talker_id = chatroom.chatroom_id
                    message.talker_name = chatroom.name
                    message.talker_type = model_im.CHAT_TYPE_GROUP
                    from_id = str(bytes(rec['ZFROMUID'].Value))
                    message.sender_id = from_id
                    members = self.chatroom_members.get(id, None)
                    if members is not None:
                        for member in members:
                            if from_id == member.member_id:
                                message.sender_name = member.display_name
                    message.is_sender = message.sender_id == self.user
                    message.type = self.parse_message_type(rec['ZCONTENTTYPE'].Value)
                    message.send_time = int(float(self.macTime_to_unixTime(rec['ZDTIME'].Value)))
                    message.content =  str(rec['ZCONTENT'].Value)
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                        try:
                            obj = json.loads(message.content)
                            message.content = obj['previewUrl']
                        except:
                            traceback.print_exc()
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
                        try:
                            obj = json.loads(message.content)
                            message.content = obj['url']
                        except:
                            traceback.print_exc()
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                        try:
                            obj = json.loads(message.content)
                            message.content = obj['resource']
                        except:
                            traceback.print_exc()
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.location_obj = message.create_location()
                        message.location_id = self.get_location(message.location_obj, message.talker_type, message.send_time, message.content)
                    self.im.db_insert_table_message(message)
                self.im.db_commit()

    def get_location(self, location, talker_type, msg_time, param = None):
        if talker_type == model_im.CHAT_TYPE_FRIEND \
            or talker_type == model_im.CHAT_TYPE_OFFICIAL \
            or talker_type == model_im.CHAT_TYPE_SHOP:
            db = SQLiteParser.Database.FromNode(self.dbNode)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZLOCATION')
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZNAME', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if param != rec['Z_PK'].Value:
                    continue
                location.source = self.dbNode.AbsolutePath
                location.latitude = rec['ZLATITUDE'].Value
                location.longitude = rec['ZLONGITUDE'].Value
                location.address = rec['ZNAME'].Value
                location.timestamp = msg_time
                self.im.db_insert_table_location(location)
                self.im.db_commit()
                return location.location_id
        elif talker_type == model_im.CHAT_TYPE_GROUP:
            location.source = self.dbNode.AbsolutePath
            location.latitude = param.split(',')[1]
            location.longitude = param.split(',')[0]
            location.address = param.split(',')[2]
            location.timestamp = msg_time
            self.im.db_insert_table_location(location)
            self.im.db_commit()
            return location.location_id
        else:
            return None
        return None

    def parse_message_type(self, t):
        msg_type = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if t == 1:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if t == 3:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        if t == 66:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_VOIP
        if t == 52:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_CONTACT_CARD
        if t == 8:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        if t == 65:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_LINK
        if t == 5 or t == 33:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
        if t == 2:
            msg_type = model_im.MESSAGE_CONTENT_TYPE_VOICE
        return msg_type

    def macTime_to_unixTime(self, t):
        return t + 978278400 + 8 * 3600
        
def analyze_wangxin(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = WangxinParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))
    pr.Build('旺信')
    gc.collect()
    return pr
