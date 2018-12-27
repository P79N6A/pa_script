# _*_ coding:utf-8 _*_

from PA_runtime import *
import clr
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
except:
    pass
del clr

from PA.Common.Utilities.Types import TimeStampFormats

import System.Data.SQLite as SQLite
import model_im

import re
import hashlib
import shutil
import traceback
import json
import time

from System.Text import *

import base64
import hashlib
import hmac
from System.Security.Cryptography import *

VERSION_APP_VALUE = 1

class KakaoTalkParser(model_im.IM):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node
        self.messageNode = node.Parent.GetByPath('/Message.db$')
        self.extractDeleted = extract_deleted
        self.db = None
        self.im = model_im.IM()
        self.cachepath = ds.OpenCachePath("KakaoTalk")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = self.node.AbsolutePath
        rdb_name = self.node.PathWithMountPoint
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.recoverDB = self.cachepath + "\\" +md5_rdb.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\KakaoTalkSource'
        self.account_id = ''
        self.account_name = '未知用户'
        self.chatgroup = []
        self.publicchat = []
        self.friendchat = []
        self.secretchat = []

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def parse(self):
        if self.need_parse(self.cachedb, VERSION_APP_VALUE):
            if os.path.exists(self.cachepath):
                shutil.rmtree(self.cachepath)
            os.mkdir(self.cachepath)
            self.db_create(self.cachedb)
            #self._copytocache(self.node.Parent.PathWithMountPoint)
            self.analyze_data()
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
        models = []
        models_im = model_im.GenerateModel(self.cachedb).get_models()
        models.extend(models_im)
        return models
    
    def parse_account(self):
        '''解析账户数据'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZUSER')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if rec.IsDeleted == 1:
                        continue
                    account = model_im.Account()
                    if canceller.IsCancellationRequested:
                        break
                    if self._db_record_get_int_value(rec, 'Z_PK') == 1:
                        account.account_id = self._db_record_get_int_value(rec, 'ZACCOUNTID')
                        self.account_id = account.account_id
                        account.nickname = self._db_record_get_string_value(rec, 'ZNICKNAME')
                        username = self._db_record_get_string_value(rec, 'ZNAME')
                        account.username = username if username is not '' else account.nickname
                        self.account_name = account.username
                        account.photo = self._db_record_get_string_value(rec, 'ZPHOTOURL')
                        telephone = self._db_record_get_string_value(rec, 'ZPHONENUMBER')
                        account.telephone = self._decode_telephone(telephone)
                        account.signature = self._db_record_get_string_value(rec, 'ZSTATUSMESSAGE')
                        account.source = self.node.AbsolutePath
                        self.db_insert_table_account(account)
                        break
                except:
                    pass
            self.db_commit()
        except Exception as e:
            print(e)

    @staticmethod
    def _decode_telephone(telephone):
        '''解码手机号'''
        return ''

    def parse_members(self):
        '''解析成员数据（好友、私密聊天成员、讨论组成员、公开聊天成员）'''
        '''
        1. 解析ZCHAT表，获取聊天类型
        2. 根据ZCHAT表中解析出的Z_PK到Z_1MEMBER表中获取对应的成员id
        3. 根据获取到的成员id到ZUSER中获取到成员的详细信息
        '''
        try:
            friend_chat_id = []
            friend_chat_deleted_id = []
            secret_member_chat_id = []
            secret_member_chat_deleted_id = []
            chatgroup_member_chat_id = []
            chatgroup_member_chat_deleted_id = []
            publicgroup_member_chat_id = []
            publicgroup_member_chat_deleted_id = []
            self.zpk2zid = {}
            self.public_chat = {}
            self.public_delete_chat = {}
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZCHAT')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    chatid = self._db_record_get_int_value(rec, 'Z_PK')
                    chat_deleted = rec.IsDeleted
                    if chatid == 0:
                        continue
                    self.zpk2zid[chatid] = self._db_record_get_int_value(rec, 'ZID')
                    #讨论组
                    if self._db_record_get_int_value(rec, 'ZRELFAVORITE') != 0:
                        if chat_deleted:
                            chatgroup_member_chat_deleted_id.append(chatid)
                            self.chatgroup.append(chatid)
                        else:
                            chatgroup_member_chat_id.append(chatid)
                            self.chatgroup.append(chatid)
                    #私密聊天
                    elif self._db_record_get_int_value(rec, 'ZUSERID') != 0:
                        if chat_deleted:
                            secret_member_chat_deleted_id.append(chatid)
                            self.secretchat.append(chatid)
                        else:
                            secret_member_chat_id.append(chatid)
                            self.secretchat.append(chatid)
                    #公开聊天
                    elif self._db_record_get_int_value(rec, 'ZLINKID') != 0:
                        if chat_deleted:
                            publicgroup_member_chat_deleted_id.append(chatid)
                            self.public_delete_chat[self._db_record_get_int_value(rec, 'ZLINKID')] = chatid
                            self.publicchat.append(chatid)
                        else:
                            publicgroup_member_chat_id.append(chatid)
                            self.public_chat[self._db_record_get_int_value(rec, 'ZLINKID')] = chatid
                            self.publicchat.append(chatid)
                    #好友
                    else:
                        if chat_deleted:
                            friend_chat_deleted_id.append(self._db_record_get_int_value(rec, 'Z_PK'))
                            self.friendchat.append(chatid)
                        else:
                            friend_chat_id.append(self._db_record_get_int_value(rec, 'Z_PK'))
                            self.friendchat.append(chatid)
                    deleted = rec.IsDeleted
                except:
                    traceback.print_exc()
            
            friend_id = []
            friend_deleted_id = []
            secret_member_id = []
            secret_member_deleted_id = []
            chatgroup_member_id = {}
            self.chatgroup_member_id = {}
            chatgroup_member_deleted_id = {}
            self.chatgroup_member_deleted_id = {}
            publicgroup_member_id = {}
            self.publicgroup_member_id = {}
            publicgroup_member_deleted_id = {}
            self.publicgroup_member_deleted_id = {}
            ts = SQLiteParser.TableSignature('Z_1MEMBERS')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    chatid = self._db_record_get_int_value(rec, 'Z_1CHATS')
                    memberid = self._db_record_get_int_value(rec, 'Z_45MEMBERS')
                    if chatid == 0 or memberid == 0:
                        continue
                    deleted = rec.IsDeleted                    
                    #讨论组
                    if chatid in chatgroup_member_chat_deleted_id and deleted:
                        if memberid not in chatgroup_member_deleted_id:
                            chatgroup_member_deleted_id[memberid] = []
                            chatgroup_member_deleted_id[memberid].append(chatid)
                        else:
                            chatgroup_member_deleted_id[memberid].append(chatid)
                        if chatid not in self.chatgroup_member_deleted_id:
                            self.chatgroup_member_deleted_id[chatid] = [1]
                            self.chatgroup_member_deleted_id[chatid].append(memberid)
                        else:
                            self.chatgroup_member_deleted_id[chatid].append(memberid)
                    elif chatid in chatgroup_member_chat_id and not deleted:
                        if memberid not in chatgroup_member_id:
                            chatgroup_member_id[memberid] = []
                            chatgroup_member_id[memberid].append(chatid)
                        else:
                            chatgroup_member_id[memberid].append(chatid)
                        if chatid not in self.chatgroup_member_id:
                            self.chatgroup_member_id[chatid] = [1]
                            self.chatgroup_member_id[chatid].append(memberid)
                        else:
                            self.chatgroup_member_id[chatid].append(memberid)
                    #秘密聊天
                    elif chatid in secret_member_chat_deleted_id and deleted:
                        secret_member_deleted_id.append(member_id)
                    elif chatid in secret_member_chat_id and not deleted:
                        secret_member_id.append(memberid)
                    #公开聊天
                    elif chatid in publicgroup_member_chat_deleted_id and deleted:
                        if memberid not in publicgroup_member_deleted_id:
                            publicgroup_member_deleted_id[memberid] = []
                            publicgroup_member_deleted_id[memberid].append(chatid)
                        else:
                            publicgroup_member_deleted_id[memberid].append(chatid)
                        if chatid not in self.publicgroup_member_deleted_id:
                            self.publicgroup_member_deleted_id[chatid] = [1]
                            self.publicgroup_member_deleted_id[chatid].append(memberid)
                        else:
                            self.publicgroup_member_deleted_id[chatid].append(memberid)
                    elif chatid in publicgroup_member_chat_id and not deleted:
                        if memberid not in publicgroup_member_id:
                            publicgroup_member_id[memberid] = []
                            publicgroup_member_id[memberid].append(chatid)
                        else:
                            publicgroup_member_id[memberid].append(chatid)
                        if chatid not in self.publicgroup_member_id:
                            self.publicgroup_member_id[chatid] = [1]
                            self.publicgroup_member_id[chatid].append(memberid)
                        else:
                            self.publicgroup_member_id[chatid].append(memberid)
                    #好友
                    elif chatid in friend_chat_deleted_id and deleted:
                        friend_deleted_id.append(memberid)
                    elif chatid in friend_chat_id and not deleted:
                        friend_id.append(memberid)
                except:
                    traceback.print_exc()
            chatgroup_member_deleted_id[1] = self.chatgroup_member_deleted_id.keys()
            chatgroup_member_id[1] = self.chatgroup_member_id.keys()
            publicgroup_member_deleted_id[1] = self.publicgroup_member_deleted_id.keys()
            publicgroup_member_id[1] = self.publicgroup_member_id.keys()
            friend_id = list(set(friend_id))
            friend_deleted_id = list(set(friend_deleted_id))
            secret_member_id = list(set(secret_member_id))
            secret_member_deleted_id = list(set(secret_member_deleted_id))
            ts = SQLiteParser.TableSignature('ZUSER')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    deleted = rec.IsDeleted
                    userid = self._db_record_get_int_value(rec, 'Z_PK')
                    account_id = self.account_id
                    friendid = self._db_record_get_string_value(rec, 'ZID')
                    email = self._db_record_get_string_value(rec, 'ZEMAIL')
                    headpic = self._db_record_get_string_value(rec, 'ZLARGETHUMBNAILURL')
                    name = self._db_record_get_string_value(rec, 'ZNAME')
                    nickname = self._db_record_get_string_value(rec, 'ZNICKNAME')
                    phonenumber = self._decode_telephone(self._db_record_get_string_value(rec, 'ZPHONENUMBER'))
                    data_package = (account_id, friendid, email, headpic, name, nickname, phonenumber, deleted)
                    #解析好友聊天成员信息
                    if userid in friend_id and not deleted:
                        self.parse_friend(data_package)
                    elif userid in friend_deleted_id and deleted:
                        self.parse_friend(data_package)
                    #解析私密聊天成员信息(因为私密聊天必须是好友，所以在解析好友时不做重复插入)
                    if userid in secret_member_id and not deleted:
                        pass
                    elif userid in secret_member_deleted_id and deleted:
                        pass
                    #解析讨论组成员信息（群组成员）
                    if userid in chatgroup_member_id.keys() and not deleted:
                        for chatgroupid in chatgroup_member_id[userid]:
                            self.parse_chatroom_member(data_package, self.zpk2zid[chatgroupid])
                    elif userid in chatgroup_member_deleted_id.keys() and deleted:
                        for chatgroupid in chatgroup_member_deleted_id[userid]:
                            self.parse_chatroom_member(data_package, self.zpk2zid[chatgroupid])
                    #解析公开聊天成员信息（群组成员）
                    if userid in publicgroup_member_id.keys() and not deleted:
                        for groupid in publicgroup_member_id[userid]:
                            self.parse_chatroom_member(data_package, self.zpk2zid[groupid])
                    elif userid in publicgroup_member_deleted_id.keys() and deleted:
                        for groupid in publicgroup_member_deleted_id[userid]:
                            self.parse_chatroom_member(data_package, self.zpk2zid[groupid])
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_friend(self, data):
        '''解析好友数据'''
        try:
            friend = model_im.Friend()
            friend.account_id = data[0]
            friend.friend_id = data[1]
            friend.email = data[2]
            friend.photo = data[3]
            friend.nickname = data[5]
            friend.fullname = data[4] if data[4] is not '' else friend.nickname
            friend.telephone = data[6]
            friend.deleted = data[7]
            friend.type = model_im.FRIEND_TYPE_FRIEND
            self.db_insert_table_friend(friend)
        except:
            pass

    def parse_chatroom_member(self, data, chatroom_id):
        '''解析群组成员数据'''
        try:
            chatroomMember = model_im.ChatroomMember()
            chatroomMember.account_id = data[0]
            chatroomMember.chatroom_id = chatroom_id
            chatroomMember.member_id = data[1]
            chatroomMember.email = data[2]
            chatroomMember.photo = data[3]
            chatroomMember.display_name = data[4] if data[4] is not '' else data[5]
            chatroomMember.telephone = data[6]
            chatroomMember.deleted = data[7]
            self.db_insert_table_chatroom_member(chatroomMember)
        except:
            pass

    def parse_chatroom(self):
        '''解析群组数据'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZFAVORITE')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if self._db_record_get_int_value(rec, 'ZCHAT') == 0:
                        continue
                    if self._db_record_get_int_value(rec, 'Z_PK') == 0:
                        continue
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = self.account_id
                    chatid = self._db_record_get_int_value(rec, 'ZCHAT')
                    chatroom.chatroom_id = self.zpk2zid[chatid] if chatid != 0 else 0
                    chatroom.member_count = len(self.chatgroup_member_id[self._db_record_get_int_value(rec, 'ZCHAT')]) + 1
                    chatroom.deleted = rec.IsDeleted
                    chatroom.type = model_im.CHATROOM_TYPE_TEMP
                    chatroom.name = self._db_record_get_string_value(rec, 'ZTYPENAME')
                    chatroom.source = self.node.AbsolutePath
                    self.db_insert_table_chatroom(chatroom)
                except:
                    traceback.print_exc()
            ts = SQLiteParser.TableSignature('ZOPENLINK')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if self._db_record_get_int_value(rec, 'ZLINKID') == 0:
                        continue
                    if self._db_record_get_int_value(rec, 'Z_PK') == 0:
                        continue
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = self.account_id
                    chatid = self.public_chat[self._db_record_get_int_value(rec, 'ZLINKID')]
                    chatroom.chatroom_id = self.zpk2zid[chatid] if chatid != 0 else 0
                    chatroom.photo = self._db_record_get_string_value(rec, 'ZLINKIMAGEURL')
                    chatroom.member_count = len(self.publicgroup_member_id[self.public_chat[self._db_record_get_int_value(rec, 'ZLINKID')]]) + 1
                    chatroom.deleted = rec.IsDeleted
                    chatroom.type = model_im.CHATROOM_TYPE_NORMAL
                    chatroom.name = self._db_record_get_string_value(rec, 'ZLINKNAME')
                    chatroom.source = self.node.AbsolutePath
                    self.db_insert_table_chatroom(chatroom)
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_message(self):
        '''解析消息数据'''
        mesasgeNode = self.node.Parent.GetByPath('/Message.sqlite')
        try:
            db = SQLiteParser.Database.FromNode(mesasgeNode, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('Message')
            friendchat = [self.zpk2zid[i] for i in self.friendchat]
            secretchat = [self.zpk2zid[i] for i in self.secretchat]
            chatgroup = [self.zpk2zid[i] for i in self.chatgroup]
            publicchat = [self.zpk2zid[i] for i in self.publicchat]
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if self._db_record_get_int_value(rec, 'id') == 0:
                        continue
                    if self._db_record_get_int_value(rec, 'chatId') == 0:
                        continue
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = self.account_id
                    message.talker_id = self._db_record_get_int_value(rec, 'chatId')
                    sender_id = self._db_record_get_string_value(rec, 'userId')
                    message.sender_id = sender_id
                    message.msg_id = self._db_record_get_int_value(rec, 'id')
                    type = self._db_record_get_int_value(rec, 'type')
                    if type == 51:
                        pass
                    encrypt_message = self._db_record_get_string_value(rec, 'message')
                    decrypt_message = self._decrypt(encrypt_message, sender_id)
                    if decrypt_message is None:
                        decrypt_message = ''
                    encrypt_attachment = self._db_record_get_string_value(rec, 'attachment')
                    decrypt_attachment = self._decrypt(encrypt_attachment, sender_id)
                    if decrypt_attachment is None:
                        decrypt_attachment = ''
                    if type == 0:  #未知消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = '<未知消息>' + str(decrypt_message) + '   ' + str(decrypt_attachment)
                    elif type == 1:  #文本
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = decrypt_message
                    elif type == 2:  #图片
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            url = json.loads(decrypt_attachment)['url']
                            media_name = re.sub('http.*com', '', url).replace('/', '_')
                            nodes = self.node.Parent.Search(media_name + '$')
                            if len(list(nodes)) == 0:
                                message.media_path = url
                            else:
                                message.media_path = list(nodes)[0].AbsolutePath
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<图片消息>' + decrypt_attachment
                    elif type == 3:  #视频
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                            url = json.loads(decrypt_attachment)['url']
                            media_name = re.sub('http.*com', '', url).replace('/', '_')
                            nodes = self.node.Parent.Search(media_name + '$')
                            if len(list(nodes)) == 0:
                                message.media_path = url
                            else:
                                message.media_path = list(nodes)[0].AbsolutePath
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<视频消息>' + decrypt_attachment
                    elif type == 5:  #音频
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                            url = json.loads(decrypt_attachment)['url']
                            media_name = re.sub('http.*com', '', url).replace('/', '_')
                            nodes = self.node.Parent.Search(media_name + '$')
                            if len(list(nodes)) == 0:
                                message.media_path = url
                            else:
                                message.media_path = list(nodes)[0].AbsolutePath
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<音频消息>' + decrypt_attachment
                    elif type == 13:  #日历（发起事件）
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            event = json.loads(decrypt_attachment.replace(r'\"', ''))
                            title = event['title']
                            starttime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(event['os'][0]['stAt']))
                            alerttime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(event['os'][0]['alAt']))
                            endtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(event['os'][0]['edAt']))
                            url = event['os'][1]['url']
                            message.content = '事件标题：' +  title + '\n提醒时间：' + starttime + '\n提醒时间：' + alerttime + '\n截止时间：' + endtime + '\n链接：' + url
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<日历消息>' + decrypt_attachment
                    elif type == 14:  #群投票
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            event = json.loads(decrypt_attachment)
                            title = event['os'][0]['tt']
                            option_lists = ' '.join([i['tt'] for i in event['os'][0]['its']])
                            url = event['os'][1]['url']
                            message.content = '投票标题' +  title + '\n选项：' + option_lists + '\n链接：' + url
                        except:
                            traceback.print_exc()
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<群投票>' + decrypt_attachment
                    elif type == 16:  #地理位置
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                            loc = json.loads(decrypt_attachment)
                            lat = loc['lat']
                            lng = loc['lng']
                            addr = loc['a']
                            location = model_im.Location()
                            message.location_id = location.location_id
                            location.latitude = lat
                            location.longitude = lng
                            location.address = addr
                            self.db_insert_table_location(location)
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<地理位置>' + decrypt_message
                    elif type == 17:  #个人资料
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            info = json.loads(decrypt_attachment)
                            headpic = info['fullProfileImageUrl']
                            nickname = info['nickName']
                            userid = info['userId']
                            message.content = '<名片>\n用户id:' + str(userid) + '\n用户名：' + str(nickname) + '\n头像链接：' + str(headpic)
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<个人资料>' + decrypt_attachment
                    elif type == 20:  #表情
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            info = json.loads(decrypt_attachment)
                            emotion = info['path']
                            message.content = '<表情消息>' + emotion
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<表情消息>'
                    elif type == 24:  #群公告
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = decrypt_attachment
                    elif type == 26:  #系统提示消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = '<系统提示消息>' + str(decrypt_message) + '   ' + str(decrypt_attachment)
                    elif type == 51:  #通话消息
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            info = json.loads(decrypt_message)
                            csip = info['csIP']
                            callid = info['callId']
                            duration = info['duration']
                            message.content = '<通话>ip:' + str(csip) + '\n通话标识：' + str(callid) + '\n通话时长：' + str(duration)
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<通话>' + decrypt_attachment
                    elif type == 71:  #系统消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
                        message.content = '<系统消息>' + decrypt_attachment
                    elif type == 96:  #未知消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = '<未知消息>' + str(decrypt_message) + '   ' + str(decrypt_attachment)
                    elif type == 97:  #投票（讨论组）
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            info = json.loads(decrypt_attachment)
                            voteId = info['voteId']
                            title = info['title']
                            option_lists = ' '.join([i['tt'] for i in info['os'][0]['its']])
                            url = info['os'][1]['url']
                            message.content = '投票id' + str(voteId) + '\n投票标题：' +  title + '\n选项：' + option_lists + '\n链接：' + url
                        except:
                            traceback.print_exc()
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = '<投票>' + decrypt_attachment
                    elif type == 98:  #未知消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = '<未知消息>'  + str(decrypt_message) + '   ' + str(decrypt_attachment)
                    message.send_time = self.format_mac_timestamp(self._db_record_get_int_value(rec, 'sentAt'))
                    talker_type = self._db_record_get_int_value(rec, 'chatId')
                    if talker_type in friendchat:
                        message.talker_type = model_im.CHAT_TYPE_FRIEND
                    elif talker_type in secretchat:
                        message.talker_type = model_im.CHAT_TYPE_SECRET
                    elif talker_type in chatgroup:
                        message.talker_type = model_im.CHAT_TYPE_GROUP
                    elif talker_type in publicchat:
                        message.talker_type = model_im.CHAT_TYPE_GROUP
                    message.source = mesasgeNode.AbsolutePath
                    message.deleted = rec.IsDeleted
                    self.db_insert_table_message(message)
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_feed(self):
        '''解析动态数据'''

    def parse_feed_comment(self):
        '''解析评论点赞数据'''

    def analyze_data(self):
        '''分析数据'''
        self.parse_account()
        self.parse_members()
        self.parse_message()
        self.parse_chatroom()


    def _copytocache(self, source):
        sourceDir = source
        targetDir = self.sourceDB
        try:
            if not os.path.exists(targetDir):
                shutil.copytree(sourceDir, targetDir)
        except Exception as e:
            print(e)

    @staticmethod
    def format_mac_timestamp(mac_time, v = 10):
        """
        from mac-timestamp generate unix time stamp
        """
        date = 0
        date_2 = mac_time
        if mac_time < 1000000000:
            date = mac_time + 978307200
        else:
            date = mac_time
            date_2 = date_2 - 978278400 - 8 * 3600
        s_ret = date if v > 5 else date_2
        return int(s_ret)

    @staticmethod
    def _db_record_get_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            return record[column].Value
        return default_value

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

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_blob_value(reader, index, default_value=None):
        if not reader.IsDBNull(index):
            try:
                return bytes(reader.GetValue(index))
            except Exception as e:
                return default_value
        else:
            return default_value

        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _get_timestamp(timestamp):
        try:
            if isinstance(timestamp, (long, float, str, Int64)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
                return timestamp
        except:
            return None

    @staticmethod
    def _makeKeyWithIdentifier(userid):
        xxxxxx1 = 1
        xxxxxx2 = 2
        v3 = userid
        v24 = bytearray('\x00'*24)
        v25 = bytearray('\x00'*24)
        v26 = bytearray('\x00'*24)
        v27 = bytearray('\x00'*24)
        v28 = bytearray('\x00'*24)
        byte_103332478 = bytearray("\x16\x08\x09\x6F\x02\x17\x2B\x08\x21\x21\x0A\x10\x03\x03\x07\x06")
        if (v3):
            v4 = str(userid)
            v5 = v4
            v6 = 0
            while (v6 < 0x10):
                if len(v5) <= v6:
                    break
                v24[v6] = v5[v6]
                v6 = v6 + 1
            v8 = bytearray('\x00'*32)
            v9 = v8
            v10 = v9
            v11 = v8
            v12 = 32
            if (v12):
                v23 = v10
                v13 = v12 + 19            
                if (v12 == 20):
                    v14 = 20
                else:
                    v14 = v12 - 20 * 1
                v26[0:16] = v24[0:16]
                #v11[0:v12] = bytearray('\x00'*v12)
                if (v13 >= 0x14):
                    v15 = 0L
                    v16 = 2;
                    v17 = 0
                    while (v16 > v17):
                        v17 = v15 + 1
                        v18 = 16;
                        v19 = 32
                        while (v19):
                            v19 -= 8
                            v26[v18] = v17 >> v19
                            v18 += 1
                        v28 = bytearray(hmac.new(byte_103332478, v26[0:20], hashlib.sha1).digest())
                        v25[0:20] = v28[0:20]
                        v27 = bytearray(hmac.new(byte_103332478,v28,hashlib.sha1).digest())
                        v20 = 0L
                        v28[0:20] = v27[0:20]                   
                        while (v20 != 20):                    
                            v25[v20] ^= v27[v20]
                            v20 += 1                   
                        if (v16 == v17):
                            v21 = v14
                        else:
                            v21 = 20L
                    
                        for i in range(20*v15,20*v15 + v21):                    
                            v11[i] = v25[i-20*v15]
                        v15 += 1
                        # memcpy(&v11[20 * v15++], v25, v21);
            v10 = v23
        return v10

    @staticmethod
    def _aes_decrypt(key, content):
        iv = b"\x0f\x08\x01\x00\x19\x47\x25\xdc\x15\xf5\x17\xe0\xe1\x15\x0c\x35"
        aes = Aes.Create()
        aes.Mode = CipherMode.CBC
        aes.Padding = PaddingMode.PKCS7
        aes.Key = Convert.FromBase64String(base64.b64encode(key))
        aes.IV = Convert.FromBase64String(base64.b64encode(iv))
        result = None
        try:
            transform = aes.CreateDecryptor(aes.Key, aes.IV)
            memory_stream = MemoryStream(content)
            crypto_stream = CryptoStream(memory_stream, transform, CryptoStreamMode.Read)
            srDecrypt = StreamReader(crypto_stream)
            result = srDecrypt.ReadToEnd()
        except Exception as e:
            print(e)
        return result

    def _decrypt(self, content, userid):
        data = content
        data = Convert.FromBase64String(data)
        key = self._makeKeyWithIdentifier(int(userid))
        x = self._aes_decrypt(key, data)
        return x

def analyze_apple_kakaotalk(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(KakaoTalkParser(node, extractDeleted, extractSource).parse())
    pr.Build('KakaoTalk')
    return pr

def execute(node, extractDeleted):
    return analyze_apple_kakaotalk(node, extractDeleted, False)