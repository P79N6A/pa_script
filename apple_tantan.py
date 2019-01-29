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
from System.Text import Encoding

import re
import hashlib
import shutil
import traceback
import json
import time

VERSION_APP_VALUE = 1
MESSAGE_CONTENT_TYPE_MOMENT = 90


SQL_CREATE_TABLE_MESSAGE = '''
    create table if not exists message(
        account_id TEXT, 
        talker_id TEXT,
        talker_name TEXT,
        sender_id TEXT,
        sender_name TEXT,
        is_sender INT,
        msg_id TEXT, 
        type INT,
        content TEXT,
        media_path TEXT,
        send_time INT,
        location_id INT,
        deal_id INT,
        link_id INT,
        status INT,
        talker_type INT,
        source TEXT,
        deleted INT DEFAULT 0, 
        repeated INT DEFAULT 0,
        moment_id INT)'''

SQL_INSERT_TABLE_MESSAGE = '''
    insert into message(account_id, talker_id, talker_name, sender_id, sender_name, is_sender, msg_id, type, content, 
                        media_path, send_time, location_id, deal_id, link_id, status, talker_type, source, deleted, repeated, moment_id) 
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

class TantanParser(model_im.IM):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node.Parent.Parent.Parent.Parent
        self.messageNode = node.Parent.GetByPath('/Message.db$')
        self.extractDeleted = extract_deleted
        self.db = None
        self.im = model_im.IM()
        self.cachepath = ds.OpenCachePath("Tantan")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = self.node.AbsolutePath
        rdb_name = self.node.PathWithMountPoint
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.recoverDB = self.cachepath + "\\" +md5_rdb.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\TantanSource'
        self.friend = {}
        self.sticker = {}
        self.conversation = {}

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_ACCOUNT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_FRIEND
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_CHATROOM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_CHATROOM_MEMBER
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MESSAGE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_FEED
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_FEED_LIKE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_FEED_COMMENT
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_LOCATION
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_DEAL
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_SEARCH
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_FAVORITE
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_FAVORITE_ITEM
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_LINK
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_BROWSE_HISTORY
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = model_im.SQL_CREATE_TABLE_LOGS
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_message(self, column):
        self.db_insert_table(SQL_INSERT_TABLE_MESSAGE, column.get_values())

    def parse(self):
        if self.need_parse(self.cachedb, VERSION_APP_VALUE):
            try:
                if os.path.exists(self.cachepath):
                    shutil.rmtree(self.cachepath)
                os.mkdir(self.cachepath)
            except:
                pass
            self.db_create(self.cachedb)
            self.analyze_data()
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
        models = []
        models_im = GenerateModel(self.cachedb).get_models()
        models.extend(models_im)
        return models

    def analyze_data(self):
        '''分析数据'''
        nodes = self.node.Search('database.sqlite$')
        if nodes is None:
            return
        for node in nodes:
            self.parse_account(node)
            self.parse_friend(node)
            self.parse_conversation(node)
            self.parse_sticker(node)
            self.parse_message(node)
            self.parse_feed_comment(node)
            self.parse_feed(node)
            # self.parse_feed_member()
            # self.parse_feed()
            # self.parse_search()
    
    def parse_account(self, node):
        '''解析账户数据'''
        try:
            userNode = node
            userDir = userNode.PathWithMountPoint
            userid = re.findall('\d+', userDir)[-1]
            db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(userDir))
            db.Open()
            db_cmd = SQLite.SQLiteCommand(db)
            try:
                if db is None:
                    return
                db_cmd.CommandText = '''select * from User where objectID = {}'''.format(userid)
                sr = db_cmd.ExecuteReader()
                while(sr.Read()):
                    create_time = self._get_timestamp(self._db_reader_get_double_value(sr, 0))
                    age = self._db_reader_get_int_value(sr, 3)
                    signature = self._db_reader_get_string_value(sr, 9)
                    gender = self._db_reader_get_int_value(sr, 11)
                    name = self._db_reader_get_string_value(sr, 12)
                    if not IsDBNull(sr[2]):
                        tree = BPReader.GetTree(MemoryRange.FromBytes(sr.GetValue(2)))
                        if tree is None:
                            break
                        bplist_dic = self.bplist2json(tree)
                        phone_number = ''
                        country = ''
                        city = ''
                        district = ''
                        headpic = ''
                        if 'settingsDictionary' in bplist_dic:
                            settingsDictionary = bplist_dic['settingsDictionary']
                            if 'phoneNumber' in settingsDictionary:
                                phoneNumber = settingsDictionary['phoneNumber']
                                if 'number' in phoneNumber:
                                    phone_number = phoneNumber['number']
                        if 'locationDictionary' in bplist_dic:
                            locationDictionary =  bplist_dic['locationDictionary']
                            if 'region' in locationDictionary:
                                region = locationDictionary['region']
                                if 'country' in region:
                                    country = region['country']
                                if 'city' in region:
                                    city = region['city']
                                if 'district' in region:
                                    district = region['district']
                        if 'pictureDictionaries' in bplist_dic:
                            pictureDictionaries = bplist_dic['pictureDictionaries']
                            if 'key' in pictureDictionaries:
                                key = pictureDictionaries['key']
                                if 'url' in key:
                                    headpic = key['url']
                    account = model_im.Account()
                    account.account_id = userid
                    account.nickname = name
                    account.username = name
                    account.telephone = phone_number
                    account.photo = headpic
                    account.gender = 2 if gender == 1 else 1 if gender == 2 else 0
                    account.age = age
                    account.country = country
                    account.city = city
                    account.address = country + city + district
                    account.signature = signature
                    self.db_insert_table_account(account)
            except:
                traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def bplist2json(self, value):
        if str(type(value)) != "<type 'Dictionary[str, IKNode]'>" and str(type(value)) != "<type 'BPObjectTreeNode'>" and str(type(value)) != "<type 'List[IKNode]'>":
            return value
        d = {}
        for c, j in enumerate(value):
            key = 'key' if j.Key is None else j.Key
            if key == 'attachments':
                pass
            v = '' if j.Value is None else j.Value
            v = self.bplist2json(v)
            d[str(key)] = v
        if d == {}:
            if str(type(value)) == "<type 'List[IKNode]'>":
                if len(value) == 0:
                    return ''
                else:
                    return self.bplist2json(value.Value)
            if str(type(value)) == "<type 'BPObjectTreeNode'>":
                return self.bplist2json(value.Value)
            return str(value.Value)
        return d

    def parse_friend(self, node):
        '''解析好友数据'''
        try:
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('User')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    if IsDBNull(rec['objectID'].Value) or rec['objectID'].Value == 0:
                        continue
                    if not IsDBNull(rec['resource'].Value):
                        tree = BPReader.GetTree(MemoryRange.FromBytes(rec['resource'].Value))
                        if tree is None:
                            break
                        bplist_dic = self.bplist2json(tree)
                        phone_number = ''
                        country = ''
                        city = ''
                        district = ''
                        headpic = ''
                        if 'settingsDictionary' in bplist_dic:
                            settingsDictionary = bplist_dic['settingsDictionary']
                            if 'phoneNumber' in settingsDictionary:
                                phoneNumber = settingsDictionary['phoneNumber']
                                if 'number' in phoneNumber:
                                    phone_number = phoneNumber['number']
                        if 'locationDictionary' in bplist_dic:
                            locationDictionary =  bplist_dic['locationDictionary']
                            if 'region' in locationDictionary:
                                region = locationDictionary['region']
                                if 'country' in region:
                                    country = region['country']
                                if 'city' in region:
                                    city = region['city']
                                if 'district' in region:
                                    district = region['district']
                        if 'pictureDictionaries' in bplist_dic:
                            pictureDictionaries = bplist_dic['pictureDictionaries']
                            if 'key' in pictureDictionaries:
                                key = pictureDictionaries['key']
                                if 'url' in key:
                                    headpic = key['url']
                    friend = model_im.Friend()
                    userDir = node.PathWithMountPoint
                    userid = re.findall('\d+', userDir)[-1]
                    friend.account_id = userid
                    friend.friend_id = self._db_record_get_int_value(rec, 'objectID')
                    if friend.friend_id == 0:
                        continue
                    friend.nickname = self._db_record_get_string_value(rec, 'name')
                    if friend.friend_id not in self.friend.keys():
                        self.friend[friend.friend_id] = friend.nickname
                    friend.photo = headpic
                    friend.fullname = friend.nickname
                    friend.telephone = phone_number
                    friend.age = self._db_record_get_int_value(rec, 'age')
                    gender = self._db_record_get_int_value(rec, 'gender')
                    friend.gender = 2 if gender == 1 else 1 if gender == 2 else 0
                    friend.signature = self._db_record_get_string_value(rec, 'userDescription')
                    friend.deleted = rec.IsDeleted
                    self.db_insert_table_friend(friend)
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_conversation(self, node):
        '''解析会话数据'''
        userid = re.findall('\d+', node.PathWithMountPoint)[-1]
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        try:
            ts = SQLiteParser.TableSignature('Conversation')
            self.conversation = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if self._db_record_get_string_value(rec, 'objectID') == '':
                        continue
                    pk = self._db_record_get_string_value(rec, 'primaryKeyID')
                    cnt = self._db_record_get_blob_value(rec, 'otherUser_primaryKeyID')
                    if pk not in self.conversation.keys():
                        self.conversation[pk] = cnt
                except:
                    pass
        except:
            pass

    def parse_message(self, node):
        '''解析消息数据'''
        try:
            db = SQLiteParser.Database.FromNode(node, canceller)
            userDir = node.PathWithMountPoint
            userid = re.findall('\d+', userDir)[-1]
            if db is None:
                return
            ts = SQLiteParser.TableSignature('Message')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    message = Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = userid
                    message.talker_id = self._db_record_get_int_value(rec, 'conversationID')
                    otherUserId = self.conversation[message.talker_id] if message.talker_id in self.conversation else message.talker_id
                    message.talker_name = self.friend[otherUserId] if otherUserId in self.friend else otherUserId
                    message.sender_id = self._db_record_get_int_value(rec, 'owner_primaryKeyID')
                    message.sender_name = self.friend[message.sender_id] if message.sender_id in self.friend else message.sender_id
                    message.msg_id = self._db_record_get_string_value(rec, 'objectID')
                    content_type = self._db_record_get_int_value(rec, 'type')  #0:文本消息1:图片2:视频3：语音消息4:表情包5:私密真心话（文本消息）6:地理位置系统消息
                    if content_type == 0:  #文本消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 1:  #图片消息
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            local_dic = self._db_record_get_blob_value(rec, 'pendingMediaDictionary')
                            tree = BPReader.GetTree(MemoryRange.FromBytes(rec['mediaDictionaries'].Value))
                            if tree is None:
                                break
                            image_json = self.bplist2json(tree)
                            if local_dic is not None:
                                local_info = json.loads(local_dic)
                                media_name = local_info['CacheImagePathKey'] if 'CacheImagePathKey' in local_info else ''
                                media_nodes = self.node.Search(media_name +  '$')
                                if len(list(media_nodes)) != 0:
                                    message.media_path = list(media_nodes)[0].AbsolutePath
                                else:
                                    message.media_path = image_json['key']['url']
                            else:
                                message.media_path = image_json['key']['url']
                        except:
                            traceback.print_exc()
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 2:  #视频
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                            local_dic = self._db_record_get_blob_value(rec, 'mediaDatas')
                            tree = BPReader.GetTree(MemoryRange.FromBytes(rec['mediaDictionaries'].Value))
                            if tree is None:
                                break
                            video_json = self.bplist2json(tree)
                            print(video_json)
                            if local_dic is not None:
                                local_info = json.loads(local_dic)
                                media_name = local_info['url'] if 'url' in local_info else ''
                                media_nodes = self.node.Search(media_name +  '$')
                                if len(list(media_nodes)) != 0:
                                    message.media_path = list(media_nodes)[0].AbsolutePath
                                else:
                                    message.media_path = audio_json['key']['url']
                            else:
                                message.media_path = audio_json['key']['url']
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 3:  #语音
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                            local_dic = self._db_record_get_blob_value(rec, 'pendingMediaDictionary')
                            tree = BPReader.GetTree(MemoryRange.FromBytes(rec['mediaDictionaries'].Value))
                            if tree is None:
                                break
                            audio_json = self.bplist2json(tree)
                            if local_dic is not None:
                                local_info = json.loads(local_dic)
                                media_name = local_info['url'] if 'url' in local_info else ''
                                media_type = local_info['mediaType'] if 'mediaType' in local_info else ''
                                duration = local_info['duration'] if 'duration' in local_info else ''
                                media_nodes = self.node.Search(media_name +  '$')
                                if len(list(media_nodes)) != 0:
                                    message.media_path = list(media_nodes)[0].AbsolutePath
                                else:
                                    message.media_path = audio_json['key']['url']
                            else:
                                message.media_path = audio_json['key']['url']
                        except:
                            traceback.print_exc()
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 4:  #表情包
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_EMOJI
                            sticker_pk = self._db_record_get_string_value(rec, 'sticker_primaryKeyID')
                            sticker_cnt = self.sticker[sticker_pk]
                            sticker_cnt = json.loads(sticker_cnt)
                            message.media_path = sticker_cnt['url']
                        except:
                            traceback.print_exc()
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 5:  #私密真心话
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 6:  #地理位置
                        try:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                            location = model_im.Location()
                            location_info = self._db_record_get_blob_value(rec, 'locationDictionary')
                            location_info = json.loads(location_info)
                            message.location_id = location.location_id
                            location.latitude = location_info['coordinates'][0]
                            location.longitude = location_info['coordinates'][1]
                            location.address = location_info['address'] + location_info['name']
                            self.db_insert_table_location(location)
                        except:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 7:  #撤回消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = self._db_record_get_string_value(rec, 'value')
                    elif content_type == 9:  #转发动态
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.moment_id = self._db_record_get_int_value(rec, 'momentID')
                        message.content = "[转发动态]" + str(message.moment_id)
                    message.send_time = self._get_timestamp(self._db_record_get_int_value(rec, 'createdTime'))
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.deleted = rec.IsDeleted
                    if message.talker_id != 0:
                         self.db_insert_table_message(message)
                except:
                    pass
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_sticker(self, node):
        '''解析表情数据'''
        userid = re.findall('\d+', node.PathWithMountPoint)[-1]
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        try:
            ts = SQLiteParser.TableSignature('Sticker')
            self.sticker = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if self._db_record_get_string_value(rec, 'objectID') == '':
                        continue
                    pk = self._db_record_get_string_value(rec, 'primaryKeyID')
                    cnt = self._db_record_get_blob_value(rec, 'pictureDictionary')
                    if pk not in self.sticker.keys():
                        self.sticker[pk] = cnt
                except:
                    pass
        except:
            pass

    def parse_feed(self, node):
        '''解析动态数据'''
        userid = re.findall('\d+', node.PathWithMountPoint)[-1]
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        try:
            ts = SQLiteParser.TableSignature('Moment')
            self.sticker = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if self._db_record_get_string_value(rec, 'objectID') == '':
                        continue
                    feed = model_im.Feed()
                    #多媒体数据
                    media_url = ''
                    tree = BPReader.GetTree(MemoryRange.FromBytes(rec['mediaDictionaries'].Value))
                    if tree is None:
                        break
                    media_json = self.bplist2json(tree)
                    if media_json is not None:
                        media_url = media_json['key']['url']
                    #获赞数
                    likescount = self._db_record_get_int_value(rec, 'likesCount')
                    #评论数
                    commentcount = self._db_record_get_int_value(rec, 'commentsCount')
                    #动态id
                    feedpk = self._db_record_get_int_value(rec, 'primaryKeyID')
                    #动态时间
                    createtime = self._db_record_get_int_value(rec, 'createdTime')
                    #动态位置
                    location = self._db_record_get_blob_value(rec, 'locationDictionary')
                    if location is not None:
                        location = json.loads(location)
                    #location = Encoding.UTF8.GetString(rec['locationDictionary'].Value) if not IsDBNull(rec['locationDictionary'].Value) else None
                    if location is not None:
                        try:
                            coordinates = location['coordinates']
                        except:
                            coordinates = ''
                        latitude = 0
                        longitude = 0
                        address = ''
                        if coordinates is not '':
                            latitude = coordinates[0]
                            longitude = coordinates[1]
                            address = location['address'] + location['name']
                        location = model_im.Location()
                        feed.location_id = location.location_id  # 地址ID[INT]
                        location.latitude = latitude
                        location.longitude = longitude
                        location.address = address
                        self.db_insert_table_location(location)
                    #发送者
                    senderid = self._db_record_get_int_value(rec, 'owner_primaryKeyID')
                    if senderid == -1:
                        senderid = -2
                    #是否给该条动态点赞
                    isliked = self._db_record_get_int_value(rec, 'haveLiked')
                    #动态文本
                    content = self._db_record_get_string_value(rec, 'value')
                    feed.account_id = userid  # 账号ID[TEXT]
                    feed.sender_id = senderid  # 发布者ID[TEXT]
                    feed.content = content  # 文本[TEXT]
                    if re.findall("image", media_url):
                        feed.image_path = media_url
                    elif re.findall("video", media_url):
                        feed.video_path = media_url
                    feed.send_time = createtime  # 发布时间[INT]
                    feed.likecount = likescount  # 赞数量[INT]
                    feed.commentcount = commentcount  # 评论数量[INT]
                    feed.comment_id = feedpk
                    feed.deleted = rec.IsDeleted
                    self.db_insert_table_feed(feed)
                except:
                    pass
            self.db_commit()
        except:
            pass
        
    def parse_feed_comment(self, node):
        '''解析评论数据'''
        userid = re.findall('\d+', node.PathWithMountPoint)[-1]
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        try:
            ts = SQLiteParser.TableSignature('comment')
            self.comment = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if self._db_record_get_string_value(rec, 'objectID') == '':
                        continue
                    pk = self._db_record_get_string_value(rec, 'momentID')
                    cnt = self._db_record_get_blob_value(rec, 'primaryKeyID')
                    if pk not in self.sticker.keys():
                        self.comment[pk] = [cnt]
                    else:
                        if cnt not in self.comment[pk]:
                            self.comment[pk].append(cnt)
                    comments = model_im.FeedComment()
                    comments.comment_id = self._db_record_get_string_value(rec, 'momentID')  # 评论ID[INT]
                    comments.sender_id = self._db_record_get_string_value(rec, 'owner_primaryKeyID')  # 发布者ID[TEXT]
                    if comments.sender_id == '-1':
                        comments.sender_id = -2
                    comments.sender_name = self.friend[comments.sender_id] if comments.sender_id in self.friend else comments.sender_id  # 发布者昵称[TEXT]
                    comments.content = self._db_record_get_string_value(rec, 'value')  # 评论内容[TEXT]
                    comments.create_time = self._get_timestamp(self._db_record_get_int_value(rec, 'createdTime'))  # 发布时间[INT]
                    comments.deleted = rec.IsDeleted
                    self.db_insert_table_feed_comment(comments)
                except:
                    pass
            self.db_commit()
        except:
            pass

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
        if not IsDBNull(record[column].Value):
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
                return Encoding.UTF8.GetString(value)
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
    def _db_reader_get_double_value(reader, index, default_value=0.0):
        return reader.GetDouble(index) if not reader.IsDBNull(index) else default_value

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
            else:
                return None
        except:
            return None

class Message(model_im.Message):
    def __init__(self):
        super(Message, self).__init__()
        self.moment_id = None

    def get_values(self):
        a = super(Message, self).get_values() + (self.moment_id,)
        return super(Message, self).get_values() + (self.moment_id,)

class GenerateModel(model_im.GenerateModel):
    def _get_feed_comments(self, account_id, comment_id, deleted):
        models = []
        sql = '''select distinct sender_id, sender_name, ref_user_id, ref_user_name, content, create_time, source, deleted, repeated
                 from feed_comment
                 where comment_id={}'''.format(comment_id)
        try:
            cmd = self.db.CreateCommand()
            cmd.CommandText = sql
            r = cmd.ExecuteReader()
            while r.Read():
                if canceller.IsCancellationRequested:
                    break
                try:
                    sender_id = self._db_reader_get_string_value(r, 0)
                    sender_name = self._db_reader_get_string_value(r, 1, None)
                    ref_user_id = self._db_reader_get_string_value(r, 2, None)
                    ref_user_name = self._db_reader_get_string_value(r, 3, None)
                    timestamp = self._db_reader_get_int_value(r, 5, None)
                    source = self._db_reader_get_string_value(r, 6)
                    deleted = self._db_reader_get_int_value(r, 7, None)


                    comment = Common.MomentComment()
                    if sender_id not in [None, '']:
                        comment.Sender.Value = self._get_user_intro(account_id, sender_id, sender_name)
                    if ref_user_id not in [None, '']:
                        comment.Receiver.Value = self._get_user_intro(account_id, ref_user_id, ref_user_name)
                    comment.Content.Value = self._db_reader_get_string_value(r, 4)
                    if timestamp:
                        ts = self._get_timestamp(timestamp)
                        if ts:
                            comment.TimeStamp.Value = ts
                    comment.SourceFile.Value = source
                    comment.Deleted = self._convert_deleted_status(deleted)
                    models.append(comment)
                except Exception as e:
                    if deleted == 0:
                        TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        except Exception as e:
            if deleted == 0:
                TraceService.Trace(TraceLevel.Error, "model_im.py Error: LINE {}".format(traceback.format_exc()))
        return models

def analyze_apple_tantan(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(TantanParser(node, extractDeleted, extractSource).parse())
    pr.Build('探探')
    return pr

def execute(node, extractDeleted):
    return analyze_apple_tantan(node, extractDeleted, False)