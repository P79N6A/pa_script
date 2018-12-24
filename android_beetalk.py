# _*_ coding:utf-8 _*_

__author__ = "xiaoyuge"

from PA_runtime import *
import clr
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('model_callrecord')
except:
    pass
del clr

from PA.Common.Utilities.Types import TimeStampFormats

import System.Data.SQLite as SQLite
import model_im
import model_callrecord

import re
import os
import hashlib
import shutil
import traceback

VERSION_APP_VALUE = 4

class BeeTalkParser(model_im.IM, model_callrecord.MC):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node
        self.extractDeleted = extract_deleted
        self.db = None
        self.im = model_im.IM()
        self.mc = model_callrecord.MC()
        self.cachepath = ds.OpenCachePath("BeeTalk")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = self.node.AbsolutePath
        rdb_name = self.node.PathWithMountPoint
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.recoverDB = self.cachepath + "\\" +md5_rdb.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\BeetalkSourceDB'
        self.cacheNode = self.node.Parent.Parent
        self.account_id = None
        self.account_name = '未知用户'

    def db_create_table(self):
        model_im.IM.db_create_table(self)
        model_callrecord.MC.db_create_table(self)

    def parse(self):
        if self.need_parse(self.cachedb, VERSION_APP_VALUE):
            if os.path.exists(self.cachepath):
                shutil.rmtree(self.cachepath)
            os.mkdir(self.cachepath)
            self.db_create(self.cachedb)
            self.analyze_data()
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_commit()
            self.db_close()
        models = []
        models_im = model_im.GenerateModel(self.cachedb).get_models()
        models.extend(models_im)
        print(len(models))
        models_record = model_callrecord.Generate(self.cachedb).get_models()
        models.extend(models_record)
        return models
    
    def parse_account(self, dbPath):
        '''解析账户数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select a.user_id, a.account_name, b.name, b.gender, 
                b.birthday, b.signature, a.deleted from bb_account_info as a 
                left join bb_user_info as b on a.user_id = b.userid'''
            sr = db_cmd.ExecuteReader()
            try:
                if not sr.HasRows:
                    account = model_im.Account()
                    account.account_id = re.sub('\D', '', os.path.basename(self.node.AbsolutePath))
                    account.username = '未知用户名'
                    account.source = self.node.AbsolutePath
                    account.deleted = 0
                    self.account_id = account.account_id
                    self.account_name = account.username
                    self.db_insert_table_account(account)
                while(sr.Read()):
                    account = model_im.Account()
                    account.account_id = sr.GetInt64(0) if not sr.IsDBNull(0) else ''
                    account.telephone = self._db_reader_get_string_value(sr, 1)
                    account.username = sr.GetString(2) if not sr.IsDBNull(3) else '未知用户'
                    account.birthday = self._db_reader_get_int_value(sr, 4)
                    account.gender = 1 if self._db_reader_get_int_value(sr, 3) == 0 else 0
                    account.signature = self._db_reader_get_string_value(sr, 5)
                    account.source = self.node.AbsolutePath
                    account.deleted = self._db_reader_get_int_value(sr, 6)
                    self.account_id = account.account_id
                    self.account_name = account.username
                    self.db_insert_table_account(account)
            except:
                traceback.print_exc()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_friend(self, dbPath):
        '''解析好友（联系人）数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select a.userid, a.birthday, a.customized_id, a.gender, 
                a.name, a.signature, b.id, a.deleted from bb_user_info as a 
                left join bb_buddy_id_info as b on a.userid = b.userid'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    friend.account_id = self.account_id
                    friend.deleted = self._db_reader_get_int_value(sr, 7)
                    friend.friend_id = self._db_reader_get_int_value(sr, 0)
                    friend.birthday = self._db_reader_get_int_value(sr, 1)
                    friend.nickname = self._db_reader_get_string_value(sr, 4)
                    friend.fullname = self._db_reader_get_string_value(sr, 4)
                    friend.source = self.node.AbsolutePath
                    friend.gender = 1 if self._db_reader_get_int_value(sr, 3) == 0 else 0
                    friend.signature = self._db_reader_get_string_value(sr, 5)
                    friend.type = model_im.FRIEND_TYPE_FRIEND if not IsDBNull(sr[6]) else model_im.FRIEND_TYPE_NONE
                    self.db_insert_table_friend(friend)
                except:
                    traceback.print_exc()
            sr.Close()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_chatroom(self, dbPath):
        '''解析群组数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select a.discussionid, a.title, a.memberver, count(b.memberid) as member_count, a.deleted from bb_discussion_info as a 
                left join bb_discussion_member_info as b on a.discussionid = b.discussionid group by a.discussionid'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = self.account_id
                    chatroom.chatroom_id = self._db_reader_get_int_value(sr, 0)
                    chatroom.create_time = self._db_reader_get_int_value(sr, 2)
                    chatroom.deleted = self._db_reader_get_int_value(sr, 4)
                    chatroom.member_count = self._db_reader_get_int_value(sr, 3)
                    chatroom.name = self._db_reader_get_string_value(sr, 1)
                    chatroom.source = self.node.AbsolutePath
                    self.db_insert_table_chatroom(chatroom)
                except:
                    traceback.print_exc()
            sr.Close()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_chatroom_member(self, dbPath):
        '''解析群组成员数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if self.db is None:
                return
            db_cmd.CommandText = '''select a.discussionid, a.memberid, b.birthday, b.gender, b.name, b.signature, a.deleted from bb_discussion_member_info as a 
                left join bb_user_info as b on a.memberid = b.userid'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom_member = model_im.ChatroomMember()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom_member.account_id = self.account_id
                    chatroom_member.chatroom_id = self._db_reader_get_int_value(sr, 0)
                    chatroom_member.deleted = self._db_reader_get_int_value(sr, 6)
                    chatroom_member.display_name = self._db_reader_get_string_value(sr, 4)
                    chatroom_member.member_id = self._db_reader_get_int_value(sr, 1)
                    chatroom_member.source = self.node.AbsolutePath
                    chatroom_member.birthday = self._db_reader_get_int_value(sr, 2)
                    chatroom_member.gender = self._db_reader_get_int_value(sr, 3)
                    chatroom_member.signature = self._db_reader_get_string_value(sr, 5)
                    self.db_insert_table_chatroom_member(chatroom_member)
                except:
                    traceback.print_exc()
            sr.Close()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_message(self, dbPath):
        '''解析消息数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select a.content, a.msgid, a.fromId, a.metatag, a.timestamp, a.userid, b.name, a.deleted from bb_chat_msg_info as a 
                left join bb_user_info as b on a.userid = b.userid'''
            sr = db_cmd.ExecuteReader()
            record_id = 0
            while (sr.Read()):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = self.account_id
                    message.deleted =  self._db_reader_get_int_value(sr, 7)
                    message.msg_id = self._db_reader_get_int_value(sr, 1)
                    message.is_sender = 1 if self._db_reader_get_int_value(sr, 2) == 0 else 0
                    message.send_time = self._db_reader_get_int_value(sr, 4)
                    message.sender_id = self._db_reader_get_int_value(sr, 5)
                    message.sender_name = self._db_reader_get_string_value(sr, 6)
                    message.source = self.node.AbsolutePath
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.talker_id = self._db_reader_get_int_value(sr, 5)
                    message.talker_name = self._db_reader_get_string_value(sr, 6)
                    metatag = self._db_reader_get_string_value(sr, 3)
                    if re.match('text', metatag) or re.match('vcinvite', metatag):
                        if not IsDBNull(sr[0]):
                            message.content = str(self._db_reader_get_blob_value(sr, 0))[4::].decode('utf-8')
                        else:
                            message.content = ''
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif re.match('v1.', metatag):
                        if not IsDBNull(sr[0]):
                            message.content = str(self._db_reader_get_blob_value(sr, 0))[:-2:].decode('utf-8')
                        else:
                            message.content = ''
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif re.match('sticker', metatag):
                        if not IsDBNull(sr[0]):
                            message.content = '表情' + str(self._db_reader_get_blob_value(sr, 0))[1::].decode('utf-8')
                        else:
                            message.content = ''
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif re.match('img', metatag):
                        img_hex = str(self._db_reader_get_blob_value(sr, 0)).encode('hex')
                        data = []  #data用于存储格式化的字节信息
                        for i, item in enumerate(img_hex):  #生成data列表
                            if i%2==0:
                                data.append(img_hex[i]+img_hex[i+1])
                            else:
                                pass
                        for i, d in enumerate(data):
                            if d == '12':
                                img_start = i+2
                                img_lens = int(data[i+1], 16)
                                img_end = img_start + img_lens
                                img_name = img_hex[img_start*2: img_end*2: ].decode('hex').decode('utf-8')
                                break
                        nodes = self.cacheNode.Search('/' + img_name + '.*\..*$')
                        if len(list(nodes)) == 0:
                            message.content = '<图片消息>:' + img_name
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        for node in nodes:
                            message.media_path = node.AbsolutePath
                            message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            break
                    elif re.match('vn', metatag):
                        vn_name = str(self._db_reader_get_blob_value(sr, 0))[2:34:].decode('utf-8')
                        nodes = self.cacheNode.Search('/' + vn_name + '.*\..*$')
                        if len(list(nodes)) == 0:
                            message.content = '<音频消息>:' + vn_name
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        for node in nodes:
                            message.media_path = node.AbsolutePath
                            message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                            break
                    elif re.match('loc', metatag):
                        message.content = '<地理位置消息>：' + str(self._db_reader_get_blob_value(sr, 0))[2:-10:].decode('utf-8')
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif re.match('vcall', metatag):
                        record_id += 1
                        local_number = self.account_name
                        counter_number = self._db_reader_get_string_value(sr, 6)
                        record = model_callrecord.Records()
                        record.id = record_id
                        record.local_number = local_number
                        record.phone_number = counter_number
                        call_infos = str(self._db_reader_get_blob_value(sr, 0)).decode('utf-8').replace('"', '').replace(':', ',').replace('{', '').replace('}', '').split(',')
                        for i,call_info in enumerate(call_infos):
                            if call_info == 'duration':
                                record.duration = call_infos[i+1]
                                break
                        if record.duration == 0:
                            record.type = 3
                        elif metatag == 'vcall.r':
                            record.type = 1
                        elif metatag == 'vcall':
                            record.type = 2
                        else:
                            record.type = 0
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = '<通话消息> 通话时长:' + record.duration + '秒'
                        record.date = message.send_time if message.send_time is not None else 0
                        record.deleted = self._db_reader_get_int_value(sr, 7)
                        record.source = self.node.AbsolutePath
                        self.db_insert_table_call_records(record)
                    self.db_insert_table_message(message)
                except:
                    traceback.print_exc()
            sr.Close()
            db_cmd.CommandText = '''select a.content, a.msgid, a.userId, a.metatag, a.timestamp, a.userid, b.name, a.discussionid, c.title, a.deleted
                from bb_discussion_chat_msg_info as a left join bb_user_info as b on a.userId = b.userid
                left join bb_discussion_info as c on a.discussionid = c.discussionid'''
            sr = db_cmd.ExecuteReader()
            record_id = 0
            while (sr.Read()):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = self.account_id
                    message.deleted =  self._db_reader_get_int_value(sr, 9)
                    message.msg_id = self._db_reader_get_int_value(sr, 1)
                    message.is_sender = 1 if self._db_reader_get_int_value(sr, 2) == 0 else 0
                    message.send_time = self._db_reader_get_int_value(sr, 4)
                    message.sender_id = self._db_reader_get_int_value(sr, 5)
                    message.sender_name = self._db_reader_get_string_value(sr, 6)
                    message.source = self.node.AbsolutePath
                    message.talker_type = model_im.CHAT_TYPE_GROUP
                    message.talker_id = self._db_reader_get_int_value(sr, 7)
                    message.talker_name = self._db_reader_get_string_value(sr, 8)
                    metatag = self._db_reader_get_string_value(sr, 3)
                    if re.match('text', metatag):
                        message.content = str(self._db_reader_get_blob_value(sr, 0))[10::].decode('utf-8')
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif re.match('img', metatag):
                        data = str(self._db_reader_get_blob_value(sr, 0))
                        thumb_start = int(data.encode('hex')[0:2:], 16)
                        thumb_lens = int(str(data).encode('hex')[(thumb_start-2)*2:(thumb_start-1)*2:], 16)
                        img_start = (thumb_start+thumb_lens+1)*2
                        img_lens = int(str(data).encode('hex')[(thumb_start+thumb_lens)*2:(thumb_start+thumb_lens+1)*2:], 16)
                        img_end = img_start+img_lens*2
                        img_name = str(data).encode('hex')[img_start:img_end:].decode('hex').decode('utf-8')
                        nodes = self.cacheNode.Search('/' + img_name + '.*\..*$')
                        if len(list(nodes)) == 0:
                            message.content = '<图片消息>:' + img_name
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        for node in nodes:
                            message.media_path = node.AbsolutePath
                            message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            break
                    self.db_insert_table_message(message)
                except Exception as e:
                    traceback.print_exc(e)
            sr.Close()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_feed(self, dbPath):
        '''解析动态数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if self.db is None:
                return
            db_cmd.CommandText = '''select a.item_id, a.mediaBytesInfo, a.timestamp, a.user_id, 
                group_concat(b."action") as "action", group_concat(b.comment) as comment, 
                group_concat(b.timestamp) as comment_time, group_concat(b.user_id) as comment_user_id, group_concat(b.commend_id) as comment_id, a.deleted
                from bb_dl_item_info as a left join bb_dl_comment_info as b on a.item_id = b.item_id group by a.item_id'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    feed = model_im.Feed()
                    if canceller.IsCancellationRequested:
                        break
                    feed.account_id = self.account_id
                    feed.sender_id = self._db_reader_get_int_value(sr, 3)
                    feed.send_time = self._db_reader_get_int_value(sr, 2)
                    location_value = ''  #location_value用于保存提取出的位置信息的值
                    img_name = []  #img_name列表用于保存图片名称
                    text_content = ''  #text_content用于保存提取出的正文内容
                    try:
                        value = str(self._db_reader_get_blob_value(sr, 1)).encode('hex')
                        data = []  #data用于存储格式化的字节信息
                        for i, item in enumerate(value):  #生成data列表
                            if i%2==0:
                                data.append(value[i]+value[i+1])
                            else:
                                pass
                        for i, d in enumerate(data):  #判断与获取位置信息
                            if d == '42' and data[i+1] == '4a':
                                location_start = i+3
                                location_lens = int(data[i+2], 16)
                                location_end = location_start + location_lens
                                location_value = value[location_start*2:location_end*2:].decode('hex').decode('utf-8')
                            if d == '1a' and data[i + 2] == '0a':
                                img_start = i + 4
                                img_lens = int(data[i + 3], 16)
                                img_end = img_start + img_lens
                                img_name.append(value[img_start*2: img_end*2: ].decode('hex').decode('utf-8'))
                            if d == '22':
                                text_start = i + 2
                                text_lens = int(data[i + 1], 16)
                                text_end = text_start + text_lens
                                text_content = value[text_start*2: text_end*2: ].decode('hex').decode('utf-8')
                        if location_value is '':
                            location_value = '无位置信息'
                        location = model_im.Location()
                        feed.location_id = location.location_id
                        location.address = location_value
                        self.db_insert_table_location(location)
                        feed.content = text_content
                        image_name = []
                        for image in img_name:
                            nodes = self.cacheNode.Search('/' + image + '.*\..*$')
                            for node in nodes:
                                image_name.append(node.AbsolutePath)
                        feed.image_path = ','.join(image_name)
                    except:
                        pass
                    actions = self._db_reader_get_string_value(sr, 4).split(',')
                    comment_id = self._db_reader_get_string_value(sr, 8).split(',')
                    likes = []
                    comments = []
                    like_count = 0
                    comment_count = 0
                    for i, action in enumerate(actions):
                        if action == 'like':
                            like_count += 1
                            likes.append(comment_id[i])
                        elif not IsDBNull(action):
                            comment_count += 1
                            comments.append(comment_id[i])
                    feed.likes = ','.join(likes)
                    feed.likecount = like_count
                    feed.comments = ','.join(comment_id)
                    feed.commentcount = comment_count
                    feed.deleted = self._db_reader_get_int_value(sr, 9)
                    if feed.content is not None and feed.image_path is not None and not (text_content == '' and location_value == '无位置信息'):
                        self.db_insert_table_feed(feed)
                except:
                    traceback.print_exc()
            sr.Close()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_feed_comment(self, dbPath):
        '''解析评论点赞数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if self.db is None:
                return
            db_cmd.CommandText = '''select action, comment, commend_id, item_id, timestamp, user_id, deleted from bb_dl_comment_info'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    feed_like = model_im.FeedLike()
                    feed_comment = model_im.FeedComment()
                    if canceller.IsCancellationRequested:
                        break
                    if sr[0] == 'like':
                        feed_like.like_id = self._db_reader_get_int_value(sr, 2)
                        feed_like.sender_id = self._db_reader_get_int_value(sr, 5)
                        feed_like.sender_name = 'BeeTalkUser'
                        feed_like.create_time = self._db_reader_get_int_value(sr, 4)
                        feed_like.deleted = self._db_reader_get_int_value(sr, 6)
                        feed_like.source = self.node.AbsolutePath
                        self.db_insert_table_feed_like(feed_like)
                    elif not IsDBNull(sr[4]):
                        feed_comment.comment_id = self._db_reader_get_int_value(sr, 2)
                        feed_comment.sender_id = self._db_reader_get_int_value(sr, 5)
                        feed_comment.sender_name = 'BeeTalkUser'
                        feed_comment.content = self._db_reader_get_string_value(sr, 1)
                        feed_comment.create_time = self._db_reader_get_int_value(sr, 4)
                        feed_comment.deleted = self._db_reader_get_int_value(sr, 6)
                        feed_comment.source = self.node.AbsolutePath
                        self.db_insert_table_feed_comment(feed_comment)
                except:
                    traceback.print_exc()
            sr.Close()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def analyze_data(self):
        '''分析数据'''
        self.read_deleted_records()
        db_path = self.recoverDB
        if db_path is not None:
            self.parse_account(db_path)
            self.parse_friend(db_path)
            self.parse_chatroom(db_path)
            self.parse_chatroom_member(db_path)
            self.parse_message(db_path)
            self.parse_feed(db_path)
            self.parse_feed_comment(db_path)

    def read_deleted_records(self):
        '''获取删除数据保存至删除数据库'''
        self.create_deleted_db()
        
        self.rdb = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.rdb.Open()
        self.rdb_cmd = SQLite.SQLiteCommand(self.rdb)

        dbid = re.findall('\d+', os.path.basename(self.node.AbsolutePath))[0]
        fs = self.node.FileSystem
        nodes = fs.Search('buzz_' + dbid + '.db$')
        for node in nodes:
            self.fnode = node
            break
        
        self.rdb_trans = self.rdb.BeginTransaction()
        self.read_deleted_table_account()
        self.read_deleted_table_contact()
        self.read_deleted_table_group_user_id()
        self.read_deleted_table_group()
        self.read_deleted_table_group_user()
        self.read_deleted_table_message()
        self.read_deleted_table_group_chat()
        self.read_deleted_table_feed()
        self.read_deleted_table_feed_comments()
        self.rdb_trans.Commit()

        self.rdb_cmd.Dispose()
        self.rdb.Close()

    def create_deleted_db(self):
        '''创建删除数据库'''
        if os.path.exists(self.recoverDB):
            os.remove(self.recoverDB)
        db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        if db_cmd is not None:
            db_cmd.CommandText = '''create table if not exists bb_account_info
                (user_id INTEGER, account_name TEXT, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_user_info
                (userid INTEGER, name TEXT, gender INTEGER, birthday INTEGER, signature TEXT, customized_id TEXT, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_buddy_id_info
                (id INTEGER, userid INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_discussion_info
                (discussionid INTEGER, title TEXT, memberver INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_discussion_member_info
                (discussionid INTEGER, memberid INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_chat_msg_info
                (msgid INTEGER, content BLOB, fromId INTEGER, metatag TEXT, timestamp INTEGER, userid INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_dl_item_info
                (item_id INTEGER, mediaBytesInfo BLOB, timeStamp INTEGER, user_id INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_dl_comment_info
                (item_id INTEGER, action TEXT, comment TEXT, timestamp INTEGER, user_id INTEGER, commend_id INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists bb_discussion_chat_msg_info
                (content BLOB, msgid INTEGER, userId INTEGER, metatag TEXT, timestamp INTEGER, discussionid INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
        db_cmd.Dispose()
        db.Close()

    def read_deleted_table_account(self):
        '''恢复账号数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_account_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not rec.IsDeleted:
                        param = (rec['`user_id`'].Value, rec['`account_name`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_account_info(user_id, account_name, deleted) values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except:
            traceback.print_exc()


    def read_deleted_table_contact(self):
        '''恢复联系人表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_user_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['`userid`'].Value) and rec['`userid`'].Value != 0:
                        param = (rec['`userid`'].Value, rec['`name`'].Value, rec['`gender`'].Value, rec['`birthday`'].Value, rec['`signature`'].Value, rec['`customized_id`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_user_info(userid, name, gender, birthday, signature, customized_id, deleted) values(?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_group_user_id(self):
        '''恢复群组成员表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_buddy_id_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['`id`'].Value) and rec['`id`'].Value != 0 and not IsDBNull(rec['`userid`'].Value) and rec['`userid`'].Value != 0:
                        param = (rec['`id`'].Value, rec['`userid`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_buddy_id_info(id, userid, deleted) values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_group(self):
        '''恢复聊天室数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_discussion_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if rec['`discussionid`'].Value != 0 and not IsDBNull(rec['`discussionid`'].Value):
                        param = (rec['`discussionid`'].Value, rec['`title`'].Value, rec['`memberver`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_discussion_info(discussionid, title, memberver, deleted) values(?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_group_user(self):
        '''恢复聊天室成员数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_discussion_member_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['`discussionid`'].Value) and rec['`discussionid`'].Value != 0 and not IsDBNull(rec['`memberid`'].Value) and rec['`memberid`'].Value != 0:
                        param = (rec['`discussionid`'].Value, rec['`memberid`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_discussion_member_info(discussionid, memberid, deleted) values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_message(self):
        '''恢复聊天数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_chat_msg_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['`msgid`'].Value) and rec['`msgid`'].Value != 0:
                        param = (rec['`msgid`'].Value, rec['`content`'].Value, rec['`fromId`'].Value, rec['`metatag`'].Value, rec['`timestamp`'].Value, rec['`userid`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_chat_msg_info(msgid, content, fromId, metatag, timestamp, userid, deleted) values(?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_group_chat(self):
        '''恢复群组会话数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_discussion_chat_msg_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['`msgid`'].Value) and rec['`msgid`'].Value != 0:
                        param = (rec['`content`'].Value, rec['`msgid`'].Value, rec['`userId`'].Value, rec['`metatag`'].Value, rec['`timestamp`'].Value, rec['`discussionid`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_discussion_chat_msg_info(content, msgid, userId, metatag, timestamp, discussionid, deleted) values(?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_feed(self):
        '''恢复动态数据'''
        try:
            node = self.fnode
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_dl_item_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['`item_id`'].Value) and rec['`item_id`'].Value != 0 and not IsDBNull(rec['`user_id`'].Value) and rec['`user_id`'].Value != 0 and not IsDBNull(rec['`mediaBytesInfo`'].Value):
                        param = (rec['`item_id`'].Value, rec['`mediaBytesInfo`'].Value, rec['`timeStamp`'].Value, rec['`user_id`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_dl_item_info(item_id, mediaBytesInfo, timeStamp, user_id, deleted) values(?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_feed_comments(self):
        '''恢复动态评论点赞数据'''
        try:
            node = self.fnode
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('bb_dl_comment_info')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['`commend_id`'].Value) and rec['`commend_id`'].Value != 0 and not IsDBNull(rec['`user_id`'].Value) and rec['`user_id`'].Value != 0:
                        param = (rec['`item_id`'].Value, rec['`action`'].Value, rec['`comment`'].Value, rec['`timestamp`'].Value, rec['`user_id`'].Value, rec['`commend_id`'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into bb_dl_comment_info(item_id, action, comment, timestamp, user_id, commend_id, deleted) values(?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def db_insert_to_deleted_table(self, sql, values):
        '''插入数据到恢复数据库'''
        try:
            if self.rdb_cmd is not None:
                self.rdb_cmd.CommandText = sql
                self.rdb_cmd.Parameters.Clear()
                for value in values:
                    param = self.rdb_cmd.CreateParameter()
                    param.Value = value
                    self.rdb_cmd.Parameters.Add(param)
                self.rdb_cmd.ExecuteNonQuery()
        except Exception as e:
            print(e)

    def _copytocache(self, source):
        sourceDir = source
        targetDir = self.sourceDB
        try:
            if not os.path.exists(targetDir):
                shutil.copytree(sourceDir, targetDir)
        except Exception as e:
            print(e)

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
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
                return timestamp
        except:
            return None

def analyze_android_beetalk(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(BeeTalkParser(node, extractDeleted, extractSource).parse())
    pr.Build('蜜语')
    return pr

def execute(node, extractDeleted):
    return analyze_android_beetalk(node, extractDeleted, False)