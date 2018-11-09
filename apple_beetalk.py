# _*_ coding:utf-8 _*_
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
import hashlib
import shutil
import traceback

VERSION_APP_VALUE = 2

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
        self.account_id = None

    def db_create_table(self):
        model_im.IM.db_create_table(self)
        model_callrecord.MC.db_create_table(self)

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
            db_cmd.CommandText = '''select a.ZUSER, a.ZCONTACTSLASTSYNC, a.ZACCOUNT, b.ZFORMATTEDNICKNAME, b.ZBIRTHDAY, 
                b.ZGENDER, a.deleted, b.deleted from ZBTLINKEDACCOUNT as a left join ZBTUSER as b on a.ZUSER = b.Z_PK group by a.ZUSER'''
            sr = db_cmd.ExecuteReader()
            try:
                while(sr.Read()):
                    account = model_im.Account()
                    account.account_id = self._db_reader_get_int_value(sr, 0)
                    account.telephone = self._db_reader_get_string_value(sr, 2)
                    account.username = self._db_reader_get_string_value(sr, 3)
                    if not IsDBNull(sr[4]):
                        dstart = DateTime(1970,1,1,0,0,0)
                        try:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetInt32(4))
                        except:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetDouble(4))
                        account.birthday = int((cdate - dstart).TotalSeconds)
                    account.gender = self._db_reader_get_int_value(sr, 5)
                    account.source = self.node.AbsolutePath
                    account.deleted = 0
                    self.db_insert_table_account(account)
            except:
                traceback.print_exc()
            self.account_id = account.account_id
            self.account_name = account.username
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
            db_cmd.CommandText = '''select distinct Z_PK, ZLINKEDACCOUNT, ZACCOUNT, ZNAME, deleted from ZBTLINKEDCONTACT '''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    friend.account_id = self._db_reader_get_int_value(sr, 1)
                    friend.deleted = self._db_reader_get_int_value(sr, 4)
                    friend.friend_id = '手机联系人' + str(sr[0]) if not IsDBNull(sr[0]) else 0
                    friend.fullname = self._db_reader_get_string_value(sr, 3)
                    friend.source = self.node.AbsolutePath
                    friend.telephone = self._db_reader_get_string_value(sr, 2)
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    self.db_insert_table_friend(friend)
                except:
                    traceback.print_exc()
            sr.Close()
            self.db_commit()

            db_cmd.CommandText = '''select distinct Z_PK, ZGENDER, ZBIRTHDAY, ZFORMATTEDNICKNAME, ZSIGNATURE, ZUNIQUEID, deleted from ZBTUSER'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    friend.deleted = self._db_reader_get_int_value(sr, 6)
                    friend.account_id = self.account_id
                    friend.friend_id = self._db_reader_get_int_value(sr, 0)
                    friend.nickname = self._db_reader_get_string_value(sr, 3)
                    friend.source = self.node.AbsolutePath
                    friend.gender = self._db_reader_get_int_value(sr, 1)
                    if not IsDBNull(sr[2]):
                        dstart = DateTime(1970,1,1,0,0,0)
                        try:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetInt32(2))
                        except:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetDouble(2))
                        friend.birthday = int((cdate - dstart).TotalSeconds)
                    friend.signature = self._db_reader_get_string_value(sr, 4) 
                    friend.type = model_im.FRIEND_TYPE_NONE
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
            db_cmd.CommandText = '''select distinct a.Z_PK, a.ZGROUPID, a.ZMEMBERLISTVERSION, a.ZFORMATTEDNAME, a.ZOWNER, count(b.Z_13GROUPS), a.deleted, b.deleted from ZBTGROUP as a left join Z_13USERS as b on a.Z_PK = b.Z_13GROUPS'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = self.account_id
                    chatroom.chatroom_id = self._db_reader_get_int_value(sr, 0)
                    chatroom.create_time = self._db_reader_get_int_value(sr, 2)
                    chatroom.deleted = sr[6] or sr[7]
                    chatroom.member_count = self._db_reader_get_int_value(sr, 5)
                    chatroom.name = self._db_reader_get_string_value(sr, 3)
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
        fs = self.node.FileSystem
        try:
            if self.db is None:
                return
            db_cmd.CommandText = '''select distinct a.Z_13GROUPS, a.Z_23USERS, b.ZFORMATTEDNICKNAME, b.ZBIRTHDAY, b.ZGENDER, a.deleted, b.deleted from Z_13USERS as a 
                left join ZBTUSER as b on a.Z_23USERS = b.Z_PK'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom_member = model_im.ChatroomMember()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom_member.account_id = self.account_id
                    chatroom_member.chatroom_id = self._db_reader_get_int_value(sr, 0)
                    chatroom_member.deleted = sr[5] or sr[6]
                    chatroom_member.display_name = self._db_reader_get_string_value(sr, 2)
                    chatroom_member.member_id = self._db_reader_get_int_value(sr, 1)
                    chatroom_member.source = self.node.AbsolutePath
                    if not IsDBNull(sr[3]):
                        dstart = DateTime(1970,1,1,0,0,0)
                        try:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetInt32(3))
                        except:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetDouble(3))
                        chatroom_member.birthday = int((cdate - dstart).TotalSeconds)
                    chatroom_member.gender = self._db_reader_get_int_value(sr, 4)
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
        fs = self.node.FileSystem
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select distinct a.Z_PK, a.ZISOUTGOING, a.ZISWHISPER, a.ZMEDIASTATUS, a.ZSTATUS, 
            a.ZTYPE, a.ZWHISPERDURATION, a.ZCHAT, b.ZUSER, c.ZFORMATTEDNICKNAME, b.ZUSER1, d.ZFORMATTEDNICKNAME, 
            b.ZGROUP, f.ZFORMATTEDNAME, a.ZUSER, e.ZFORMATTEDNICKNAME, a.ZTIMESTAMP, a.ZDATA, a.deleted from ZBTMESSAGE as a 
            left join ZBTCHAT as b on a.ZCHAT = b.Z_PK left join ZBTUSER as c on b.ZUSER = c.Z_PK and c.deleted = 0 left join ZBTUSER as d 
            on b.ZUSER1 = d.Z_PK and d.deleted = 0 left join ZBTUSER as e on a.ZUSER = e.Z_PK and e.deleted = 0 left join ZBTGROUP as f on b.ZGROUP = f.Z_PK where a.ZCHAT is not null'''
            sr = db_cmd.ExecuteReader()
            location_id = 0
            record_id = 0
            while (sr.Read()):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    fs = self.node.FileSystem
                    image_name = None
                    thumbnail_name = None
                    text_content = None
                    latitude = None
                    location_name = None
                    longitude = None
                    voice_msg = None
                    call_type = None
                    call_duration = None
                    message.account_id = self.account_id
                    message.deleted =  sr[18]
                    message.is_sender = self._db_reader_get_int_value(sr, 1)
                    message.msg_id = self._db_reader_get_int_value(sr, 0)
                    if not IsDBNull(sr[16]):
                        dstart = DateTime(1970,1,1,0,0,0)
                        try:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetInt32(16))
                        except:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetDouble(16))
                        message.send_time = int((cdate - dstart).TotalSeconds)
                    message.sender_id = self._db_reader_get_int_value(sr, 14)
                    message.sender_name = self._db_reader_get_string_value(sr, 15)
                    message.source = self.node.AbsolutePath
                    message.talker_type = model_im.CHAT_TYPE_GROUP if not IsDBNull(sr[12]) else model_im.CHAT_TYPE_FRIEND
                    message.talker_id = self._db_reader_get_int_value(sr, 12) if not IsDBNull(sr[12]) else self._db_reader_get_int_value(sr, 8) if not IsDBNull(sr[8]) else self._db_reader_get_int_value(sr, 10) if not IsDBNull(sr[10]) else None
                    message.talker_name = self._db_reader_get_string_value(sr, 13) if not IsDBNull(sr[13]) else self._db_reader_get_string_value(sr, 9) if not IsDBNull(sr[9]) else self._db_reader_get_string_value(sr, 11) if not IsDBNull(sr[11]) else '未知聊天名'
                    if not IsDBNull(sr[17]):
                        tree = BPReader.GetTree(MemoryRange.FromBytes(sr.GetValue(17)))
                        if tree is None:
                            break
                        for i in range(len(tree)):
                            test_Key = '' if tree[i].Key is None else tree[i].Key
                            test_Value = '' if tree[i].Value is None else tree[i].Value
                            test = tree[i].Value
                            line = '{' + str(test_Key) + ':' + str(test_Value) + '}'
                            print(line)
                            if test_Key == 'kImageImageUrlKey':
                                image_name = test_Value
                                nodes = fs.Search(image_name + '$')
                                for node in nodes:
                                    message.media_path = node.AbsolutePath
                                    break
                                message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                            elif test_Key == 'kImageThumbUrlKey':
                                thumbnail_name = test_Value
                            elif test_Key == 'kMessageTextKey':
                                text_content = test_Value
                                message.content = text_content
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            elif test_Key == 'kLocationLat':
                                latitude = test_Value
                                message.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                            elif test_Key == 'kLocationTitle':
                                location_name = test_Value
                            elif test_Key == 'kLocationLong':
                                longitude = test_Value
                            elif test_Key == 'kVNUrlKey':
                                voice_msg = test_Value
                                nodes = fs.Search(voice_msg + '$')
                                for node in nodes:
                                    message.media_path = node.AbsolutePath
                                    break
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                            elif test_Key == 'kCallInfoType':
                                call_type = test_Value
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VOIP
                            elif test_Key == 'kCallInfoDuration':
                                call_duration = test_Value
                            extra_id = self._db_reader_get_int_value(sr, 0)
                        if latitude is not None or longitude is not None or location_name is not None:  #如果包含有位置数据，就把数据插入到位置表中
                            location = model_im.Location()
                            message.extra_id = location.location_id
                            location.latitude = latitude
                            location.longitude = longitude
                            location.address = location_name
                            location.timestamp = message.send_time if message.send_time is not None else 0
                            location.deleted = sr[18]
                            location.source = self.node.AbsolutePath
                            self.db_insert_table_location(location)
                        if call_type is not None or call_duration is not None:  #如果包含有通话数据，就把数据插入到通话记录表中
                            record_id += 1
                            local_number = self.account_name
                            counter_number = self._db_reader_get_string_value(sr, 9) if not IsDBNull(sr[9]) else self._db_reader_get_string_value(sr, 11) if not IsDBNull(sr[11]) else ''
                            record = model_callrecord.Records()
                            record.id = record_id
                            record.local_number = local_number
                            record.phone_number = counter_number
                            record.type = call_type
                            record.duration = call_duration
                            record.date = message.send_time if message.send_time is not None else 0
                            record.deleted = sr[18]
                            record.source = self.node.AbsolutePath
                            self.db_insert_table_call_records(record)
                        print('-'*20)
                    self.db_insert_table_message(message)
                except:
                    traceback.print_exc()
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
        fs = self.node.FileSystem
        try:
            if self.db is None:
                return
            db_cmd.CommandText = '''select a.Z_PK, a.feed_user, a.ZTIMESTAMP, a.ZDATA, a.ZLOCATION, group_concat(b.ZUSER) as comment_user, 
                group_concat(b.ZTIMESTAMP) as comment_date, group_concat(b.ZACTION), group_concat(b.ZCONTENT), group_concat(b.Z_PK), a.deleted from (
                select Z_PK, ZUSER as feed_user, ZTIMESTAMP, ZDATA, ZLOCATION, deleted from ZBTBUZZBASEITEM where deleted = 0) as a 
                left join (select * from ZBTBUZZBASECOMMENT where deleted = 0) as b on b.ZITEM1 = a.Z_PK group by a.Z_PK'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    feed = model_im.Feed()
                    if canceller.IsCancellationRequested:
                        break
                    feed.account_id = self.account_id
                    feed.sender_id = self._db_reader_get_int_value(sr, 0)
                    if not IsDBNull(sr[3]):
                        tree = BPReader.GetTree(MemoryRange.FromBytes(sr.GetValue(3)))
                        if tree is None:
                            break
                        for i in range(len(tree)):
                            test_Key = '' if tree[i].Key is None else tree[i].Key
                            test_Value = '' if tree[i].Value is None else tree[i].Value
                            test = tree[i].Value
                            line = '{' + str(test_Key) + ':' + str(test_Value) + '}'
                            #print(line)
                            if test_Key == 'buzzItemMemoKey':
                                feed.content = test_Value
                        #print('--'*20)
                    if not IsDBNull(sr[2]):
                        dstart = DateTime(1970,1,1,0,0,0)
                        try:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetInt32(2))
                        except:
                            cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetDouble(2))
                        feed.send_time = int((cdate - dstart).TotalSeconds)
                    comment_actions = self._db_reader_get_string_value(sr, 7)
                    comment_action_list = comment_actions.split(',') if not IsDBNull(sr[7]) and sr[7] != '' else []
                    like_count = 0
                    comment_count = 0
                    comment_id = []
                    comment_flag = []
                    likes_id = []
                    likes_flag = []
                    for i, comment_action in enumerate(comment_action_list):
                        if comment_action == 'like':
                            like_count += 1
                            likes_flag.append(i)
                        elif not IsDBNull(comment_action):
                            comment_count += 1
                            comment_flag.append(i)
                    comment_id_list = self._db_reader_get_string_value(sr, 9).split(',')
                    for i in comment_flag:
                        comment_id.append(comment_id_list[i])
                    for i in likes_flag:
                        likes_id.append(comment_id_list[i])
                    feed.likes = ','.join(likes_id)
                    feed.likecount = like_count
                    feed.comments = ','.join(comment_id)
                    feed.commentcount = comment_count
                    feed.deleted = self._db_reader_get_int_value(sr, 10)
                    if not IsDBNull(sr[4]):
                        tree = BPReader.GetTree(MemoryRange.FromBytes(sr.GetValue(4)))
                        if tree is None:
                            break
                        for i in range(len(tree)):
                            test_Key = '' if tree[i].Key is None else tree[i].Key
                            test_Value = '' if tree[i].Value is None else tree[i].Value
                            #print('{' + str(test_Key) + ':' + str(test_Value) + '}')
                            if test_Key == 'buzzLocationLatKey':
                                latitude = test_Value
                            if test_Key == 'buzzLocationLongKey':
                                longitude = test_Value
                            if test_Key == 'buzzLocationFormattedAddressKey':
                                location_name = test_Value
                        if latitude is not None or longitude is not None or location_name is not None:  #如果包含有位置数据，就把数据插入到位置表中
                            location = model_im.Location()
                            feed.location = location.location_id
                            location.latitude = latitude
                            location.longitude = longitude
                            location.address = location_name
                            location.timestamp = feed.send_time if feed.send_time is not None else 0
                            location.deleted = self._db_reader_get_int_value(sr, 10)
                            location.source = self.node.AbsolutePath
                            self.db_insert_table_location(location)
                        #print('~~'*20)
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
        fs = self.node.FileSystem
        try:
            if self.db is None:
                return
            db_cmd.CommandText = '''select a.Z_PK, a.ZUSER, b.ZFORMATTEDNICKNAME , a.ZTIMESTAMP, a.ZACTION, a.ZCONTENT, a.deleted from (
                select Z_PK, ZUSER, ZTIMESTAMP, ZACTION, ZCONTENT, deleted from ZBTBUZZBASECOMMENT where deleted = 0) as a 
                left join ZBTUSER as b on a.ZUSER = b.Z_PK where b.deleted = 0'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    feed_like = model_im.FeedLike()
                    feed_comment = model_im.FeedComment()
                    if canceller.IsCancellationRequested:
                        break
                    if sr[4] == 'like':
                        feed_like.like_id = self._db_reader_get_int_value(sr, 0)
                        feed_like.sender_id = self._db_reader_get_int_value(sr, 1)
                        feed_like.sender_name = 'BeeTalkUser'
                        if not IsDBNull(sr[3]):
                            dstart = DateTime(1970,1,1,0,0,0)
                            try:
                                cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetInt32(3))
                            except:
                                cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetDouble(3))
                            feed_like.create_time = int((cdate - dstart).TotalSeconds)
                        feed_like.deleted = self._db_reader_get_int_value(sr, 6)
                        feed_like.source = self.node.AbsolutePath
                        self.db_insert_table_feed_like(feed_like)
                    elif not IsDBNull(sr[4]):
                        feed_comment.comment_id = self._db_reader_get_int_value(sr, 0)
                        feed_comment.sender_id = self._db_reader_get_int_value(sr, 1)
                        feed_comment.sender_name = 'BeeTalkUser'
                        feed_comment.content = self._db_reader_get_string_value(sr, 5)
                        if not IsDBNull(sr[3]):
                            dstart = DateTime(1970,1,1,0,0,0)
                            try:
                                cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetInt32(3))
                            except:
                                cdate = TimeStampFormats.GetTimeStampEpoch1Jan2001(sr.GetDouble(3))
                            feed_comment.create_time = int((cdate - dstart).TotalSeconds)
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
        nodes = fs.Search('BTBuzz-' + dbid + 'v\d+$')
        for node in nodes:
            self.fnode = node
            break

        self.read_deleted_table_account()
        self.read_deleted_table_user()
        self.read_deleted_table_contact()
        self.read_deleted_table_group()
        self.read_deleted_table_group_user()
        self.read_deleted_table_message()
        self.read_deleted_table_chat()
        self.read_deleted_table_feed()
        self.read_deleted_table_feed_comments()

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
            db_cmd.CommandText = '''create table if not exists ZBTLINKEDACCOUNT
                (ZUSER INTEGER, ZCONTACTSLASTSYNC INTEGER, ZACCOUNT TEXT, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZBTUSER
                (Z_PK INTEGER, ZFORMATTEDNICKNAME TEXT, ZBIRTHDAY INTEGER, ZGENDER INTEGER, ZSIGNATURE TEXT, ZUNIQUEID TEXT, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZBTLINKEDCONTACT
                (Z_PK INTEGER, ZLINKEDACCOUNT INTEGER, ZACCOUNT TEXT, ZNAME TEXT, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZBTGROUP
                (Z_PK INTEGER, ZGROUPID INTEGER, ZMEMBERLISTVERSION INTEGER, ZFORMATTEDNAME TEXT, ZOWNER INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists Z_13USERS
                (Z_13GROUPS INTEGER, Z_23USERS INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZBTMESSAGE
                (Z_PK INTEGER, ZISOUTGOING INTEGER, ZISWHISPER INTEGER, ZMEDIASTATUS INTEGER, ZSTATUS INTEGER, ZTYPE INTEGER, 
                ZWHISPERDURATION INTEGER, ZCHAT INTEGER, ZUSER INTEGER, ZTIMESTAMP INTEGER, ZWHISPERSTARTTIME INTEGER, ZDATA BLOB, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZBTCHAT
                (Z_PK INTEGER, ZGROUP INTEGER, ZUSER INTEGER, ZUSER1 INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZBTBUZZBASEITEM
                (Z_PK INTEGER, ZUSER INTEGER, ZTIMESTAMP INTEGER, ZDATA BLOB, ZLOCATION BLOB, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZBTBUZZBASECOMMENT
                (Z_PK INTEGER, ZUSER INTEGER, ZITEM1 INTEGER, ZTIMESTAMP INTEGER, ZACTION Text, ZCONTENT TEXT, deleted INTEGER)'''
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
            ts = SQLiteParser.TableSignature('ZBTLINKEDACCOUNT')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not rec.IsDeleted:
                        param = (rec['ZUSER'].Value, rec['ZCONTACTSLASTSYNC'].Value, rec['ZACCOUNT'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTLINKEDACCOUNT(ZUSER, ZCONTACTSLASTSYNC, ZACCOUNT, deleted) values(?, ?, ?, ?)''', param)
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
            ts = SQLiteParser.TableSignature('ZBTLINKEDCONTACT')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['Z_PK'].Value) and rec['Z_PK'].Value != 0:
                        param = (rec['Z_PK'].Value, rec['ZLINKEDACCOUNT'].Value, rec['ZACCOUNT'].Value, rec['ZNAME'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTLINKEDCONTACT(Z_PK, ZLINKEDACCOUNT, ZACCOUNT, ZNAME, deleted) values(?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_user(self):
        '''恢复用户表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZBTUSER')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if rec['Z_PK'].Value != 0:
                        param = (rec['Z_PK'].Value, rec['ZFORMATTEDNICKNAME'].Value, rec['ZBIRTHDAY'].Value, rec['ZGENDER'].Value, rec['ZSIGNATURE'].Value, rec['ZUNIQUEID'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTUSER(Z_PK, ZFORMATTEDNICKNAME, ZBIRTHDAY, ZGENDER, ZSIGNATURE, ZUNIQUEID, deleted) values(?, ?, ?, ?, ?, ?, ?)''', param)
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
            ts = SQLiteParser.TableSignature('ZBTGROUP')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if rec['Z_PK'].Value != 0 and not IsDBNull(rec['ZGROUPID'].Value):
                        param = (rec['Z_PK'].Value, rec['ZGROUPID'].Value, rec['ZMEMBERLISTVERSION'].Value, rec['ZFORMATTEDNAME'].Value, rec['ZOWNER'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTGROUP(Z_PK, ZGROUPID, ZMEMBERLISTVERSION, ZFORMATTEDNAME, ZOWNER, deleted) values(?, ?, ?, ?, ?, ?)''', param)
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
            ts = SQLiteParser.TableSignature('Z_13USERS')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['Z_13GROUPS'].Value) and rec['Z_13GROUPS'].Value != 0:
                        param = (rec['Z_13GROUPS'].Value, rec['Z_23USERS'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into Z_13USERS(Z_13GROUPS, Z_23USERS, deleted) values(?, ?, ?)''', param)
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
            ts = SQLiteParser.TableSignature('ZBTMESSAGE')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['Z_PK'].Value) and rec['Z_PK'].Value != 0:
                        param = (rec['Z_PK'].Value, rec['ZISOUTGOING'].Value, rec['ZISWHISPER'].Value, rec['ZMEDIASTATUS'].Value, rec['ZSTATUS'].Value, rec['ZTYPE'].Value, 
                                 rec['ZWHISPERDURATION'].Value, rec['ZCHAT'].Value, rec['ZUSER'].Value, rec['ZTIMESTAMP'].Value, rec['ZWHISPERSTARTTIME'].Value, rec['ZDATA'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTMESSAGE(Z_PK, ZISOUTGOING, ZISWHISPER, ZMEDIASTATUS, ZSTATUS, ZTYPE, 
                ZWHISPERDURATION, ZCHAT, ZUSER, ZTIMESTAMP, ZWHISPERSTARTTIME, ZDATA, deleted) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_chat(self):
        '''恢复会话数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZBTCHAT')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['Z_PK'].Value) and rec['Z_PK'].Value != 0:
                        param = (rec['Z_PK'].Value, rec['ZGROUP'].Value, rec['ZUSER'].Value, rec['ZUSER1'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTCHAT(Z_PK, ZGROUP, ZUSER, ZUSER1, deleted) values(?, ?, ?, ?, ?)''', param)
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
            ts = SQLiteParser.TableSignature('ZBTBUZZBASEITEM')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['Z_PK'].Value) and rec['Z_PK'].Value != 0 and not IsDBNull(rec['ZUSER'].Value) and rec['ZUSER'].Value != 0:
                        param = (rec['Z_PK'].Value, rec['ZUSER'].Value, rec['ZTIMESTAMP'].Value, rec['ZDATA'].Value, rec['ZLOCATION'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTBUZZBASEITEM(Z_PK, ZUSER, ZTIMESTAMP, ZDATA, ZLOCATION, deleted) values(?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_feed_comments(self):
        '''恢复动态评论点赞数据'''
        #Z_PK INTEGER, ZUSER INTEGER, ZITEM1 INTEGER, ZTIMESTAMP INTEGER, ZACTION INTEGER, ZCONTENT TEXT, deleted INTEGER
        try:
            node = self.fnode
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZBTBUZZBASECOMMENT')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['Z_PK'].Value) and rec['Z_PK'].Value != 0 and not IsDBNull(rec['ZUSER'].Value) and rec['ZUSER'].Value != 0:
                        param = (rec['Z_PK'].Value, rec['ZUSER'].Value, rec['ZITEM1'].Value, rec['ZTIMESTAMP'].Value, rec['ZACTION'].Value, rec['ZCONTENT'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into ZBTBUZZBASECOMMENT(Z_PK, ZUSER, ZITEM1, ZTIMESTAMP, ZACTION, ZCONTENT, deleted) values(?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def db_insert_to_deleted_table(self, sql, values):
        '''插入数据到恢复数据库'''
        try:
            self.rdb_trans = self.rdb.BeginTransaction()
            if self.rdb_cmd is not None:
                self.rdb_cmd.CommandText = sql
                self.rdb_cmd.Parameters.Clear()
                for value in values:
                    param = self.rdb_cmd.CreateParameter()
                    param.Value = value
                    self.rdb_cmd.Parameters.Add(param)
                self.rdb_cmd.ExecuteNonQuery()
            self.rdb_trans.Commit()
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

def analyze_apple_beetalk(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(BeeTalkParser(node, extractDeleted, extractSource).parse())
    pr.Build('Beetalk')
    return pr

def execute(node, extractDeleted):
    return analyze_apple_beetalk(node, extractDeleted, False)