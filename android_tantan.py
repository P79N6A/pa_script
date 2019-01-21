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

VERSION_APP_VALUE = 1.1

class TantanParser(model_im.IM):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node.Parent
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
        self.comment_count = {}

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
        models_im = GenerateModel(self.cachedb).get_models()
        models.extend(models_im)
        return models

    def analyze_data(self):
        '''分析数据'''
        nodes = self.node.Search('core_v\d+_\d+$')
        if nodes is None:
            return
        for node in nodes:
            self.parse_account(node)
            self.parse_friend(node)
            self.parse_conversation(node)
            self.parse_sticker(node)
            self.parse_message(node)
            self.parse_feed(node)
    
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
                db_cmd.CommandText = '''select * from users where id_c = {}'''.format(userid)
                sr = db_cmd.ExecuteReader()
                while(sr.Read()):
                    create_time = self._get_timestamp(self._db_reader_get_double_value(sr, 12))
                    age = self._db_reader_get_int_value(sr, 10)
                    signature = self._db_reader_get_string_value(sr, 9)
                    gender = self._db_reader_get_int_value(sr, 28)
                    name = self._db_reader_get_string_value(sr, 2)
                    country = self._db_reader_get_string_value(sr, 7)
                    city = self._db_reader_get_string_value(sr, 6)
                    district = self._db_reader_get_string_value(sr, 5)
                    phone_number = None
                    if not IsDBNull(sr[27]):
                        phone_len = int(sr[27][5])
                        data = str(bytes(sr.GetValue(27)))
                        phone_number = data.encode('hex')[12:(phone_len+6)*2:].decode('hex').decode('utf-8')
                    picture = None
                    if not IsDBNull(sr[11]):
                        lst = []
                        dic = {}
                        for i, byte in enumerate(sr[11]):
                            if int(byte) == 1:  #start
                                dic = {}
                                dic['start'] = i+1
                            if int(byte) == 26:  #end
                                dic['end'] = i
                                lst.append(dic)
                        data = str(bytes(sr.GetValue(11)))
                        picture = data.encode('hex')[lst[0]['start']*2:lst[0]['end']*2:].decode('hex').decode('utf-8')
                    account = model_im.Account()
                    account.account_id = userid
                    account.nickname = name
                    account.username = name
                    account.telephone = phone_number
                    account.photo = picture
                    account.gender = 2 if gender == 1 else 1 if gender == 2 else 0
                    account.age = age
                    account.country = country
                    account.city = city
                    account.address = country + city + district
                    account.signature = signature
                    account.source = node.AbsolutePath
                    self.db_insert_table_account(account)
            except:
                traceback.print_exc()
            self.db_commit()
            sr.Close()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_friend(self, node):
        '''解析好友数据'''
        try:
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('users')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    if IsDBNull(rec['id_c'].Value) or rec['id_c'].Value == 0:
                        continue
                    friend = model_im.Friend()
                    userDir = node.PathWithMountPoint
                    userid = re.findall('\d+', userDir)[-1]
                    phone_number = None
                    if not IsDBNull(rec['settings_c'].Value):
                        phone_len = int(rec['settings_c'].Value[5])
                        data = str(bytes(rec['settings_c'].Value))
                        phone_number = data.encode('hex')[12:(phone_len+6)*2:].decode('hex').decode('utf-8')
                    picture = None
                    if not IsDBNull(rec['pictures_c'].Value):
                        lst = []
                        dic = {}
                        for i, byte in enumerate(rec['pictures_c'].Value):
                            if int(byte) == 1:  #start
                                dic = {}
                                dic['start'] = i+1
                            if int(byte) == 26:  #end
                                dic['end'] = i
                                lst.append(dic)
                        data = str(bytes(rec['pictures_c'].Value))
                        if len(lst) != 0:
                            picture = data.encode('hex')[lst[0]['start']*2:lst[0]['end']*2:].decode('hex').decode('utf-8')
                    friend.account_id = userid
                    friend.friend_id = self._db_record_get_int_value(rec, 'id_c')
                    if friend.friend_id == 0:
                        continue
                    friend.nickname = self._db_record_get_string_value(rec, 'name_c')
                    if friend.friend_id not in self.friend.keys():
                        self.friend[friend.friend_id] = friend.nickname
                    friend.photo = picture
                    friend.fullname = friend.nickname
                    friend.telephone = phone_number
                    friend.age = self._db_record_get_int_value(rec, 'age_c')
                    gender = self._db_record_get_int_value(rec, 'gender_c')
                    friend.gender = 2 if gender == 1 else 1 if gender == 2 else 0
                    friend.signature = self._db_record_get_string_value(rec, 'description_c')
                    friend.source = node.AbsolutePath
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
        ts = SQLiteParser.TableSignature('conversations')
        self.conversation = {}
        for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
            try:
                if self._db_record_get_int_value(rec, '_id') == 0:
                    continue
                pk = self._db_record_get_string_value(rec, 'id_c')
                cnt = self._db_record_get_blob_value(rec, 'otherUser_c')
                if pk not in self.conversation.keys():
                    self.conversation[pk] = cnt
            except:
                traceback.print_exc()

    def parse_message(self, node):
        '''解析消息数据'''
        try:
            db = SQLiteParser.Database.FromNode(node, canceller)
            userDir = node.PathWithMountPoint
            userid = re.findall('\d+', userDir)[-1]
            if db is None:
                return
            ts = SQLiteParser.TableSignature('messages')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    message = Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = userid
                    message.talker_id = self._db_record_get_int_value(rec, 'cid_c')
                    otherUserId = self.conversation[message.talker_id] if message.talker_id in self.conversation else message.talker_id
                    message.talker_name = self.friend[otherUserId] if otherUserId in self.friend else otherUserId
                    message.sender_id = self._db_record_get_int_value(rec, 'owner_c')
                    message.sender_name = self.friend[message.sender_id] if message.sender_id in self.friend else message.sender_id
                    message.msg_id = self._db_record_get_string_value(rec, '_id')
                    iscomment = 1 if not IsDBNull(rec['moment_c'].Value) else 0
                    content = self._db_record_get_string_value(rec, 'value_c')
                    if not iscomment:
                        if content == '[贴纸表情]':  #贴纸表情
                            message.type = model_im.MESSAGE_CONTENT_TYPE_EMOJI
                            sticker_index = ''
                            if not IsDBNull(rec['api_only_accessory_c'].Value):
                                value = rec['api_only_accessory_c'].Value
                                start = 0
                                length = 0
                                for i, byte in enumerate(value):
                                    if int(byte) == 26:
                                        start = i+2
                                        length = int(value[i+1])
                                        break
                                data = str(bytes(value))
                                sticker_index = data.encode('hex')[start*2:(start+length)*2:].decode('hex').decode('utf-8')
                            if sticker_index != '':
                                message.media_path = self.sticker[sticker_index] if sticker_index in self.sticker else ''
                            else:
                                message.content = content
                                message.type = model_im.MEESAGE_CONTENT_TYPE_TEXT
                        elif content == '[语音]':  #语音消息
                            try:
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                                media_url = self.parse_media_from_blob(rec, 'media_c')
                                message.media_path = media_url[0] if len(media_url) != 0 else None
                                if message.media_path == None:
                                    message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                                    message.content = self._db_record_get_string_value(rec, 'value_c')
                            except:
                                traceback.print_exc()
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                                message.content = self._db_record_get_string_value(rec, 'value_c')
                        elif content == '[视频]':  #视频
                            try:
                                message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                                media_url = self.parse_media_from_blob(rec, 'media_c')
                                message.media_path = media_url[0] if len(media_url) != 0 else None
                                if message.media_path == None:
                                    message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                                    message.content = self._db_record_get_string_value(rec, 'value_c')
                            except:
                                traceback.print_exc()
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                                message.content = self._db_record_get_string_value(rec, 'value_c')
                        elif content == '[照片]':  #照片
                            try:
                                message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                                media_url = self.parse_media_from_blob(rec, 'media_c')
                                message.media_path = media_url[0] if len(media_url) != 0 else None
                                if message.media_path == None:
                                    message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                                    message.content = self._db_record_get_string_value(rec, 'value_c')
                            except:
                                traceback.print_exc()
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                                message.content = self._db_record_get_string_value(rec, 'value_c')
                        elif content == '[地理位置]':  #地理位置
                            try:
                                message.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                                location = model_im.Location()
                                loc = rec['location_c'].Value
                                if not IsDBNull(loc):
                                    loc = Encoding.UTF8.GetString(loc)
                                else:
                                    loc = ''
                                loc_address = self.illegal_char(loc)
                                message.location_id = location.location_id
                                location.address = loc_address
                                self.db_insert_table_location(location)
                            except:
                                traceback.print_exc()
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                                message.content = self._db_record_get_string_value(rec, 'location_c')
                        else:
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            message.content = content
                    else:
                        comments = model_im.FeedComment()
                        comments.comment_id = self._db_record_get_string_value(rec, 'moment_c')  # 评论ID[INT]
                        if comments.comment_id not in self.comment_count:
                            self.comment_count[comments.comment_id] = 1
                        else:
                            self.comment_count[comments.comment_id] += 1
                        comments.sender_id = message.sender_id
                        comments.sender_name = message.sender_name
                        comments.content = content
                        comments.create_time = self._get_timestamp(self._db_record_get_int_value(rec, 'createdTime_c'))
                        comments.source = node.AbsolutePath
                        comments.deleted = rec.IsDeleted
                        self.db_insert_table_feed_comment(comments)
                    message.send_time = self._get_timestamp(self._db_record_get_int_value(rec, 'createdTime_c'))
                    message.source = node.AbsolutePath
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.deleted = rec.IsDeleted
                    if message.talker_id != 0 and message.type is not None:
                        self.db_insert_table_message(message)
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    @staticmethod
    def parse_media_from_blob(record, column):
        '''从blob中获取媒体链接'''
        media_url = []
        if not IsDBNull(record[column].Value):
            lst = []
            dic = {}
            for i, byte in enumerate(record[column].Value):
                if int(byte) == 1:  #start
                    dic = {}
                    dic['start'] = i+1
                if int(byte) == 26:  #end
                    if 'start' in dic:
                        dic['end'] = i
                        lst.append(dic)
            data = str(bytes(record[column].Value))
            if len(lst) != 0:
                for l in lst:
                    media_url.append(data.encode('hex')[l['start']*2:l['end']*2:].decode('hex').decode('utf-8'))
        return media_url

    def parse_sticker(self, node):
        '''解析表情数据'''
        userid = re.findall('\d+', node.PathWithMountPoint)[-1]
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        ts = SQLiteParser.TableSignature('stickers')
        self.sticker = {}
        for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
            try:
                if self._db_record_get_int_value(rec, '_id') == 0:
                    continue
                pk = self._db_record_get_string_value(rec, 'id_c')
                media_url = self.parse_media_from_blob(rec, 'pictures_c')
                cnt = media_url[0] if len(media_url) != 0 else ''
                if pk not in self.sticker.keys():
                    self.sticker[pk] = cnt
            except:
                traceback.print_exc()

    def parse_feed(self, node):
        '''解析动态数据'''
        print(self.comment_count)
        userid = re.findall('\d+', node.PathWithMountPoint)[-1]
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        ts = SQLiteParser.TableSignature('moments')
        flag = 0
        for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
            try:
                if self._db_record_get_string_value(rec, '_id') == '':
                    continue
                feed = model_im.Feed()
                #多媒体数据
                media_url = self.parse_media_from_blob(rec, 'media_c')
                image_url = []
                video_url = []
                for url in media_url:
                    if re.findall('images', url):
                        image_url.append(url)
                    if re.findall('videos', url):
                        video_url.append(url)
                #获赞数
                likescount = self._db_record_get_int_value(rec, 'likes_count_c')
                #动态id
                feedpk = self._db_record_get_string_value(rec, 'id_c')
                #评论数
                comment_count = self.comment_count[feedpk] if feedpk in self.comment_count else 0
                #动态时间
                createtime = self._db_record_get_int_value(rec, 'createdTime_c')
                #动态位置
                if not IsDBNull(rec['location_c'].Value):
                    location = model_im.Location()
                    feed.location_id = location.location_id  # 地址ID[INT]
                    loc = rec['location_c'].Value
                    if not IsDBNull(loc):
                        loc = Encoding.UTF8.GetString(loc)
                    else:
                        loc = ''
                    location.address = self.illegal_char(loc)
                    self.db_insert_table_location(location)
                #发送者
                senderid = self._db_record_get_int_value(rec, 'owner_c')
                #动态文本
                content = self._db_record_get_string_value(rec, 'value_c')
                feed.account_id = userid  # 账号ID[TEXT]
                feed.sender_id = senderid  # 发布者ID[TEXT]
                feed.content = content  # 文本[TEXT]
                feed.image_path = ','.join(image_url)  # 链接[TEXT]
                feed.video_path = ','.join(video_url)
                feed.send_time = createtime  # 发布时间[INT]
                feed.likecount = likescount  # 赞数量[INT]
                feed.commentcount = comment_count
                feed.comment_id = feedpk
                feed.source = node.AbsolutePath
                feed.deleted = rec.IsDeleted
                if feed.sender_id != 0:
                    self.db_insert_table_feed(feed)
            except:
                pass
        self.db_commit()
     
    def _copytocache(self, source):
        sourceDir = source
        targetDir = self.sourceDB
        try:
            if not os.path.exists(targetDir):
                shutil.copytree(sourceDir, targetDir)
        except Exception as e:
            print(e)

    @staticmethod
    def illegal_char(s):
        s = re.compile( 
            u"[^\u4e00-\u9fa5]+").sub('', s)
        return s

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
                value = reader[index]
                return Encoding.UTF8.GetString(value)
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

def analyze_android_tantan(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(TantanParser(node, extractDeleted, extractSource).parse())
    pr.Build('探探')
    return pr

def execute(node, extractDeleted):
    return analyze_android_tantan(node, extractDeleted, False)