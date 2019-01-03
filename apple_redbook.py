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

class RedBookParser(model_im.IM):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node
        self.messageNode = node.Parent.GetByPath('/Message.db$')
        self.extractDeleted = extract_deleted
        self.db = None
        self.im = model_im.IM()
        self.cachepath = ds.OpenCachePath("RedBook")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = self.node.AbsolutePath
        rdb_name = self.node.PathWithMountPoint
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.recoverDB = self.cachepath + "\\" +md5_rdb.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\RedBookSource'
        self.friend = {}
        self.image_url = {}
        self.video_url = {}

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

    def analyze_data(self):
        '''分析数据'''
        self.parse_account()
        self.parse_friend()
        self.parse_feed_member()
        self.parse_message()
        self.parse_feed()
        self.parse_search()
    
    def parse_account(self):
        '''解析账户数据'''
        try:
            userNode = self.node.Parent.GetByPath('/default.realm')
            if userNode is None:
                return
            userDir = userNode.PathWithMountPoint
            f = open(userDir, 'rb')
            content = f.read()
            f.close()
            user = []
            for item in self.extract_json(content):
                try:
                    account = model_im.Account()
                    item = json.loads(item)
                    userid = item["userid"] if "userid" in item else ""
                    if userid in user:
                        continue
                    else:
                        user.append(userid)
                    picture = item["imageb"]
                    picture = re.sub('@.*', '', picture)
                    birthday = item["birthday"]
                    timeArray = time.strptime(birthday, '%Y-%m-%d')
                    timestamp = time.mktime(timeArray)
                    birthday = int(timestamp)
                    nickname = item["nickname"]
                    desc = item["desc"]
                    gender = item["gender"]
                    account.account_id = userid
                    account.nickname = nickname
                    account.username = nickname
                    account.photo = picture
                    account.gender = 1 if gender == 0 else 2 if gender == 1 else 0
                    account.signature = desc
                    account.birthday = birthday
                    account.deleted = 0 if len(user) == 1 else 1
                    self.db_insert_table_account(account)
                    friend = model_im.Friend()
                    friend.account_id = userid
                    friend.friend_id = userid
                    friend.nickname = nickname
                    friend.fullname = nickname
                    friend.photo = picture
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    friend.signature = desc
                    if userid is not None:
                        self.friend[userid] = nickname
                    if userid not in user:
                        self.db_insert_table_account(account)
                except:
                    traceback.print_exc()
            self.account_id = user[0]
            self.db_commit()
        except Exception as e:
            print(e)

    def extract_json(self, content):
        content = self.illegal_char(content.decode('utf-8', errors="ignore"))
        l = []
        s = ''
        flag = 0
        for w in content:
            if w == '{':
                flag += 1
            if flag != 0:
                s += w
            if w == '}':
                flag -= 1
                if flag == 0:
                    l.append(s)
                    s = ''
        return l

    @staticmethod
    def illegal_char(s):
        s = re.compile( 
            u"[^"
            u"\u4e00-\u9fa5"
            u"\u0041-\u005A"
            u"\u0061-\u007A"
            u"\u0030-\u0039"
            u"\u3002\uFF1F\uFF01\uFF0C\u3001\uFF1B\uFF1A\u300C\u300D\u300E\u300F\u2018\u2019\u201C\u201D\uFF08\uFF09\u3014\u3015\u3010\u3011\u2014\u2026\u2013\uFF0E\u300A\u300B\u3008\u3009"
            u"\!\@\#\$\%\^\&\*\(\)\-\=\[\]\{\}\\\|\;\'\:\"\,\.\/\<\>\?\/\*\+"
            u"]+").sub('', s)
        return s

    def parse_friend(self):
        '''解析好友数据'''
        try:
            node = self.node.Parent.GetByPath('/PrivateMessage.db')
            db =SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('XYPMMessageUserModel')
            self.friend_id = []
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    if IsDBNull(rec['__objectID'].Value):
                        continue
                    friend.account_id = re.sub('.*@', '', self._db_record_get_string_value(rec, 'local_user_id'))
                    friend.friend_id = self._db_record_get_string_value(rec, 'user_id')
                    friend.nickname = self._db_record_get_string_value(rec, 'nickname')
                    friend.fullname = self._db_record_get_string_value(rec, 'nickname')
                    friend.photo = re.sub('@.*', '', self._db_record_get_string_value(rec, 'image'))
                    isfriend = self._db_record_get_int_value(rec, 'is_friend')
                    if isfriend == 1:
                        self.friend_id.append(friend.friend_id)
                    friend.type = model_im.FRIEND_TYPE_NONE if isfriend == 0 else model_im.FRIEND_TYPE_FRIEND
                    friend.signature = self._db_record_get_string_value(rec, 'desc')
                    follow_status = self._db_record_get_string_value(rec, 'follow_status')  #还需要多测试几种关注状态
                    self.friend[friend.friend_id] = friend.nickname
                    friend.deleted = rec.IsDeleted
                    if friend.account_id is not '' and friend.friend_id is not '':
                        self.db_insert_table_friend(friend)
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_feed_member(self):
        '''解析动态成员数据'''
        try:
            node = self.node.Parent.GetByPath('/ExploreFeed.db')
            db =SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('XYPHUser')
            self.id2userid = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    if IsDBNull(rec['__objectID'].Value):
                        continue
                    friend.account_id = self.account_id
                    id = self._db_record_get_string_value(rec, '__objectID')
                    if IsDBNull(rec['__objectID'].Value):
                        continue
                    friend.friend_id = self._db_record_get_string_value(rec, 'userid')
                    if friend.friend_id is "":
                        continue
                    self.id2userid[id] = friend.friend_id
                    friend.nickname = self._db_record_get_string_value(rec, 'nickname')
                    friend.fullname = self._db_record_get_string_value(rec, 'nickname')
                    friend.photo = re.sub('@.*', '', self._db_record_get_string_value(rec, 'images'))
                    isfriend = 1 if friend.friend_id in self.friend_id else 0
                    friend.type = model_im.FRIEND_TYPE_NONE if isfriend == 0 else model_im.FRIEND_TYPE_FRIEND
                    self.friend[friend.friend_id] = friend.nickname
                    friend.deleted = rec.IsDeleted
                    self.db_insert_table_friend(friend)
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_message(self):
        '''解析消息数据'''
        mesasgeNode = self.node.Parent.GetByPath('/PrivateMessage.db')
        ImageNode = self.node.Parent.GetByPath('/PrivateMessageImage')
        try:
            db = SQLiteParser.Database.FromNode(mesasgeNode, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('XYPMMessageMessageModel')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = self.account_id
                    message.talker_id = self._db_record_get_string_value(rec, 'chat_id')
                    message.sender_id = self._db_record_get_string_value(rec, 'sender_id')
                    message.sender_name = self.friend[message.sender_id]
                    message.msg_id = self._db_record_get_string_value(rec, 'message_id')
                    content_type = self._db_record_get_int_value(rec, 'content_type')  #0:未知1:文本消息2:媒体文件3：link消息4:系统消息
                    if content_type == 0:
                        continue
                    elif content_type == 1:  #文本消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                        message.content = self._db_record_get_string_value(rec, 'content')
                    elif content_type == 2:  #图片消息
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                        image_name = self._db_record_get_string_value(rec, 'local_image_name')
                        if image_name != '':
                            nodes = ImageNode.Search(image_name + '$')
                            if len(list(nodes)) == 0:
                                content_json = json.loads(self._db_record_get_string_value(rec, 'content'))
                                if 'link' in content_json:
                                    message.media_path = content_json['link']
                                else:
                                    message.content = content_json
                                    message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            else:
                                message.media_path = list(nodes)[0].AbsolutePath
                        else:
                            content_json = json.loads(self._db_record_get_string_value(rec, 'content'))
                            if 'link' in content_json:
                                message.media_path = content_json['link']
                            else:
                                message.content = content_json
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif content_type == 3:  #link消息
                        link = model_im.Link()
                        message.link_id = link.link_id
                        message.type = model_im.MESSAGE_CONTENT_TYPE_LINK
                        content = self._db_record_get_string_value(rec, 'content').replace('\n', '\\n')
                        try:
                            link_json = json.loads(content)
                            if 'link' in link_json:
                                link.url = link_json['link']
                            if 'title' in link_json:
                                link.title = link_json['title']
                            if 'image' in link_json:
                                link.image = link_json['image']
                            link.deleted = rec.IsDeleted
                            self.db_insert_table_link(link)
                        except:
                            message.content = content
                            message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif content_type == 4:  #系统消息
                        content_json = json.loads(self._db_record_get_string_value(rec, 'content'))
                        if 'content' in content_json:
                            message.content = content_json['content']
                            message.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
                        else:
                            message.content = self._db_record_get_string_value(rec, 'content')
                            message.type = model_im.MESSAGE_CONTENT_TYPE_SYSTEM
                    message.send_time = self._get_timestamp(self._db_record_get_int_value(rec, 'create_time'))
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    message.deleted = rec.IsDeleted
                    self.db_insert_table_message(message)
                except:
                    traceback.print_exc()
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_feed(self):
        '''解析动态数据'''
        feedNode = self.node.Parent.GetByPath('/ExploreFeed.db')
        try:
            db = SQLiteParser.Database.FromNode(feedNode, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('XYExploreNote')
            self.parse_feed_image()
            self.parse_feed_video()
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    feed = model_im.Feed()
                    feed.account_id = self.account_id
                    sender_id = re.sub('XYPHUser&', '', self._db_record_get_string_value(rec, 'user'))
                    feed.sender_id = self.id2userid[sender_id]
                    feed.content = self._db_record_get_string_value(rec, 'desc')
                    image_pathes = self._db_record_get_string_value(rec, 'imagesList')
                    video_pathes = self._db_record_get_string_value(rec, 'videoInfo')
                    image_urls = []
                    video_urls = []
                    for image_path in image_pathes.split(','):
                        try:
                            image_path = re.sub('6#XYPHNoteImageInfo&', '', image_path)
                            image_urls.append(self.image_url[image_path])
                        except:
                            pass
                    feed.image_path = ','.join(image_urls)
                    for video_path in video_pathes.split(','):
                        try:
                            video_path = re.sub('XYPHVideoModel&', '', video_path)
                            video_urls.append(self.video_url[video_path])
                        except:
                            pass
                    feed.video_path = ','.join(video_urls)
                    feed.url_desc = self._db_record_get_string_value(rec, 'displayTitle')
                    feed.send_time = self._get_timestamp(self._db_record_get_string_value(rec, 'cursor_score'))
                    feed.likecount = self._db_record_get_int_value(rec, 'likes')
                    feed.deleted = rec.IsDeleted
                    self.db_insert_table_feed(feed)
                except:
                    pass
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_feed_image(self):
        '''解析动态图片数据'''
        feedNode = self.node.Parent.GetByPath('/ExploreFeed.db')
        try:
            db = SQLiteParser.Database.FromNode(feedNode, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('XYPHNoteImageInfo')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    id = self._db_record_get_string_value(rec, '__objectID')
                    url = self._db_record_get_string_value(rec, 'urlLarge')
                    self.image_url[id] = url
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def parse_feed_video(self):
        '''解析视频数据'''
        feedNode = self.node.Parent.GetByPath('/ExploreFeed.db')
        try:
            db = SQLiteParser.Database.FromNode(feedNode, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('XYPHVideoModel')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    id = self._db_record_get_string_value(rec, '__objectID')
                    gif_url = self._db_record_get_string_value(rec, 'gifUrl')
                    self.video_url[id] = gif_url
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)
        
    def parse_search(self):
        '''解析搜索记录'''
        feedNode = self.node.Parent.GetByPath('/Search')
        try:
            db = SQLiteParser.Database.FromNode(feedNode, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('XYPHRecentSearchItem')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    search = model_im.Search()
                    search.account_id = self.account_id
                    if IsDBNull(rec['__objectID'].Value):
                        continue
                    search.key = self._db_record_get_string_value(rec, 'name')
                    search.create_time = self._get_timestamp(self._db_record_get_string_value(rec, 'time'))
                    search.deleted = rec.IsDeleted
                    self.db_insert_table_search(search)
                except:
                    traceback.print_exc()
            self.db_commit()
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

def analyze_apple_redbook(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(RedBookParser(node, extractDeleted, extractSource).parse())
    pr.Build('小红书')
    return pr

def execute(node, extractDeleted):
    return analyze_apple_redbook(node, extractDeleted, False)