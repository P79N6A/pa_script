# _*_ coding:utf-8 _*_
__author__ = "xiaoyuge"

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

from System.Text import *

VERSION_APP_VALUE = 3

class IAroundParser(model_im.IM):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node
        self.extractDeleted = extract_deleted
        self.db = None
        self.im = model_im.IM()
        self.cachepath = ds.OpenCachePath("iAround")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = self.node.AbsolutePath
        rdb_name = self.node.PathWithMountPoint
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.recoverDB = self.cachepath + "\\" +md5_rdb.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\iAroundSourceDB'
        self.account_id = '-1'
        self.account_name = '未知用户'

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
            fs = self.node.FileSystem
            nodes = fs.Search('/shared_prefs/meyou_sharepreferences.xml')
            account_file_path = ''
            for node in nodes:
                account_file_path = node.PathWithMountPoint
                break
            try:
                if account_file_path is '':
                    account = model_im.Account()
                    account.account_id = '未知用户'
                    account.username = '未知用户'
                else:
                    f = open(account_file_path)
                    file_data = f.read()
                    if len(re.findall('<string name="success_login_data">(.*?)</string>', file_data)) == 0:
                        account = model_im.Account()
                        account.account_id = '未知用户'
                        account.username = '未知用户'
                    else:
                        s = re.findall('<string name="success_login_data">(.*?)</string>', file_data)[0].replace('&quot;', '').replace('\\', '')
                        icon = re.findall('icon:(.*?),', s)[0]
                        nickname = re.findall('nickname:(.*?),', s)[0]
                        userid = re.findall('userid:(.*?),', s)[0]
                        bindphone = re.findall('bindphone:(.*?),', s)[0]
                        age = re.findall('age:(.*?),', s)[0]
                        gender = 1 if re.findall('nickname:(.*?),', s)[0] == 'm' else 0
                        latitude = float(re.findall('lat:(.*?),', s)[0])/float(1000000)
                        longitude = float(re.findall('lng:(.*?),', s)[0])/float(1000000)
                        servertime = self._get_timestamp(re.findall('servertime:(.*?),', s)[0])
                    f.close()
                    account = model_im.Account()
                    account.source = self.node.AbsolutePath
                    account.account_id = userid
                    account.username = nickname
                    account.nickname = nickname
                    account.photo = icon
                    account.gender = gender
                    account.telephone = bindphone
                    account.age = age
                    account.deleted = 0
                    self.db_insert_table_account(account)
            except:
                traceback.print_exc()
            self.account_id = account.account_id
            self.account_name = account.username
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_friend(self):
        '''解析好友（联系人）数据'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('tb_near_contact')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    friend.account_id = self.account_id
                    friend.deleted = rec.IsDeleted
                    if 'fuserinfo' not in rec or IsDBNull(rec['fuserinfo'].Value):
                        continue
                    userinfo = json.loads(rec['fuserinfo'].Value.replace('\\', ''))
                    friend.friend_id = userinfo['fuid'] if 'fuid' in userinfo.keys() else self._db_record_get_string_value(rec, 'fuid')
                    friend.fullname = userinfo['fnickname'] if 'fnickname' in userinfo.keys() else ''
                    friend.nickname = userinfo['fnote'] if ('fnote' in userinfo.keys() and userinfo['fnote'] is not '') else friend.fullname
                    friend.photo = userinfo['ficon'] if 'ficon' in userinfo.keys() else None
                    if 'fgender' in userinfo.keys():
                        friend.gender = 1 if userinfo['fgender'] == '1' else 0
                    lat = float(userinfo['flat'])/float(1000000)
                    lng = float(userinfo['flng'])/float(1000000)
                    location = model_im.Location()
                    friend.location_id = location.location_id
                    location.latitude = lat
                    location.longitude = lng
                    self.db_insert_table_location(location)
                    friend.source = self.node.AbsolutePath
                    friend.type = model_im.FRIEND_TYPE_RECENT
                    self.db_insert_table_friend(friend)
                except:
                    pass
            ts = SQLiteParser.TableSignature('tb_new_fans')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    friend.account_id = self.account_id
                    friend.deleted = rec.IsDeleted
                    if 'fuinfo' not in rec or IsDBNull(rec['fuinfo'].Value):
                        continue
                    userinfo = json.loads(rec['fuinfo'].Value.replace('\\', ''))
                    friend.friend_id = userinfo['userid'] if 'userid' in userinfo.keys() else self._db_record_get_string_value(rec, 'fuid')
                    friend.fullname = userinfo['nickname'] if 'nickname' in userinfo.keys() else ''
                    friend.nickname = friend.fullname
                    friend.photo = userinfo['icon'] if 'icon' in userinfo.keys() else None
                    if 'gender' in userinfo.keys():
                        friend.gender = 1 if userinfo['gender'] == 'm' else 0
                    lat = float(userinfo['lat'])/float(1000000)
                    lng = float(userinfo['lng'])/float(1000000)
                    location = model_im.Location()
                    friend.location_id = location.location_id
                    location.latitude = lat
                    location.longitude = lng
                    self.db_insert_table_location(location)
                    friend.source = self.node.AbsolutePath
                    friend.type = model_im.FRIEND_TYPE_FANS
                    self.db_insert_table_friend(friend)
                except:
                    pass
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_chatroom(self):
        '''解析群组数据'''
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('tb_group_history_order')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if rec.IsDeleted == 1:
                        continue
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = self.account_id
                    chatroom.chatroom_id = self._db_record_get_int_value(rec, 'groupid')
                    chatroom.photo = self._db_record_get_string_value(rec, 'groupicon')
                    chatroom.member_count = self.member_count[chatroom.chatroom_id]
                    chatroom.deleted = rec.IsDeleted
                    chatroom.name = self._db_record_get_string_value(rec, 'groupname')
                    chatroom.source = self.node.AbsolutePath
                    self.db_insert_table_chatroom(chatroom)
                except:
                    pass
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_chatroom_member(self, dbPath):
        '''
        解析群组成员数据
        只拿到了加群后发过消息的群成员数据，在消息中解析
        '''
        pass

    def parse_message(self):
        '''解析消息数据'''
        fs = self.node.FileSystem
        try:
            db =SQLiteParser.Database.FromNode(self.node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('tb_personal_message')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = self.account_id
                    message.deleted = rec.IsDeleted
                    message.send_time = self._get_timestamp(self._db_record_get_int_value(rec, 'timestamp'))
                    message.talker_id = self._db_record_get_int_value(rec, 'f_uid')
                    if not 'content' in rec or IsDBNull(rec['content'].Value):
                        continue
                    content = json.loads(rec['content'].Value)
                    if 'user' in content.keys():
                        userinfo = content['user']
                        userid = userinfo['userid'] if 'userid' in userinfo.keys() else None
                        nickname = userinfo['nickname'] if 'nickname' in userinfo.keys() else ''
                        message.talker_name = nickname
                        # birthday = userinfo['birthday'] if 'birthday' in userinfo.keys() else 0
                        # gender = userinfo['gender'] if 'gender' in userinfo.keys() else 0
                        # lng = userinfo['lng'] if 'lng' in userinfo.keys() else 0
                        # note = userinfo['notes'] if 'notes' in userinfo.keys() else ''
                        # age = userinfo['age']  if 'age' in userinfo.keys() else 0
                        # icon = userinfo['icon'] if 'icon' in userinfo.keys() else None
                        # lat = userinfo['lat'] if 'lat' in userinfo.keys() else 0
                    sendtype = self._db_record_get_int_value(rec, 'sendtype')
                    message.sender_id = self._db_record_get_int_value(rec, 'f_uid') if sendtype == 2 else self.account_id if sendtype == 1 else 0
                    message.sender_name = message.talker_name if sendtype == 2 else self.account_name if sendtype == 1 else ''
                    message.is_sender = 1 if sendtype == 1 else 0
                    message.msgid = self._db_record_get_int_value(rec, 'server_id')
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    msgtype = self._db_record_get_int_value(rec, 'message_type')
                    if msgtype == 1:  #普通文本
                        message.content = content['content'] if 'content' in content.keys() else ''
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif msgtype == 2:  #jpg
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    elif msgtype == 3:  #mp3
                        media_name = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                        if media_name is not None:
                            pardir = os.path.basename(os.path.dirname(media_name))
                            filename = os.path.basename(media_name)
                            nodes = fs.Search('/' + os.path.join(pardir, filename).replace('\\', '/') + '$')
                            if nodes is None:
                                message.content = '音频消息：' + filename
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            for node in nodes:
                                message.media_path = node.AbsolutePath
                                break
                    elif msgtype == 4:  #3gp
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                    elif msgtype == 5:  #地理位置
                        location = model_im.Location()
                        loc = content['attachment'] if 'attachment' in content.keys() else None
                        if loc is not None:
                            loc = loc.split(',')
                            longitude = float(loc[0])/1000000
                            latitude = float(loc[1])/1000000
                        message.location_id = location.location_id
                        message.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                        location.latitude = latitude
                        location.longitude = longitude
                        location.address = content['content'] if 'content' in content.keys() else None
                        location.timestamp = content['datetime'] if 'datetime' in content.keys() else 0
                        self.db_insert_table_location(location)
                    elif msgtype == 6:  #png
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    elif msgtype == 9:  #表情包
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    self.db_insert_table_message(message)
                except:
                    pass
            self.db_commit()
            ts = SQLiteParser.TableSignature('tb_group_message')
            dic = {}
            self.member_count = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = self.account_id
                    message.deleted = rec.IsDeleted
                    message.send_time = self._get_timestamp(self._db_record_get_int_value(rec, 'timestamp'))
                    message.talker_id = self._db_record_get_int_value(rec, 'groupid')
                    if not 'content' in rec or IsDBNull(rec['content'].Value):
                        continue
                    msginfo = json.loads(rec['content'].Value)
                    if 'user' in msginfo.keys():
                        #群好友信息
                        userinfo = msginfo['user']
                        userid = userinfo['userid'] if 'userid' in userinfo.keys() else None
                        if userid == self._db_record_get_int_value(rec, 'userid'):
                            message.is_sender = 1
                        else:
                            message.is_sender = 0
                        message.sender_id = userid
                        nickname = userinfo['nickname'] if 'nickname' in userinfo.keys() else ''
                        message.sender_name = nickname
                        birthday = userinfo['birthday'] if 'birthday' in userinfo.keys() else 0
                        gender = userinfo['gender'] if 'gender' in userinfo.keys() else 0
                        lng = userinfo['lng'] if 'lng' in userinfo.keys() else 0
                        age = userinfo['age'] if 'age' in userinfo.keys() else 0
                        icon = userinfo['icon'] if 'icon' in userinfo.keys() else None
                        lat = userinfo['lat'] if 'lat' in userinfo.keys() else 0
                        friend = model_im.Friend()
                        member = model_im.ChatroomMember()
                        flag = 0
                        friend.account_id = self.account_id
                        member.account_id = self.account_id
                        friend.friend_id = userid
                        member.member_id = userid
                        member.chatroom_id = message.talker_id
                        friend.fullname = nickname
                        member.display_name = nickname
                        friend.nickname = nickname
                        friend.photo = icon
                        member.photo = icon
                        friend.gender = 1 if gender == 'm' else 0
                        member.gender = friend.gender
                        friend.age = age
                        member.age = age
                        friend.source = self.node.AbsolutePath
                        member.source = self.node.AbsolutePath
                        location = model_im.Location()
                        friend.location_id = location.location_id
                        location.latitude = float(lat)/float(1000000)
                        location.longitude = float(lng)/float(1000000)
                        friend.type = model_im.FRIEND_TYPE_GROUP_FRIEND
                        if not userid in dic.keys():
                            dic[userid] = [rec.IsDeleted]
                        elif rec.IsDeleted not in dic[userid]:
                            dic[userid].append(rec.IsDeleted)
                        else:
                            flag = 1
                        if flag == 0:
                            self.db_insert_table_location(location)
                            self.db_insert_table_friend(friend)
                            self.db_insert_table_chatroom_member(member)
                            if member.chatroom_id not in self.member_count.keys():
                                self.member_count[member.chatroom_id] = 1
                            else:
                                self.member_count[member.chatroom_id] += 1
                    message.msg_id = self._db_record_get_int_value(rec, 'messageid')
                    msgtype = self._db_record_get_int_value(rec, 'message_type')
                    content = json.loads(self._db_record_get_string_value(rec, 'content'))
                    if msgtype == 1:  #普通文本
                        message.content = content['content'] if 'content' in content.keys() else ''
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif msgtype == 2:  #jpg
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    elif msgtype == 3:  #mp3
                        media_name = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                        if media_name is not None:
                            pardir = os.path.basename(os.path.dirname(media_name))
                            filename = os.path.basename(media_name)
                            nodes = fs.Search('/' + os.path.join(pardir, filename).replace('\\', '/') + '$')
                            if nodes is None:
                                message.content = '音频消息:' + filename
                                message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                            for node in nodes:
                                message.media_path = node.AbsolutePath
                                break
                    elif msgtype == 4:  #3gp
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                    elif msgtype == 5:  #地理位置
                        location = model_im.Location()
                        loc = content['attachment'] if 'attachment' in content.keys() else None
                        if loc is not None:
                            loc = loc.split(',')
                            latitude = float(loc[0])/1000000
                            longitude = float(loc[1])/1000000
                        message.location_id = location.location_id
                        message.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                        location.latitude = latitude
                        location.longitude = longitude
                        location.address = content['content'] if 'content' in content.keys() else None
                        location.timestamp = content['datetime'] if 'datetime' in content.keys() else 0
                        self.db_insert_table_location(location)
                    elif msgtype == 6:  #png
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    elif msgtype == 9:  #表情包
                        message.media_path = content['attachment'].replace('\\', '') if 'attachment' in content.keys() else None
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    message.talker_type = model_im.CHAT_TYPE_GROUP
                    self.db_insert_table_message(message)
                except:
                    pass
            #print(self.member_count)
            self.db_commit()
        except Exception as e:
            print(e)

    def parse_feed(self):
        '''
        解析动态数据
        安卓遇见本地无动态数据
        '''

    def parse_feed_comment(self):
        '''
        解析评论点赞数据
        安卓遇见本地无评论点赞数据
        '''

    def analyze_data(self):
        '''分析数据'''
        self.parse_account()
        self.parse_friend()
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

def analyze_android_iaround(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(IAroundParser(node, extractDeleted, extractSource).parse())
    pr.Build('遇见')
    return pr

def execute(node, extractDeleted):
    return analyze_android_iaround(node, extractDeleted, False)