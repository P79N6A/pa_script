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
    
    def parse_account(self, dbPath):
        '''解析账户数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = """select * from cfurl_cache_receiver_data where receiver_data like '%nickname%'"""
            sr = db_cmd.ExecuteReader()
            try:
                if not sr.HasRows:
                    account = model_im.Account()
                    account.account_id = '未知用户'
                    account.username = '未知用户'
                while(sr.Read()):
                    account = model_im.Account()
                    account.source = self.node.AbsolutePath
                    account_data = json.loads(json.dumps(self._db_reader_get_string_value(sr, 2).replace('\\', '').replace('\"', "'").replace('"', '')))
                    account_dic = json.loads(account_data.replace("'", '"'))
                    account.account_id = account_dic['nickname']
                    account.username = account_dic['nickname']
                    account.nickname = account_dic['nickname']
                    account.photo = account_dic['headimgurl']
                    account.gender = 1 if account_dic['sex'] == 1 else 0
                    account.country = account_dic['country']
                    account.province = account_dic['province']
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
            db_cmd.CommandText = '''select otheruid, icon, relation, nickname, myuid, gender, sign, deleted from DBTableName_UserRelation'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    friend.account_id = self.account_id
                    friend.deleted = self._db_reader_get_int_value(sr, 7)
                    friend.friend_id = self._db_reader_get_int_value(sr, 0)
                    friend.fullname = self._db_reader_get_string_value(sr, 3)
                    friend.nickname = self._db_reader_get_string_value(sr, 3)
                    friend.photo = self._db_reader_get_string_value(sr, 1)
                    friend.gender = self._db_reader_get_int_value(sr, 5)
                    friend.source = self.node.AbsolutePath
                    friend.signature = self._db_reader_get_string_value(sr, 6)
                    relation = self._db_reader_get_int_value(sr, 2)
                    friend.type = model_im.FRIEND_TYPE_FRIEND if relation == 1 else model_im.FRIEND_TYPE_FOLLOW if relation == 3 else model_im.FRIEND_TYPE_FANS if relation == 2 else model_im.FRIEND_TYPE_NONE
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
            db_cmd.CommandText = '''select groupname, myuid, groupicon, groupId, deleted from DBTableNmae_GroupMsg'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = self.account_id
                    chatroom.chatroom_id = self._db_reader_get_int_value(sr, 3)
                    chatroom.photo = self._db_reader_get_string_value(sr, 2)
                    chatroom.deleted = self._db_reader_get_int_value(sr, 4)
                    chatroom.name = self._db_reader_get_string_value(sr, 0)
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

    def parse_message(self, dbPath):
        '''解析消息数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        fs = self.node.FileSystem
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select a.time, a.otheruid, a.msgid, a.msgstate, a.attachment, a.distance, 
            a.myuid, a.content, a.msgtype, a.deleted, b.nickname from DBTableName_P2PMsg as a left join 
            DBTableName_UserRelation as b on a.otheruid = b.otheruid'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = self.account_id
                    message.deleted = self._db_reader_get_int_value(sr, 9)
                    message.send_time = self._get_timestamp(self._db_reader_get_int_value(sr, 0))
                    message.talker_id = self._db_reader_get_int_value(sr, 1)
                    message.talker_name = self._db_reader_get_string_value(sr, 10)
                    msgstate = self._db_reader_get_int_value(sr, 3)
                    message.sender_id = self._db_reader_get_int_value(sr, 1) if msgstate == 1 else self.account_id if msgstate == 2 else 0
                    message.sender_name = self._db_reader_get_string_value(sr, 10) if msgstate == 1 else self.account_name if msgstate == 2 else ''
                    message.is_sender = 1 if msgstate == 2 else 0
                    message.msgid = self._db_reader_get_int_value(sr, 2)
                    message.talker_type = model_im.CHAT_TYPE_FRIEND
                    msgtype = self._db_reader_get_int_value(sr, 8)
                    if msgtype == 1:  #普通文本
                        message.content = self._db_reader_get_string_value(sr, 7)
                        message.type = model_im.MESSAGE_CONTENT_TYPE_TEXT
                    elif msgtype == 2:  #jpg
                        message.media_path = self._db_reader_get_string_value(sr, 4)
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
                    elif msgtype == 3:  #mp3
                        media_name = self._db_reader_get_string_value(sr, 4)
                        message.type = model_im.MESSAGE_CONTENT_TYPE_VOICE
                        nodes = fs.Search(media_name)
                        for node in nodes:
                            message.media_path = node.AbsolutePath
                            break
                    elif msgtype == 4:  #3gp
                        message.media_path = self._db_reader_get_string_value(sr, 4)
                        message.type = model_im.MESSAGE_CONTENT_TYPE_VIDEO
                    elif msgtype == 5:  #地理位置
                        location = model_im.Location()
                        loc = self._db_reader_get_string_value(sr, 4)
                        loc = loc.split(',')
                        longitude = float(loc[0])/1000000
                        latitude = float(loc[1])/1000000
                        message.location_id = location.location_id
                        message.type = model_im.MESSAGE_CONTENT_TYPE_LOCATION
                        location.latitude = latitude
                        location.longitude = longitude
                        location.address = self._db_reader_get_string_value(sr, 7)
                        self.db_insert_table_location(location)
                    elif msgtype == 6:  #png
                        message.media_path = self._db_reader_get_string_value(sr, 4)
                        message.type = model_im.MESSAGE_CONTENT_TYPE_IMAGE
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
            db_cmd.CommandText = '''select address, userid, sendTime, nickname, icon, myuid, likecount, datetime, reviewcount, 
                gender, dynamicid, sendPhotos, content, deleted from DBTableName_MySendDynamic'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    feed = model_im.Feed()
                    if canceller.IsCancellationRequested:
                        break
                    feed.account_id = self.account_id
                    feed.sender_id = self._db_reader_get_int_value(sr, 1)
                    feed.send_time = self._get_timestamp(self._db_reader_get_int_value(sr, 2))
                    feed.deleted = self._db_reader_get_int_value(sr, 13)
                    feed.likecount = self._db_reader_get_int_value(sr, 6)
                    feed.content = self._db_reader_get_string_value(sr, 12)
                    feed.commentcount = self._db_reader_get_int_value(sr, 8)
                    if not IsDBNull(sr[11]):
                        mediapathes = Encoding.UTF8.GetString(sr[11])
                    feed.image_path = mediapathes
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
            db_cmd.CommandText = ''''''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    feed_like = model_im.FeedLike()
                    feed_comment = model_im.FeedComment()
                    if canceller.IsCancellationRequested:
                        break
                    pass
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
            self.parse_message(db_path)
            self.parse_feed(db_path)

    def read_deleted_records(self):
        '''获取删除数据保存至删除数据库'''
        self.create_deleted_db()
        
        self.rdb = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.rdb.Open()
        self.rdb_cmd = SQLite.SQLiteCommand(self.rdb)

        fs = self.node.FileSystem
        nodes = fs.Search('/cn.zoega.iAround/Cache.db$')
        for node in nodes:
            self.fnode = node
            break

        self.read_deleted_table_account()
        self.read_deleted_table_user()
        self.read_deleted_table_groupmsg()
        self.read_deleted_table_p2pmsg()
        self.read_deleted_table_feed()

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
            db_cmd.CommandText = '''create table if not exists cfurl_cache_receiver_data
                (entry_ID INTEGER, IsDataOnFS INTEGER, receiver_data BLOB, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists DBTableName_UserRelation
                (otheruid INTEGER, icon TEXT, relation INTEGER, nickname TEXT, myuid INTEGER, gender INTEGER, sign TEXT, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists DBTableNmae_GroupMsg
                (groupname TEXT, myuid INTEGER, groupicon TEXT, groupId INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists DBTableName_P2PMsg
                (time INTEGER, otheruid INTEGER, msgid INTEGER, msgstate INTEGER, attachment TEXT, distance INTEGER, myuid INTEGER, content TEXT, msgtype INTEGER, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists DBTableName_MySendDynamic
                (address TEXT, userid INTEGER, sendTime INTEGER, nickname TEXT, icon TEXT, myuid INTEGER, likecount INTEGER, datetime INTEGER, reviewcount INTEGER, gender INTEGER, dynamicid INTEGER, sendPhotos BLOB, content TEXT, deleted INTEGER)'''
            db_cmd.ExecuteNonQuery()
        db_cmd.Dispose()
        db.Close()

    def read_deleted_table_account(self):
        '''恢复账号数据'''
        try:
            node = self.fnode
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('cfurl_cache_receiver_data')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not rec.IsDeleted:
                        param = (rec['entry_ID'].Value, rec['isDataOnFS'].Value, rec['receiver_data'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into cfurl_cache_receiver_data(entry_ID, isDataOnFS, receiver_data, deleted) values(?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except:
            traceback.print_exc()

    def read_deleted_table_user(self):
        '''恢复用户表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('DBTableName_UserRelation')
            dic = {}
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['otheruid'].Value) and rec['otheruid'].Value != 0:
                        if not rec['otheruid'].Value in dic.keys():
                            dic[rec['otheruid'].Value] = [rec.IsDeleted]
                        elif rec.IsDeleted not in dic[rec['otheruid'].Value]:
                            dic[rec['otheruid'].Value].append(rec.IsDeleted)
                        else:
                            continue
                        param = (rec['otheruid'].Value, rec['icon'].Value, rec['relation'].Value, rec['nickname'].Value, rec['myuid'].Value, rec['gender'].Value, rec['sign'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into DBTableName_UserRelation(otheruid, icon, relation, nickname, myuid, gender, sign, deleted) values(?, ?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_groupmsg(self):
        '''恢复聊天室数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('DBTableNmae_GroupMsg')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if rec['groupId'].Value != 0 and not IsDBNull(rec['groupId'].Value) and not (IsDBNull(rec['groupname'].Value) and IsDBNull(rec['groupicon'].Value)):
                        param = (rec['groupname'].Value, rec['myuid'].Value, rec['groupicon'].Value, rec['groupId'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into DBTableNmae_GroupMsg(groupname, myuid, groupicon, groupId, deleted) values(?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_p2pmsg(self):
        '''恢复聊天数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('DBTableName_P2PMsg')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['msgid'].Value) and rec['msgid'].Value != 0:
                        param = (rec['time'].Value, rec['otheruid'].Value, rec['msgid'].Value, rec['msgstate'].Value, rec['attachment'].Value, rec['distance'].Value, rec['myuid'].Value, rec['content'].Value, rec['msgtype'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into DBTableName_P2PMsg(time, otheruid, msgid, msgstate, attachment, distance, myuid, content, msgtype, deleted) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_feed(self):
        '''恢复动态数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('DBTableName_MySendDynamic')
            for rec in db.ReadTableRecords(ts, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if not IsDBNull(rec['userid'].Value) and rec['userid'].Value != 0:
                        param = (rec['address'].Value, rec['userid'].Value, rec['sendTime'].Value, rec['nickname'].Value, rec['icon'].Value, rec['myuid'].Value, 
                                 rec['likecount'].Value, rec['datetime'].Value, rec['reviewcount'].Value, rec['gender'].Value, rec['dynamicid'].Value, rec['sendPhotos'].Value, rec['content'].Value, rec.IsDeleted)
                        self.db_insert_to_deleted_table('''insert into DBTableName_MySendDynamic(address, userid, sendTime, nickname, icon, myuid, 
                likecount, datetime, reviewcount, gender, dynamicid, sendPhotos, content, deleted) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', param)
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
            if isinstance(timestamp, (long, float, str, Int64)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
                return timestamp
        except:
            return None

def analyze_apple_iaround(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(IAroundParser(node, extractDeleted, extractSource).parse())
    pr.Build('遇见')
    return pr

def execute(node, extractDeleted):
    return analyze_apple_iaround(node, extractDeleted, False)