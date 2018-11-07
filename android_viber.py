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

import System.Data.SQLite as SQLite
import model_callrecord
import model_im

import re
import hashlib
import shutil
import traceback

VERSION_APP_VALUE = 2

class ViberParser(model_im.IM, model_callrecord.MC):
    def __init__(self, node, extract_deleted, extract_source):
        self.node = node
        self.db = None
        self.im = model_im.IM()
        self.mc = model_callrecord.MC()
        self.cachepath = ds.OpenCachePath("Viber")
        md5_db = hashlib.md5()
        md5_rdb = hashlib.md5()
        db_name = self.node.AbsolutePath
        rdb_name = self.node.PathWithMountPoint
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        md5_rdb.update(rdb_name.encode(encoding = 'utf-8'))
        self.cachedb = self.cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
        self.recoverDB = self.cachepath + "\\" +md5_rdb.hexdigest().upper() + ".db"
        self.sourceDB = self.cachepath + '\\ViberSourceDB'

    def db_create_table(self):
        model_im.IM.db_create_table(self)
        model_callrecord.MC.db_create_table(self)

    def parse(self):
        if self.need_parse(self.cachedb, VERSION_APP_VALUE):
            if os.path.exists(self.cachepath):
                shutil.rmtree(self.cachepath)
            os.mkdir(self.cachepath)
            self.db_create(self.cachedb)
            self._copytocache(self.node.Parent.PathWithMountPoint)
            self.analyze_normal_data()
            self.analyze_deleted_data()
            self.db_insert_table_version(model_im.VERSION_KEY_DB, model_im.VERSION_VALUE_DB)
            self.db_insert_table_version(model_im.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.db_close()
        models = []
        models_im = model_im.GenerateModel(self.cachedb).get_models()
        models_callrecord = model_callrecord.Generate(self.cachedb).get_models()
        models.extend(models_im)
        models.extend(models_callrecord)
        return models

    def parse_account(self, node, dbPath, deleteFlag):
        '''解析账户数据(viber_messages)'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select _id, display_name, number from participants_info where _id = 1'''
            sr = db_cmd.ExecuteReader()
            try:
                while(sr.Read()):
                    account = model_im.Account()
                    account.account_id = self._db_reader_get_int_value(sr, 0)
                    account.telephone = self._db_reader_get_string_value(sr, 2)
                    account.username = self._db_reader_get_string_value(sr, 1)
                    account.source = node.AbsolutePath
                    account.deleted = deleteFlag
                    self.db_insert_table_account(account)
            except:
                traceback.print_exc()
            self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_friend(self, node, dbPath, deleteFlag):
        '''解析好友（联系人）数据'''
        db1 = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db1.Open()
        db_cmd1 = SQLite.SQLiteCommand(db1)
        if deleteFlag is 1:
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cachedb))
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if db1 is None:
                return
            try:
                db_cmd1.CommandText = '''select a.native_id, a.display_name, group_concat(b.data2) from phonebookcontact as a 
                    left join phonebookdata as b on a._id = b.contact_id group by a._id'''
                sr = db_cmd1.ExecuteReader()
                while (sr.Read()):
                    try:
                        friend = model_im.Friend()
                        if canceller.IsCancellationRequested:
                            break
                        friend.account_id = "1"
                        friend.deleted = deleteFlag
                        friend.friend_id = 'p' + str(sr[0]) if not IsDBNull(sr[0]) else 0
                        friend.nickname = self._db_reader_get_string_value(sr, 1)
                        friend.source = node.AbsolutePath
                        friend.telephone = self._db_reader_get_string_value(sr, 2)
                        friend.type = model_im.FRIEND_TYPE_FRIEND
                        self.db_insert_table_friend(friend)
                    except:
                        traceback.print_exc()
                sr.Close()
            except:
                pass
            db_cmd1.Dispose()
            db1.Close()
            db2 = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
            db2.Open()
            db_cmd2 = SQLite.SQLiteCommand(db2)
            try:
                db_cmd2.CommandText = '''select _id, display_name, number from participants_info'''
                sr = db_cmd2.ExecuteReader()
                while (sr.Read()):
                    try:
                        friend = model_im.Friend()
                        if canceller.IsCancellationRequested:
                            break
                        friend.account_id = "1"
                        friend.deleted = deleteFlag
                        friend.friend_id = self._db_reader_get_int_value(sr, 0)
                        friend.nickname = self._db_reader_get_string_value(sr, 1)
                        friend.source = self.node.AbsolutePath
                        friend.telephone = self._db_reader_get_string_value(sr, 2)
                        friend.type = model_im.FRIEND_TYPE_FRIEND
                        self.db_insert_table_friend(friend)
                    except:
                        traceback.print_exc()
                sr.Close()
            except:
                pass
            if deleteFlag is 0: 
                self.db_commit()
            db_cmd2.Dispose()
            db2.Close()
        except Exception as e:
            print(e)

    def parse_chatroom(self, dbPath, deleteFlag):
        '''解析群组数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        if deleteFlag is 1:
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cachedb))
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select _id, date, participant_id_1, participant_id_2, participant_id_3, participant_id_4, name from conversations where conversation_type = 1'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = "1"
                    chatroom.chatroom_id = self._db_reader_get_int_value(sr, 0)
                    chatroom.create_time = self._get_timestamp(sr[1]) if not IsDBNull(sr[1]) else 0
                    chatroom.deleted = deleteFlag
                    if sr[5] != 0:
                        count = 4
                    elif sr[4] != 0:
                        count = 3
                    elif sr[3] != 0:
                        count = 2
                    else:
                        count = 0
                    chatroom.member_count = count
                    chatroom.name = self._db_reader_get_string_value(sr, 6)
                    chatroom.source = self.node.AbsolutePath
                    self.db_insert_table_chatroom(chatroom)
                except:
                    traceback.print_exc()
            sr.Close()
            if deleteFlag is 0: 
                self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_chatroom_member(self, dbPath, deleteFlag):
        '''解析群组成员数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        if deleteFlag is 1:
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cachedb))
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
        fs = self.node.FileSystem
        try:
            if self.db is None:
                return
            db_cmd.CommandText = '''select a._id, a.date, a.participant_id_1, b.number, b.display_name, a.participant_id_2, c.number, c.display_name, 
            a.participant_id_3, d.number, d.display_name, a.participant_id_4, e.number, e.display_name, a.name from conversations as a 
            left join participants_info as b on a.participant_id_1 = b._id left join participants_info as c on a.participant_id_2 = c._id
            left join participants_info as d on a.participant_id_3 = d._id left join participants_info as e on a.participant_id_4 = e._id where conversation_type = 1'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    tags = [2, 5, 8, 11]
                    for tag in tags:
                        if IsDBNull(sr[tag]) or sr[tag] == 0:
                            break
                        chatroom_member = model_im.ChatroomMember()
                        chatroom_member.account_id = "1"
                        chatroom_member.chatroom_id = self._db_reader_get_int_value(sr, 0)
                        chatroom_member.deleted = deleteFlag
                        chatroom_member.display_name = self._db_reader_get_string_value(sr, tag+2)
                        chatroom_member.member_id = self._db_reader_get_int_value(sr, tag)
                        chatroom_member.source = self.node.AbsolutePath
                        chatroom_member.telephone = self._db_reader_get_string_value(sr, tag+1)
                        self.db_insert_table_chatroom_member(chatroom_member)
                except:
                    traceback.print_exc()
            sr.Close()
            if deleteFlag is 0: 
                self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_message(self, dbPath, deleteFlag):
        '''解析消息数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        if deleteFlag is 1:
            self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cachedb))
            self.db.Open()
            self.db_cmd = SQLite.SQLiteCommand(self.db)
        fs = self.node.FileSystem
        try:
            db_cmd.CommandText = '''select a.body, a.send_type, a.extra_uri, a._id, a.msg_date, 
                a.participant_id, b.display_name, a.send_type, a.conversation_type, a.conversation_id, 
                c.name from messages as a left join participants_info as b on a.participant_id = b._id 
                left join conversations as c on a.conversation_id = b._id'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = "1"
                    message.content = self._db_reader_get_string_value(sr, 0)
                    message.deleted =  deleteFlag
                    message.is_sender = self._db_reader_get_int_value(sr, 1)
                    test = sr[2]
                    if not IsDBNull(sr[2]) and isinstance(sr[2], str):
                        mediaPath = re.sub('.*/', '', sr[2])
                        nodes = fs.Search(mediaPath)
                        for node in nodes:
                            message.media_path = node.AbsolutePath
                            break
                    message.msg_id = self._db_reader_get_int_value(sr, 3)
                    message.send_time = self._get_timestamp(sr[4]) if not IsDBNull(sr[4]) else 0
                    message.sender_id = self._db_reader_get_int_value(sr, 5)
                    message.sender_name = self._db_reader_get_string_value(sr, 6)
                    message.source = self.node.AbsolutePath
                    message.status = model_im.MESSAGE_STATUS_SENT if sr[7] == 1 or sr[7] == 0 else model_im.MESSAGE_STATUS_UNSENT
                    message.talker_type = model_im.CHAT_TYPE_GROUP if sr[8] == 1 else model_im.CHAT_TYPE_OFFICIAL if sr[8] == 2 else model_im.CHAT_TYPE_FRIEND if sr[8] == 0 else model_im.CHAT_TYPE_SYSTEM
                    message.talker_id = self._db_reader_get_int_value(sr, 9)
                    message.talker_name = self._db_reader_get_string_value(sr, 10)
                    self.db_insert_table_message(message)
                except:
                    traceback.print_exc()
            sr.Close()
            if deleteFlag is 0: 
                self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)

    def parse_call_record(self, dbPath, deleteFlag):
        '''解析通话记录数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select a._id, a.date, a.duration, a.type, a.canonized_number, b.display_name 
                from messages_calls as a left join participants_info as b on a.canonized_number = b.number'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    record = model_callrecord.Records()
                    if canceller.IsCancellationRequested:
                        break
                    record.date = self._get_timestamp(sr[1]) if not IsDBNull(sr[1]) else 0
                    record.deleted = deleteFlag
                    record.duration = self._db_reader_get_int_value(sr, 2)
                    record.id = self._db_reader_get_int_value(sr, 0)
                    record.name = self._db_reader_get_string_value(sr, 5)
                    record.phone_number = self._db_reader_get_string_value(sr, 4)
                    record.source = self.node.AbsolutePath
                    record.type = self._db_reader_get_int_value(sr, 3)
                    self.db_insert_table_call_records(record)
                except:
                    traceback.print_exc()
            sr.Close()
            if deleteFlag is 0: 
                self.db_commit()
            db_cmd.Dispose()
            db.Close()
        except Exception as e:
            print(e)


    def analyze_normal_data(self):
        '''分析正常数据'''
        fs = self.node.FileSystem
        nodes = fs.Search('/viber_data$')
        for node in nodes:
            account_db = node.PathWithMountPoint
            break
        if account_db is not None:
            self.parse_friend(node, account_db, 0)
        db_path = self.node.PathWithMountPoint
        if db_path is not None:
            self.parse_account(node, db_path, 0)
            self.parse_friend(self.node, db_path, 0)
            self.parse_chatroom(db_path, 0)
            self.parse_chatroom_member(db_path, 0)
            self.parse_call_record(db_path, 0)
            self.parse_message(db_path, 0)

    def analyze_deleted_data(self):
        '''分析删除数据'''
        self.read_deleted_records()
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cachedb))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        if self.recoverDB is not None:
            self.parse_friend(self.node, self.recoverDB, 1)
            self.parse_chatroom(self.recoverDB, 1)
            self.parse_chatroom_member(self.recoverDB, 1)
            self.parse_call_record(self.recoverDB, 1)
            self.parse_message(self.recoverDB, 1)

    def read_deleted_records(self):
        '''获取删除数据保存至删除数据库'''
        self.create_deleted_db()

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans.Commit()

        self.read_deleted_table_participant()
        self.read_deleted_table_contact()
        self.read_deleted_table_contact_number()
        self.read_deleted_table_conversation()
        self.read_deleted_table_message()
        self.read_deleted_table_call()

        self.db_cmd.Dispose()
        self.db.Close()

    def create_deleted_db(self):
        '''创建删除数据库'''
        if os.path.exists(self.recoverDB):
            os.remove(self.recoverDB)
        db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        if db_cmd is not None:
            db_cmd.CommandText = '''create table if not exists participants_info
                (_id INTEGER, display_name TEXT, number TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists phonebookcontact
                (_id INTEGER, native_id INTEGER, display_name TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists phonebookdata
                (contact_id INTEGER, data2 TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists conversations
                (conversation_type INTEGER, _id INTEGER, date INTEGER, participant_id_1 INTEGER, participant_id_2 INTEGER, participant_id_3 INTEGER, participant_id_4 INTEGER, name TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists messages
                (body TEXT, send_type INTEGER, extra_uri TEXT, _id INTEGER, msg_date INTEGER, participant_id INTEGER, conversation_type INTEGER, conversation_id INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists messages_calls
                (_id INTEGER, date INTEGER, duration INTEGER, type INTEGER, canonized_number TEXT)'''
            db_cmd.ExecuteNonQuery()
        db_cmd.Dispose()
        db.Close()

    def read_deleted_table_participant(self):
        '''恢复参与者表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('participants_info')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['_id'].Value, rec['display_name'].Value, rec['number'].Value)
                    self.db_insert_to_deleted_table('''insert into participants_info(_id, display_name, number) values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_contact(self):
        '''恢复联系人表数据'''
        try:
            node = self.node.Parent.GetByPath('/viber_data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('phonebookcontact')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['_id'].Value, rec['native_id'].Value, rec['display_name'].Value)
                    self.db_insert_to_deleted_table('''insert into phonebookcontact(_id, native_id, display_name) values(?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_contact_number(self):
        '''恢复联系方式表数据'''
        try:
            node = self.node.Parent.GetByPath('/viber_data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('phonebookdata')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['contact_id'].Value, rec['data2'].Value)
                    self.db_insert_to_deleted_table('''insert into phonebookdata(contact_id, data2) values(?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_conversation(self):
        '''恢复会话表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('conversations')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['conversation_type'].Value, rec['_id'].Value, rec['date'].Value, rec['participant_id_1'].Value, rec['participant_id_2'].Value, rec['participant_id_3'].Value, rec['participant_id_4'].Value, rec['name'].Value)
                    self.db_insert_to_deleted_table('''insert into conversations(conversation_type, _id, date, participant_id_1, participant_id_2, participant_id_3, participant_id_4, name) values(?, ?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_message(self):
        '''恢复消息表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('messages')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['body'].Value, rec['send_type'].Value, rec['extra_uri'].Value, rec['_id'].Value, rec['msg_date'].Value, rec['participant_id'].Value, rec['send_type'].Value, rec['conversation_type'].Value, rec['conversation_id'].Value)
                    self.db_insert_to_deleted_table('''insert into messages(body, send_type, extra_uri, _id, msg_date, participant_id, send_type, conversation_type, conversation_id) values(?, ?, ?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_call(self):
        '''恢复通话记录表数据'''
        try:
            node = self.node
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('messages_calls')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['_id'].Value, rec['date'].Value, rec['duration'].Value, rec['type'].Value, rec['canonized_number'].Value)
                    self.db_insert_to_deleted_table('''insert into messages_calls(_id, date, duration, type, canonized_number) values(?, ?, ?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def db_insert_to_deleted_table(self, sql, values):
        '''插入数据到恢复数据库'''
        try:
            self.db_trans = self.db.BeginTransaction()
            if self.db_cmd is not None:
                self.db_cmd.CommandText = sql
                self.db_cmd.Parameters.Clear()
                for value in values:
                    param = self.db_cmd.CreateParameter()
                    param.Value = value
                    self.db_cmd.Parameters.Add(param)
                self.db_cmd.ExecuteNonQuery()
            self.db_trans.Commit()
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

def analyze_android_viber(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(ViberParser(node, extractDeleted, extractSource).parse())
    pr.Build('Viber')
    return pr

def execute(node, extractDeleted):
    return analyze_ios_viber(node, extractDeleted, False)