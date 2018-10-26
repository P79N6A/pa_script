#coding=utf-8
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
            self._copytocache(self.node.PathWithMountPoint)
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
        '''解析账户数据'''
        db = SQLite.SQLiteConnection('Data Source = {}; ReadOnly = True'.format(dbPath))
        db.Open()
        db_cmd = SQLite.SQLiteCommand(db)
        try:
            if db is None:
                return
            db_cmd.CommandText = '''select key, value from Data'''
            sr = db_cmd.ExecuteReader()
            account_id = "1"
            country = ''
            telephone = ''
            username = ''
            while (sr.Read()):
                try:
                    account = model_im.Account()
                    if canceller.IsCancellationRequested:
                        break
                    if sr[0] == '_myCountryCode':
                        country = sr[1]
                    elif sr[0] == '_myFormattedPhoneNumber':
                        telephone = sr[1]
                    elif sr[0] == '_myUserName':
                        username = sr[1]
                except:
                    pass
            sr.Close()
            try:
                account.account_id = account_id
                account.country = str(country) if not IsDBNull(country) else ''
                account.telephone = str(telephone) if not IsDBNull(telephone) else ''
                account.username = str(username) if not IsDBNull(username) else ''
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

    def parse_friend(self, dbPath, deleteFlag):
        '''解析好友（联系人）数据'''
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
            db_cmd.CommandText = '''select a.Z_PK, a.ZMAINNAME, a.ZSUFFIXNAME, group_concat(b.ZPHONE) 
                from ZABCONTACT as a left join ZABCONTACTNUMBER as b on a.Z_PK = b.ZCONTACT group by a.Z_PK '''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    friend = model_im.Friend()
                    if canceller.IsCancellationRequested:
                        break
                    friend.account_id = "1"
                    friend.deleted = deleteFlag
                    firstName = self._db_reader_get_string_value(sr, 1)
                    secondName = self._db_reader_get_string_value(sr, 2)
                    friend.friend_id = self._db_reader_get_int_value(sr, 0)
                    friend.nickname = firstName + secondName
                    friend.source = self.node.AbsolutePath
                    friend.telephone = self._db_reader_get_string_value(sr, 3)
                    self.db_insert_table_friend(friend)
                except:
                    traceback.print_exc()
            sr.Close()
            if deleteFlag is 0: 
                self.db_commit()
            db_cmd.Dispose()
            db.Close()
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
            db_cmd.CommandText = '''select Z_PK, ZCONTEXT, ZDATE, ZROLE, ZNAME from ZCONVERSATION where ZCONTEXT is not null'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom = model_im.Chatroom()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom.account_id = "1"
                    chatroom.chatroom_id = self._db_reader_get_int_value(sr, 0)
                    #chatroom.create_time = sr[2]  #未能识别为有效的DateTime
                    chatroom.deleted = deleteFlag
                    chatroom.member_count = self._db_reader_get_int_value(sr, 3) + 1
                    chatroom.name = self._db_reader_get_string_value(sr, 4)
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
            db_cmd.CommandText = '''select a.Z_PK, b.ZPHONENUMBERINDEX, c.ZDISPLAYFULLNAME, c.ZICONID, d.ZPHONE 
                from ZCONVERSATION as a left join ZCONVERSATIONROLE as b on a.Z_PK = b.ZCONVERSATION left join ZMEMBER as c on b.ZPHONENUMBERINDEX = c.Z_PK 
                    left join ZPHONENUMBER as d on b.ZPHONENUMBERINDEX = d.ZMEMBER where a.ZCONTEXT is not null'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    chatroom_member = model_im.ChatroomMember()
                    if canceller.IsCancellationRequested:
                        break
                    chatroom_member.account_id = "1"
                    chatroom_member.chatroom_id = self._db_reader_get_int_value(sr, 0)
                    chatroom_member.deleted = deleteFlag
                    chatroom_member.display_name = self._db_reader_get_string_value(sr, 2)
                    chatroom_member.member_id = self._db_reader_get_int_value(sr, 1)
                    photoPath = ''
                    if not IsDBNull(sr[3]):
                        nodes = fs.Search(self._db_reader_get_string_value(sr, 3))
                        for node in nodes:
                            photoPath = node.AbsolutePath
                            break
                    chatroom_member.photo = photoPath
                    chatroom_member.source = self.node.AbsolutePath
                    chatroom_member.telephone = self._db_reader_get_string_value(sr, 4)
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
            if db is None:
                return
            db_cmd.CommandText = '''select a.Z_PK, a.ZTEXT, a.ZSTATE, a.ZLOCATION, b.ZNAME, a.ZDATE, a.ZPHONENUMINDEX,
                 a.ZCONVERSATION, c.ZDISPLAYFULLNAME from ZVIBERMESSAGE as a left join ZATTACHMENT as b 
                 on a.ZATTACHMENT = b.Z_PK left join ZMEMBER as c on a.ZPHONENUMINDEX = c.Z_PK'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    message = model_im.Message()
                    if canceller.IsCancellationRequested:
                        break
                    message.account_id = "1"
                    message.content = self._db_reader_get_string_value(sr, 1)
                    message.deleted =  deleteFlag
                    message.is_sender = 1 if sr[2] is not 'received' else 0
                    mediaPath = ''
                    if not IsDBNull(sr[4]):
                        aa = sr[4]
                        nodes = fs.Search(self._db_reader_get_string_value(sr, 4))
                        for node in nodes:
                            mediaPath = node.AbsolutePath
                            break
                    message.media_path = mediaPath
                    message.msg_id = self._db_reader_get_int_value(sr, 0)
                    #message.send_time = sr[5]
                    message.sender_id = self._db_reader_get_int_value(sr, 6)
                    message.sender_name = self._db_reader_get_string_value(sr, 8)
                    message.source = self.node.AbsolutePath
                    message.status = model_im.MESSAGE_STATUS_SENT if sr[2] is 'send' or sr[2] is 'dilivered' else model_im.MESSAGE_STATUS_UNSENT if sr[2] is 'pending' or sr[2] is 'pendingNotSent' else model_im.MESSAGE_STATUS_DEFAULT
                    message.talker_id = self._db_reader_get_int_value(sr, 7)
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

    def parse_location(self, dbPath, deleteFlag):
        '''解析地理位置数据'''
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
            db_cmd.CommandText = '''select Z_PK, ZMESSAGE, ZDATE, ZLATITUDE, ZLONGITUDE, ZADDRESS from ZVIBERLOCATION'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    location = model_im.Location()
                    if canceller.IsCancellationRequested:
                        break
                    location.address = self._db_reader_get_string_value(sr, 5)
                    location.deleted = deleteFlag
                    location.latitude = sr[3]
                    location.location_id = self._db_reader_get_int_value(sr, 0)
                    location.longitude = sr[4]
                    location.source = self.node.AbsolutePath
                    #location.timestamp = sr[2]  #该字符串未被识别为有效的 DateTime
                    self.db_insert_table_location(location)
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
            db_cmd.CommandText = '''select a.Z_PK, a.ZDATE, a.ZDURATION, a.ZCALLTYPE, b.ZPHONENUMBER, c.ZDISPLAYFULLNAME 
                from ZRECENT as a left join ZRECENTSLINE as b on a.ZRECENTSLINE = b.Z_PK left join ZMEMBER as c on b.ZPHONENUMINDEX = c.Z_PK'''
            sr = db_cmd.ExecuteReader()
            while (sr.Read()):
                try:
                    record = model_callrecord.Records()
                    if canceller.IsCancellationRequested:
                        break
                    #record.date = sr[1]
                    record.deleted = deleteFlag
                    record.duration = self._db_reader_get_int_value(sr, 2)
                    record.id = self._db_reader_get_int_value(sr, 0)
                    record.name = self._db_reader_get_string_value(sr, 5)
                    record.phone_number = self._db_reader_get_string_value(sr, 4)
                    record.source = self.node.AbsolutePath
                    callType = 0
                    if not IsDBNull(sr[3]):
                        if re.match('incoming', str(sr[3])):
                            callType = 1
                        elif re.match('outgoing', str(sr[3])):
                            callType = 2
                        elif re.match('missed', str(sr[3])):
                            callType = 3
                    record.type = callType
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
        nodes = fs.Search('/Documents/Settings.data$')
        for node in nodes:
            account_db = node.PathWithMountPoint
            break
        if account_db is not None:
            self.parse_account(node, account_db, 0)
        db_path = self.sourceDB + '\\Contacts.data'
        if db_path is not None:
            self.parse_friend(db_path, 0)
            self.parse_chatroom(db_path, 0)
            self.parse_chatroom_member(db_path, 0)
            self.parse_location(db_path, 0)
            self.parse_call_record(db_path, 0)
            self.parse_message(db_path, 0)

    def analyze_deleted_data(self):
        '''分析删除数据'''
        self.read_deleted_records()
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.cachedb))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        if self.recoverDB is not None:
            self.parse_friend(self.recoverDB, 1)
            self.parse_chatroom(self.recoverDB, 1)
            self.parse_chatroom_member(self.recoverDB, 1)
            self.parse_location(self.recoverDB, 1)
            self.parse_call_record(self.recoverDB, 1)
            self.parse_message(self.recoverDB, 1)

    def read_deleted_records(self):
        '''获取删除数据保存至删除数据库'''
        self.create_deleted_db()

        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.recoverDB))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans.Commit()

        self.read_deleted_table_contact()
        self.read_deleted_table_contact_number()
        self.read_deleted_table_conversation()
        self.read_deleted_table_conversation_role()
        self.read_deleted_table_member()
        self.read_deleted_table_phonenumber()
        self.read_deleted_table_message()
        self.read_deleted_table_attachment()
        self.read_deleted_table_location()
        self.read_deleted_table_recent()
        self.read_deleted_table_recentline()

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
            db_cmd.CommandText = '''create table if not exists ZABCONTACT
                (Z_PK INTEGER, ZMAINNAME TEXT, ZSUFFIXNAME TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZABCONTACTNUMBER
                (ZCONTACT INTEGER, ZPHONE TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZCONVERSATION
                (Z_PK INTEGER, ZCONTEXT TEXT, ZDATE INTEGER, ZROLE INTEGER, ZNAME TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZCONVERSATIONROLE
                (ZCONVERSATION INTEGER, ZPHONENUMBERINDEX INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZMEMBER
                (Z_PK INTEGER, ZDISPLAYFULLNAME TEXT, ZICONID TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZPHONENUMBER
                (ZMEMBER INTEGER, ZPHONE TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZVIBERMESSAGE
                (Z_PK INTEGER, ZTEXT TEXT, ZSTATE TEXT, ZLOCATION INTEGER, ZDATE INTEGER, ZPHONENUMINDEX INTEGER, ZCONVERSATION INTEGER, ZATTACHMENT INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZATTACHMENT
                (Z_PK INTEGER, ZNAME TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZVIBERLOCATION
                (Z_PK INTEGER, ZMESSAGE INTEGER, ZDATE INTEGER, ZLATITUDE FLOAT, ZLONGITUDE FLOAT, ZADDRESS TEXT)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZRECENT
                (Z_PK INTEGER, ZDATE INTEGER, ZDURATION INTEGER, ZCALLTYPE TEXT, ZRECENTSLINE INTEGER)'''
            db_cmd.ExecuteNonQuery()
            db_cmd.CommandText = '''create table if not exists ZRECENTSLINE
                (Z_PK INTEGER, ZPHONENUMBER TEXT, ZPHONENUMINDEX INTEGER)'''
            db_cmd.ExecuteNonQuery()
        db_cmd.Dispose()
        db.Close()

    def read_deleted_table_contact(self):
        '''恢复联系人表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZABCONTACT')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZMAINNAME'].Value, rec['ZSUFFIXNAME'].Value)
                    self.db_insert_to_deleted_table('''insert into ZABCONTACT(Z_PK, ZMAINNAME, ZSUFFIXNAME) values(?, ?, ?)''', param)
                except:
                    traceback.print_exc()
        except Exception as e:
            print(e)

    def read_deleted_table_contact_number(self):
        '''恢复联系方式表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZABCONTACTNUMBER')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['ZCONTACT'].Value, rec['ZPHONE'].Value)
                    self.db_insert_to_deleted_table('''insert into ZABCONTACTNUMBER(ZCONTACT, ZPHONE) values(?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_conversation(self):
        '''恢复会话表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZCONVERSATION')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZCONTEXT'].Value, rec['ZDATE'].Value, rec['ZROLE'].Value, rec['ZNAME'].Value)
                    self.db_insert_to_deleted_table('''insert into ZCONVERSATION(Z_PK, ZCONTEXT, ZDATE, ZROLE, ZNAME) values(?, ?, ?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_conversation_role(self):
        '''恢复会话联系人表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZCONVERSATIONROLE')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['ZCONVERSATION'].Value, rec['ZPHONENUMBERINDEX'].Value)
                    self.db_insert_to_deleted_table('''insert into ZCONVERSATIONROLE(ZCONVERSATION, ZPHONENUMBERINDEX) values(?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_member(self):
        '''恢复会话成员表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZMEMBER')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZDISPLAYFULLNAME'].Value, rec['ZICONID'].Value)
                    self.db_insert_to_deleted_table('''insert into ZMEMBER(Z_PK, ZDISPLAYFULLNAME, ZICONID) values(?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_phonenumber(self):
        '''恢复通话记录电话表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZPHONENUMBER')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['ZMEMBER'].Value, rec['ZPHONE'].Value)
                    self.db_insert_to_deleted_table('''insert into ZPHONENUMBER(ZMEMBER, ZPHONE) values(?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_message(self):
        '''恢复消息表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZVIBERMESSAGE')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZTEXT'].Value, rec['ZSTATE'].Value, rec['ZLOCATION'].Value, rec['ZDATE'].Value, rec['ZPHONENUMINDEX'].Value, rec['ZCONVERSATION'].Value, rec['ZATTACHMENT'].Value, rec['ZPHONENUMINDEX'].Value)
                    self.db_insert_to_deleted_table('''insert into ZVIBERMESSAGE(Z_PK, ZTEXT, ZSTATE, ZLOCATION, ZDATE, ZPHONENUMINDEX, ZCONVERSATION, ZATTACHMENT, ZPHONENUMINDEX) values(?, ?, ?, ?, ?, ?, ?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_attachment(self):
        '''恢复附件表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZATTACHMENT')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZNAME'].Value)
                    self.db_insert_to_deleted_table('''insert into ZATTACHMENT(Z_PK, ZNAME) values(?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_location(self):
        '''恢复位置表数据'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZVIBERLOCATION')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZMESSAGE'].Value, rec['ZDATE'].Value, rec['ZLATITUDE'].Value, rec['ZLONGITUDE'].Value, rec['ZADDRESS'].Value)
                    self.db_insert_to_deleted_table('''insert into ZVIBERLOCATION(Z_PK, ZMESSAGE, ZDATE, ZLATITUDE, ZLONGITUDE, ZADDRESS) values(?, ?, ?, ?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_recent(self):
        '''恢复通话记录1'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZRECENT')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZDATE'].Value, rec['ZDURATION'].Value, rec['ZCALLTYPE'].Value, rec['ZRECENTSLINE'].Value)
                    self.db_insert_to_deleted_table('''insert into ZRECENT(Z_PK, ZDATE, ZDURATION, ZCALLTYPE, ZRECENTSLINE) values(?, ?, ?, ?, ?)''', param)
                except:
                    pass
        except Exception as e:
            print(e)

    def read_deleted_table_recentline(self):
        '''恢复通话记录2'''
        try:
            node = self.node.GetByPath('/Contacts.data')
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            ts = SQLiteParser.TableSignature('ZRECENTSLINE')
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    param = (rec['Z_PK'].Value, rec['ZPHONENUMBER'].Value, rec['ZPHONENUMINDEX'].Value)
                    self.db_insert_to_deleted_table('''insert into ZRECENTSLINE(Z_PK, ZPHONENUMBER, ZPHONENUMINDEX) values(?, ?, ?)''', param)
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
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, int) and len(str(timestamp)) == 10:
                return timestamp
        except:
            return None

def analyze_ios_viber(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(ViberParser(node, extractDeleted, extractSource).parse())
    pr.Build('Viber')
    return pr

def execute(node, extractDeleted):
    return analyze_ios_viber(node, extractDeleted, False)