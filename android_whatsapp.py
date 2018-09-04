#coding=utf-8
from PA_runtime import *
import PA_runtime
import model_im
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

import os
import System
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from collections import defaultdict
from System.Data.SQLite import *
import shutil


def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def GetInt64(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def GetBlob(reader, idx):
    return reader.GetValue(idx) if not reader.IsDBNull(idx) else None

def GetFloat(reader, idx):
    return reader.GetFloat(idx) if not reader.IsDBNull(idx) else 0

def moveFileto(sourceDir,  targetDir): 
    shutil.copy(sourceDir,  targetDir)

def get_all_files(path, dicts):
    if os.path.isdir(path):
        results = os.listdir(path)
        for i in results:
            path_extend = path + '/' + i
            if os.path.isdir(path_extend):
                get_all_files(path_extend, dicts)
            else:
                dicts[i] = path+'/'+i

class WhatsApp(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.account_id = None
        self.account_pushname = None
        self.whatsapp = model_im.IM()
        self.cache = ds.OpenCachePath("whatsapp")
        self.contacts_dicts = {}
        self.groups_dicts = {}
        

    def get_account(self):
        account = model_im.Account()
        account_node =  self.root.GetByPath("/shared_prefs/com.whatsapp_preferences.xml")
        if account_node is None:
            return
        es = []
        try:
            account_node.Data.seek(0)
            xml = XElement.Parse(account_node.read())
            es = xml.Elements("string")
        except Exception as e:
            print e
        for rec in es:
            if rec.Attribute('name') and rec.Attribute('name').Value == 'push_name':
                name = rec.FirstNode.Value
                account.nickname = name.decode('utf-8')
                self.account_pushname = name.decode('utf-8')
            if rec.Attribute('name') and rec.Attribute('name').Value == 'registration_jid':
                account.account_id =  rec.FirstNode.Value + "@s.whatsapp.net"
                self.account_id = account.account_id
                account.telephone = rec.FirstNode.Value
            if rec.Attribute('name') and rec.Attribute('name').Value == 'my_current_status':
                account.signature = rec.FirstNode.Value
        try:
            if self.account_id is None:
                self.account_id = "unknown"
                account.account_id = self.account_id
                account.source = "whatsapp:" + self.account_id
            if self.account_pushname is None:
                self.account_pushname = "unknown"
            self.whatsapp.db_insert_table_account(account)
        except Exception as e:
            pass
        self.whatsapp.db_commit()


    def _get_friends_groups_id(self):
        friends_path = self.root.GetByPath("/databases/msgstore.db")
        wal_path = self.root.GetByPath("/databases/msgstore.db-wal")
        friends = [] 
        groups = []
        if friends_path is None:
            return
        if wal_path is None:
            connection = System.Data.SQLite.SQLiteConnection('Data Source = {0}; ReadOnly = True'.format(friends_path))
        if wal_path is not None:
            moveFileto(friends_path.PathWithMountPoint, self.cache)
            moveFileto(wal_path.PathWithMountPoint, self.cache)
            tmp_path = self.cache+"/msgstore.db"
            connection = System.Data.SQLite.SQLiteConnection('Data Source = {0}'.format(tmp_path))
        try:
            connection.Open()
            cmd = System.Data.SQLite.SQLiteCommand(connection)
            cmd.CommandText = '''
                select key_remote_jid, subject from chat_list where subject is null
            '''
            reader = cmd.ExecuteReader()
            while reader.Read():
                row = GetString(reader, 0)
                if row.find("-") == -1:
                    friends.append(row)
                else:
                    groups.append(row)

            cmd.Dispose()
            cmd.CommandText = '''
                select key_remote_jid, subject from chat_list where subject is not null
            '''
            g_results = cmd.ExecuteReader()
            while g_results.Read():
                group_id = GetString(g_results, 0)
                groups.append(group_id)
                self.groups_dicts[group_id] = GetString(g_results, 1)
        except Exception as e:
            print(e)
        if connection != None:
            connection.Close()
        return friends,groups

    def get_friends(self):
        friends, groups = self._get_friends_groups_id()
        contacts_path = self.root.GetByPath("/databases/wa.db")
        wal_path = self.root.GetByPath("/databases/wa.db-wal")
        if contacts_path is None:
            return
        if wal_path is None:
             connection = System.Data.SQLite.SQLiteConnection('Data Source = {0}; ReadOnly = True'.format(contacts_path.PathWithMountPoint))
        else:
            moveFileto(contacts_path.PathWithMountPoint, self.cache)
            moveFileto(wal_path.PathWithMountPoint, self.cache)
            tmp_path = self.cache + "/wa.db"
            connection = System.Data.SQLite.SQLiteConnection('Data Source = {0}'.format(tmp_path))
        try:
            connection.Open()
            cmd = System.Data.SQLite.SQLiteCommand(connection)
            cmd.CommandText = '''
                select * from wa_contacts where is_whatsapp_user = 1
            '''
            reader = cmd.ExecuteReader()
            if reader:
                while reader.Read():
                    friend_id = GetString(reader, 1)
                    if friend_id.find("net") != -1 and friend_id in friends:
                        friend = model_im.Friend()
                        friend.account_id = self.account_id
                        friend.friend_id = friend_id
                        friend.nickname = GetString(reader, 7)
                        self.contacts_dicts[friend_id] = GetString(reader, 7)
                        friend.telephone = GetString(reader, 5)
                        friend.signature = GetString(reader, 3)
                        try:
                            self.whatsapp.db_insert_table_friend(friend)
                        except Exception as e:
                            pass
            else:
                friend.account_id = self.account_id
                for i in friends:
                    friend.friend_id = i
                    friend.nickname = "unknown"
                    try:
                        self.whatsapp.db_insert_table_friend(friend)
                    except Exception as e:
                        pass
        except Exception as e:
            pass
        if connection != None:
            connection.Close()
        self.whatsapp.db_commit()

    def get_groups(self):
        chatroom_path = self.root.GetByPath("/databases/msgstore.db")
        wal_path = self.root.GetByPath("/databases/msgstore.db-wal")
        if chatroom_path is None:
            return
        if wal_path is None:
            path_msg = self.root.GetByPath("/databases/msgstore.db").PathWithMountPoint
        else:
            path_msg = self.cache + "/msgstore.db"

        friends, groups = self._get_friends_groups_id()
        conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}".format(path_msg))
        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = '''
                select key_remote_jid,count(jid) as count from chat_list left join group_participants 
                where chat_list.key_remote_jid = group_participants.gjid and subject is not null group by key_remote_jid
            '''
            reader = cmd.ExecuteReader()
            dicts = {}
            while reader.Read():
                group_id = GetString(reader, 0)
                group_count = GetInt64(reader, 1)
                dicts[group_id] = group_count
            
            cmd.Dispose()
            cmd.CommandText = """
                select key_remote_jid as group_id , subject as title, sort_timestamp as create_time, jid as member_id , admin from chat_list left join 
                group_participants where chat_list.key_remote_jid = group_participants.gjid and admin = 2
            """
            reader = cmd.ExecuteReader()
            while reader.Read():
                chatroom = model_im.Chatroom()
                chatroom.account_id = self.account_id
                chatroom.chatroom_id = GetString(reader, 0)
                chatroom.name = GetString(reader, 1)
                chatroom.create_time = int(str(GetInt64(reader, 2))[:-3])
                create_person = GetString(reader, 0)[:13] +"@s.whatsapp.net"
                chatroom.creator_id = create_person
                if GetString(reader, 3):
                    chatroom.owner_id = GetString(reader, 3)
                else:
                    chatroom.owner_id = self.account_id
                chatroom.member_count = dicts.get(GetString(reader, 0))
                chatroom.type = 1
                try:
                    self.whatsapp.db_insert_table_chatroom(chatroom)
                except Exception as e:
                    pass

            cmd.Dispose()
            # 找出创建失败的群
            cmd.CommandText = '''
                select key_remote_jid,sort_timestamp from chat_list where subject is null and last_read_message_table_id is null
            '''
            # chatroom_fail = model_im.Chatroom()
            reader_fail = cmd.ExecuteReader()
            while reader_fail.Read():
                chatroom_fail = model_im.Chatroom()
                chatroom_fail.chatroom_id = GetString(reader_fail, 0)
                chatroom_fail.account_id = self.account_id
                chatroom_fail.create_time = int(str(GetInt64(reader_fail, 1))[:-3])
                create_person_fail = GetString(reader_fail, 0)[:13] +"@s.whatsapp.net"
                chatroom_fail.creator_id =  create_person_fail
                chatroom_fail.owner_id = create_person_fail
                chatroom_fail.member_count = 1
                chatroom_fail.type = 2 # 临时群
                groups_id = GetString(reader_fail, 0)
                start_index = groups_id.find("-")
                end_index = groups_id.find("@")
                name = groups_id[start_index+1:end_index]
                self.groups_dicts[groups_id] = name
                chatroom_fail.name = name
                try:
                    self.whatsapp.db_insert_table_chatroom(chatroom_fail)
                except Exception as e:
                    pass
        except Exception as e:
            pass
        if conn != None:
            conn.Close()
        self.whatsapp.db_commit()

    def get_group_member(self):
        # member = model_im.ChatroomMember()
        gmember_path = self.root.GetByPath("/databases/msgstore.db")
        #chatroom_path = self.cache + "/msgstore.db"
        wal_path = self.root.GetByPath("/databases/msgstore.db-wal")
        if gmember_path is None:
            return
        if wal_path is None:
            path_msg = self.root.GetByPath("/databases/msgstore.db").PathWithMountPoint
        else:
            path_msg = self.cache + "/msgstore.db"
        try:
            conn = System.Data.SQLite.SQLiteConnection('Data Source = {0}'.format(path_msg))
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = '''
                select gjid, jid from group_participants where pending = 0
            '''
            reader = cmd.ExecuteReader()
            while reader.Read():
                member = model_im.ChatroomMember()
                groups = GetString(reader, 0)
                member_id = GetString(reader, 1)
                if member_id == "":
                    member.member_id = self.account_id
                else:
                    member.member_id = member_id
                member.account_id = self.account_id
                member.chatroom_id = groups
                if member_id in self.contacts_dicts:
                    member.display_name = self.contacts_dicts[member_id]
                else:
                    member.display_name = "unknown"
                try:
                    self.whatsapp.db_insert_table_chatroom_member(member)
                except Exception as e:
                    pass
        except Exception as e:
            pass
        if conn != None:
            conn.Close()
        self.whatsapp.db_commit()

    def get_friend_messages(self):
        # message = model_im.Message()
        message_path = self.cache + "/msgstore.db"
        if message_path is None:
            return
        wal_path = self.root.GetByPath("/databases/msgstore.db-wal")
        if wal_path is None:
            path_msg = self.root.GetByPath("/databases/msgstore.db").PathWithMountPoint
        else:
            path_msg = self.cache + "/msgstore.db"

        try:
            conn = System.Data.SQLite.SQLiteConnection('Data Source = {0}'.format(path_msg))
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = """
                 select key_remote_jid as group_id, status, key_from_me, data, remote_resource, timestamp, media_url, media_mime_type, media_wa_type, media_name ,latitude ,longitude from messages  
                 where key_remote_jid in (select key_remote_jid from chat_list where subject is null and last_read_message_table_id is not null)
            """
            reader= cmd.ExecuteReader()
            while reader.Read():
                message = model_im.Message()
                message.talker_type = 1 # 好友聊天
                message.account_id = self.account_id
                message.is_sender = GetInt64(reader, 2)
                media_type = GetString(reader, 8)
                content = GetString(reader, 3)
                status = GetInt64(reader, 1)
                message.media_path = ""
                send_time =int(str(GetInt64(reader, 5))[:-3])
                message.talker_id = GetString(reader, 0)
                if GetString(reader, 0) in self.contacts_dicts:
                    message.talker_name = self.contacts_dicts[GetString(reader, 0)]
                message.content = content
                if GetInt64(reader, 2) == 1:
                    message.sender_id = self.account_id
                    message.sender_name = self.account_pushname
                else:
                    message.sender_id = GetString(reader, 0)
                    if GetString(reader, 0) in self.contacts_dicts:
                        message.sender_name = self.contacts_dicts[GetString(reader, 0)]
                    else:
                        message.sender_name = GetString(reader, 0)
                if status == 1:
                    message.status = 1
                else:
                    message.status = 0
                
                if media_type == '0':
                    message.type = 1

                elif media_type == '1':
                    message.type = 2
                    if GetString(reader, 9):
                        message.media_path = "Root/storage/sdcard0/whatsapp/Media/WhatsApp Images/Sent/" + GetString(reader, 9)

                elif media_type == '2':
                    message.type = 3
                    audio_path = "Root/storage/sdcard0/whatsapp/Media/WhatsApp Voice Notes/Sent"
                    dicts = {}
                    get_all_files(audio_path, dicts)
                    if GetString(reader, 9) in dicts:
                        message.media_path = dicts.get(GetString(reader, 9))

                elif media_type == '3':
                    message.type = 4
                    if GetString(reader, 9):
                        message.media_path = "Root/storage/sdcard0/whatsapp/Media/WhatsApp Video/Sent/" + GetString(reader, 9)

                elif media_type == '5':
                    if GetFloat(reader, 9) > 0 and GetFloat(reader, 10) > 0:
                        location = model_im.Location()
                        message.type = 7
                        message.extra_id = str(location.location_id)
                        location.longitude = GetFloat(reader, 10)
                        location.latitude = GetFloat(reader, 9)
                        try:
                            self.whatsapp.db_insert_table_location(location)
                        except Exception as e:
                            pass


                message.content = content
                message.send_time = send_time
                try:
                    self.whatsapp.db_insert_table_message(message)
                except Exception as e:
                    pass
        except Exception as e:
            print(e)
        if conn != None:
            conn.Close()
        self.whatsapp.db_commit()

    def get_group_messages(self):
        message_path = self.cache + "/msgstore.db"
        if message_path is None:
            return
        wal_path = self.root.GetByPath("/databases/msgstore.db-wal")
        if wal_path is None:
            path_msg = self.root.GetByPath("/databases/msgstore.db").PathWithMountPoint
        else:
            path_msg = self.cache + "/msgstore.db"
        
        conn = System.Data.SQLite.SQLiteConnection('Data Source = {0}'.format(path_msg))
        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = """
                 select key_remote_jid as group_id, key_from_me, data, remote_resource, timestamp, media_url, media_mime_type, media_wa_type, media_name  ,latitude ,longitude, status from messages  where   
                key_remote_jid in (select distinct(gjid)  from group_participants a where  jid is not null)
            """
            reader= cmd.ExecuteReader()
            while reader.Read():
                message = model_im.Message()
                message.talker_type = 2 # 群聊天
                message.account_id = self.account_id
                groups_id = GetString(reader, 0)
                message.is_sender = GetInt64(reader, 1)
                media_type = GetString(reader, 7)
                content = GetString(reader, 2)
                message.media_path = ""
                send_time =int(str(GetInt64(reader, 4))[:-3])
                message.content = content
                message.talker_id =  GetString(reader, 0)
                status = GetInt64(reader, 12)
                
                if groups_id in self.groups_dicts:
                    message.talker_name = self.groups_dicts[groups_id]

                if GetInt64(reader, 1) == 1 and GetString(reader, 3) == "":
                    message.sender_id = self.account_id
                    message.sender_name = self.account_pushname

                elif GetInt64(reader, 1) == 1 and GetString(reader, 3) != "":
                    message.sender_id = GetString(reader, 3)
                    if GetString(reader, 3) in self.contacts_dicts:
                        message.sender_name = self.contacts_dicts[GetString(reader, 3)]
                    else:
                        message.sender_name = GetString(reader, 3) 
                elif GetInt64(reader, 1) == 0:
                    sender_id = GetString(reader, 3)
                    message.sender_id = sender_id
                    if sender_id in self.contacts_dicts:
                        message.sender_name = self.contacts_dicts[sender_id]
                    else:
                        message.sender_name = GetString(reader, 3)  

                if status == 1:
                    message.status = 1
                else:
                    message.status = 0
                
                if media_type == '0':
                    message.type = 1

                elif media_type == '1':
                    message.type = 2
                    if GetString(reader, 8):
                        message.media_path = "Root/storage/sdcard0/whatsapp/Media/WhatsApp Images/Sent/" + GetString(reader, 8)

                elif media_type == '2':
                    message.type = 3
                    audio_path = r"Root/storage/sdcard0/whatsapp/Media/WhatsApp Voice Notes/Sent"
                    dicts = {}
                    get_all_files(audio_path, dicts)
                    if GetString(reader, 8) in dicts:
                        message.media_path = dicts.get(GetString(reader, 8))

                elif media_type == '3':
                    message.type = 4
                    if GetString(reader, 8):
                        message.media_path = "Root/storage/sdcard0/whatsapp/Media/WhatsApp Video/Sent/" + GetString(reader, 8)
                
                elif media_type == '5':
                    if GetFloat(reader, 9) > 0 and GetFloat(reader, 10) > 0:
                        location = model_im.Location()
                        message.type = 7
                        message.extra_id = str(location.location_id)
                        location.longitude = GetFloat(reader, 10)
                        location.latitude = GetFloat(reader, 9)
                        try:
                            self.whatsapp.db_insert_table_location(location)
                        except Exception as e:
                            pass
                

                message.content = content
                message.send_time = send_time
                try:
                    self.whatsapp.db_insert_table_message(message)
                except Exception as e:
                    pass
        except Exception as e:
            print(e)
        if conn != None:
            conn.Close()
        self.whatsapp.db_commit()


    # recover data
    def get_recover_account(self):
        return True

    def get_recover_friends_groups_id(self):
        friends = []
        groups = []
        fg_node = self.root.GetByPath("/databases/msgstore.db")
        if fg_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(fg_node)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("chat_list")
            SQLiteParser.Tools.AddSignatureToTable(tb, "key_remote_jid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableDeletedRecords(tbs, False):
                if rec[1].find("-") != -1:
                    groups.append(rec[1])
                elif rec[1].find("net") != -1:
                    friends.append(rec[1])
        except Exception as e:
            pass
        return friends, groups

    def get_recover_friends(self):
        contacts_node = self.root.GetByPath("/databases/wa.db")
        friends, groups = self.get_recover_friends_groups_id() 
        if contacts_node is None:
            return
        db = SQLiteParser.Database.FromNode(contacts_node)
        if db is not None:
            try:
                if "wa_contacts" not in db.Tables:
                    return
                if friends is None:
                    return
                tbs = SQLiteParser.TableSignature("wa_contacts")
                SQLiteParser.Tools.AddSignatureToTable(tbs, "jid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                for rec in db.ReadTableDeletedRecords(tbs, False):
                    if rec[1] in friends:
                        friends = model_im.Friend()
                        friends.account_id = self.account_id
                        friends.deleted = 1
                        friends.friend_id = rec[1]
                        if rec[3]:
                            friends.signature = rec[3]
                        try:
                            self.whatsapp.db_insert_table_friend(friends)
                        except Exception as e:
                            pass
            except Exception as e:
                pass
        else:
            if friends is not None:
                for i in friends:
                    no_name_friends = model_im.Friend()
                    no_name_friends.account_id = self.account_id
                    no_name_friends.friend_id = i
                    try:
                        self.whatsapp.db_insert_table_friend(no_name_friends)
                    except Exception as e:
                        pass

        self.whatsapp.db_commit()


    def get_recover_groups(self):
        pass


    def get_recover_message(self):
        message_node = self.root.GetByPath("/databases/msgstore.db")
        if message_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(message_node)
            if db is None:
                return
            tbs = SQLiteParser.TableSignature("messages")
            SQLiteParser.Tools.AddSignatureToTable(tbs, "key_remote_jid", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        except Exception as e:
            pass

    def parse(self):

        db_path = self.cache + "/whatsapp_1.0.db"
        self.whatsapp.db_create(db_path)
        self.get_account()
        self.get_friends()
        self.get_groups()
        self.get_group_member()
        self.get_friend_messages()
        self.get_group_messages()       
        self.whatsapp.db_close()
        mount_dir = self.root.FileSystem.MountPoint
        generate = model_im.GenerateModel(db_path, mount_dir)
        results = generate.get_models()

        return results

def analyze_whatsapp(node, extractDeleted, extractSource):
    
    # nfs = FileSystem.FromLocalDir(r'E:\HUAWEI NXT-AL10_7.0_861918038118833_logic(1)\Apps\com.whatsapp')
    pr = ParserResults()
    results = WhatsApp(node, extractDeleted, extractSource).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("WhatsApp")
    return pr

