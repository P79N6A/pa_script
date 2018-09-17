#coding=utf-8
from PA_runtime import *
import PA_runtime
import model_im
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
except:
    pass
del clr

import os
import System
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from collections import defaultdict
from System.Data.SQLite import *
import shutil
import json


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
        self.source_path = self.root.GetByPath("/databases/msgstore.db")
        self.dest_path = self.cache + "/msgstore.db"

    def get_account(self):
        account = model_im.Account()
        account.source = "WhatsApp"
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



    def read_file_path(self):
        node = self.root.GetByPath("/databases/msgstore.db")
        wal_node = self.root.GetByPath("/databases/msgstore.db-wal")
        if node is None:
            return
        if wal_node is None:
            return True
        else:
            moveFileto(node.PathWithMountPoint, self.cache)
            moveFileto(wal_node.PathWithMountPoint, self.cache)
            return False


    def _get_friends_groups_id(self):
        if self.read_file_path():
            connection = System.Data.SQLite.SQLiteConnection('Data Source = {0}; ReadOnly = True'.format(self.source_path.PathWithMountPoint))
        else:
            connection = System.Data.SQLite.SQLiteConnection('Data Source = {0}'.format(self.dest_path))

        friends = [] 
        groups = []
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
        if self.read_file_path():
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(self.source_path.PathWithMountPoint))
        else:
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}".format(self.dest_path))
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
                chatroom.source = "WhatsApp"
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
                chatroom_fail.source = "WhatsApp"
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
        if self.read_file_path():
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(self.source_path.PathWithMountPoint))
        else:
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}".format(self.dest_path))

        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = '''
                select gjid, jid from group_participants where pending = 0
            '''
            reader = cmd.ExecuteReader()
            while reader.Read():
                if GetString(reader, 0).find("broadcast") != -1:
                    continue
                member = model_im.ChatroomMember()
                member.source = "WhatsApp"
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
        if self.read_file_path():
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(self.source_path.PathWithMountPoint))
        else:
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}".format(self.dest_path))
        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = """
                 select key_remote_jid as group_id, status, key_from_me, data, remote_resource, timestamp, media_url, media_mime_type, media_wa_type, media_name ,latitude ,longitude from messages  
                 where key_remote_jid in (select key_remote_jid from chat_list where subject is null and last_read_message_table_id is not null)
            """
            reader= cmd.ExecuteReader()
            fs = self.root.FileSystem
            while reader.Read():
                message = model_im.Message()
                message.source = "WhatsApp"
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
                        img_name = GetString(reader, 9)
                        img_node = fs.Search(img_name) 
                        for i in img_node:
                            img_path = i.AbsolutePath
                            message.media_path = img_path

                elif media_type == '2':
                    message.type = 3
                    voice_name = GetString(reader, 9)
                    if voice_name:
                        voice_node = fs.Search(voice_name)
                        for i in voice_node:
                            voice_path = i.AbsolutePath
                            message.media_path = voice_path

                elif media_type == '3':
                    message.type = 4
                    if GetString(reader, 0):
                        video_name = GetString(reader, 9)
                        video_node = fs.Search(video_name)
                        for i in video_node:
                            video_path = i.AbsolutePath
                            message.media_path =  video_path

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
        if self.read_file_path():
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(self.source_path.PathWithMountPoint))
        else:
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}".format(self.dest_path))
        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = """
                 select key_remote_jid as group_id, key_from_me, data, remote_resource, timestamp, media_url, media_mime_type, media_wa_type, media_name  ,latitude ,longitude, status from messages  where   
                key_remote_jid in (select distinct(gjid)  from group_participants a where  jid is not null)
            """
            reader= cmd.ExecuteReader()
            fs = self.root.FileSystem
            while reader.Read():
                if GetString(reader, 0).find("broadcast") != -1:
                    continue
                message = model_im.Message()
                message.source = "WhatsApp"
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
                        img_name = GetString(reader, 8)
                        img_node = fs.Search(img_name) 
                        for i in img_node:
                            img_path = i.AbsolutePath
                            message.media_path = img_path

                elif media_type == '2':
                    message.type = 3
                    if GetString(reader, 8):
                        voice_name = GetString(reader, 8)
                        voice_node = fs.Search(voice_name)
                        for i in voice_node:
                            voice_path = i.AbsolutePath
                            message.media_path = voice_path

                elif media_type == '3':
                    message.type = 4
                    if GetString(reader, 8):
                        video_name = GetString(reader, 8)
                        video_node = fs.Search(video_name)
                        for i in video_node:
                            video_path = i.AbsolutePath
                            message.media_path = video_path
                
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


    def get_feeds(self):
        if self.read_file_path():
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(self.source_path.PathWithMountPoint))
        else:
            conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}".format(self.dest_path))
        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = """
                select key_remote_jid as group_id, key_from_me, data, remote_resource, timestamp, media_url, media_caption, media_mime_type, media_wa_type, media_name  ,latitude ,longitude, status from messages 
            """
            reader= cmd.ExecuteReader()
            fs = self.root.FileSystem
            while reader.Read():
                try:
                    media_type = GetString(reader, 8)
                    feed_id = GetString(reader, 0)
                    # 好友状态
                    if feed_id.find("broadcast") == -1:
                        continue
                    feed = model_im.Feed()
                    feed.source = "WhatsApp"
                    feed.account_id = self.account_id
                    if GetInt64(reader, 1) == 1:
                        feed.sender_id = self.account_id
                    else:
                        feed.sender_id = GetString(reader, 3)
                    if GetString(reader, 2):
                        feed.content = GetString(reader, 2)
                    if GetString(reader, 5):
                        feed.urls = json.dumps(GetString(reader, 5))
                    if GetString(reader, 6):
                        feed.content = GetString(reader, 6)
                    
                    if media_type == '0':
                        feed.type = 1

                    elif media_type == '1':
                        feed.type = 2
                        if GetString(reader, 9):
                            img_name = GetString(reader, 9)
                            img_node = fs.Search(img_name) 
                            for i in img_node:
                                img_path = i.AbsolutePath
                                feed.media_path = img_path

                    elif media_type == '2':
                        feed.type = 3
                        if GetString(reader, 9):
                            voice_name = GetString(reader, 9)
                            voice_node = fs.Search(voice_name)
                            for i in voice_node:
                                voice_path = i.AbsolutePath
                                feed.media_path = voice_path

                    elif media_type == '3':
                        feed.type = 4
                        if GetString(reader, 9):
                            video_name = GetString(reader, 9)
                            video_node = fs.Search(video_name)
                            for i in video_node:
                                video_path = i.AbsolutePath
                                feed.media_path = video_path

                    feed.send_time = int(str(GetInt64(reader, 4))[:-3])

                except Exception as e:
                    print(e)
                try:
                    self.whatsapp.db_insert_table_feed(feed)
                except Exception as e:
                    pass
        except Exception as e:
            peint(e)
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
        if not os.path.exists(self.root.PathWithMountPoint + "/databases/msgstore.db"):
            return
        db_path = self.cache + "/whatsapp_1.0.db"
        self.whatsapp.db_create(db_path)
        self.get_account()
        self.get_friends()
        self.get_groups()
        self.get_group_member()
        self.get_friend_messages()
        self.get_group_messages()
        self.get_feeds()       
        self.whatsapp.db_close()
        generate = model_im.GenerateModel(db_path)
        results = generate.get_models()

        return results

def analyze_whatsapp(node, extractDeleted, extractSource):
    
    pr = ParserResults()
    pr.Categories = DescripCategories.WhatsApp
    results = WhatsApp(node, extractDeleted, extractSource).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("WhatsApp")               
    return pr

