#coding=utf-8
# @Author  : xutao
# @File    : ${android_facebook}.py

import PA_runtime
from PA_runtime import *
import model_im
import os
from System.Data.SQLite import *
import shutil
import System
from System.Xml.Linq import *
import json
from System.Xml.XPath import Extensions as XPathExtensions
SafeLoadAssembly("model_im")

def GetString(reader, idx):
    return reader.GetString(idx) if not reader.IsDBNull(idx) else ""

def GetInt64(reader, idx):
    return reader.GetInt64(idx) if not reader.IsDBNull(idx) else 0

def GetBlob(reader, idx):
    return reader.GetValue(idx) if not reader.IsDBNull(idx) else None

def GetFloat(reader, idx):
    return reader.GetFloat(idx) if not reader.IsDBNull(idx) else 0


class Facebook(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.cache = ds.OpenCachePath("Facebook")
        self.account_id = None
        self.account_name = None
        self.facebook_db = model_im.IM()
        self.contacts_dict = {}


    def get_account_id(self):
        user_node = self.root.Parent.GetByPath("/shared_prefs/acra_criticaldata_store.xml")
        if user_node is None:
            return
        es = []
        try:
            user_node.Data.seek(0)
            xml = XElement.Parse(user_node.read())
            es = xml.Elements("string")
        except Exception as e:
            print(e)
            #pass
        for rec in es:
            if rec.Attribute("name") and rec.Attribute("name").Value == "USER_ID":
                user_id = rec.FirstNode.Value
                self.account_id = "FACEBOOK:" + user_id
        

    def get_friends(self):
        friends_node = self.root.GetByPath("threads_db2")
        if friends_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(friends_node)
            if db is None and "thread_users" not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("thread_users")
            if self.extractDeleted:
                SQLiteParser.Tools.AddSignatureToTable(tbs, "user_key", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tbs, "is_friend", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
                SQLiteParser.Tools.AddSignatureToTable(tbs, "commerce_page_type", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)     
                SQLiteParser.Tools.AddSignatureToTable(tbs, "is_commerce", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                if canceller.IsCancellationRequested:
                    return
                if "profile_type" in rec and rec["profile_type"].Value == "user":
                    rec_id = rec["user_key"].Value
                    if rec_id == self.account_id:
                        account = model_im.Account()
                        account.source = "Facebook"
                        if rec.Deleted == DeletedState.Deleted:
                            account.deleted = 1
                        account.account_id = self.account_id
                        if "name" in rec and (not rec["name"].IsDBNull):
                            self.account_name = rec["name"].Value
                            account.nickname = rec["name"].Value
                            self.contacts_dict[rec_id] = rec["name"].Value
                        if "profile_pic_square" in rec and (not rec["profile_pic_square"].IsDBNull):
                            tmp = rec["profile_pic_square"].Value
                            tmp_json = json.loads(tmp)
                            tmp_dict = tmp_json[-1:][0]
                            account.photo = tmp_dict.get("url")
                        try:
                            self.facebook_db.db_insert_table_account(account)
                        except Exception as e:
                            print(e)
                            #pass

                    elif rec["is_friend"].Value == 1:
                        friend = model_im.Friend()
                        friend.account_id = self.account_id
                        friend.friend_id = rec_id
                        friend.source = "Facebook"
                        if rec.Deleted == DeletedState.Deleted:
                            friend.deleted = 1
                        if "name" in rec and (not rec["name"].IsDBNull):
                            friend.nickname = rec["name"].Value
                            self.contacts_dict[rec_id] = rec["name"].Value
                        if "profile_pic_square" in rec and (not rec["profile_pic_square"].IsDBNull):
                            tmp = rec["profile_pic_square"].Value
                            json_tmp = json.loads(tmp)
                            dict_tmp = json_tmp[-1:][0]
                            friend.photo = dict_tmp.get("url")
                        try:
                            self.facebook_db.db_insert_table_friend(friend)
                        except Exception as e:
                            print(e)
                            #pass
        except Exception as e:
            print(e)
            #pass
        self.facebook_db.db_commit()

    
    def get_friends_chat(self):
        friends_chat = self.root.GetByPath("threads_db2")
        if friends_chat is None:
            return
        conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(friends_chat.PathWithMountPoint))
        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = '''
                select messages.thread_key, text, sender, messages.timestamp_ms, messages.timestamp_sent_ms, shares, msg_type,pending_send_media_attachment from messages, folders where messages.thread_key = folders.thread_key and messages.thread_key like "ONE%"
            '''
            reader= cmd.ExecuteReader()
            fs = self.root.FileSystem
            while reader.Read():
                if canceller.IsCancellationRequested:
                    return
                message = model_im.Message()
                message.source = "Facebook"
                message.account_id = self.account_id
                message.source = "Facebook"
                send_info = GetString(reader, 2) if GetString(reader, 2) else None
                msg_type = GetInt64(reader, 6) if GetInt64(reader, 6) else None 
                if send_info:
                    send_dic = json.loads(send_info)
                    if "name" in send_dic:
                        message.sender_name = send_dic.get("name")
                    if "user_key" in send_dic:
                        message.sender_id = send_dic.get("user_key")
                        if send_dic.get("user_key") == self.account_id:
                            message.is_sender = 1
                    thread_key = GetString(reader, 0)
                    talk_id = "FACEBOOK:" + thread_key[len("ONE_TO_ONE:"):len("ONE_TO_ONE:")+len("000000000000000")]
                    message.talker_id = talk_id
                    if talk_id in self.contacts_dict:
                        message.talker_name = self.contacts_dict.get(talk_id)
                    message.content = GetString(reader, 1)
                    if GetString(reader, 1):
                        message.type = 1
                    if GetInt64(reader, 4):
                        message.send_time = int(str(GetInt64(reader, 4))[:-3])
                    else:
                        message.send_time = int(str(GetInt64(reader, 3))[:-3])

                    message.talker_type = 1 # 好友聊天
                    attachment = GetString(reader, 7)
                    if attachment:
                        attachment_str = json.loads(attachment)
                        attachment_dict = attachment_str[0]
                        if "type" in attachment_dict:
                            tmp_type = attachment_dict["type"] 
                            if tmp_type == "VIDEO":
                                message.type = 3
                            elif tmp_type == "PHOTO":
                                message.type = 2
                            elif tmp_type == "VIDEO":
                                message.type = 4
                            if "uri" in attachment_dict:
                                media_path = attachment_dict.get("uri")
                                if media_path.startswith("file:"):
                                    lists = re.split("/", media_path)
                                    media_name = lists[-1]
                                    media_path_list = fs.Search(media_name)
                                    for i in media_path_list:
                                        message.media_path = i.AbsolutePath
                                elif media_path.startswith("http"):
                                    message.media_path = media_path
                    
                    if GetString(reader, 5):
                        share_list = json.loads(GetString(reader, 5))
                        share_dict = share_list[0]
                        if "media" in share_dict:
                            media_info = share_dict.get("media")
                            if "type" in media_info[0] and media_info[0]["type"] == "VIDEO":
                                message.type = 4
                            if "type" in media_info[0] and media_info[0]["type"] == "LINK":
                                message.type = 8
                        if "href" in share_dict:
                            message.media_path = share_dict["href"]
                        if "name" in share_dict:
                            message.content = share_dict.get("name")
                    try:
                        self.facebook_db.db_insert_table_message(message)       
                    except Exception as e:
                        print(e)
                        #pass
                elif send_info == "" and msg_type == -1:   # 无效的信息
                    continue
        except Exception as e:
            print(e)
            #pass
        self.facebook_db.db_commit()


    def get_groups_chat(self):
        groups_chat = self.root.GetByPath("threads_db2")
        if groups_chat is None:
            return
        conn = System.Data.SQLite.SQLiteConnection("Data Source = {0}; ReadOnly = True".format(groups_chat.PathWithMountPoint))
        try:
            conn.Open()
            cmd = System.Data.SQLite.SQLiteCommand(conn)
            cmd.CommandText = '''
                select messages.thread_key, text, sender, messages.timestamp_ms, messages.timestamp_sent_ms, shares, msg_type,pending_send_media_attachment from messages, folders where messages.thread_key = folders.thread_key and messages.thread_key like "GROUP%"
            '''
            reader= cmd.ExecuteReader()
            fs = self.root.FileSystem
            while reader.Read():
                if canceller.IsCancellationRequested:
                    return
                message = model_im.Message()
                message.source = "Facebook"
                message.account_id = self.account_id
                send_info = GetString(reader, 2) if GetString(reader, 2) else None
                msg_type = GetInt64(reader, 6) if GetInt64(reader, 6) else None 
                if send_info:
                    send_dic = json.loads(send_info)
                    if "name" in send_dic:
                        message.sender_name = send_dic.get("name")
                    if "user_key" in send_dic:
                        message.sender_id = send_dic.get("user_key")
                        if send_dic.get("user_key") == self.account_id:
                            message.is_sender = 1
                    
                    message.talker_id = GetString(reader, 0)
                    message.content = GetString(reader, 1)
                    if GetString(reader, 1):
                        message.type = 1
                    if GetInt64(reader, 4):
                        message.send_time = int(str(GetInt64(reader, 4))[:-3])
                    else:
                        message.send_time = int(str(GetInt64(reader, 3))[:-3])

                    message.talker_type = 2 # 群聊天
                    attachment = GetString(reader, 7)
                    if attachment:
                        attachment_str = json.loads(attachment)
                        attachment_dict = attachment_str[0]
                        if "type" in attachment_dict:
                            tmp_type = attachment_dict["type"] 
                            if tmp_type == "VIDEO":
                                message.type = 3
                            elif tmp_type == "PHOTO":
                                message.type = 2
                            elif tmp_type == "VIDEO":
                                message.type = 4
                            if "uri" in attachment_dict:
                                media_path = attachment_dict.get("uri")
                                if media_path.startswith("file:"):
                                    lists = re.split("/", media_path)
                                    media_name = lists[-1]
                                    media_path_list = fs.Search(media_name)
                                    for i in media_path_list:
                                        message.media_path = i.AbsolutePath
                                elif media_path.startswith("http"):
                                    message.media_path = media_path
                    
                    if GetString(reader, 5):
                        share_list = json.loads(GetString(reader, 5))
                        share_dict = share_list[0]
                        if "media" in share_dict:
                            media_info = share_dict.get("media")
                            if "type" in media_info[0] and media_info[0]["type"] == "VIDEO":
                                message.type = 4
                            if "type" in media_info[0] and media_info[0]["type"] == "LINK":
                                message.type = 8
                        if "href" in share_dict:
                            message.media_path = share_dict["href"]
                        if "name" in share_dict:
                            message.content = share_dict.get("name")
                    try:
                        self.facebook_db.db_insert_table_message(message)       
                    except Exception as e:
                        print(e)
                        #pass
                elif send_info == "" and msg_type == -1:   # 无效的信息
                    continue
        except Exception as e:
            print(e)
            #pass
        self.facebook_db.db_commit()
        

    def get_recover_friends_chat(self):
        fmessage_node = self.root.GetByPath("threads_db2")
        if fmessage_node is None:
            return
        db = SQLiteParser.Database.FromNode(fmessage_node)
        if db is None and "messages" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("messages")
        SQLiteParser.Tools.AddSignatureToTable(tbs, "thread_key", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        SQLiteParser.Tools.AddSignatureToTable(tbs, "msg_type", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
        SQLiteParser.Tools.AddSignatureToTable(tbs, "msg_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        fs = self.root.FileSystem
        for rec in db.ReadTableDeletedRecords(tbs, False):
            if canceller.IsCancellationRequested:
                return
            send_info = rec["sender"].Value if "sender" in rec else None
            msg_type = rec["msg_type"].Value if "msg_type" in rec else None
            if  "thread_key" in rec and rec["thread_key"].Value.find("ONE_TO_ONE") != -1 and send_info:     # 好友聊天
                message = model_im.Message()
                message.deleted = 1
                message.account_id = self.account_id
                message.talker_type = 1
                message.msg_id = rec["msg_id"].Value if "msg_id" in rec else None
                thread_key = rec["thread_key"].Value
                talk_id = "FACEBOOK:" + thread_key[len("ONE_TO_ONE:"):len("ONE_TO_ONE:")+len("000000000000000")]
                message.talker_id = talk_id
                if talk_id in self.contacts_dict:
                    message.talker_name = self.contacts_dict.get(talk_id)
                if "sender" in rec and (not rec["sender"].IsDBNull):
                    send_info = rec["sender"].Value
                    if send_info:
                        send_dict = json.loads(send_dict)
                        if "name" in send_dict:
                            message.sender_name = send_dict.get("name")
                        if "user_key" in send_dict:
                            message.sender_id = send_dict.get("user_key")
                            if message.sender_id == self.account_id:
                                message.is_sender = 1
                if "text" in rec and (not recp["Text"].IsDBNull):
                    message.content = rec["text"].Value
                    message.type = 1
                if "timestamp_sent_ms" in rec and (not rec["timestamp_sent_ms"].IsDBNull):
                    message.send_time = int(str(rec["timestamp_sent_ms"].Value)[:-3])
                else:
                    if "timestamp_ms" in rec and (not rec["timestamp_ms"].IsDBNull):
                        message.send_time = int(str(rec["timestamp_ms"].Value)[:-3])
                if "pending_send_media_attachment" in rec and (not rec["pending_send_media_attachment"].IsDBNull):
                    attachment = rec["pending_send_media_attachment"].Value
                    if attachment:
                        attachment_str = json.loads(attachment)
                        attachment_dict = attachment_str[0]
                        if "type" in attachment_dict:
                            tmp_type = attachment_dict["type"] 
                            if tmp_type == "VIDEO":
                                message.type = 3
                            elif tmp_type == "PHOTO":
                                message.type = 2
                            elif tmp_type == "VIDEO":
                                message.type = 4
                            if "uri" in attachment_dict:
                                media_path = attachment_dict.get("uri")
                                if media_path.startswith("file:"):
                                    lists = re.split("/", media_path)
                                    media_name = lists[-1]
                                    media_path_list = fs.Search(media_name)
                                    for i in media_path_list:
                                        message.media_path = i.AbsolutePath
                                elif media_path.startswith("http"):
                                    message.media_path = media_path



                if "shares" in rec and (not rec["shares"].IsDBNull):
                    share_list = json.loads(rec["shares"].Value)
                    share_dict = share_list[0]
                    if "media" in share_dict:
                        media_info = share_dict.get("media")
                        if "type" in media_info[0] and media_info[0]["type"] == "VIDEO":
                            message.type = 4
                        if "type" in media_info[0] and media_info[0]["type"] == "LINK":
                            message.type = 8
                    if "href" in share_dict:
                        message.media_path = share_dict["href"]
                    if "name" in share_dict:
                        message.content = share_dict.get("name")
                try:
                    self.facebook_db.db_insert_table_message(message)
                except Exception as e:
                    print(e)
                    
            elif send_info == "" and msg_type == -1:   # 无效的信息
                    continue
        self.facebook_db.db_commit()

    def get_recover_groups_chat(self):
        gmessage_node = self.root.GetByPath("threads_db2")
        if gmessage_node is None:
            return
        db = SQLiteParser.Database.FromNode(gmessage_node)
        if db is None and "messages" not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("messages")
        SQLiteParser.Tools.AddSignatureToTable(tbs, "thread_key", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        SQLiteParser.Tools.AddSignatureToTable(tbs, "msg_type", SQLiteParser.FieldType.Int, SQLiteParser.FieldConstraints.NotNull)
        SQLiteParser.Tools.AddSignatureToTable(tbs, "msg_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        fs = self.root.FileSystem
        for rec in db.ReadTableDeletedRecords(tbs, False):
            if canceller.IsCancellationRequested:
                return
            send_info = rec["sender"].Value if "sender" in rec else None
            msg_type = rec["msg_type"].Value if "msg_type" in rec else None
            if  "thread_key" in rec and rec["thread_key"].Value.find("GROUP") != -1 and send_info:     # 群聊天
                message = model_im.Message()
                message.deleted = 1
                message.account_id = self.account_id
                message.talker_type = 2
                message.msg_id = rec["msg_id"].Value if "msg_id" in rec else None
                thread_key = rec["thread_key"].Value
                message.talker_id = thread_key
                if "sender" in rec and (not rec["sender"].IsDBNull):
                    send_info = rec["sender"].Value
                    if send_info:
                        send_dict = json.loads(send_dict)
                        if "name" in send_dict:
                            message.sender_name = send_dict.get("name")
                        if "user_key" in send_dict:
                            message.sender_id = send_dict.get("user_key")
                            if message.sender_id == self.account_id:
                                message.is_sender = 1
                if "text" in rec and (not recp["Text"].IsDBNull):
                    message.content = rec["text"].Value
                    message.type = 1
                if "timestamp_sent_ms" in rec and (not rec["timestamp_sent_ms"].IsDBNull):
                    message.send_time = int(str(rec["timestamp_sent_ms"].Value)[:-3])
                else:
                    if "timestamp_ms" in rec and (not rec["timestamp_ms"].IsDBNull):
                        message.send_time = int(str(rec["timestamp_ms"].Value)[:-3])
                if "pending_send_media_attachment" in rec and (not rec["pending_send_media_attachment"].IsDBNull):
                    attachment = rec["pending_send_media_attachment"].Value
                    if attachment:
                        attachment_str = json.loads(attachment)
                        attachment_dict = attachment_str[0]
                        if "type" in attachment_dict:
                            tmp_type = attachment_dict["type"] 
                            if tmp_type == "VIDEO":
                                message.type = 3
                            elif tmp_type == "PHOTO":
                                message.type = 2
                            elif tmp_type == "VIDEO":
                                message.type = 4
                            if "uri" in attachment_dict:
                                media_path = attachment_dict.get("uri")
                                if media_path.startswith("file:"):
                                    lists = re.split("/", media_path)
                                    media_name = lists[-1]
                                    media_path_list = fs.Search(media_name)
                                    for i in media_path_list:
                                        message.media_path = i.AbsolutePath
                                elif media_path.startswith("http"):
                                    message.media_path = media_path
                if "shares" in rec and (not rec["shares"].IsDBNull):
                    share_list = json.loads(rec["shares"].Value)
                    share_dict = share_list[0]
                    if "media" in share_dict:
                        media_info = share_dict.get("media")
                        if "type" in media_info[0] and media_info[0]["type"] == "VIDEO":
                            message.type = 4
                        if "type" in media_info[0] and media_info[0]["type"] == "LINK":
                            message.type = 8
                    if "href" in share_dict:
                        message.media_path = share_dict["href"]
                    if "name" in share_dict:
                        message.content = share_dict.get("name")
                try:
                    self.facebook_db.db_insert_table_message(message)
                except Exception as e:
                    print(e)
            elif send_info == "" and msg_type == -1:   # 无效的信息
                    continue
        
        self.facebook_db.db_commit()


    def parse(self):

        db_path = self.cache + "/facebook_db.db"
        self.facebook_db.db_create(db_path)
        self.get_account_id()
        self.get_friends()
        self.get_friends_chat()
        self.get_groups_chat()
        self.get_recover_friends_chat()
        self.get_recover_groups_chat()
        self.facebook_db.db_close()

        generate = model_im.GenerateModel(db_path)
        results = generate.get_models()
        return results 



def analyze_facebook(node, extractDeleted, extractSource):
    pr = ParserResults()
    results = Facebook(node, extractDeleted, extractSource).parse()
    if results:
        pr.Models.AddRange(results)
    pr.Build("Facebook")
    return pr