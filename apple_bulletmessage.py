#coding=utf-8
from PA_runtime import *
import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference("bcp_im")
    clr.AddReference("model_map")
except:
    pass
del clr

import System
from System.Xml.Linq import *
from System.Data.SQLite import *
import model_im
import model_map
import json


CHAT_TYPE_FRIEND = 1  # 好友聊天
CHAT_TYPE_GROUP = 2  # 群聊天
CHAT_TYPE_SYSTEM = 3  # 系统消息
CHAT_TYPE_OFFICIAL = 4  # 公众号
CHAT_TYPE_SUBSCRIBE = 5  # 订阅号
CHAT_TYPE_SHOP = 6  # 商家
CHAT_TYPE_SECRET = 7  # 私密聊天

MESSAGE_TYPE_SYSTEM = 1
MESSAGE_TYPE_SEND = 2
MESSAGE_TYPE_RECEIVE = 3

MESSAGE_CONTENT_TYPE_TEXT = 1  # 文本
MESSAGE_CONTENT_TYPE_IMAGE = 2  # 图片
MESSAGE_CONTENT_TYPE_VOICE = 3  # 语音
MESSAGE_CONTENT_TYPE_VIDEO = 4  # 视频
MESSAGE_CONTENT_TYPE_EMOJI = 5  # 表情
MESSAGE_CONTENT_TYPE_CONTACT_CARD = 6  # 名片
MESSAGE_CONTENT_TYPE_LOCATION = 7  # 坐标
MESSAGE_CONTENT_TYPE_LINK = 8  # 链接
MESSAGE_CONTENT_TYPE_VOIP = 9  # 网络电话
MESSAGE_CONTENT_TYPE_ATTACHMENT = 10  # 附件
MESSAGE_CONTENT_TYPE_RED_ENVELPOE = 11  # 红包
MESSAGE_CONTENT_TYPE_RECEIPT = 12  # 转账
MESSAGE_CONTENT_TYPE_AA_RECEIPT = 13  # 群收款
MESSAGE_CONTENT_TYPE_CHARTLET = 14
MESSAGE_CONTENT_TYPE_SYSTEM = 99  # 系统


def convert_to_unixtime(timestamp):
    try:
        if len(str(timestamp)) == 13:
            timestamp = int(str(timestamp)[0:10])
        elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
            timestamp = 0
        elif len(str(timestamp)) == 10:
            timestamp = timestamp
        return timestamp
    except Exception as e:
        pass


class bulletMessage(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.bulletMessage = model_im.IM()
        self.cache = ds.OpenCachePath("bulletMessage")
        self.contact_list = {}
        self.group_info = {}

    def parse(self):

        db_path = model_map.md5(self.cache, self.root.AbsolutePath)
        self.bulletMessage.db_create(db_path)
        self.main()
        self.bulletMessage.db_close()

        tmp_dir = ds.OpenCachePath("tmp")
        # PA_runtime.save_cache_path("05005", db_path, tmp_dir)
        im_models = model_im.GenerateModel(db_path).get_models()
        # map_models = model_map.Genetate(db_path).get_models()
        results = []
        results.extend(im_models)
        # results.extend(map_models)
        return results


    def main(self):
        account_nodes = self.root.Parent.Parent.Parent.GetByPath("/Documents/")
        if account_nodes is None:
            return
        account_node_lists = account_nodes.Files
        if account_node_lists is None:
            return
        for a_node in account_node_lists:
            if a_node.Name.startswith("SMDataBase_") and a_node.Name.find("-") == -1:
                account_id = a_node.Name[a_node.Name.find("_")+1:a_node.Name.find(".")]
                self.get_account(a_node)
                self.get_friends(a_node, account_id)


        messages_nodes = self.root.Parent.Parent.Parent.GetByPath("/Documents/NIMSDK/93b8bdf673198ce4bb42d4356e7ee5ba/Users/")
        if messages_nodes is None:
            return
        message_node_lists = messages_nodes.Children
        if message_node_lists is None:
            return
        for m_node in message_node_lists:
            node = m_node.GetByPath("/message.db")
            self.get_friends_message(node, m_node.Name)
            self.get_groups_message(node, m_node.Name)
            self.get_chatroom(m_node.Name)


    def get_account(self, node):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'ZUSER' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("ZUSER")
        for rec in db.ReadTableRecords(tbs, False):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            if canceller.IsCancellationRequested:
                return
            try:
                account = model_im.Account()
                if rec.Deleted == DeletedState.Deleted:
                    account.deleted = 1
                account.source = node.AbsolutePath
                if "ZUSERNIMACCID" in rec and (not rec["ZUSERNIMACCID"].IsDBNull):
                    account.account_id = rec["ZUSERNIMACCID"].Value
                if "ZUSERNAME" in rec and (not rec["ZUSERNAME"].IsDBNull):
                    account.nickname = rec["ZUSERNAME"].Value
                    self.contact_list[account.account_id] = account.nickname
                if "ZUSERAVATAR" in rec and (not rec["ZUSERAVATAR"].IsDBNull):
                    account.photo = rec["ZUSERAVATAR"].Value
                if "ZUSERPHONE" in rec and (not rec["ZUSERPHONE"].IsDBNull):
                    account.telephone = rec["ZUSERPHONE"].Value
                if account.account_id:
                    self.bulletMessage.db_insert_table_account(account)
            except Exception as e:
                TraceService.Trace(TraceLevel.Info, e)
        self.bulletMessage.db_commit()


    def get_friends(self, node, account_id):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'ZSMUSER' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("ZSMUSER")
        for rec in db.ReadTableRecords(tbs, False):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            if canceller.IsCancellationRequested:
                return
            try:
                friend = model_im.Friend()
                friend.account_id = account_id
                friend.type = 1
                if rec.Deleted == DeletedState.Deleted:
                    friend.deleted = 1
                friend.source = node.AbsolutePath
                if "ZACCID" in rec and (not rec["ZACCID"].IsDBNull):
                    friend.friend_id = rec["ZACCID"].Value
                if "ZNICKNAME" in rec and (not rec["ZNICKNAME"].IsDBNull):
                    friend.nickname = rec["ZNICKNAME"].Value
                    self.contact_list[friend.friend_id] = friend.nickname
                if "ZAVATARURL" in rec and (not rec["ZAVATARURL"].IsDBNull):
                    friend.photo = rec["ZAVATARURL"].Value
                if friend.account_id and friend.friend_id:
                    self.bulletMessage.db_insert_table_friend(friend)
            except Exception as e:
                TraceService.Trace(TraceLevel.Info, e)
        self.bulletMessage.db_commit()


    def get_friends_message(self, node, account_id):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'session_table' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("session_table")
        for rec in db.ReadTableRecords(tbs, True, False):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            if canceller.IsCancellationRequested:
                return
            try:
                # 好友聊天
                if "session_type" in rec and rec["session_type"].Value == 0:
                    table_name = None
                    talker_id = None

                    if "session_id" in rec and (not rec["session_id"].IsDBNull):
                        talker_id = rec["session_id"].Value
                    if "name" in rec and (not rec["name"].IsDBNull):
                        table_name = rec["name"].Value  

                    m_tbs = SQLiteParser.TableSignature(table_name)
                    for m_rec in db.ReadTableRecords(m_tbs, True, False):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                        try:
                            if canceller.IsCancellationRequested:
                                return
                            message = model_im.Message()
                            message.account_id = account_id
                            if m_rec.Deleted == DeletedState.Deleted:
                                message.deleted = 1
                            message.source = node.AbsolutePath
                            message.talker_id = talker_id
                            # 默认都是好友聊天
                            message.talker_type = CHAT_TYPE_FRIEND  # 好友聊天
                            # send_id
                            if "msg_from_id" in m_rec and (not m_rec["msg_from_id"].IsDBNull):
                                message.sender_id = m_rec["msg_from_id"].Value
                                if m_rec["msg_from_id"].Value in self.contact_list:
                                    message.sender_name = self.contact_list[m_rec["msg_from_id"].Value]
                                if m_rec["msg_from_id"].Value == account_id:
                                    message.is_sender = 1
                            
                            if "msg_id" in m_rec and (not m_rec["msg_id"].IsDBNull):
                                message.msg_id = m_rec["msg_id"].Value

                            if "msg_time" in m_rec and (not m_rec["msg_time"].IsDBNull):
                                message.send_time = convert_to_unixtime(m_rec["msg_time"].Value)

                            # 判断消息类型
                            if "msg_type" in m_rec and (not m_rec["msg_type"].IsDBNull):
                                
                                # text
                                if m_rec["msg_type"].Value == 0:
                                    message.type = MESSAGE_CONTENT_TYPE_TEXT
                                    message.content = m_rec["msg_text"].Value

                                # img
                                elif m_rec["msg_type"].Value == 1:
                                    message.type = MESSAGE_CONTENT_TYPE_IMAGE
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "url" in data:
                                            message.media_path = data["url"]
                                        if "name" in data:
                                            message.content = data["name"]
                                    except Exception as e:
                                        pass

                                # audio
                                elif m_rec["msg_type"].Value == 2:
                                    message.type = MESSAGE_CONTENT_TYPE_VOICE
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "url" in data:
                                            message.media_path = data["url"]
                                    except Exception as e:
                                        pass
                                    message.content = m_rec["msg_text"].Value
                                
                                # video
                                elif m_rec["msg_type"].Value == 3:
                                    message.type = MESSAGE_CONTENT_TYPE_VIDEO
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "url" in data:
                                            message.media_path = data["url"]
                                    except Exception as e:
                                        pass
                                    message.content = m_rec["msg_text"].Value

                                # share location
                                elif m_rec["msg_type"].Value == 4:
                                    message.type = MESSAGE_CONTENT_TYPE_LOCATION
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        loc = model_im.Location()
                                        if "lng" in data:
                                            loc.longitude = data["lng"]
                                        if "lat" in data:
                                            loc.latitude = data["lat"]
                                        if "title" in data:
                                            message.content = data["title"]
                                            loc.address = data["title"]
                                        self.bulletMessage.db_insert_table_location(loc)
                                    except Exception as e:
                                        pass

                                # call
                                elif m_rec["msg_type"].Value == 5:
                                    message.type = MESSAGE_CONTENT_TYPE_VOIP
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "duration" in data:
                                            message.content = "通话拨打时长 " + str(data["duration"]) + "s"
                                    except Exception as e:
                                        pass

                                # system messages
                                elif m_rec["msg_type"].Value == 10:
                                    message.type = CHAT_TYPE_SYSTEM
                                    message.content = m_rec["msg_text"].Value
                                
                                # card
                                elif m_rec["msg_type"].Value == 100 and m_rec["client_type"].Value == 1:
                                    message.type = MESSAGE_CONTENT_TYPE_CONTACT_CARD
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "data" in data:
                                            if "SMCardName" in data["data"]:
                                                message.content = data["data"]["SMCardName"]
                                    except Exception as e:
                                        pass
                                
                                # 红包
                                elif m_rec["msg_type"].Value == 100 and (not m_rec["push_content"].IsDBNull):
                                    message.type = MESSAGE_CONTENT_TYPE_RED_ENVELPOE
                                    message.content = m_rec["push_content"].Value

                            if message.account_id and message.talker_id:
                                self.bulletMessage.db_insert_table_message(message)
                        except Exception as e:
                            TraceService.Trace(TraceLevel.Info, e)
            except Exception as e:
                TraceService.Trace(TraceLevel.Info, e)
        self.bulletMessage.db_commit()


    def get_groups_message(self, node, account_id):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'session_table' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("session_table")
        for rec in db.ReadTableRecords(tbs, True, False):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            if canceller.IsCancellationRequested:
                return
            try:
                # 群聊天
                if "session_type" in rec and rec["session_type"].Value == 1:
                    table_name = None
                    talker_id = None

                    if "session_id" in rec and (not rec["session_id"].IsDBNull):
                        talker_id = rec["session_id"].Value
                    if "name" in rec and (not rec["name"].IsDBNull):
                        table_name = rec["name"].Value

                    m_tbs = SQLiteParser.TableSignature(table_name)
                    for m_rec in db.ReadTableRecords(m_tbs, True, False):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                        try:
                            if canceller.IsCancellationRequested:
                                return
                            message = model_im.Message()
                            message.account_id = account_id
                            if m_rec.Deleted == DeletedState.Deleted:
                                message.deleted = 1
                            message.source = node.AbsolutePath
                            message.talker_id = talker_id
                            # 默认都是好友聊天
                            message.talker_type = CHAT_TYPE_GROUP  # 群聊天
                            # send_id
                            if "msg_from_id" in m_rec and (not m_rec["msg_from_id"].IsDBNull):
                                message.sender_id = m_rec["msg_from_id"].Value
                                if m_rec["msg_from_id"].Value in self.contact_list:
                                    message.sender_name = self.contact_list[m_rec["msg_from_id"].Value]
                                if m_rec["msg_from_id"].Value == account_id:
                                    message.is_sender = 1
                            
                            if "msg_id" in m_rec and (not m_rec["msg_id"].IsDBNull):
                                message.msg_id = m_rec["msg_id"].Value

                            if "msg_time" in m_rec and (not m_rec["msg_time"].IsDBNull):
                                message.send_time = convert_to_unixtime(m_rec["msg_time"].Value)

                            # 判断消息类型
                            if "msg_type" in m_rec and (not m_rec["msg_type"].IsDBNull):
                                
                                # text
                                if m_rec["msg_type"].Value == 0:
                                    message.type = MESSAGE_CONTENT_TYPE_TEXT
                                    message.content = m_rec["msg_text"].Value

                                # img
                                elif m_rec["msg_type"].Value == 1:
                                    message.type = MESSAGE_CONTENT_TYPE_IMAGE
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "url" in data:
                                            message.media_path = data["url"]
                                        if "name" in data:
                                            message.content = data["name"]
                                    except Exception as e:
                                        pass

                                # audio
                                elif m_rec["msg_type"].Value == 2:
                                    message.type = MESSAGE_CONTENT_TYPE_VOICE
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "url" in data:
                                            message.media_path = data["url"]
                                    except Exception as e:
                                        pass
                                    message.content = m_rec["msg_text"].Value
                                
                                # video
                                elif m_rec["msg_type"].Value == 3:
                                    message.type = MESSAGE_CONTENT_TYPE_VIDEO
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "url" in data:
                                            message.media_path = data["url"]
                                    except Exception as e:
                                        pass
                                    message.content = m_rec["msg_text"].Value

                                # share location
                                elif m_rec["msg_type"].Value == 4:
                                    message.type = MESSAGE_CONTENT_TYPE_LOCATION
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        loc = model_im.Location()
                                        if "lng" in data:
                                            loc.longitude = data["lng"]
                                        if "lat" in data:
                                            loc.latitude = data["lat"]
                                        if "title" in data:
                                            message.content = data["title"]
                                            loc.address = data["title"]
                                        self.bulletMessage.db_insert_table_location(loc)
                                    except Exception as e:
                                        pass

                                # call 或者系统消息
                                elif m_rec["msg_type"].Value == 5:
                                    try:
                                        message.type = MESSAGE_CONTENT_TYPE_VOIP
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "duration" in data:
                                            message.content = "通话拨打时长 " + str(data["duration"]) + "s"
                                        else:
                                            message.type = MESSAGE_CONTENT_TYPE_SYSTEM
                                            if "data" in data:
                                                sysyem_data = data["data"]
                                                self.get_chatroom_info(m_rec["msg_content"].Value)
                                                if "uinfos" in sysyem_data:
                                                    if len(sysyem_data["uinfos"]) == 1:
                                                        if "3" in sysyem_data["uinfos"][0]:
                                                            message.content = sysyem_data["uinfos"][0]["3"] + " 离开了群"
                                                    elif len(sysyem_data["uinfos"]) == 2:
                                                        if "3" in sysyem_data["uinfos"][0]:
                                                            message.content = sysyem_data["uinfos"][0]["3"] + " 进入了群"
                                    except Exception as e:
                                        pass

                                # system messages
                                elif m_rec["msg_type"].Value == 10:
                                    message.type = CHAT_TYPE_SYSTEM
                                    message.content = m_rec["msg_text"].Value
                                
                                # card
                                elif m_rec["msg_type"].Value == 100 and m_rec["client_type"].Value == 1:
                                    message.type = MESSAGE_CONTENT_TYPE_CONTACT_CARD
                                    try:
                                        data = json.loads(m_rec["msg_content"].Value)
                                        if "data" in data:
                                            if "SMCardName" in data["data"]:
                                                message.content = data["data"]["SMCardName"]
                                    except Exception as e:
                                        pass

                                # 红包
                                elif m_rec["msg_type"].Value == 100 and (not m_rec["push_content"].IsDBNull):
                                    message.type = MESSAGE_CONTENT_TYPE_RED_ENVELPOE
                                    message.content = m_rec["push_content"].Value

                            if message.account_id and message.talker_id:
                                self.bulletMessage.db_insert_table_message(message)
                        except Exception as e:
                            TraceService.Trace(TraceLevel.Info, e)
            except Exception as e:
                TraceService.Trace(TraceLevel.Info, e)
        self.bulletMessage.db_commit()

    
    def get_chatroom(self, account_id):
        try:
            for g_id, g_name in self.group_info.items():
                chatroom = model_im.Chatroom()
                chatroom.account_id = account_id
                chatroom.chatroom_id = g_id
                chatroom.name = g_name
                chatroom.type = 1
                try:
                    self.bulletMessage.db_insert_table_chatroom(chatroom)
                except Exception as e:
                    pass
        except Exception as e:
            pass
        self.bulletMessage.db_commit()


    def get_chatroom_info(self, data):
        try:
            info = json.loads(data)
            if "data" in info and "tinfo" in info["data"] and "1" in info["data"]["tinfo"] and "3" in info["data"]["tinfo"]:
                    if info["data"]["tinfo"]["1"] not in self.group_info:
                        self.group_info[info["data"]["tinfo"]["1"]] = info["data"]["tinfo"]["3"]
        except Exception as e:
            pass


def analyze_bulletMessage(node, extractDeleted, extractSource):
    
    pr = ParserResults()
    # pr.Categories = DescripCategories.bulletMessage
    results = bulletMessage(node, extractDeleted, extractSource).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("子弹短信")               
    return pr