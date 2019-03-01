#coding=utf-8

__author__ = "Xu Tao"

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
    clr.AddReference("safe_read_sqlite")
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


class Blued(object):

    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.blued = model_im.IM()
        self.cache = ds.OpenCachePath("Blued")
        self.friend_list = {}
        self.group_list = {}

    
    def parse(self):
        db_path = model_map.md5(self.cache, self.root.AbsolutePath)
        self.blued.db_create(db_path)
        self.main()
        self.blued.db_close()

        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path("05005", db_path, tmp_dir)
        im_models = model_im.GenerateModel(db_path).get_models()
        # map_models = model_map.Genetate(db_path).get_models()
        results = []
        if results:
            results.extend(im_models)
        # results.extend(map_models)
        return results
    

    def other_parse(self):
        db_path = model_map.md5(self.cache, self.root.AbsolutePath)
        self.blued.db_create(db_path)
        self.get_account(self.root)
        node_lists = self.root.Parent.Children
        if len(node_lists) == 0:
            return
        for node in node_lists:
            if node.Name.endswith("DB.sqlite"):
                account_id = node.Name.replace("DB.sqlite","")
                self.get_groups_friends(node, account_id)
                self.get_messages(node, account_id)
        self.blued.db_close()

        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path("05005", db_path, tmp_dir)
        im_models = model_im.GenerateModel(db_path).get_models()
        # map_models = model_map.Genetate(db_path).get_models()
        results = []
        if results:
            results.extend(im_models)
        # results.extend(map_models)
        return results

    
    def main(self):
        account_node = self.root.Parent.Parent.Parent.GetByPath("LoginDB.sqlite")
        self.get_account(account_node)
        node_lists = self.root.Parent.Parent.Parent.Children
        for node in node_lists:
            if node.Name.endswith("DB.sqlite"):
                account_id = node.Name.replace("DB.sqlite","")
                self.get_groups_friends(node, account_id)
                self.get_messages(node, account_id)

    def get_account(self, node):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'loginUserTable' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("loginUserTable")
        for rec in db.ReadTableRecords(tbs, False):
            try:
                if canceller.IsCancellationRequested:
                    return
                account = model_im.Account()
                account.source = node.AbsolutePath
                if "uid" in rec and (not rec["uid"].IsDBNull):
                    account.account_id = rec["uid"].Value
                if "password" in rec and (not rec["password"].IsDBNull):
                    account.password = rec["password"].Value
                if "loginDataString" in rec and (not rec["loginDataString"].IsDBNull):
                    data = json.loads(rec["loginDataString"].Value)
                    if "name" in data:
                        account.nickname = data["name"]
                    if "avatar" in data:
                        account.photo = data["avatar"]
                    if "birthday" in data:
                        account.birthday = data["birthday"]
                    if "age" in data:
                        account.age = data["age"]
                if account.account_id:
                    self.blued.db_insert_table_account(account)
            except Exception as e:
                pass
        self.blued.db_commit()


    def get_groups_friends(self, node, account_id):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'sessionTable' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("sessionTable")
        for rec in db.ReadTableRecords(tbs, False):
            try:
                if canceller.IsCancellationRequested:
                    return
                # 群
                if "sessionType" in rec and rec["sessionType"].Value == 3:
                    group = model_im.Chatroom()
                    group.account_id = account_id
                    group.source = node.AbsolutePath
                    group.type = 1
                    if "sessionAvatar" in rec and (not rec["sessionAvatar"].IsDBNull):
                        group.photo = rec["sessionAvatar"].Value
                    if "sessionName" in rec and (not rec["sessionName"].IsDBNull):
                        group.name = rec["sessionName"].Value
                    if "sessionId" in rec and (not rec["sessionId"].IsDBNull):
                        group.chatroom_id = rec["sessionId"].Value
                    if group.chatroom_id and group.name:
                        self.group_list[group.chatroom_id] = group.name
                    if group.account_id and group.chatroom_id:
                        self.blued.db_insert_table_chatroom(group)
                # 好友
                elif  "sessionType" in rec and rec["sessionType"].Value == 2:
                    friend = model_im.Friend()
                    friend.account_id = account_id
                    friend.source = node.AbsolutePath
                    friend.type = 1
                    if "sessionAvatar" in rec and (not rec["sessionAvatar"].IsDBNull):
                        friend.photo = rec["sessionAvatar"].Value
                    if "sessionName" in rec and (not rec["sessionName"].IsDBNull):
                        friend.nickname = rec["sessionName"].Value
                    if "sessionId" in rec and (not rec["sessionId"].IsDBNull):
                        friend.friend_id = rec["sessionId"].Value
                    if friend.friend_id and friend.nickname:
                        self.friend_list[friend.friend_id] = friend.nickname
                    if friend.account_id and friend.friend_id:
                        self.blued.db_insert_table_friend(friend)
            except Exception as e:
                pass
        self.blued.db_commit()

    
    def get_messages(self, node, account_id):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'messageTable' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("messageTable")
        for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                messages = model_im.Message()
                messages.account_id = account_id
                messages.source = node.AbsolutePath
                if rec.Deleted ==  DeletedState.Deleted:
                    messages.deleted = 1
                if "sessionId" in rec and (not rec["sessionId"].IsDBNull):
                    messages.talker_id = rec["sessionId"].Value
                    if rec["sessionId"].Value in self.friend_list.keys():
                        messages.talker_type = 1
                        messages.talker_name = self.friend_list[rec["sessionId"].Value]
                    elif rec["sessionId"].Value in self.group_list.keys():
                        messages.talker_type = 2
                        messages.talker_name = self.group_list[rec["sessionId"].Value]
                if "fromId" in rec and (not rec["fromId"].IsDBNull):
                    messages.sender_id = rec["fromId"].Value
                    if rec["fromId"].Value == int(account_id):
                        messages.is_sender = 1
                if "fromUserName" in rec and (not rec["fromUserName"].IsDBNull):
                    messages.sender_name = rec["fromUserName"].Value
                if "sendTime" in rec and (not rec["sendTime"].IsDBNull):
                    messages.send_time = convert_to_unixtime(rec["sendTime"].Value)
                if "messageType" in rec and (not rec["messageType"].IsDBNull):
                    messages.type = 1 
                    msg_type = rec["messageType"].Value

                    if msg_type == 1:  # text
                        messages.type = 1 
                        messages.content = rec["messageContent"].Value

                    elif msg_type == 2: # img
                        messages.type = 2
                        if rec["messageContent"].Value.find("||") != -1:
                            messages.media_path = rec["messageContent"].Value.split("||")[1]
                        elif rec["messageContent"].Value == "deleted":
                            messages.content = rec["messageContent"].Value
                        else:
                            messages.media_path = rec["messageContent"].Value
                    
                    elif msg_type == 3: # voice
                        messages.type = 3
                        if rec["messageContent"].Value.find("||") != -1:
                            messages.media_path = rec["messageContent"].Value.split("||")[1]
                        else:
                            messages.content = rec["messageContent"].Value

                    elif msg_type == 4: # location
                        messages.type = 7
                        try:
                            lng,lat,addr = rec["messageContent"].Value.split(",")
                            location = messages.create_location()
                            location.latitude = lat
                            location.longitude = lng
                            location.address = addr
                            messages.insert_db(self.blued)
                        except Exception as e:
                            pass
                    
                    elif msg_type == 5:  # video
                        messages.type = 4
                        if rec["messageContent"].Value.find("||") != -1:
                            messages.media_path = rec["messageContent"].Value.split("||")[1]
                        else:
                            messages.media_path = rec["messageContent"].Value

                    elif msg_type == 6:
                        messages.content = rec["messageContent"].Value

                    elif msg_type == 10:
                        messages.content = rec["messageContent"].Value

                    elif msg_type == 11:
                        messages.type = 99
                        messages.content = rec["messageContent"].Value

                    elif msg_type == 12:
                        messages.type = 99
                        messages.content = rec["messageContent"].Value

                    elif msg_type == 13:
                        messages.type = 99
                        messages.content = rec["messageContent"].Value

                    elif msg_type == 14:
                        messages.type = 99
                        messages.content = rec["messageContent"].Value

                    elif msg_type == 24:
                        messages.type = 2
                        messages.content = "照片已销毁"

                    elif msg_type == 41:  # 直播
                        try:
                            if "msgExtra" in rec and (not rec["msgExtra"].IsDBNull):
                                data = json.loads(rec["msgExtra"].Value)
                        except Exception as e:
                            pass

                    # elif msg_type == 75:  # live
                    #     try:
                    #         if "msgExtra" in rec and (not rec["msgExtra"].IsDBNull):
                    #             data = json.loads(rec["msgExtra"].Value)

                    #     except Exception as e:
                    #         pass
                    
                    elif msg_type == 55:  # 撤回
                        messages.type = 99
                        messages.content = rec["fromUserName"].Value + "  撤回了该条消息。"

                    elif msg_type == 56:  # card
                        try:
                            messages.type = 6
                            if "messageContent" in rec and (not rec["messageContent"].IsDBNull):
                                data = json.loads(rec["messageContent"].Value)
                                if "avatar" in data:
                                    messages.media_path = data["avatar"]
                                if  "name" in data:
                                    name = data["name"]
                        except Exception as e:
                            pass

                    elif msg_type == 58:  # 直播
                        try:
                            if "messageContent" in rec and (not rec["messageContent"].IsDBNull):
                                data = json.loads(rec["messageContent"].Value)
                                if "gif" in data:
                                    messages.media_path = data["gif"]
                        except Exception as e:
                            pass

                    elif msg_type == 41:  # live
                        try:
                            if "msgExtra" in rec and (not rec["msgExtra"].IsDBNull):
                                data = json.loads(rec["msgExtra"].Value)
                                messages.content = rec["messageContent"].Value
                                if "avatar" in data:
                                    messages.media_path = data["avatar"]
                        except Exception as e:
                            pass

                    elif msg_type == 67:  # feed
                        try:
                            if "messageContent" in rec and (not rec["messageContent"].IsDBNull):
                                data = json.loads(rec["msgExtra"].Value)
                                messages.content = rec["messageContent"].Value
                                feed = model_im.Feed()
                                if "feed_img_url" in data:
                                    feed.image_path = data["feed_img_url"]
                                if "feed_text" in data:
                                    feed.content = data["feed_text"]
                                if "feed_time" in data:
                                    feed.send_time = convert_to_unixtime(data["feed_time"])
                                feed.account_id = account_id
                                if "fromId" in rec and (not rec["fromId"].IsDBNull):
                                    feed.sender_id = rec["fromId"].Value
                                self.blued.db_insert_table_feed(feed)
                        except Exception as e:
                            pass

                    elif msg_type == 75:  # live
                        try:
                            if "messageContent" in rec and (not rec["messageContent"].IsDBNull):
                                data = json.loads(rec["msgExtra"].Value)
                                messages.content = rec["messageContent"].Value
                                if "avatar" in data:
                                    messages.media_path = data["avatar"]
                        except Exception as e:
                            pass
            except Exception as e:
                print(e)
            if messages.account_id and messages.talker_id and messages.sender_id:  
                self.blued.db_insert_table_message(messages)
        self.blued.db_commit()


def analyze_blued(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    results = Blued(node, extract_Deleted, extract_Source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Blued")
    return pr


def analyze_blued_other(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    results = Blued(node, extract_Deleted, extract_Source).other_parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("Blued")
    return pr