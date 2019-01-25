#coding=utf-8

__author__ = "Xu Tao"

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

import PA_runtime
from PA_runtime import *

import System
from System.Xml.Linq import *
from System.Data.SQLite import *
import model_im
import model_map
import json
import traceback

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

        im_models = model_im.GenerateModel(db_path).get_models()
        # map_models = model_map.Genetate(db_path).get_models()
        results = []
        results.extend(im_models)
        # results.extend(map_models)
        return results


    def main(self):
        self.get_account(self.root)
        self.get_groups_friends(self.root)
        self.get_messages(self.root)


    def get_account(self, node):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'UserAccountsModel' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("UserAccountsModel")
        for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                account = model_im.Account()
                account.source = node.AbsolutePath
                if "`uid`" in rec and (not rec["`uid`"].IsDBNull):
                    account.account_id = rec["`uid`"].Value
                if "`passwordSha`" in rec and (not rec["`passwordSha`"].IsDBNull):
                    account.password = rec["`passwordSha`"].Value
                if "`username`" in rec and (not rec["`username`"].IsDBNull):
                    account.username = rec["`username`"].Value
                if "`loginresult`" in rec and (not rec["`loginresult`"].IsDBNull):
                    data = json.loads(rec["`loginresult`"].Value)
                    if "data" in data:
						v = data["data"]
						for item in v:
							if "name" in item:
								account.nickname = item["name"]
							if "avatar" in item:
								account.photo = item["avatar"]
                if account.account_id:
                    self.blued.db_insert_table_account(account)
            except Exception as e:
                traceback.print_exc()
        self.blued.db_commit()


    def get_groups_friends(self, node):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'SessionModel' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("SessionModel")
        for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                # 群
                if "`sessionType`" in rec and rec["`sessionType`"].Value == 3:
                    group = model_im.Chatroom()
                    group.source = node.AbsolutePath
                    group.type = 1
                    if "`loadName`" in rec and (not rec["`loadName`"].IsDBNull):
                        group.account_id = rec["`loadName`"].Value
                    if "`sessionAvatar`" in rec and (not rec["`sessionAvatar`"].IsDBNull):
                        group.photo = rec["`sessionAvatar`"].Value
                    if "`sessionNickName`" in rec and (not rec["`sessionNickName`"].IsDBNull):
                        group.name = rec["`sessionNickName`"].Value
                    if "`sessionId`" in rec and (not rec["`sessionId`"].IsDBNull):
                        group.chatroom_id = rec["`sessionId`"].Value
                    if group.chatroom_id and group.name:
                        self.group_list[group.chatroom_id] = group.name
                    if group.account_id and group.chatroom_id:
                        self.blued.db_insert_table_chatroom(group)
                # 好友
                elif  "`sessionType`" in rec and rec["`sessionType`"].Value == 2:
                    friend = model_im.Friend()
                    friend.source = node.AbsolutePath
                    friend.type = 1
                    if "`loadName`" in rec and (not rec["`loadName`"].IsDBNull):
                        friend.account_id = rec["`loadName`"].Value
                    if "`sessionAvatar`" in rec and (not rec["`sessionAvatar`"].IsDBNull):
                        friend.photo = rec["`sessionAvatar`"].Value
                    if "`sessionNickName`" in rec and (not rec["`sessionNickName`"].IsDBNull):
                        friend.nickname = rec["`sessionNickName`"].Value
                    if "`sessionId`" in rec and (not rec["`sessionId`"].IsDBNull):
                        friend.friend_id = rec["`sessionId`"].Value
                    if friend.friend_id and friend.nickname:
                        self.friend_list[friend.friend_id] = friend.nickname
                    if friend.account_id and friend.friend_id:
                        self.blued.db_insert_table_friend(friend)
            except Exception as e:
                traceback.print_exc()
        self.blued.db_commit()


    def get_messages(self, node):
        db = SQLiteParser.Database.FromNode(node, canceller)
        if db is None:
            return
        if 'ChattingModel' not in db.Tables:
            return
        tbs = SQLiteParser.TableSignature("ChattingModel")
        for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                messages = model_im.Message()
                messages.source = node.AbsolutePath
                if rec.Deleted ==  DeletedState.Deleted:
                    messages.deleted = 1
                if "`toId`" in rec and (not rec["`toId`"].IsDBNull):
                    messages.talker_id = rec["`toId`"].Value
                    if rec["`toId`"].Value in self.friend_list.keys():
                        messages.talker_type = 1
                        messages.talker_name = self.friend_list[rec["`toId`"].Value]
                    elif rec["`toId`"].Value in self.group_list.keys():
                        messages.talker_type = 2
                        messages.talker_name = self.group_list[rec["`toId`"].Value]
                if "`loadName`" in rec and (not rec["`loadName`"].IsDBNull):
                    messages.account_id = rec["`loadName`"].Value
                if "`fromId`" in rec and (not rec["`fromId`"].IsDBNull):
                    messages.sender_id = rec["`fromId`"].Value
                    if messages.account_id == messages.sender_id:
                        messages.is_sender = 1
                if "`nickName`" in rec and (not rec["`nickName`"].IsDBNull):
                    messages.sender_name = rec["`nickName`"].Value
                if "`msgTimestamp`" in rec and (not rec["`msgTimestamp`"].IsDBNull):
                    messages.send_time = convert_to_unixtime(rec["`msgTimestamp`"].Value)

                if "`msgType`" in rec and (not rec["`msgType`"].IsDBNull):
                    messages.type = 1 
                    msg_type = rec["`msgType`"].Value

                    if msg_type == 1: # text
                        messages.content = rec["`msgContent`"].Value

                    elif msg_type == 2: # pics
                        messages.type = 2
                        if rec["`msgContent`"].Value.find("||") == -1:
                            messages.media_path = rec["`msgContent`"].Value
                        else:
                            messages.content = "已销毁"
                            messages.media_path = rec["`msgContent`"].Value

                    elif msg_type == 3: # voice
                        messages.type = 3
                        if rec["`msgContent`"].Value.find(",,") == -1:
                            messages.media_path = rec["`msgContent`"].Value.split(",,")[0]
                    
                    elif msg_type == 4: # location
                        messages.type = 7
                        try:
                            lng,lat,addr = rec["`msgContent`"].Value.split(",")
                            location = messages.create_location()
                            location.latitude = lat
                            location.longitude = lng
                            location.address = addr
                            messages.insert_db(self.blued)
                        except Exception as e:
                            pass
                    
                    elif msg_type == 6: # 表情包
                        messages.content = rec["`msgContent`"].Value

                    elif msg_type == 55:  # 撤回
                        messages.type = 99
                        messages.content = rec["`nickName`"].Value+" 撤回了一条消息"

                    elif msg_type == 24 and msg_type == 25:
                        messages.type = 2
                        messages.content = "闪照"
                        messages.media_path = rec["`msgContent`"].Value
                    
                    elif msg_type == 11:
                        messages.type = 99
                        messages.content = rec["`msgContent`"].Value

                    elif msg_type == 12:
                        messages.type = 99
                        messages.content = rec["`msgContent`"].Value

                    elif msg_type == 13:
                        messages.type = 99
                        messages.content = rec["`msgContent`"].Value

                    elif msg_type == 105:
                        messages.content = "[收到一条无法识别的信息，请升级至最新版查看]"
                    
                    elif msg_type == 90:  # 推荐
                        messages.content = rec["`msgContent`"].Value
                    
                    elif msg_type == 53:
                        messages.type = 9
                        call_data = json.loads(rec["`msgContent`"].Value)
                        if "total_time" in call_data:
                            during_time = call_data["total_time"]

                    elif msg_type == 32: # 分享直播间
                        messages.content = rec["`msgContent`"].Value
            except Exception as e:
                traceback.print_exc()
            if messages.account_id and messages.talker_id and messages.sender_id:  
                self.blued.db_insert_table_message(messages)
        self.blued.db_commit()


hitdict =  {'(?i)com.soft.blued/databases/blued2015.db$':('Blued',ParserResults())}    

def checkhit(root):
    nodes = []
    global hitdict
    for re in hitdict.keys():                 
        node = root.FileSystem.Search(re)
        if(len(list(node)) != 0):
            nodes.append((node,hitdict[re]))
    return nodes

def analyze_blued(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    nodes = checkhit(node)
    if len(nodes) != 0:
        progress.Start()
        for anode in nodes:
            for root in anode[0]:
                results = Blued(root, extract_Deleted, extract_Source).parse()
                if results:
                    pr.Models.AddRange(results)
                    pr.Build("Blued")
                return pr