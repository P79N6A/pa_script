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
from safe_read_sqlite import SqliteByCSharp

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
        self.friend_list = {}
        self.group_list = {}
        self.all_send_nick_name = {}

    def parse(self):
        db_path = model_map.md5(self.cache, self.root.AbsolutePath)
        self.bulletMessage.db_create(db_path)
        self.main()
        self.bulletMessage.db_close()

        # tmp_dir = ds.OpenCachePath("tmp")
        # PA_runtime.save_cache_path("05005", db_path, tmp_dir)
        im_models = model_im.GenerateModel(db_path).get_models()
        # map_models = model_map.Genetate(db_path).get_models()
        results = []
        results.extend(im_models)
        # results.extend(map_models)
        return results

    def main(self):
        if self.root is None:
            return
        node_lists = self.root.Children
        if node_lists is None:
            return
        for a_node in node_lists:
            if a_node.Type == NodeType.Directory:
                self.friend_list = {}
                self.group_list = {}
                self.all_send_nick_name = {}
                account_id = a_node.Name
                self.get_account(a_node, account_id)
                self.get_friend(a_node, account_id)
                self.get_groups_member(a_node, account_id)
                self.get_groups(a_node, account_id)
                self.get_all_sendname(a_node)
                self.get_messages(a_node, account_id)


    def get_account(self, node, account_id):
        a_node = node.GetByPath("nim_cache.db")
        if a_node is None:
            return
        try:
            conn = SqliteByCSharp(a_node, self.cache)
            with conn as cmd:

                cmd.CommandText = 'select account, name, icon, sign, gender, mobile from uinfo where account = {0}'.format(account_id)
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        a_id = SqliteByCSharp.GetString(reader, 0)
                        nickname = SqliteByCSharp.GetString(reader, 1)
                        avtar = SqliteByCSharp.GetString(reader, 2)
                        sign = SqliteByCSharp.GetString(reader, 3)
                        gender = SqliteByCSharp.GetInt64(reader, 4)
                        phone = SqliteByCSharp.GetString(reader, 5)
                        
                        account = model_im.Account()
                        account.source = a_node.AbsolutePath
                        account.account_id = a_id
                        account.nickname = nickname
                        account.photo = avtar
                        account.gender = gender
                        account.signature = sign
                        account.telephone = phone

                        if account.account_id:
                            self.bulletMessage.db_insert_table_account(account)
                    except Exception as e:
                        pass
        except Exception as e:
            pass
        self.bulletMessage.db_commit()


    def get_friend(self, node, account_id):
        f_node = node.GetByPath("nim_cache.db")
        if f_node is None:
            return
        try:
            conn = SqliteByCSharp(f_node, self.cache)
            with conn as cmd:

                cmd.CommandText = '''
                    select uinfo.account,uinfo.name,uinfo.icon,uinfo.sign,uinfo.gender,uinfo.email,
                    uinfo.mobile from uinfo,friend where uinfo.account = friend.account
                    '''
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        friend_id = SqliteByCSharp.GetString(reader, 0)
                        nickname = SqliteByCSharp.GetString(reader, 1)
                        avtar = SqliteByCSharp.GetString(reader, 2)
                        sign = SqliteByCSharp.GetString(reader, 3)
                        gender = SqliteByCSharp.GetInt64(reader, 4)
                        email = SqliteByCSharp.GetString(reader, 5)
                        phone = SqliteByCSharp.GetString(reader, 6)
                        
                        self.friend_list[friend_id] = nickname

                        friend = model_im.Friend()
                        friend.source = f_node.AbsolutePath
                        friend.account_id = account_id
                        friend.friend_id = friend_id
                        friend.nickname = nickname
                        friend.photo = avtar
                        friend.telephone = phone
                        friend.signature = sign
                        friend.email = email
                        friend.type = model_im.FRIEND_TYPE_FRIEND

                        if friend.account_id and friend.friend_id:
                            self.bulletMessage.db_insert_table_friend(friend)
                    except Exception as e:
                        print(e)
        except Exception as e:
            print(e)
        self.bulletMessage.db_commit()


    def get_groups_member(self, node, account_id):
        g_node = node.GetByPath("nim_cache.db")
        if g_node is None:
            return
        try:
            conn = SqliteByCSharp(g_node, self.cache)
            with conn as cmd:

                cmd.CommandText = '''
                    select tid, account, nick, join_time from tuser
                    '''
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        chatroom_id = SqliteByCSharp.GetString(reader, 0)
                        memeber_id = SqliteByCSharp.GetString(reader, 1)
                        display_name = SqliteByCSharp.GetString(reader, 2)
                        
                        member = model_im.ChatroomMember()
                        member.source = g_node.AbsolutePath
                        member.account_id = account_id
                        member.chatroom_id = chatroom_id
                        member.member_id = memeber_id
                        member.display_name = display_name

                        if member.account_id and member.chatroom_id and member.member_id:
                            self.bulletMessage.db_insert_table_chatroom_member(member)
                    except Exception as e:
                        pass
        except Exception as e:
            pass
        self.bulletMessage.db_commit()


    def get_groups(self, node, account_id):
        groups_node = node.GetByPath("nim_cache.db")
        if groups_node is None:
            return
        try:
            conn = SqliteByCSharp(groups_node, self.cache)
            with conn as cmd:

                cmd.CommandText = '''
                    select id,name,creator,level,count,introduce,create_time,icon from team
                    '''
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        chatroom_id = SqliteByCSharp.GetString(reader, 0)
                        name = SqliteByCSharp.GetString(reader, 1)
                        creator_id = SqliteByCSharp.GetString(reader, 2)
                        max_member_count = SqliteByCSharp.GetInt64(reader, 3)
                        member_count = SqliteByCSharp.GetInt64(reader, 4)
                        notice = SqliteByCSharp.GetString(reader, 5)
                        create_time = SqliteByCSharp.GetInt64(reader, 6)
                        photo = SqliteByCSharp.GetString(reader, 7)
                        
                        self.group_list[chatroom_id] = name

                        chatroom = model_im.Chatroom()
                        chatroom.source = groups_node.AbsolutePath
                        chatroom.account_id = account_id
                        chatroom.chatroom_id = chatroom_id
                        chatroom.name = name
                        chatroom.creator_id = creator_id
                        chatroom.max_member_count = max_member_count
                        chatroom.member_count = member_count
                        chatroom.notice = notice
                        chatroom.photo = photo
                        chatroom.type = 1  # 普通群

                        if chatroom.account_id and chatroom.chatroom_id:
                            self.bulletMessage.db_insert_table_chatroom(chatroom)
                    except Exception as e:
                        print(e)
        except Exception as e:
            print(e)
        self.bulletMessage.db_commit()

    
    def get_all_sendname(self, node):
        s_node = node.GetByPath("msg.db")
        if s_node is None:
            return
        try:
            conn = SqliteByCSharp(s_node, self.cache)
            with conn as cmd:

                cmd.CommandText = '''
                    select account, nick from sender_nick
                    '''
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        self.all_send_nick_name[SqliteByCSharp.GetString(reader, 0)] = SqliteByCSharp.GetString(reader, 1)
                    except Exception as e:
                        pass
        except Exception as e:
            pass    

    def get_messages(self, node, account_id):
        m_node = node.GetByPath("msg.db")
        if m_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(m_node, canceller)
            if db is None:
                return
            if 'msghistory' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("msghistory")
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    messages = model_im.Message()
                    messages.account_id = account_id
                    messages.source = m_node.AbsolutePath
                    if rec.Deleted ==  DeletedState.Deleted:
                        messages.deleted = 1
                    messages.type = 1  # 文本

                    if "id" in rec and (not rec["id"].IsDBNull):
                        messages.talker_id = rec["id"].Value
                        if rec["id"].Value in self.friend_list.keys():
                            messages.talker_type = CHAT_TYPE_FRIEND
                            messages.talker_name = self.friend_list[rec["id"].Value]
                        if rec["id"].Value in self.group_list.keys():
                            messages.talker_type = CHAT_TYPE_GROUP 
                            messages.talker_name = self.group_list[rec["id"].Value]
                    
                    if "fromid" in rec and (not rec["fromid"].IsDBNull):
                        messages.sender_id = rec["fromid"].Value
                        if rec["fromid"].Value in self.all_send_nick_name:
                            messages.sender_name = self.all_send_nick_name[rec["fromid"].Value]
                        if rec["fromid"].Value == account_id:
                            messages.is_sender = 1

                    if "time" in rec and (not rec["time"].IsDBNull):
                        messages.send_time = convert_to_unixtime(rec["time"].Value)
                    
                    # 判断消息类型
                    if "msgtype" in rec and (not rec["msgtype"].IsDBNull):
                        
                        # text
                        if rec["msgtype"].Value == 0:
                            messages.type = MESSAGE_CONTENT_TYPE_TEXT
                            messages.content = rec["content"].Value

                        # img
                        elif rec["msgtype"].Value == 1:
                            messages.type = MESSAGE_CONTENT_TYPE_IMAGE
                            try:
                                data = json.loads(rec["attach"].Value)
                                if "url" in data:
                                    messages.media_path = data["url"]
                                if "name" in data:
                                    messages.content = data["name"]
                            except Exception as e:
                                pass

                        # audio
                        elif rec["msgtype"].Value == 2:
                            messages.type = MESSAGE_CONTENT_TYPE_VOICE
                            try:
                                data = json.loads(rec["attach"].Value)
                                if "url" in data:
                                    messages.media_path = data["url"]
                            except Exception as e:
                                pass
                            messages.content = rec["content"].Value
                        
                        # video
                        elif rec["msgtype"].Value == 3:
                            messages.type = MESSAGE_CONTENT_TYPE_VIDEO
                            try:
                                data = json.loads(rec["attach"].Value)
                                if "url" in data:
                                    messages.media_path = data["url"]
                            except Exception as e:
                                pass
                            messages.content = rec["content"].Value

                        # share location
                        elif rec["msgtype"].Value == 4:
                            messages.type = MESSAGE_CONTENT_TYPE_LOCATION
                            try:
                                data = json.loads(rec["attach"].Value)
                                loc = model_im.Location()
                                if "lng" in data:
                                    loc.longitude = data["lng"]
                                if "lat" in data:
                                    loc.latitude = data["lat"]
                                if "title" in data:
                                    messages.content = data["title"]
                                    loc.address = data["title"]
                                self.bulletMessage.db_insert_table_location(loc)
                            except Exception as e:
                                pass

                        # call 或者系统消息
                        elif rec["msgtype"].Value == 5:
                            try:
                                messages.type = MESSAGE_CONTENT_TYPE_VOIP
                                data = json.loads(rec["attach"].Value)
                                if "duration" in data:
                                    messages.content = "通话拨打时长 " + str(data["duration"]) + "s"
                                else:
                                    messages.type = MESSAGE_CONTENT_TYPE_SYSTEM
                                    if "data" in data:
                                        sysyem_data = data["data"]
                                        self.get_chatroom_info(rec["attach"].Value)
                                        if "uinfos" in sysyem_data:
                                            if len(sysyem_data["uinfos"]) == 1:
                                                if "3" in sysyem_data["uinfos"][0]:
                                                    messages.content = sysyem_data["uinfos"][0]["3"] + " 离开了群"
                                            elif len(sysyem_data["uinfos"]) == 2:
                                                if "3" in sysyem_data["uinfos"][0]:
                                                    messages.content = sysyem_data["uinfos"][0]["3"] + " 进入了群"
                            except Exception as e:
                                pass

                        # system messages
                        elif rec["msgtype"].Value == 10:
                            messages.type = CHAT_TYPE_SYSTEM
                            messages.content = rec["content"].Value
                        
                        # card
                        elif rec["msgtype"].Value == 100 and rec["fromclient"].Value == 1:
                            messages.type = MESSAGE_CONTENT_TYPE_CONTACT_CARD
                            try:
                                data = json.loads(rec["attach"].Value)
                                if "data" in data:
                                    if "SMCardName" in data["data"]:
                                        messages.content = data["data"]["SMCardName"]
                            except Exception as e:
                                pass

                        # 红包
                        elif rec["msgtype"].Value == 100 and (not rec["push"].IsDBNull):
                            messages.type = MESSAGE_CONTENT_TYPE_RED_ENVELPOE
                            messages.content = rec["push"].Value

                        if messages.account_id and messages.talker_id:
                            self.bulletMessage.db_insert_table_message(messages)
                except Exception as e:
                    print(e)  
        except Exception as e:
            print(e)
        self.bulletMessage.db_commit()
    
    def get_chatroom_info(self, data):
        try:
            info = json.loads(data)
            if "data" in info and "tinfo" in info["data"] and "1" in info["data"]["tinfo"] and "3" in info["data"]["tinfo"]:
                    if info["data"]["tinfo"]["1"] not in self.group_info:
                        self.group_info[info["data"]["tinfo"]["1"]] = info["data"]["tinfo"]["3"]
        except Exception as e:
            pass

hitdict =  {'(?i)com.bullet.messenger/fef5a045d42e55b17455ce99544955df':('子弹短信',ParserResults())}    

def checkhit(root):
    nodes = []
    global hitdict
    for re in hitdict.keys():                 
        node = root.FileSystem.Search(re)
        if(len(list(node)) != 0):
            nodes.append((node,hitdict[re]))
    return nodes


def analyze_bulletMessage(node, extractDeleted, extractSource):
    TraceService.Trace(TraceLevel.Info,"正在分析安卓聊天宝...")
    pr = ParserResults()
    nodes = checkhit(node)
    if len(nodes) != 0:
        progress.Start()
        for anode in nodes:
            for root in anode[0]:
                results = bulletMessage(root, extractDeleted, extractSource).parse()
                if results:
                    pr.Models.AddRange(results)
                    pr.Build("聊天宝")
                    TraceService.Trace(TraceLevel.Info,"安卓聊天宝分析完成！")               
                return pr
    else:
        TraceService.Trace(TraceLevel.Info,"安卓聊天宝未找到对应节点，跳过分析") 
        progress.Skip()
        return pr
