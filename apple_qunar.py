#coding:utf-8

__author__ = "Xu Tao"

import PA_runtime
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference("model_im")
    clr.AddReference("model_map")
    clr.AddReference("model_ticketing")
except:
    pass
del clr
import System
from System.Xml.Linq import *
from System.Data.SQLite import *
import model_im
import model_map
import model_ticketing
import json


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
        

class Qunar(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extractDeleted = extract_Deleted
        self.extractSource = extract_Source
        self.account_list = []
        self.cache = ds.OpenCachePath("Qunar")
        self.qunar_db = model_ticketing.Ticketing()

    def parse(self):
        db_path = model_map.md5(self.cache, self.root.AbsolutePath)
        self.qunar_db.db_create(db_path)
        self.main()
        self.qunar_db.db_close()

        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path("05005", db_path, tmp_dir)
        im_models = model_im.GenerateModel(db_path).get_models()
        map_models = model_map.Genetate(db_path).get_models()
        results = []
        results.extend(im_models)
        results.extend(map_models)
        return results


    def main(self):
        nodes = self.root.Parent.Parent.Parent.GetByPath("/Documents/QOCDB")
        if nodes is None:
            return
        node_lists = nodes.Files
        if node_lists is None:
            return
        for node in node_lists:
            if node.Name.startswith("DB_") and node.Name.find(".") == -1:
                filename = node.Name
                index = filename.find("_")
                account_id = filename[index+1:]
                self.get_accounts(node, account_id)
                self.get_groups(node)
                self.get_group_member(node, account_id)
                self.get_group_messages(node, account_id)
        self.get_passager()

    def get_accounts(self, node, account_id):
        try:
            # results = []
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            if 'QOCFriend' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("QOCFriend")
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                if canceller.IsCancellationRequested:
                    return
                try:
                    account = model_im.Account()
                    if rec.Deleted == DeletedState.Deleted:
                        account.deleted = 1
                    account.source = node.AbsolutePath
                    if "im_user_id" in rec and (not rec["im_user_id"].IsDBNull):
                        account.account_id = rec["im_user_id"].Value
                    if "name" in rec and (not rec["name"].IsDBNull):
                        account.nickname = rec["name"].Value
                    if "img" in rec and (not rec["img"].IsDBNull):
                        account.photo = rec["img"].Value
                    if account.account_id == account_id:
                        self.qunar_db.db_insert_table_account(account)
                        # results.append(account)
                        self.account_list.append(account)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info,"qunar(ios) get_accounts record failed")
            # return results
        except Exception as e:
            # print(e)
            TraceService.Trace(TraceLevel.Error,"qunar(ios) get_accounts failed")
        self.qunar_db.db_commit()

    def get_groups(self, node):
        try:
            results = []
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            if 'QOCGroupDetail' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("QOCGroupDetail")
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                if canceller.IsCancellationRequested:
                    return
                try:
                    groups = model_im.Chatroom()
                    groups.source = node.AbsolutePath
                    groups.type = 1
                    if rec.Deleted ==  DeletedState.Deleted:
                        groups.deleted = 1
                    if "owner_id" in rec and (not rec["owner_id"].IsDBNull):
                        groups.account_id = rec["owner_id"].Value
                    if "session_id" in rec and (not rec["session_id"].IsDBNull):
                        groups.chatroom_id = rec["session_id"].Value
                    if "title" in rec and (not rec["title"].IsDBNull):
                        groups.name = rec["title"].Value
                    if "members_num" in rec and (not rec["members_num"].IsDBNull):
                        groups.member_count = rec["members_num"].Value
                    if "img_url" in rec and (not rec["img_url"].IsDBNull):
                        groups.photo = rec["img_url"].Value
                    if groups.account_id and groups.chatroom_id:
                        self.qunar_db.db_insert_table_chatroom(groups)
                        # resluts.append(groups)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info,"qunar(ios) get_groups record failed")
            # return results
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"qunar(ios) get_groups failed")
            # return []
        self.qunar_db.db_commit()
        
    def get_group_member(self, node, account_id):
        try:
            # results = []
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            if 'QOCGroupMember' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("QOCGroupMember")
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                if canceller.IsCancellationRequested:
                    return
                try:
                    groups_member = model_im.ChatroomMember()
                    groups_member.account_id = account_id
                    groups_member.source = node.AbsolutePath
                    if rec.Deleted ==  DeletedState.Deleted:
                        groups_member.deleted = 1
                    if "group_id" in rec and (not rec["group_id"].IsDBNull):
                        groups_member.chatroom_id = rec["group_id"].Value
                    if "friend_id" in rec and (not rec["friend_id"].IsDBNull):
                        groups_member.member_id = rec["friend_id"].Value
                        if self.account_list:
                            # 根据之前得到的account赋值
                            for account in self.account_list:
                                if account.account_id == rec["friend_id"].Value:
                                    groups_member.display_name = account.nickname
                                    groups_member.photo = account.photo
                    if groups_member.chatroom_id and groups_member.member_id:
                        self.qunar_db.db_insert_table_chatroom_member(groups_member)
                        # results.append(groups_member)
                except Exception as e:
                    print(e)
                    # TraceService.Trace(TraceLevel.Info,"qunar(ios) get_group_member record failed")
            # return results
        except Exception as e:
            print(e)
            # TraceService.Trace(TraceLevel.Error,"qunar(ios) get_group_member failed")
            # return []
        self.qunar_db.db_commit()


    def get_group_messages(self, node, account_id):
        try:
            # results = []
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            if 'QOCMessage' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("QOCMessage")
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                if canceller.IsCancellationRequested:
                    return
                try:
                    messages = model_im.Message()
                    messages.source = node.AbsolutePath
                    if rec.Deleted ==  DeletedState.Deleted:
                        messages.deleted = 1
                    messages.account_id = account_id
                    messages.type = 2
                    # 群id
                    if "sId" in rec and (not rec["sId"].IsDBNull):
                        messages.talker_id = rec["sId"].Value
                    # 1-系统 2-发送 3-receive
                    messages.type = 2
                    # 发送者id
                    if "fromId" in rec and (not rec["fromId"].IsDBNull):
                        messages.sender_id = rec["fromId"].Value
                        if rec["fromId"].Value == account_id:
                            messages.is_sender = 1
                    # 消息id
                    if "messageId" in rec and (not rec["messageId"].IsDBNull):
                        messages.msg_id = rec["messageId"].Value
                    if "type" in rec and (not rec["type"].IsDBNull) and "content" in rec:
                        message_type = rec["type"].Value

                        json_data = ""
                        # 文本
                        if message_type == 1:
                            messages.type = MESSAGE_CONTENT_TYPE_TEXT
                            messages.content = rec["content"].Value
                        # 图片
                        if message_type == 2:
                            try:
                                messages.type = MESSAGE_CONTENT_TYPE_IMAGE
                                json_data = json.loads(rec["content"].Value)
                                if "url" in json_data:
                                    messages.media_path = json_data["url"]
                            except Exception as e:
                                TraceService.Trace(TraceLevel.Error,"{0},{1}".format(e,rec.Deleted))

                        # 位置
                        if message_type == 4:
                            try:
                                messages.type = MESSAGE_CONTENT_TYPE_LOCATION
                                json_data = json.loads(rec["content"].Value)
                                if "g" in json_data:
                                    loc = json_data["g"]
                                    try:
                                        lat,lng = loc.split(",")
                                        share_loc = model_im.Location()
                                        share_loc.source = node.AbsolutePath
                                        share_loc.latitude = float(lat)
                                        share_loc.longitude = float(lng)
                                        if "st" in rec and (not rec["st"].IsDBNull):
                                            share_time = convert_to_unixtime(rec["st"].Value)
                                        messages.extra_id = share_loc.location_id
                                        self.qunar_db.db_insert_table_location(share_loc)
                                    except Exception as e:
                                        pass
                                if "title" in json_data:
                                    messages.content = json_data["title"]
                                if "imgurl" in json_data:
                                    messages.media_path = json_data["imgurl"]
                            except Exception as e:
                                TraceService.Trace(TraceLevel.Error,"{0},{1}".format(e,rec.Deleted))

                        # 链接
                        if message_type == 13:
                            try:
                                messages.type = MESSAGE_CONTENT_TYPE_LINK
                                json_data = json.loads(rec["content"].Value)
                                content = ""
                                if "title" in json_data:
                                    content += json_data["title"] + "; "
                                if "items" in json_data:
                                    for item in json_data["items"]:
                                        content += item["itemText"] + "; "
                                if content is not None:
                                    messages.content = content
                            except Exception as e:
                                TraceService.Trace(TraceLevel.Error,"{0},{1}".format(e,rec.Deleted))

                        if message_type == 15:
                            try:
                                messages.type = MESSAGE_CONTENT_TYPE_LINK
                                json_data = json.loads(rec["content"].Value)
                                content = ""
                                if "item" in json_data:
                                    for item in json_data["item"]:
                                        if "itemText" in item:
                                            content += item["itemText"] + "| "
                                    if content is not None:
                                        messages.content = content
                            except Exception as e:
                                TraceService.Trace(TraceLevel.Error,"{0},{1}".format(e,rec.Deleted))

                        if message_type == 16:
                            try:
                                messages.type = MESSAGE_CONTENT_TYPE_LINK
                                json_data = json.loads(rec["content"].Value)
                                content = ""
                                if "items" in json_data:
                                    for item in json_data["items"]:
                                        content += item["itemText"] + "; "
                                if content is not None:
                                    messages.content = content
                            except Exception as e:
                                TraceService.Trace(TraceLevel.Error,"{0},{1}".format(e,rec.Deleted))

                        if message_type == 19:
                            try:
                                messages.type = MESSAGE_CONTENT_TYPE_LINK
                                json_data = json.loads(rec["content"].Value)
                                content = ""
                                if "price" in json_data:
                                    content += json_data["price"] + "| "
                                if "status" in json_data:
                                    content += json_data["status"] + "| "
                                if "products" in json_data:
                                    for item in json_data:
                                        if "body" in item:
                                            for i in item["body"]:
                                                for j in i:
                                                    if "value" in j:
                                                        content += j["value"] + "| "
                                        if "title" in item:
                                            for k in item["title"]:
                                                if "value" in j:
                                                    content += j["value"] + "| "
                                if content is not None:
                                    messages.content = content
                            except Exception as e:
                                TraceService.Trace(TraceLevel.Error,"{0},{1}".format(e,rec.Deleted))
                        
                    else:
                        messages.content = rec["content"].Value

                    if "st" in rec and (not rec["st"].IsDBNull):
                        messages.send_time = convert_to_unixtime(rec["st"].Value)
                    if messages.account_id and messages.sender_id:
                        self.qunar_db.db_insert_table_message(messages)
                        # results.append(messages)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info,"qunar(ios) get_group_messages record failed")
            # return results
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"qunar(ios) get_group_messages failed")
            # return []
        self.qunar_db.db_commit()
    

    def get_order_ticket(self):
        pass

    def get_passager(self):
        try:
            node = self.root
            if node is None:
                return
            bplist = BPReader.GetTree(node)
            if "kUserCacheKey" in bplist:
                if "kLastUserId" in bplist["kUserCacheKey"]:
                    owner_id = bplist["kUserCacheKey"]["kLastUserId"].Value
            if "cachePassengerKey" in bplist:
                passenger = model_map.Passenger()
                passenger.source = "去哪儿旅行"
                passenger.sourceFile = node.AbsolutePath
                passenger_info = bplist["cachePassengerKey"]
                if "passengerEmail" in passenger_info:
                    pass
                if "passengerMobile" in passenger_info:
                    passenger.phone = str(passenger_info["passengerMobile"].Value)
                if "SOrderPassengerIDCard" in passenger_info:
                    passenger.certificate_code = str(passenger_info["SOrderPassengerIDCard"].Value)
                if "passengerName" in passenger_info:
                    passenger.name = passenger_info["passengerName"].Value
                self.qunar_db.db_insert_table_passenger(passenger)
            self.qunar_db.db_commit()
        except Exception as e:
            # print(e)
            TraceService.Trace(TraceLevel.Error,"qunar(ios) get_passager failed")

def analyze_qunar(node, extract_Deleted, extract_Source):
    # faker_node = FileSystem.FromLocalDir(r"C:\Users\xutao\Desktop\去哪儿\去哪儿 (1)\private\var\mobile\Containers\Data\Application\49856083-6CFA-4514-B679-3EFC4471998E")
    pr = ParserResults()
    results = Qunar(node, extract_Deleted, extract_Source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("去哪儿旅行")
    return pr
