#coding:utf-8

import PA_runtime
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
clr.AddReference('PNFA.InfraLib.Exts')
clr.AddReference("PNFA.Formats.NextStep")
try:
    clr.AddReference('model_ticketing')
    clr.AddReference('bcp_other')
    clr.AddReference("model_im")
    clr.AddReference("bcp_gis")
except:
    pass
del clr
import os
from collections import defaultdict
import re
import pickle
import System
from System.Xml.Linq import *
from System.Xml.XPath import Extensions as XPathExtensions
from System.Data.SQLite import *
import model_ticketing
import json
import model_map
import model_im
import bcp_gis
from PA.Formats.NextStep import *
from PA.InfraLib.Extensions import PlistHelper


MESSAGE_STATUS_DEFAULT = 0
MESSAGE_STATUS_UNSENT = 1
MESSAGE_STATUS_SENT = 2
MESSAGE_STATUS_UNREAD = 3
MESSAGE_STATUS_READ = 4

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


"""
0未知、1已使用、2未使用、3已退票、9其他
"""
TICKET_STATUS_UNKNOWN = "0"
TICKET_STATUS_USED = "1"
TICKET_STATUS_UNUSE = "2"
TICKET_STATUS_REFUND = "3"
TICKET_STATUS_OTHER = "9"


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
        

class Ctrip(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extractDeleted = extract_Deleted
        self.extractSource = extract_Source
        self.ctrip = model_ticketing.Ticketing()
        self.contact_dicts = {}
        self.auth_dicts = {}
        self.check_auth = ""
        self.cache = ds.OpenCachePath("携程")

    # 账户信息
    def get_account(self):
        account_node = self.root.GetByPath("Documents/CTChatV2")
        if account_node is None:
            return
        for filename in account_node.Files:
            if filename.Name.startswith("m"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'user_info' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature('user_info')
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account = model_ticketing.model_im.Account()
                            account.source = filename.AbsolutePath
                            if rec.Deleted == DeletedState.Deleted:
                                account.deleted = 1
                            account.sourceFile = filename.AbsolutePath
                            if "userID" in rec and (not rec["userID"].IsDBNull):
                                account.account_id = rec["userID"].Value
                            if "nickName" in rec and (not rec["nickName"].IsDBNull):
                                account.nickname = rec["nickName"].Value
                            if "avatar" in rec and (not rec["avatar"].IsDBNull):
                                account.photo = rec["avatar"].Value
                            if "age" in rec and (not rec["age"].IsDBNull):
                                account.age = rec["age"].Value
                            if "gender" in rec and(not rec["gender"].IsDBNull):
                                account.gender = rec["gender"].Value
                            if account.account_id:
                                self.ctrip.db_insert_table_account(account)
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()


    def get_contact_name(self):
        contact_node = self.root.GetByPath("Documents/CTChatV2")
        if contact_node is None:
            return
        for filename in contact_node.Files:
            if filename.Name.startswith("m"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'contact' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("contact")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted, True):
                        if canceller.IsCancellationRequested:
                            return
                        if "id" in rec and "name" in rec:
                            self.contact_dicts[rec["id"].Value] = rec["name"].Value
                except Exception as e:
                    print(e)

    def get_groups_member(self):
        groups_member_node = self.root.GetByPath("Documents/CTChatV2")
        if groups_member_node is None:
            return
        for filename in groups_member_node.Files:
            if filename.Name.startswith("m"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'group_member' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("group_member")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account_id = filename.Name.replace(".db","")
                            groups_member = model_ticketing.model_im.ChatroomMember()
                            groups_member.source = filename.AbsolutePath
                            if rec.Deleted ==  DeletedState.Deleted:
                                groups_member.deleted = 1
                            groups_member.account_id = account_id
                            if "conversationID" in rec and (not rec["conversationID"].IsDBNull):
                                groups_member.chatroom_id = rec["conversationID"].Value
                            if "userId" in rec and (not rec["userId"].IsDBNull):
                                groups_member.member_id = rec["userId"].Value
                            if "g_nickName" in rec and (not rec["g_nickName"].IsDBNull):
                                groups_member.display_name = rec["g_nickName"].Value
                            if "userAvatar" in rec and (not rec["userAvatar"].IsDBNull):
                                groups_member.photo = rec["userAvatar"].Value
                            if groups_member.account_id and groups_member.chatroom_id and groups_member.member_id:
                                self.ctrip.db_insert_table_chatroom_member(groups_member)
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()


    def get_groups(self):
        groups_node = self.root.GetByPath("Documents/CTChatV2")
        if groups_node is None:
            return
        for filename in groups_node.Files:
            if filename.Name.startswith("m"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'group_info' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("group_info")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account_id = filename.Name.replace(".db","")
                            groups = model_ticketing.model_im.Chatroom()
                            groups.source = filename.AbsolutePath
                            if rec.Deleted ==  DeletedState.Deleted:
                                groups.deleted = 1
                            groups.account_id = account_id
                            if "conversationID" in rec and (not rec["conversationID"].IsDBNull):
                                groups.chatroom_id = rec["conversationID"].Value
                            if "groupName" in rec and (not rec["groupName"].IsDBNull):
                                groups.name = rec["groupName"].Value
                            if "memberCount" in rec and (not rec["memberCount"].IsDBNull):
                                groups.member_count = rec["memberCount"].Value
                        except Exception as e:
                            print(e)
                        if groups.account_id and groups.chatroom_id and groups.member_count:
                            self.ctrip.db_insert_table_chatroom(groups)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()


    def get_messages(self):
        messages_node = self.root.GetByPath("Documents/CTChatV2")
        if messages_node is None:
            return
        for filename in messages_node.Files:
            if filename.Name.startswith("m"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'message_v2' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("message_v2")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account_id = filename.Name.replace(".db","")
                            messages = model_ticketing.model_im.Message()
                            messages.source = filename.AbsolutePath
                            if rec.Deleted ==  DeletedState.Deleted:
                                messages.deleted = 1
                            messages.account_id = account_id
                            messages.talker_type = 2
                            if "conversationID" in rec and (not rec["conversationID"].IsDBNull):
                                messages.talker_id = rec["conversationID"].Value
                            if "msgFrom" in rec and (not rec["msgFrom"].IsDBNull):
                                messages.sender_id = rec["msgFrom"].Value
                                if rec["msgFrom"].Value in self.contact_dicts:
                                    messages.sender_name = self.contact_dicts[rec["msgFrom"].Value]
                                if rec["msgFrom"].Value.lower() == account_id.lower():
                                    messages.is_sender = 1
                            if "messageID" in rec and (not rec["messageID"].IsDBNull):
                                messages.msg_id = rec["messageID"].Value
                            if "msgType" in rec and (not rec["msgType"].IsDBNull):
                                chat_content = ""
                                if rec["msgType"].Value == 0:
                                    messages.type = MESSAGE_CONTENT_TYPE_TEXT
                                    messages.content = rec["msgBody"].Value

                                elif rec["msgType"].Value == 1:
                                    messages.type = MESSAGE_CONTENT_TYPE_IMAGE
                                    chat_content = json.loads(rec["msgBody"].Value)
                                    try:
                                        if "url" in chat_content:
                                            messages.media_path = chat_content["url"]
                                    except Exception as e:
                                        messages.content = rec["msgBody"].Value

                                elif rec["msgType"].Value == 2:
                                    messages.type = MESSAGE_CONTENT_TYPE_LINK
                                    try:
                                        chat_content = json.loads(rec["msgBody"].Value)
                                        if "title" in chat_content:
                                            messages.content = chat_content["title"]
                                        if "url" in chat_content:
                                            messages.media_path = chat_content["url"]
                                    except Exception as e:
                                        messages.content = rec["msgBody"].Value

                                elif rec["msgType"].Value == 4:
                                    messages.type = MESSAGE_CONTENT_TYPE_VOICE
                                    try:
                                        chat_content = json.loads(rec["msgBody"].Value)
                                        if "title" in chat_content:
                                            messages.content = chat_content["title"]
                                        if "audio" in chat_content and "url" in chat_content["audio"]:
                                            messages.media_path = chat_content["audio"]["url"]
                                    except Exception as e:
                                        messages.content = rec["msgBody"].Value

                                elif rec["msgType"].Value == 6:
                                    messages.type = MESSAGE_CONTENT_TYPE_LOCATION
                                    try:
                                        chat_location = model_ticketing.model_im.Location()
                                        chat_content = json.loads(rec["msgBody"].Value)
                                        if "location" in chat_content and "address" in chat_content["location"]:
                                            chat_location.address = chat_content["location"]["address"]
                                        if "location" in chat_content and "lng" in chat_content["location"]:
                                            chat_location.longitude = chat_content["location"]["lng"]
                                        if "location" in chat_content and "lat" in chat_content["location"]:
                                            chat_location.latitude = chat_content["location"]["lat"]
                                        if "timestamp" in rec and (not rec["timestamp"].IsDBNull):
                                            chat_location.timestamp = convert_to_unixtime(rec["timestamp"].Value)
                                        self.ctrip.db_insert_table_location(chat_location)
                                        messages.extra_id = chat_location.location_id
                                    except Exception as e:
                                        messages.content = rec["msgBody"].Value

                                elif rec["msgType"].Value == 7:
                                    try:
                                        messages.type = MESSAGE_CONTENT_TYPE_LINK
                                        chat_content = json.loads(rec["msgBody"].Value)
                                        if "ext" in chat_content and chat_content["ext"]:
                                            if "extendInfo" in chat_content and chat_content["extendInfo"]:
                                                messages.media_path = chat_content["extendInfo"]["toJumpUrl"]
                                            if "dataInfoList" in chat_content and chat_content["dataInfoList"]:
                                                messages.media_path = chat_content["dataInfoList"]["text"]
                                        elif "ext" in chat_content and not chat_content["ext"]:
                                            if "title" in chat_content:
                                                messages.content = chat_content["title"]
                                    except Exception as e:
                                        messages.content = rec["msgBody"].Value
                                        
                            if "isRead" in rec and (not rec["isRead"].IsDBNull):
                                if rec["isRead"].Value == 1:
                                    messages.status = MESSAGE_STATUS_READ
                                else:
                                    messages.status = MESSAGE_STATUS_UNREAD
                            
                            if "timestamp" in rec and (not rec["timestamp"].IsDBNull):
                                messages.send_time = convert_to_unixtime(rec["timestamp"].Value)

                            if messages.account_id and messages.sender_id:
                                self.ctrip.db_insert_table_message(messages)
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()

    # 订单信息
    def get_order_ticket(self):
        order_node = self.root.GetByPath("Documents/ctrip_scheduleinfo.db")
        if order_node is None:
            return
        try:
            db = SQLiteParser.Database.FromNode(order_node, canceller)
            if db is None:
                return
            tb = SQLiteParser.TableSignature("schedule_offlinecache")
            for rec in db.ReadTableRecords(tb, self.extractDeleted):
                if   ("userId" in rec and self.check_auth in self.auth_dicts and rec["userId"].Value.lower() == self.auth_dicts[self.check_auth]):
                    return
                if canceller.IsCancellationRequested:
                    return
                if "key" in rec and rec["key"].Value == "scheduleList":
                        if "val" in rec and (not rec["val"].IsDBNull):
                            try:
                                payment_list = json.loads(rec["val"].Value)
                                if "cardGroupList" in payment_list and "cardList" in payment_list["cardGroupList"][0]:
                                    for order in payment_list["cardGroupList"][0]["cardList"]:
                                        order_ticket = model_map.LocationJourney()
                                        order_ticket.source = "携程"
                                        order_ticket.sourceFile = order_node.AbsolutePath
                                        if rec.Deleted ==  DeletedState.Deleted:
                                            order_ticket.deleted = 1
                                        if "userId" in rec and (not rec["userId"].IsDBNull):
                                            order_ticket.account_id = rec["userId"].Value.lower()
                                        if "ticketCard" in order and "scenicSpotName" in order["ticketCard"]:
                                            order_ticket.materials_name =  order["ticketCard"]["scenicSpotName"]
                                        if "ticketCard" in order and "orderId" in order["ticketCard"]:
                                            order_ticket.order_num = order["ticketCard"]["orderId"]
                                        if "ticketCard" in order and "recommendPlayTime" in order["ticketCard"]:
                                            order_ticket.remark = order["ticketCard"]["recommendPlayTime"]
                                        if "ticketCard" in order and "location" in order["ticketCard"]:
                                            tmp_data = order["ticketCard"]["location"]
                                            if "longitude" in tmp_data:
                                                order_ticket.destination_longitude = float(tmp_data["longitude"])
                                            if "latitude" in tmp_data:
                                                order_ticket.destination_latitude = float(tmp_data["latitude"])
                                            if "scenicSpotName" in tmp_data:
                                                order_ticket.materials_name = tmp_data["materials_name"]
                                        self.ctrip.db_insert_table_journey(order_ticket)
                            except Exception as e:
                                print(e)
        except Exception as e:
            print(e)
        self.ctrip.db_commit()


    def get_order_ticket_method_two(self):
        order_node = self.root.GetByPath("Library/Caches/CTMyCtrip")
        for filename in order_node.Files:
            if filename.Name.startswith("pocketData_V2_"):
                plist = PlistHelper.ReadPlist(filename)
                object_list = list(plist["$objects"])
                for i in object_list:
                    if  len(str(i)) == 64:
                        self.auth_dicts[str(i)] = filename.Name[14:25].lower()
                        
            
    def get_route_from_dict(self, bp, uid):
        values = {}
        attrs = dir(bp[uid])
        if "Keys" in attrs:
            for key in bp[uid].Keys:
                if key in ["bus.history.type"]:
                    values[key] =  bp[uid][key].Value
                else:
                    tmp = bp[uid][key].Value
                    values[key] = bp[tmp]
        else:
            pass
        return values


    def get_order_data(self):
        fs = self.root.FileSystem
        results = fs.Search("myctrip_offlineorder.txt")
        result = None
        if results:
            for files_node in results:
                if files_node.Name.startswith("myctrip"):
                    result = files_node
            if result is None:
                return
            file_data = result.PathWithMountPoint
            with open(file_data, 'r') as f:
                order_json = json.loads(f.read())
                if "Auth" in order_json:
                    auth_value = order_json["Auth"]
                    self.check_auth = auth_value
                    if "OrderEnities" in order_json:
                        tmp_data = order_json["OrderEnities"]
                        for item in tmp_data:
                            order_ticket =  model_map.LocationJourney()
                            order_ticket.source = "携程"
                            order_ticket.sourceFile = result.AbsolutePath
                            if auth_value in self.auth_dicts:
                                order_ticket.account_id = self.auth_dicts[auth_value].lower()
                            if "OrderName" in item:
                                order_ticket.materials_name = item["OrderName"]
                            if "OrderTotalPrice" in item:
                                order_ticket.purchase_price = item["OrderTotalPrice"]
                            if "Longitude" in item:
                                order_ticket.destination_longitude = item["Longitude"]
                            if "Latitude" in item:
                                order_ticket.destination_latitude = item["Latitude"]
                            if "OrderID" in item:
                                order_ticket.sequence_name = item["OrderID"]
                            if "BookingDate" in item:
                                order_ticket.order_time = self.transfer_time(item["BookingDate"])
                            if "PiaoOrderItems" in item:
                                for i in item["PiaoOrderItems"]:
                                    if "Address" in i:
                                        order_ticket.destination_address = i["Address"]
                                    if "ProductID" in i:
                                        order_ticket.order_num = i["ProductID"]
                            self.ctrip.db_insert_table_journey(order_ticket)

            self.ctrip.db_commit()
                                
    @staticmethod
    def get_dict_from_bplist(bp, dict_ind):
        values = {}
        for key in bp[dict_ind].Keys:
            val_ind = bp[dict_ind][key].Value
            values[key] = bp[val_ind]
        return values
    
    @staticmethod
    def get_btree_node_str(b, k, d = ""):
        if k in b.Children and b.Children[k] is not None:
            try:
                return str(b.Children[k].Value)
            except:
                return d
        return d

    @staticmethod
    def transfer_time(value):
        reg = re.compile(".*\((.*)\+.*\).*")
        results = re.match(reg, value)
        if results:
            return convert_to_unixtime(results.groups()[0])



    # 历史访问记录
    def get_pageview_cache(self):
        pass

    def parse(self):

        db_path = self.cache + "\\ctrip.db"
        self.ctrip.db_create(db_path)
        self.get_account()
        self.get_groups_member()
        self.get_groups()
        self.get_messages()
        self.get_order_ticket_method_two()
        self.get_order_data()
        self.get_order_ticket()
        self.get_pageview_cache()
        self.ctrip.db_close()

        tmp_dir = ds.OpenCachePath("tmp")
        PA_runtime.save_cache_path(bcp_gis.NETWORK_APP_TICKET_CTRIP, db_path, tmp_dir)

        models = []
        im_results = model_im.GenerateModel(db_path).get_models()
        ticket_results = model_map.Genetate(db_path).get_models()
        models.extend(im_results)
        models.extend(ticket_results)
        return models

def analyze_ctrip(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    results = Ctrip(node, extract_Deleted, extract_Source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("携程")
    return pr

