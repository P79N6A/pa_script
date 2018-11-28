#coding:utf-8

import PA_runtime
from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
clr.AddReference('PNFA.InfraLib.Exts')
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
import hashlib

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
    if len(str(timestamp)) == 13:
        timestamp = int(str(timestamp)[0:10])
    elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
        timestamp = 0
    elif len(str(timestamp)) == 10:
        timestamp = timestamp
    return timestamp


def md5(cache_path, node_path):
    m = hashlib.md5()   
    m.update(node_path.encode(encoding = 'utf-8'))
    db_path = cache_path + "\\" + m.hexdigest() + ".db"
    return db_path


class Ctrip(object):

    def __init__(self, node, extract_Deleted, extract_Source):
        self.root = node
        self.extractDeleted = extract_Deleted
        self.extractSource = extract_Source
        self.ctrip = model_ticketing.Ticketing()
        self.contact_dicts = {}
        self.current_id = None
        self.cache = ds.OpenCachePath("携程")


    def get_account(self):
        for filename in self.root.Files:
            if filename.Name.startswith("CTChat2") and filename.Name.endswith(".db"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'USER_INFO' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature('USER_INFO')
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account = model_im.Account()
                            account.source = filename.AbsolutePath
                            if rec.Deleted == DeletedState.Deleted:
                                account.deleted = 1
                            account.sourceFile = filename.AbsolutePath
                            if "USER_ID" in rec and (not rec["USER_ID"].IsDBNull):
                                account.account_id = rec["USER_ID"].Value
                            if "NICK_NAME" in rec and (not rec["NICK_NAME"].IsDBNull):
                                account.nickname = rec["NICK_NAME"].Value
                            if "AVATAR" in rec and (not rec["AVATAR"].IsDBNull):
                                account.photo = rec["AVATAR"].Value
                            if "AGE" in rec and (not rec["AGE"].IsDBNull):
                                account.age = rec["AGE"].Value
                            if "GENDER" in rec and(not rec["GENDER"].IsDBNull):
                                account.gender = rec["GENDER"].Value
                            if account.account_id:
                                self.ctrip.db_insert_table_account(account)
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()


    def get_contact_name(self):
        for filename in self.root.Files:
           if filename.Name.startswith("CTChat2") and filename.Name.endswith(".db"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'CONTACT_INFO' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("CONTACT_INFO")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted, True):
                        if canceller.IsCancellationRequested:
                            return
                        if "CONTACT_ID" in rec and "NICK_NAME" in rec:
                            self.contact_dicts[rec["CONTACT_ID"].Value] = rec["NICK_NAME"].Value
                except Exception as e:
                    print(e)


    
    def get_groups_member(self):
        for filename in self.root.Files:
            if filename.Name.startswith("CTChat2") and filename.Name.endswith(".db"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'GROUP_MEMBER' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("GROUP_MEMBER")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account_id = filename.Name.replace(".db","").replace("CTChat2_","").lower()
                            groups_member = model_im.ChatroomMember()
                            groups_member.source = filename.AbsolutePath
                            if rec.Deleted ==  DeletedState.Deleted:
                                groups_member.deleted = 1
                            groups_member.account_id = account_id
                            if "CONVERSATION_ID" in rec and (not rec["CONVERSATION_ID"].IsDBNull):
                                groups_member.chatroom_id = rec["CONVERSATION_ID"].Value
                            if "USER_ID" in rec and (not rec["USER_ID"].IsDBNull):
                                groups_member.member_id = rec["USER_ID"].Value
                            if "NICK_NAME" in rec and (not rec["NICK_NAME"].IsDBNull):
                                groups_member.display_name = rec["NICK_NAME"].Value
                            if "USER_AVATAR" in rec and (not rec["USER_AVATAR"].IsDBNull):
                                groups_member.photo = rec["USER_AVATAR"].Value
                            if groups_member.account_id and groups_member.chatroom_id and groups_member.member_id:
                                self.ctrip.db_insert_table_chatroom_member(groups_member)
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()


    def get_groups(self):
        for filename in self.root.Files:
            if filename.Name.startswith("CTChat2") and filename.Name.endswith(".db"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'GROUP_INFO' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("GROUP_INFO")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account_id = filename.Name.replace(".db","").replace("CTChat2_","").lower()
                            groups = model_im.Chatroom()
                            groups.source = filename.AbsolutePath
                            if rec.Deleted ==  DeletedState.Deleted:
                                groups.deleted = 1
                            groups.account_id = account_id
                            if "CONVERSATION_ID" in rec and (not rec["CONVERSATION_ID"].IsDBNull):
                                groups.chatroom_id = rec["CONVERSATION_ID"].Value
                            if "GROUP_NAME" in rec and (not rec["GROUP_NAME"].IsDBNull):
                                groups.chatroom_id = rec["GROUP_NAME"].Value
                            if "MEMBER_COUNT" in rec and (not rec["MEMBER_COUNT"].IsDBNull):
                                groups.member_count = rec["MEMBER_COUNT"].Value
                        except Exception as e:
                            print(e)
                        if groups.account_id and groups.chatroom_id and groups.member_count:
                            self.ctrip.db_insert_table_chatroom(groups)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()


    def get_messages(self):
        for filename in self.root.Files:
            if filename.Name.startswith("CTChat2") and filename.Name.endswith(".db"):
                try:
                    db = SQLiteParser.Database.FromNode(filename, canceller)
                    if db is None:
                        return
                    if 'MESSAGE' not in db.Tables:
                        return
                    tb = SQLiteParser.TableSignature("MESSAGE")
                    for rec in db.ReadTableRecords(tb, self.extractDeleted):
                        if canceller.IsCancellationRequested:
                            return
                        try:
                            account_id = filename.Name.replace(".db","").replace("CTChat2_","").lower()
                            messages = model_im.Message()
                            messages.source = filename.AbsolutePath
                            if rec.Deleted ==  DeletedState.Deleted:
                                messages.deleted = 1
                            messages.account_id = account_id
                            messages.talker_type = 2
                            if "CONVERSATION_ID" in rec and (not rec["CONVERSATION_ID"].IsDBNull):
                                messages.talker_id = rec["CONVERSATION_ID"].Value
                            if "MSG_FROM" in rec and (not rec["MSG_FROM"].IsDBNull):
                                messages.sender_id = rec["MSG_FROM"].Value
                                if rec["MSG_FROM"].Value in self.contact_dicts:
                                    messages.sender_name = self.contact_dicts[rec["MSG_FROM"].Value]
                                if rec["MSG_FROM"].Value.lower() == account_id.lower():
                                    messages.is_sender = 1
                            if "MESSAGE_ID" in rec and (not rec["MESSAGE_ID"].IsDBNull):
                                messages.msg_id = rec["MESSAGE_ID"].Value
                            if "MSG_TYPE" in rec and (not rec["MSG_TYPE"].IsDBNull):
                                chat_content = ""
                                if rec["MSG_TYPE"].Value == "0":
                                    messages.type = MESSAGE_CONTENT_TYPE_TEXT
                                    messages.content = rec["MSG_BODY"].Value

                                elif rec["MSG_TYPE"].Value == "1007":
                                    messages.type = MESSAGE_CONTENT_TYPE_IMAGE
                                    chat_content = json.loads(rec["MSG_BODY"].Value)
                                    try:
                                        if "ext" in chat_content and "richlist" in chat_content["ext"]:
                                            if "text" in chat_content["ext"]["richlist"][0]:
                                                messages.content = chat_content["ext"]["richlist"][0]["text"]
                                    except Exception as e:
                                        messages.content = rec["MSG_BODY"].Value

                                elif rec["MSG_TYPE"].Value == "1":
                                    messages.type = MESSAGE_CONTENT_TYPE_IMAGE
                                    chat_content = json.loads(rec["MSG_BODY"].Value)
                                    try:
                                        if "url" in chat_content:
                                            messages.media_path = chat_content["url"]
                                    except Exception as e:
                                        messages.content = rec["MSG_BODY"].Value

                                elif rec["MSG_TYPE"].Value == "2":
                                    messages.type = MESSAGE_CONTENT_TYPE_LINK
                                    try:
                                        chat_content = json.loads(rec["MSG_BODY"].Value)
                                        if "title" in chat_content:
                                            messages.content = chat_content["title"]
                                        if "url" in chat_content:
                                            messages.media_path = chat_content["url"]
                                    except Exception as e:
                                        messages.content = rec["MSG_BODY"].Value

                                elif rec["MSG_TYPE"].Value == "4":
                                    messages.type = MESSAGE_CONTENT_TYPE_VOICE
                                    try:
                                        chat_content = json.loads(rec["MSG_BODY"].Value)
                                        if "title" in chat_content:
                                            messages.content = chat_content["title"]
                                        if "audio" in chat_content and "url" in chat_content["audio"]:
                                            messages.media_path = chat_content["audio"]["url"]
                                    except Exception as e:
                                        messages.content = rec["MSG_BODY"].Value

                                elif rec["MSG_TYPE"].Value == "6":
                                    messages.type = MESSAGE_CONTENT_TYPE_LOCATION
                                    try:
                                        chat_location = model_im.Location()
                                        chat_content = json.loads(rec["MSG_BODY"].Value)
                                        if "location" in chat_content and "address" in chat_content["location"]:
                                            chat_location.address = chat_content["location"]["address"]
                                        if "location" in chat_content and "lng" in chat_content["location"]:
                                            chat_location.longitude = chat_content["location"]["lng"]
                                        if "location" in chat_content and "lat" in chat_content["location"]:
                                            chat_location.latitude = chat_content["location"]["lat"]
                                        if "TIMESTAMP" in rec and (not rec["TIMESTAMP"].IsDBNull):
                                            chat_location.timestamp = convert_to_unixtime(rec["TIMESTAMP"].Value)
                                        self.ctrip.db_insert_table_location(chat_location)
                                        messages.extra_id = chat_location.location_id
                                    except Exception as e:
                                        messages.content = rec["MSG_BODY"].Value

                                elif rec["MSG_TYPE"].Value == "7":
                                    try:
                                        messages.type = MESSAGE_CONTENT_TYPE_LINK
                                        chat_content = json.loads(rec["MSG_BODY"].Value)
                                        if "ext" in chat_content and chat_content["ext"]:
                                            if "extendInfo" in chat_content and chat_content["extendInfo"]:
                                                messages.media_path = chat_content["extendInfo"]["toJumpUrl"]
                                            if "dataInfoList" in chat_content and chat_content["dataInfoList"]:
                                                messages.media_path = chat_content["dataInfoList"]["text"]
                                            if "answer" in chat_content and chat_content["answer"]:
                                                messages.content = chat_content["answer"]
                                        elif "ext" in chat_content and not chat_content["ext"]:
                                            if "title" in chat_content:
                                                messages.content = chat_content["title"]
                                    except Exception as e:
                                        messages.content = rec["MSG_BODY"].Value
                            if "IS_READ" in rec and (not rec["IS_READ"].IsDBNull):
                                if rec["IS_READ"].Value == 1:
                                    messages.status = MESSAGE_STATUS_READ
                                else:
                                    messages.status = MESSAGE_STATUS_UNREAD

                            if "TIMESTAMP" in rec and (not rec["TIMESTAMP"].IsDBNull):
                                messages.send_time = convert_to_unixtime(rec["TIMESTAMP"].Value)

                            if messages.account_id and messages.sender_id:
                                self.ctrip.db_insert_table_message(messages)
                        except Exception as e:
                            print(e)
                except Exception as e:
                    print(e)
            self.ctrip.db_commit()


    def get_current_user(self):
        current_user = self.root.Parent.GetByPath("/shared_prefs/permission_data.xml")
        if current_user is None:
            return
        es = []
        try:
            current_user.Data.seek(0)
            xml = XElement.Parse(current_user.read())
            es = xml.Elements("string")
        except Exception as e:
            print e
        for rec in es:
            if canceller.IsCancellationRequested:
                return
            if rec.Attribute('name') and rec.Attribute('name').Value == 'new_current_account_sipinfo':
                name = rec.FirstNode.Value
                try:
                    timeid, user_id = name.split(";")
                    self.current_id = user_id.lower()
                except Exception as e:
                    print(e)


    def get_order_data(self):
        fs = self.root.FileSystem
        results = fs.Search("myctrip_offlineorder.txt")
        result = None
        if results:
            for files_node in results:
                    result = files_node
            if result is None:
                return
        else:
            return
        file_data = result.PathWithMountPoint
        with open(file_data, 'r') as f:
            order_json = json.loads(f.read())
            if "OrderEnities" in order_json:
                tmp_data = order_json["OrderEnities"]
                for item in tmp_data:
                    order_ticket = model_map.LocationJourney()
                    order_ticket.source = "携程"
                    order_ticket.sourceFile = result.AbsolutePath
                    if self.current_id is not None:
                        order_ticket.account_id = self.current_id
                    if "OrderName" in item:
                        order_ticket.materials_name = item["OrderName"]
                    if "OrderTotalPrice" in item:
                        order_ticket.purchase_price = item["OrderTotalPrice"]
                    # if "OrderStatusName" in tmp_data:
                    #     if tmp_data["OrderStatusName"] == "已取消":
                    #         order_ticket.ticket_status = "99"
                    #     elif tmp_data["OrderStatusName"] == "待付款":
                    #         order_ticket.ticket_status = "99"
                    #     elif tmp_data["OrderStatusName"] == "未提交":
                    #         order_ticket.ticket_status = "99"
                    #     elif tmp_data["OrderStatusName"] == "确认中":
                    #         order_ticket.ticket_status = "99"
                    #     elif tmp_data["OrderStatusName"] == "已确认":
                    #         order_ticket.ticket_status = "99"
                    #     elif tmp_data["OrderStatusName"] == "已付款":
                    #         order_ticket.ticket_status = "99"
                    #     elif tmp_data["OrderStatusName"] == "已成交":
                    #         order_ticket.ticket_status = "99"
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
    def transfer_time(value):
        reg = re.compile(".*\((.*)\+.*\).*")
        results = re.match(reg, value)
        if results:
            return convert_to_unixtime(results.groups()[0])
                            
    def parse(self):

        db_path = md5(self.cache, self.root.AbsolutePath)
        self.ctrip.db_create(db_path)
        self.get_account()
        self.get_contact_name()
        self.get_groups_member()
        self.get_groups()
        self.get_current_user()
        self.get_messages()
        self.get_order_data()
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