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
import base64

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
        self.cache = ds.OpenCachePath("Qunar")
        self.qunar_db = model_ticketing.Ticketing()
        self.account_id = None

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
        if self.root is None:
            return
        node_lists = self.root.Files
        if node_lists is None:
            return
        for node in node_lists:
            if node.Name.startswith("qunar_im_chats_message") and node.Name.endswith(".db"):
                self.get_account(node)
                self.get_groups(node)
                self.get_group_messages(node)
                
    
    def get_account(self, node):
        try:
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            if 'im_friend' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("im_friend")
            for rec in db.ReadTableRecords(tbs, False, True):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                if canceller.IsCancellationRequested:
                    return
                try:
                    account = model_im.Account()
                    if rec.Deleted == DeletedState.Deleted:
                        account.deleted = 1
                    account.source = node.AbsolutePath
                    if "im_user_id" in rec and (not rec["im_user_id"].IsDBNull):
                        if "relation" in rec and rec["relation"].Value == 0:
                            account.account_id = rec["im_user_id"].Value
                            self.account_id = rec["im_user_id"].Value
                            if "name" in rec and (not rec["name"].IsDBNull):
                                account.nickname = rec["name"].Value
                            if "img" in rec and (not rec["img"].IsDBNull):
                                account.photo = rec["img"].Value
                            self.qunar_db.db_insert_table_account(account)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info,"qunar(ios) get_accounts record failed")
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"qunar(ios) get_accounts failed")
        self.qunar_db.db_commit()

    
    def get_groups(self, node):
        try:
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            if 'group_detail' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("group_detail")
            for rec in db.ReadTableRecords(tbs, False, True):
                if canceller.IsCancellationRequested:
                    return
                try:
                    groups = model_im.Chatroom()
                    groups.account_id = self.account_id
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
                    # if "members_num" in rec and (not rec["members_num"].IsDBNull):
                    #     groups.member_count = rec["members_num"].Value
                    if "img_url" in rec and (not rec["img_url"].IsDBNull):
                        groups.photo = rec["img_url"].Value
                    if groups.account_id and groups.chatroom_id:
                        self.qunar_db.db_insert_table_chatroom(groups)
                except Exception as e:
                    TraceService.Trace(TraceLevel.Info,"qunar(ios) get_groups record failed")
        except Exception as e:
            TraceService.Trace(TraceLevel.Error,"qunar(ios) get_groups failed")
        self.qunar_db.db_commit()


    def get_group_messages(self, node):
        try:
            db = SQLiteParser.Database.FromNode(node, canceller)
            if db is None:
                return
            if 'message_table' not in db.Tables:
                return
            tbs = SQLiteParser.TableSignature("message_table")
            for rec in db.ReadTableRecords(tbs, self.extractDeleted, True):
                if canceller.IsCancellationRequested:
                    return
                try:
                    messages = model_im.Message()
                    messages.account_id = self.account_id
                    messages.source = node.AbsolutePath
                    if rec.Deleted ==  DeletedState.Deleted:
                        messages.deleted = 1
                    messages.account_id = self.account_id
                    messages.type = 1
                    # 群id
                    if "session_id" in rec and (not rec["session_id"].IsDBNull):
                        messages.talker_id = rec["session_id"].Value
                    # 1-系统 2-发送 3-receive
                    # 发送者id
                    if "_from" in rec and (not rec["_from"].IsDBNull):
                        messages.sender_id = rec["_from"].Value
                        if rec["_from"].Value == self.account_id:
                            messages.is_sender = 1
                    # 消息id
                    if "message_id" in rec and (not rec["message_id"].IsDBNull):
                        messages.msg_id = rec["message_id"].Value
                    
                    # 处理消息
                    if "msg" in rec and (not rec["msg"].IsDBNull):
                        base_str = rec["msg"].Value.strip("\n")
                        decode_str = base64.b64decode(base_str)
                        # 处理字符串变成json可处理的格式
                        format_json_str = decode_str.decode('utf-8', errors='ignore').replace("\\","").replace('"{','{"') \
                        .replace("/**##**","").replace("**##**/",'",').replace('{"time"','"time"') \
                        .replace('}"','').replace('{"data"','"data"').replace('""', '"').replace('":",','":"",')
                        try:
                            c = json.loads(format_json_str)
                            content = ""
                            if "title" in c:
                                try:
                                    content = c["title"] 
                                    if "items" in c:
                                        for item in c["items"]:
                                            if "clickAct" in item:
                                                if "ctnt" in item["clickAct"]:
                                                    content += item["clickAct"]["ctnt"] + " |"
                                    if "imgurl" in c:
                                        content += c["imgurl"]
                                except Exception as e:
                                    pass
                            
                            if "items" in c:
                                for item in c["items"]:
                                    if "itemText" in item:
                                        content += item["itemText"]

                            if "name" in c:
                                try:
                                    media_name = c["name"]
                                    if "url" in c:
                                        messages.type = 2
                                        url = c["url"]
                                        messages.media_path = url
                                    content = media_name + " |" + url
                                except Exception as e:
                                    pass

                            if "status" in c:
                                try:
                                    ticket = model_map.LocationJourney()
                                    ticket.account_id =  messages.account_id
                                    if "orderNo" in c:
                                        ticket.order_num = c["orderNo"]
                                    if "status" in c:
                                        if c["status"] == "订单取消" or c["status"] == "订单已取消":
                                            ticket.ticket_status = 3
                                        content += c["status"] + " |"
                                    if "send_time" in rec and (not rec["send_time"].IsDBNull):
                                        ticket.order_time = convert_to_unixtime(rec["send_time"].Value)
                                    if "price" in c:
                                        try:
                                            price = c["price"].split("￥")[1]
                                            ticket.purchase_price = float(price)
                                        except Exception as e:
                                            pass
                                        content += c["price"] + " |"
                                    if "products" in c:
                                        products_info = c["products"][0]
                                        if "body" in products_info:
                                            for b_item in products_info["body"]:
                                                if b_item:
                                                    if "value" in b_item[0]:
                                                        content += b_item[0]["value"] + " |"
                                        if "title" in products_info:
                                            for t_item in products_info["title"]:
                                                if "type" in t_item:
                                                    if t_item["type"] == "icon":
                                                        break
                                                if t_item:
                                                    if "value" in t_item:
                                                        content += t_item["value"] + " |"
                                            try:
                                                if len(products_info["title"]) >= 4:
                                                    ticket.depart_address = products_info["title"][1]["value"]
                                                    ticket.destination_address = products_info["title"][3]["value"]
                                                else:
                                                    ticket.name = products_info["title"][1]["value"]
                                            except Exception as e:
                                                pass
                                    self.qunar_db.db_insert_table_journey(ticket)
                                except Exception as e:
                                    pass
                            messages.content = content
                        except Exception as e:
                            messages.content = format_json_str

                    if "send_time" in rec and (not rec["send_time"].IsDBNull):
                        messages.send_time = convert_to_unixtime(rec["send_time"].Value)
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


def analyze_qunar(node, extract_Deleted, extract_Source):
    pr = ParserResults()
    results = Qunar(node, extract_Deleted, extract_Source).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("去哪儿")
    return pr