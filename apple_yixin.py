#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
    clr.AddReference('ScriptUtils')
    clr.AddReference('android_yixin')
except:
    pass
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *


import os
import sqlite3
import json
import model_im
import bcp_im
import gc
from android_yixin import parse_yixin_msg_content 
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, base_analyze, parse_decorator, BaseParser


# app数据库版本
VERSION_APP_VALUE = 2

def analyze_yixin(root, extract_deleted, extract_source):
    if root.AbsolutePath == '/data/media/0/Android/data/im.yixin':
        return    
    tp(root.AbsolutePath)
    pr = ParserResults()
    _pr = base_analyze(AppleYiXinParser, 
                       root, 
                       bcp_im.CONTACT_ACCOUNT_TYPE_IM_YIXIN, 
                       VERSION_APP_VALUE,
                       build_name='易信',
                       db_name='yixin_i')  
    gc.collect()
    pr.Add(_pr)
    return pr
                        
    
class AppleYiXinParser(BaseParser):
    def __init__(self, node, db_name):
        super(AppleYiXinParser, self).__init__(node, db_name)
        self.root = node
        self.csm = model_im.IM()
        self.Generate = model_im.GenerateModel
        self.user_list = []

    def parse(self, BCP_TYPE, VERSION_APP_VALUE):
        self.user_list = self.get_user_list()
        if not self.user_list:
            return []
        model = super(AppleYiXinParser, self).parse(BCP_TYPE, VERSION_APP_VALUE)
        mlm = ModelListMerger()
        return list(mlm.GetUnique(model))

    def parse_main(self):
        for user in self.user_list:
            self.contacts = {}
            self.user = user
            self.parse_user()
            self.user = None
            self.contacts = None

    def get_user_list(self):
        user_list = []
        node = self.root.GetByPath('../../../Documents/')
        if node is not None:
            for file in os.listdir(node.PathWithMountPoint):
                    if file.isdigit():
                        user_list.append(file)
        return user_list

    def parse_user(self):
        self.get_user()
        self.get_contacts()
        self.get_chats()

    def get_user(self):
        if self.user is None:
            return

        account = model_im.Account()
        account.account_id = self.user
        account.username = self.user
        self.csm.db_insert_table_account(account)
        self.csm.db_commit()

    def get_contacts(self):
        if self.user is None:
            return
        
        dbPath = self.root.GetByPath('../../../Documents/' + self.user + '/msg2.db')
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'msglog' in db.Tables:
            ts = SQLiteParser.TableSignature('msglog')
            SQLiteParser.Tools.AddSignatureToTable(ts, "id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if canceller.IsCancellationRequested:
                    return
                contact = {'deleted' : rec.Deleted, 'repeated' : 0}
                contactid = rec['id'].Value
                type = rec['msg_type'].Value
                if contactid in self.contacts:
                    continue
                else:
                    self.contacts[contactid] = contact

                if type == 1:
                    friend = model_im.Friend()
                    friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.repeated = contact.get('repeated', 0)
                    friend.source = dbPath.AbsolutePath

                    friend.account_id = self.user
                    friend.friend_id = contactid
                    friend.type = model_im.FRIEND_TYPE_FRIEND
                    self.csm.db_insert_table_friend(friend)

                if type == 2:
                    chatroom = model_im.Chatroom()
                    chatroom.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    chatroom.repeated = contact.get('repeated', 0)
                    chatroom.source = dbPath.AbsolutePath

                    chatroom.account_id = self.user
                    chatroom.chatroom_id = contactid
                    self.csm.db_insert_table_chatroom(chatroom)

                if type == 6:
                    friend = model_im.Friend()
                    friend.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    friend.repeated = contact.get('repeated', 0)
                    friend.source = dbPath.AbsolutePath

                    friend.account_id = self.user
                    friend.friend_id = contactid
                    friend.type = model_im.FRIEND_TYPE_SUBSCRIBE
                    self.csm.db_insert_table_friend(friend)
            self.csm.db_commit()

    def get_chats(self):
        ''' FieldName	    SQLType        	
            serial	            INTEGER
            uuid	            TEXT
            id	                TEXT
            msg_from_id	        TEXT
            msg_from_nickname	TEXT
            msg_body	        TEXT
            msg_type	        INTEGER
            msg_content_type	INTEGER
            msg_time	        INTEGER
            msg_status	        INTEGER
            msg_param	        TEXT
            msg_sub_status	    INTEGER
            msg_server_id	    TEXT
        ''' 
        if self.user is None:
            return
        for contact_id in self.contacts.keys():
            db_node = self.root.GetByPath('../../../Documents/' + self.user + '/msg2.db')
            if not self._read_db(node=db_node):
                continue
            if 'msglog' in self.cur_db.Tables:
                for rec in self._read_table('msglog'):

                    if (self._is_empty(rec, 'serial', 'id')
                        or self._is_duplicate(rec, 'serial')):
                        continue
                    if contact_id != rec['id'].Value:
                        continue
                    contact = self.contacts.get(contact_id)
                    message = model_im.Message()
                    message.deleted = 0 if rec.Deleted == DeletedState.Intact else 1
                    message.repeated = contact.get('repeated', 0)
                    message.source = db_node.AbsolutePath

                    message.msg_id = rec['uuid'].Value.replace('-', '')
                    message.account_id = self.user
                    message.is_sender = model_im.MESSAGE_TYPE_SEND if rec['msg_from_id'].Value == message.account_id else model_im.MESSAGE_TYPE_RECEIVE
                    message.talker_id = rec['id'].Value if message.is_sender else rec['msg_from_id'].Value
                    message.sender_id = rec['msg_from_id'].Value
                    type = rec['msg_content_type'].Value
                    message.type = self.get_message_type(type)
                    message.send_time = rec['msg_time'].Value
                    message.content = parse_yixin_msg_content(rec['msg_body'].Value)
                    contact_type = rec['msg_type'].Value
                    if contact_type == 1:
                        message.talker_type = model_im.CHAT_TYPE_FRIEND
                    if contact_type == 2:
                        message.talker_type = model_im.CHAT_TYPE_GROUP
                    if contact_type == 6:
                        message.talker_type = model_im.CHAT_TYPE_OFFICIAL
                    message.media_path = self.parse_message_content(rec['msg_body'].Value, message.type)
                    if message.type == model_im.MESSAGE_CONTENT_TYPE_LOCATION:
                        message.location_obj = message.create_location()
                        message.location_id = self.get_location(message.location_obj, rec['msg_body'].Value, message.send_time)
                    self.csm.db_insert_table_message(message)

        self.csm.db_commit()

    def get_location(self, location, content, time):
        try:
            object = json.loads(content)
            location.latitude = object['location'].split(',')[0]
            location.longitude = object['location'].split(',')[1]
            location.address = object['description']
            location.type = model_im.LOCATION_TYPE_GOOGLE
            location.timestamp = time
            
            self.csm.db_insert_table_location(location)
            self.csm.db_commit()
            return location.location_id
        except:
            exc()
            return 

    def get_message_type(self, type):
        msgtype = model_im.MESSAGE_CONTENT_TYPE_TEXT
        if type == 1:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_IMAGE
        if type == 2:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VOICE
        if type == 3:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_VIDEO
        if type == 4:
            msgtype = model_im.MESSAGE_CONTENT_TYPE_LOCATION
        #if type == 7:
        #    msgtype = model_im.MESSAGE_CONTENT_TYPE_CHARTLET
        return msgtype
    
    def parse_message_content(self, content, type):
        media_path = ""
        try:
            object = json.loads(content)
            if type == model_im.MESSAGE_CONTENT_TYPE_VIDEO:
                node = self.root.GetByPath('../../../Documents/' + self.user + '/video')
                if node is not None:
                    media_path = os.path.join(node.AbsolutePath, object['filename'])
            if type == model_im.MESSAGE_CONTENT_TYPE_VOICE:
                node = self.root.GetByPath('../../../Documents/' + self.user + '/audio')
                if node is not None:
                    media_path = os.path.join(node.AbsolutePath, object['filename'])
            if type == model_im.MESSAGE_CONTENT_TYPE_IMAGE:
                node = self.root.GetByPath('../../../Documents/' + self.user + '/image')
                if node is not None:
                    media_path = os.path.join(node.AbsolutePath, object['filename'])
            #if type == model_im.MESSAGE_CONTENT_TYPE_CHARTLET:
            #    node = self.root.GetByPath('/Documents/' + self.user + '/chartlet')
            #    if node is not None:
            #        media_path = os.path.join(node.AbsolutePath, object['filename'])
        except:
            pass
        return media_path




