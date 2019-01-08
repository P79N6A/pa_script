# coding=utf-8
__author__ = "LY"
import PA_runtime
import clr
import json
from sqlite3 import *
import hashlib
clr.AddReference('System.Web')
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')
except:
    pass
del clr
from System.Data.SQLite import *
import System
from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *
from PA.InfraLib.Utils import PList
from PA.InfraLib.Extensions import PlistHelper
#from System.Collections.Generic import *
from collections import defaultdict
import logging
from  model_im import *
import uuid
#just msgdata
import json
import bcp_im
import base64

def SafeGetString(reader,i):
    try:
        if not reader.IsDBNull(i):
            return reader.GetString(i)
        else:
            return ""
    except:
        return ""

def SafeGetInt64(reader,i):
    try:
        if not reader.IsDBNull(i):
            return reader.GetInt64(i)
        else:
            return 0
    except:
        return 0
def SafeGetBlob(reader,i):
    try:
        if not reader.IsDBNull(i):
            obj = reader.GetValue(i)
            return obj #byte[]
        else:
            return ""
    except:
        return ""
def SafeGetValue(reader,i):
    try:
        if not reader.IsDBNull(i):
            obj = reader.GetValue(i)
            return obj
        else:
            return None
    except:
        return None

def analyze_apple_baidutb(root, extract_deleted, extract_source):
    try:
        pr = ParserResults()
        pr.Models.AddRange(apple_tbParser(root, extract_deleted, extract_source).parse())
        pr.Build('百度贴吧')
        return pr
    except Exception as e:
        print(e)        
class apple_tbParser(object):
    def __init__(self, app_root_dir, extract_deleted, extract_source):
        self.root = app_root_dir.Parent.Parent
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.app_name = '百度贴吧'
        self.accounts = []
        self.forumuser =collections.defaultdict(Chatroom) 
        self.friends =  collections.defaultdict(Friend)
        self.groups = collections.defaultdict(Chatroom)
        self.im = IM()
        self.cachepath = ds.OpenCachePath("baidutb")
        self.bcppath = ds.OpenCachePath("tmp")
        m = hashlib.md5()
        m.update(self.root.AbsolutePath)
        self.cachedb =  self.cachepath  + '/' + m.hexdigest().upper() + ".db"
        self.VERSION_APP_VALUE = 10000
    def parse(self):
        #self.root = r'D:\com.tencent.mobileqq'
        if self.im.need_parse(self.cachedb, self.VERSION_APP_VALUE):              
            self.im.db_create(self.cachedb)  
            self.decode_accounts()
            if len(self.accounts) == 0 :
                self.im.db_close()
                os.remove(self.cachedb)
                return
            for acc in self.accounts:
                try:
                    acc_id = acc.account_id
                    self.decode_forumuser_asgroup(acc_id)
                    self.decode_user(acc_id)
                    self.decode_groups(acc_id)
                    self.decode_msgs(acc_id)                
                except Exception as e:
                    print (e)
            self.im.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
            self.im.db_insert_table_version(VERSION_KEY_APP, self.VERSION_APP_VALUE)
            self.im.db_commit()
            self.im.db_close()
        gen = GenerateModel(self.cachedb)
        return gen.get_models()
    def decode_accounts(self):    
        try:
            node =  self.root.GetByPath('/Documents/db.sqlite')
            if node is None:
                return
            d =  node.PathWithMountPoint
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)
            command.CommandText = 'select uid ,uname from TBCLogin'            
            reader = command.ExecuteReader()            
            while reader.Read():
                try:
                    ac = Account()
                    ac.account_id = str(SafeGetInt64(reader,0))
                    ac.nickname = SafeGetString(reader,1)
                    ac.source = node.AbsolutePath
                except Exception as e:
                    print(e)
                self.accounts.append(ac)
                self.im.db_insert_table_account(ac)
            self.im.db_commit()
            reader.Close()
            command.Dispose()
            conn.Close()
        except:
            pass
    def decode_forumuser_asgroup(self,acc_id):
        node =  self.root.GetByPath('/Documents/contents/{0}/db.sqlite'.format(acc_id))
        if node is None:
            return
        d =  node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn)
        command.CommandText = "select uid,uname from tb_forumuser_list"    
        reader = command.ExecuteReader() 
        while reader.Read():
            chatroom = Chatroom()
            try:
                chatroom.account_id = acc_id                 
                chatroom.chatroom_id = str(SafeGetInt64(reader,0))
                chatroom.name = SafeGetString(reader,1)
                chatroom.source = node.AbsolutePath                                
                self.forumuser[chatroom.chatroom_id] = chatroom
                chatroom.type = CHATROOM_TYPE_TIEBA                
            except Exception as e:
                print(e)
            self.im.db_insert_table_chatroom(chatroom)
        self.im.db_commit()
        reader.Close()
        command.Dispose()
        conn.Close()
    def decode_user(self,acc_id):
        node =  self.root.GetByPath('/Documents/contents/{0}/db.sqlite'.format(acc_id))
        if node is None:
            return
        d =  node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn) 
        command.CommandText = "select uid, uname,friend_type from tb_user_list"    
        reader = command.ExecuteReader() 
        while reader.Read():
            friend = Friend()
            try:
                friend.account_id = acc_id
                friend.friend_id = str(SafeGetInt64(reader,0))
                friend.nickname = SafeGetString(reader,1)
                friend.source = node.AbsolutePath
                friendtype = SafeGetInt64(reader,2)
                if(friendtype == 0):
                    friend.type = FRIEND_TYPE_STRANGER
                elif(friendtype == 1):
                    friend.type = FRIEND_TYPE_FRIEND
                else: 
                    friend.type = FRIEND_TYPE_NONE
                self.friends[friend.account_id] = friend
            except Exception as e:
                print(e)
            self.im.db_insert_table_friend(friend)
        self.im.db_commit()
        reader.Close()
        command.Dispose()
        conn.Close()
    def decode_groups(self,acc_id):
        node =  self.root.GetByPath('/Documents/contents/{0}/db.sqlite'.format(acc_id))
        if node is None:
            return
        d =  node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn) 
        command.CommandText = "select gid, gname from tb_group_list where gname <>''"
        reader = command.ExecuteReader()
        while reader.Read():
            chatroom = Chatroom()
            try:
                chatroom.account_id = acc_id
                chatroom.chatroom_id = str(SafeGetInt64(reader,0))
                chatroom.name = SafeGetString(reader,1)
                chatroom.source = node.AbsolutePath                            
                self.groups[chatroom.chatroom_id] = chatroom
            except Exception as e:
                print(e)
            self.im.db_insert_table_chatroom(chatroom)
        self.im.db_commit()
        reader.Close()
        command.Dispose()
        conn.Close()
    def decode_msgs(self,acc_id):
        for k in self.friends.values():
            self.decode_user_msg_info(acc_id,k.friend_id)
        for k in  self.forumuser.values():
            self.decode_forumuser_msg_info(acc_id,k.chatroom_id)
        for k in self.groups.values():
            self.decode_group_msg_info(acc_id,k.chatroom_id)

    def decode_content(self,msg,content):
        try:
            j = json.loads(content)               
            msg.type =  MESSAGE_CONTENT_TYPE_LINK
            msg.talker_type = CHAT_TYPE_GROUP       
            for d in j:
                try:
                    link  = msg.create_link()
                    link.content = d['title']
                    link.url = d['url']
                except :
                    pass
                msg.insert_db(self.im)  
        except Exception as e:
            print (e)

    def decode_forumuser_msg_info(self,acc_id,forumid):
        node =  self.root.GetByPath('/Documents/contents/{0}/db.sqlite'.format(acc_id))
        if node is None:
            return
        d =  node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn) 
        command.CommandText = "select msgid,content,uid,msgtype,create_time from tb_forumusermsg_{0}".format(forumid)
        reader = command.ExecuteReader()
        while reader.Read():
            try:
                msg = Message()
                msg.account_id = acc_id
                msgid = SafeGetInt64(reader,0)
                content = SafeGetString(reader,1)  
                msg.talker_id = str(SafeGetInt64(reader,2))                              
                msgtype = SafeGetInt64(reader,3)
                msg.send_time = SafeGetInt64(reader,4)
                if msg.talker_id == acc_id:
                    msg.is_sender = 1
                else:
                    msg.is_sender = 0            
                self.decode_content(msg,content)                                         
            except Exception as e:
                print (e)
        self.im.db_commit()  
    def decode_group_msg_info(self,acc_id,groupid):
        node =  self.root.GetByPath('/Documents/contents/{0}/db.sqlite'.format(acc_id))
        if node is None:
            return
        d =  node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn) 
        command.CommandText = "select msgid,content,sendtext,uid,create_time,msgtype,duration from tb_groupmsg_{0}".format(groupid)
        try:
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    msg = Message()
                    msg.account_id = acc_id
                    msgid = SafeGetInt64(reader,0)
                    content = SafeGetString(reader,1)  
                    msg.content = SafeGetString(reader,2)  
                    msg.talker_id = str(SafeGetInt64(reader,3))       
                    msg.send_time = SafeGetInt64(reader,4)
                    msgtype = SafeGetInt64(reader,5)
                    druation = SafeGetInt64(reader,6)  
                    msg.talker_type = CHAT_TYPE_GROUP                  
                    if msg.talker_id == acc_id:
                        msg.is_sender = 1
                    else:
                        msg.is_sender = 0            
                    self.decode_usermsg_content(msg,content,msgtype)                                       
                except Exception as e:
                    print (e)
            self.im.db_commit()
        except Exception as e:
            print (e)
        return             
    def decode_usermsg_content(self,msg,content,msgtype):        
        if msgtype == 1:            
            j = json.loads(content)            
            if(msg.content  == ''):
                msg.content  = j[0]['text']    
            msg.type = MESSAGE_CONTENT_TYPE_TEXT 
        elif msgtype == 2:                 
            j = json.loads(content)
            bigpic = j[0]['big_src']
            spic = j[0]['src']                   
            msg.media_path = bigpic             
            msg.type = MESSAGE_CONTENT_TYPE_IMAGE
            #self.im.db_insert_table_link(link)
        elif msgtype == 3:
            j = json.loads(content)
            voicemd5 = j[0]['voice_md5']
            during = j[0]['during_time']
            msg.content = '语音' + voicemd5 + '\n时长'+ during 
            #msg.type = MESSAGE_CONTENT_TYPE_VOICE                
            msg.type = MESSAGE_CONTENT_TYPE_TEXT
        else:            
            msg.content = content
            msg.type = MESSAGE_CONTENT_TYPE_TEXT        
        #self.im.db_insert_table_message(msg)
        msg.insert_db(self.im)
         
    def decode_user_msg_info(self,acc_id,userid):
        node =  self.root.GetByPath('/Documents/contents/{0}/db.sqlite'.format(acc_id))
        if node is None:
            return
        d =  node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn) 
        command.CommandText = "select msgid,content,sendtext,uid,create_time,msgtype,duration from tb_usermsg_{0}".format(userid)
        reader = command.ExecuteReader()
        while reader.Read():
            try:
                msg = Message()
                msg.account_id = acc_id
                msgid = SafeGetInt64(reader,0)
                content = SafeGetString(reader,1)  
                msg.content = SafeGetString(reader,2)  
                msg.talker_id = str(SafeGetInt64(reader,3))                          
                msg.send_time = SafeGetInt64(reader,4)
                msgtype = SafeGetInt64(reader,5)
                druation = SafeGetInt64(reader,6)
                msg.talker_type = CHAT_TYPE_FRIEND                
                if msg.talker_id == acc_id:
                    msg.is_sender = 1
                else:
                    msg.is_sender = 0            
                self.decode_usermsg_content(msg,content,msgtype)                                       
            except Exception as e:
                print (e)
        self.im.db_commit()     
        return 




            
            




            



            





