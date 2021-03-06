# coding=utf-8
import clr
import PA_runtime
import json
from sqlite3 import *
import threading
import sys
clr.AddReference('System.Web')
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
try:
    clr.AddReference('model_qq')
    clr.AddReference('bcp_im')
    clr.AddReference('QQFriendNickName')
    clr.AddReference("PNFA.Formats.NextStep")
    clr.AddReference('PNFA.InfraLib.Exts')
    clr.AddReference('bcp_qq')
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
from QQFriendNickName import *
from PA.InfraLib.Utils import PList
from PA.InfraLib.Extensions import PlistHelper
#from PA.Formats.NextStep import *
from collections import defaultdict
import logging
import bcp_qq
import bcp_im
from bcp_qq import *
from model_qq import *
import model_qq
import uuid 
import hashlib
#import bcp_im
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
def SafeGetDouble(reader,i):
    try:
        if not reader.IsDBNull(i):
            return reader.GetDouble(i)
        else:
            return 0.0
    except:
        return 0.0

def SafeGetBlob(reader,i):
    try:
        if not reader.IsDBNull(i):
            obj = reader.GetValue(i)
            return obj #byte[]
        else:
            return None
    except:
        return None

def SafeGetValue(reader,i):
    try:
        if not reader.IsDBNull(i):
            obj = reader.GetValue(i)
            return obj 
        else:
            return None
    except:
        return None
hitdict =  {'(?i)Library/Preferences/com.tencent.mqq.plist$':['QQ',ParserResults()],
            '(?i)Library/Preferences/com.tencent.mQQi.plist$':['mQQi',ParserResults()],
            '(?i)Library/Preferences/com.tencent.tim.plist$':['QQ TIM',ParserResults()],
            }
progress_is_start = False
lock = threading.Lock()
def checkhit(root):
    nodes = []
    global hitdict
    for re in hitdict.keys():                 
        node = root.FileSystem.Search(re)
        if(len(list(node)) != 0):
            if len(node) > 1 : 
                i = 1
                for d in node:                                                             
                    data = [hitdict[re][0] +  "_" + str(i), hitdict[re][1]]
                    i = i+ 1
                    nodes.append((d,data))                    
            else:
                nodes.append((node[0],hitdict[re]))

    return nodes
   
def startthread(root,extdata,extract_deleted,extract_source):        
    try:
        sourceApp = extdata[0]
        QQParser(root,sourceApp, extract_deleted, extract_source).parse()
    except Exception as e:
        print (e)
        pass
    
def analyze_qq(root, extract_deleted, extract_source):
    global progress_is_start
    pr = ParserResults()
    try:             
        nodes = checkhit(root)
        threads = []      
        for node in nodes:
            try:                                                             
                arg = (node[0],node[1],extract_deleted,extract_source)                    
                t = threading.Thread(target=startthread,args= arg)   
                threads.append(t)                                  
            except:                    
                pass 
        for th in threads:
            th.start()
        for th in threads:
            th.join() 
        for node in nodes:           
            pr.Add(node[1][1])        
    except:
        pass
    if progress_is_start == True:
        progress.Finish(True)
    return pr
def progress_start():
    global lock,progress_is_start
    try:    
        if lock.acquire():
            if progress_is_start == False:
                progress_is_start = True
                progress.Start()
        lock.release()
    except:
        lock.release()
class QQParser(object):
    def __init__(self, app_root_dir,sourceApp, extract_deleted, extract_source):
        self.root = app_root_dir.Parent.Parent.Parent
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.app_name = 'QQ'
        self.friendsGroups = collections.defaultdict()
        self.friendsNickname = collections.defaultdict()
        self.groupContact = collections.defaultdict()
        self.nickname = collections.defaultdict(str)
        self.models = []
        self.accounts = []
        self.contacts = {}  # uin to contact
        self.c2cmsgtables =set()
        self.troopmsgtables =set()
        self.discussGrptables = set()
        self.troops = collections.defaultdict(Chatroom)
        self.friends = collections.defaultdict(Friend)
        self.chatroommembers = collections.defaultdict(ChatroomMember)
        self.im = IM()
        self.cachepath = ds.OpenCachePath(self.app_name) 
        self.bcppath = ds.OpenCachePath("tmp") 
        self.cachedb = collections.defaultdict()
        self.VERSION_APP_VALUE = 10000   
        self.sourceApp = sourceApp
        self.accountsProg = collections.defaultdict()
    def set_progress(self, acc_id,value):
        try:
            if self.accountsProg[acc_id] is not None and value != self.accountsProg[acc_id].Value:
                self.accountsProg[acc_id].Value = value
                print('set_progress() %d' % value)
        except Exception as e:
            pass
    def insert_account(self,ac):
        try:
            self.im.db_insert_table_account(ac)
            self.im.db_commit()
        except Exception as e:
            pass
    def parse(self):           
        try:            
            self.decode_accounts()
        except:
            pass
        if len(self.accounts) != 0:
            progress_start()
        else:
            progress.Skip()
            return   
        prog = progress['APP', self.sourceApp]
        prog.Start() 
        for acc in self.accounts:
            acc_id = acc.account_id
            m = hashlib.md5()
            accountPath = self.root.AbsolutePath + acc_id
            m.update(accountPath.encode('utf-8'))        
            cachedb =  self.cachepath  + '/' + m.hexdigest().upper() + ".db"
            self.cachedb[acc_id] = cachedb
            model = QQ.UserAccount()
            model.SourceFile = acc.source                   
            model.Account = acc.account_id                    
            model.NickName = acc.nickname
            accountprog = progress['APP', self.sourceApp]['ACCOUNT',acc_id , model]
            accountprog.Start()                    
            self.accountsProg[acc_id] = accountprog
            if self.im.need_parse(cachedb, self.VERSION_APP_VALUE):
                self.im.db_create(cachedb)
                self.insert_account(acc)            
                try:
                    if canceller.IsCancellationRequested:
                        return
                   
                    self.friendsNickname.clear()
                    self.friendsGroups.clear()
                    self.groupContact.clear()
                    self.troops.clear()                    
                    self.contacts = {}                   
                    self.c2cmsgtables =set()
                    self.troopmsgtables =set() 
                    self.discussGrptables = set() 
                    self.decode_favorites_info(acc_id)
                    self.set_progress(acc_id,1)
                    self.decode_friends(acc_id)
                    self.set_progress(acc_id,2)
                    self.decode_group_info(acc_id)                    
                    self.set_progress(acc_id,3)
                    self.decode_groupMember_info(acc_id)
                    self.set_progress(acc_id,4)
                    self.decode_discussGrp_info(acc_id)
                    self.set_progress(acc_id,6)
                    self.decode_discussgroupMember_info(acc_id)
                    self.set_progress(acc_id,8)
                    self.decode_discussGrp_messages(acc_id)
                    self.set_progress(acc_id,10)
                    self.decode_friend_messages(acc_id)
                    self.set_progress(acc_id,15)
                    self.decode_group_messages(acc_id)	                    
                    self.set_progress(acc_id,20)
                    self.decode_fts_messages(acc_id)		                   
                    self.set_progress(acc_id,25)
                    self.decode_recover_friends(acc_id)
                    self.set_progress(acc_id,27)
                    self.decode_recover_group_info(acc_id)
                    self.set_progress(acc_id,29)
                    self.decode_recover_groupMember_info(acc_id)
                    self.set_progress(acc_id,30)
                    self.decode_recover_friend_messages(acc_id)
                    self.set_progress(acc_id,32)
                    self.decode_recover_group_messages(acc_id)
                    self.set_progress(acc_id,34)
                    self.decode_recover_fts_messages(acc_id)        
                    self.set_progress(acc_id,35)
                    if canceller.IsCancellationRequested:
                        return
                    self.im.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                    self.im.db_insert_table_version(VERSION_KEY_APP, self.VERSION_APP_VALUE)
                    self.im.db_commit()
                    self.im.db_close()  
                except Exception as e:
                    print(e)
                   
        for acc in self.accounts:
            try:
                acc_id = acc.account_id
                PA_runtime.save_cache_path(bcp_im.CONTACT_ACCOUNT_TYPE_IM_QQ,self.cachedb[acc_id],self.bcppath)
                acc_id = acc.account_id            
                gen = GenerateModel(self.cachedb[acc_id],self.sourceApp,self.accountsProg[acc_id])
                gen.get_models()                
                self.set_progress(acc_id,100)
                self.accountsProg[acc_id].Finish(True)
                gen.ar.set_unique_id(self.cachedb[acc_id])
                self.get_qq_res(acc_id,gen.ar)
            except Exception as e:
                pass        
        prog.Finish(True)
        
    def get_qq_res(self,acc_id, ar):
        dicts = {}
        resnode = self.root.GetByPath('/Documents/' + acc_id)
        if resnode is None:
            return
        ar.save_res_folder(resnode, "Other")
        res = ar.parse()

    def processFavItem(self,fav,tree_fav_info,msgtype,senderid,sendername,create_time):
        try:            
            if(msgtype == 8):
                brief = tree_fav_info['brief'].Value                
                original_uri = tree_fav_info['original_uri'].Value
                title = tree_fav_info['title'].Value                             
                pic_list = tree_fav_info['pic_list'].Value     
                if  not (brief ==''  and title =='' and original_uri == ''):
                    itemcontext = fav.create_item()
                    itemcontext.type =  FAV_TYPE_LINK
                    itemcontext.source = fav.source
                    itemcontext.timestamp = create_time                    
                    itemcontext.link_title = title
                    itemcontext.link_content = brief 
                    itemcontext.link_image = original_uri
                    itemcontext.link_from = fav.source                    
                for pic in pic_list:
                    try:
                        item = fav.create_item()
                        orginPath = pic['originPath'].Value
                        url = pic['url'].Value                        
                        if orginPath is not None:                            
                            media_path = self.root.FileSystem.Search(orginPath)
                            for node in media_path:
                                item.media_path = node.AbsolutePath
                                break
                            else:
                                item.media_path = url                          
                        else:
                            item.media_path = url
                        item.type = FAV_TYPE_IMAGE
                        item.sender_id = senderid
                        item.sender_name = sendername 
                        item.source = fav.source
                        item.timestamp = create_time
                    except:
                        pass
            elif(msgtype == 2):
                brief = tree_fav_info['brief'].Value                
                title = tree_fav_info['title'].Value
                pic_list = tree_fav_info['pic_list'].Value        
                url = tree_fav_info['url'].Value 
                if not (brief == "" and title == "" and  url == ""):
                    itemcontext = fav.create_item()
                    itemcontext.type =  FAV_TYPE_LINK
                    itemcontext.source = fav.source
                    itemcontext.timestamp = create_time                    
                    itemcontext.link_title = title
                    itemcontext.link_content = brief 
                    itemcontext.link_image = url
                    itemcontext.link_url = url
                    itemcontext.link_from = fav.source          
                for pic in pic_list:
                    try:
                        item = fav.create_item()
                        orginPath = pic['originPath'].Value
                        url = pic['url'].Value
                        name = pic['name'].Valueitem
                        if orginPath is not None:                            
                            media_path = self.root.FileSystem.Search(orginPath)
                            for node in media_path:
                                item.media_path = node.AbsolutePath
                                break
                            else:
                                item.media_path = url    
                        else:
                            item.media_path = url
                        item.type = FAV_TYPE_IMAGE
                        item.sender_id = senderid
                        item.sender_name = sendername            
                        item.source = fav.source
                        item.timestamp = create_time
                    except:
                        pass                    
            elif(msgtype == 6):
                fav_file_info_data = tree_fav_info['fav_file_info_data'].Value 
                file_path = tree_fav_info['file_path'].Value 
                mem = MemoryRange.FromBytes(fav_file_info_data)
                file_info_tree  = BPReader.GetTree(mem)
                file_info = file_info_tree['file_info']
                name = file_info['name'].Value
                item = fav.create_item()
                item.type = FAV_TYPE_ATTACHMENT
                item.sender_id = senderid
                item.sender_name = sendername 
                media_path =  self.root.FileSystem.Search(file_path)
                for node in media_path:
                    item.media_path = node.AbsolutePath
                    break
                item.source = fav.source
                item.timestamp = create_time
            elif (msgtype == 7):
                longitude = tree_fav_info['longitude'].Value
                name = tree_fav_info['name'].Value
                latitude = tree_fav_info['latitude'].Value
                altitude = tree_fav_info['altitude'].Value   
                item = fav.create_item()
                item.timestamp = create_time
                item.type = FAV_TYPE_LOCATION
                item.sender = senderid
                item.sender_name = sendername 
                item.source = fav.source                
                item.location_latitude = latitude
                item.location_longitude = longitude
                item.location_address = name
                item.location_elevation = altitude    
                item.location_type = LOCATION_TYPE_GOOGLE                             
                item.source = fav.source                
            elif(msgtype == 9):            
                item = fav.create_item()                    
                item.timestamp = create_time
                note = tree_fav_info['note'].Value
                duration = tree_fav_info['duration'].Value/1000
                item.content = note + '时长:' +  str(duration)
                item.type = FAV_TYPE_VOICE
                item.sender_id = senderid
                item.sender_name = sendername 
                item.source = fav.source
        except Exception as e:
            print (e)
        pass
    def decode_favorites_info(self,acc_id):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is None:
            return
        table = 'tb_favorites_' + acc_id
        db = SQLiteParser.Database.FromNode(node,canceller)       
        if( table not in db.Tables):
            return
        ts = SQLiteParser.TableSignature(table)        
        for row in db.ReadTableRecords(ts, False):
            try:
                if canceller.IsCancellationRequested:
                    return
                fav = Favorite()
                loc =  row['local_id'].Value
                msgtype = row['type'].Value
                # 群或则个人
                author_type = row['author_type'].Value
                author_num_id = row['author_num_id'].Value
                author_str_id = row['author_str_id'].Value
                author_group_id = row['author_group_id'].Value  
                author_group_name = row['author_group_name'].Value                
                create_time = row['create_time'].Value/1000
                collect_time = row['collect_time'].Value/1000
                comm_biz_data_list = row['comm_biz_data_list'].Value
                fav_info = row['fav_info'].Value
                fav_detail = row['fav_detail'].Value                
                mem = MemoryRange.FromBytes(comm_biz_data_list)
                tree_comm_biz_data = BPReader.GetTree(mem)
                mem_fav_info = MemoryRange.FromBytes(fav_info)
                tree_fav_info =BPReader.GetTree(mem_fav_info)
                mem_fav_detail= MemoryRange.FromBytes(fav_detail)
                tree_fav_detail =BPReader.GetTree(mem_fav_detail)                    
                fav.account_id = acc_id       
                fav.source = node.AbsolutePath
                if author_type == 2 :
                    fav.talker_id =  author_group_id
                    fav.talker_name = author_group_name
                    fav.talker_type = CHAT_TYPE_GROUP
                elif author_type == 1:
                    fav.talker_id =  author_num_id
                    fav.talker_name = author_str_id
                    fav.talker_type = CHAT_TYPE_FRIEND
                fav.timestamp = collect_time        
                self.processFavItem(fav,tree_fav_info,msgtype,author_num_id,author_str_id,create_time)
                fav.insert_db(self.im)
            except:
                pass
        self.im.db_commit()		
        return
   
    def decode_fts_messages(self,acc_id):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        c2ctables = []
        trooptables =[]
        discussgroup = []
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return      
        for table in db.Tables: 
            if table.startswith("tbMap_c2c_"):
                c2ctables.append(table)       
            if table.startswith("tbMap_troop_"):
                trooptables.append(table)
            if  table.startswith("tbMap_disgroup_"):
                discussgroup.append(table)
        for table in c2ctables:
            if canceller.IsCancellationRequested:
                return
            self.decode_fts_chat_table(acc_id,table)				
        for table in trooptables:
            if canceller.IsCancellationRequested:
                return
            self.decode_fts_group_table(acc_id,table)		         
        for table in discussgroup:
            if canceller.IsCancellationRequested:
                return
            self.decode_fts_discussgroup_table(acc_id,table)		   
    def decode_fts_chat_table(self,acc_id,table):
        chat_id = table[table.rfind('_')+1:]
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')
        if node is not None:
            d = node.PathWithMountPoint             
            sql = 'select c1uin,c2time,c3type,c4flag,c7content,c8conversationuin,a.msgId from ' + table  +  ' a,tb_Index_c2cMsg_content b  where a.docid = b.docid'
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    if canceller.IsCancellationRequested:
                        return
                    msg = Message()
                    uin = SafeGetString(reader,0)
                    sendtime = SafeGetInt64(reader,1)
                    msgtype = SafeGetInt64(reader,2)
                    flag = SafeGetInt64(reader,3)
                    content =SafeGetString(reader,4)
                    msgid = str(SafeGetInt64(reader,6))	
                    msg.account_id = acc_id
                    msg.talker_id = uin
                    msg.deleted = 1
                    msg.source = node.AbsolutePath
                    try:
                        msg.talker_name = self.friends[chat_id]
                    except:
                        msg.talker_name = ''
                    if uid == acc_id:
                        msg.sender_name = self.nickname
                    else:
                        msg.sender_name = msg.talker_name
                    msg.sender_id = uin
                    msg.id = msgid
                    msg.type = MESSAGE_CONTENT_TYPE_TEXT
                    msg.content = content
                    msg.timestamp = sendtime
                    msg.talker_type = CHAT_TYPE_FRIEND				
                    self.im.db_insert_table_message(msg)
                except:					
                    pass
            self.im.db_commit()																							
        return 
    def decode_fts_group_table(self,acc_id,table):
        group_id = table[table.rfind('_')+1:]
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')
        if node is not None:
            d = node.PathWithMountPoint            
            sql = 'select c1uin,c2time,c3type,c4flag,c5nickname,c7content,c8conversationuin,c0msgId from ' + table  +  ' a,tb_Index_TroopMsg_content b  where a.docid = b.docid'
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    if canceller.IsCancellationRequested:
                        return
                    msg = Message()
                    senduin = SafeGetString(reader,0)
                    sendtime = SafeGetInt64(reader,1)
                    msgtype = SafeGetInt64(reader,2)
                    flag = SafeGetInt64(reader,3)
                    nickname = SafeGetString(reader,4)
                    content= SafeGetString(reader,5)
                    msgid = str(SafeGetInt64(reader,7))
                    msg.account_id = acc_id
                    msg.talker_id =SafeGetString(reader,6)
                    msg.source = node.AbsolutePath
                    try:					
                        msg.talker_name =  self.troops[group_id].name
                    except:
                        msg.talker_name  =''                    
                    if senduin == acc_id:                        
                        msg.sender_name = self.nickname[acc_id]
                        msg.is_sender = 1
                    else:
                        try:
                            msg.sender_name = self.chatroommembers[(group_id,senduin)].display_name        
                        except:
                            msg.sender_name = ""
                            pass
                        if msg.sender_name == ""or msg.sender_name is None:
                                try:
                                    msg.sender_name = self.chatroommembers[(group_id,senduin)].nick_name
                                except:
                                    pass
                        msg.is_sender = 0 
                    msg.deleted = 1
                    msg.sender_id = senduin
                    msg.id = msgid
                    msg.type = MESSAGE_CONTENT_TYPE_TEXT
                    msg.content = content
                    msg.timestamp = sendtime
                    msg.talker_type = CHAT_TYPE_GROUP                    
                except:					
                    pass
                self.im.db_insert_table_message(msg)
            self.im.db_commit()																							
        return 
    def decode_fts_discussgroup_table(self,acc_id,table):
        group_id = table[table.rfind('_')+1:]
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')
        if node is not None:
            d = node.PathWithMountPoint            
            sql = 'select c1uin,c2time,c3type,c4flag,c5nickname,c7content,c8conversationuin,c0msgId from ' + table  +  ' a,tb_Index_discussGrp_content b  where a.docid = b.docid'
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    if canceller.IsCancellationRequested:
                        return
                    msg = Message()
                    senduin = SafeGetString(reader,0)
                    sendtime = SafeGetInt64(reader,1)
                    msgtype = SafeGetInt64(reader,2)
                    flag = SafeGetInt64(reader,3)
                    nickname = SafeGetString(reader,4)
                    content= SafeGetString(reader,5)
                    msgid = str(SafeGetInt64(reader,7))
                    msg.account_id = acc_id
                    msg.talker_id =SafeGetString(reader,6)
                    msg.source = node.AbsolutePath
                    try:					
                        msg.talker_name =  self.troops[group_id].name
                    except:
                        msg.talker_name  =''
                    if senduin == acc_id:                        
                        msg.sender_name = self.nickname[acc_id]
                        msg.is_sender = 1
                    else:
                        try:
                            msg.sender_name = self.chatroommembers[(group_id,senduin)].display_name        
                        except:
                            msg.sender_name = ""
                            pass
                        if msg.sender_name == ""or msg.sender_name is None:
                                try:
                                    msg.sender_name = self.chatroommembers[(group_id,senduin)].nick_name
                                except:
                                    pass
                        msg.is_sender = 0 
                    msg.deleted = 1
                    msg.sender_id = senduin
                    msg.id = msgid
                    msg.type = MESSAGE_CONTENT_TYPE_TEXT
                    msg.content = content
                    msg.timestamp = sendtime
                    msg.talker_type = CHAT_TYPE_DISCUSSION                    
                except:					
                    pass
                self.im.db_insert_table_message(msg)
            self.im.db_commit()																							
        return 
    def decode_db_calls(self, acc_id):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ_Mix.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None or 'tb_callRecord' not in db.Tables:
            return
        ts = SQLiteParser.TableSignature('tb_callRecord')
        SQLiteParser.Tools.AddSignatureToTable(ts, 'uin', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'duration',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'netType', 2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'accType', 2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'msgtype', 2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'recordType', 2,2)
        for rec in db.ReadTableRecords(ts, self.extract_deleted):
            if canceller.IsCancellationRequested:
                return
            c = Call()
            c.Source.Value = self.app_name
            other_party = Party()
            main_party = Party()
            main_party.Identifier.Value = acc_id
            main_party.Name.Value = self.parties[acc_id] if acc_id in self.parties else None
            c.Deleted = main_party.Deleted = other_party.Deleted = rec.Deleted
            if 'type' in rec and rec['type'].Value in [1, 2]:
                other_party.Role.Init(PartyRole.From, MemoryRange(
                    rec['type'].Source) if self.extract_source else None)
                main_party.Role.Init(PartyRole.To, MemoryRange(
                    rec['type'].Source) if self.extract_source else None)
                if rec['type'].Value == 1:
                    c.Type.Init(CallType.Incoming, MemoryRange(
                        rec['type'].Source) if self.extract_source else None)
                elif rec['type'].Value == 2:
                    c.Type.Init(CallType.Missed, MemoryRange(
                        rec['type'].Source) if self.extract_source else None)
            if 'type' in rec and rec['type'].Value == 0:
                other_party.Role.Init(PartyRole.To, MemoryRange(
                    rec['type'].Source) if self.extract_source else None)
                main_party.Role.Init(PartyRole.From, MemoryRange(
                    rec['type'].Source) if self.extract_source else None)
                c.Type.Init(CallType.Outgoing, MemoryRange(
                    rec['type'].Source) if self.extract_source else None)
            if main_party.HasContent:
                c.Parties.Add(main_party)
            if other_party.HasContent:
                c.Parties.Add(other_party)

            SQLiteParser.Tools.ReadColumnToField[str](
                rec, "uin", other_party.Identifier, self.extract_source, lambda x: str(x))
            SQLiteParser.Tools.ReadColumnToField(
                rec, "nickname", other_party.Name, self.extract_source)
            SQLiteParser.Tools.ReadColumnToField[TimeSpan](
                rec, "duration", c.Duration, self.extract_source, lambda x: TimeSpan.FromSeconds(x))
            SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, "time", c.TimeStamp, self.extract_source, lambda x: TimeStamp(
                TimeStampFormats.GetTimeStampEpoch1Jan1970(x), True))
            if c.TimeStamp.Value and not c.TimeStamp.Value.IsValidForSmartphone():
                c.TimeStamp.Init(None, None)
            if c.Duration.Value == 0 and c.Type.Value == c.Type.Incoming:
                c.Type.Value = CallType.Missed
            if 'msgtype' in rec and rec['msgtype'].Value == 2:
                c.VideoCall.Init(True, MemoryRange(
                    rec['msgtype'].Source) if self.extract_source else Non)
            if c.HasContent:
                self.models.append(c)

    # account decoding
    def decode_accounts(self):
        node = self.root.GetByPath('/Documents/contents/QQAccountsManager')
        if node is None:
            return
        bp = BPReader(node.Data).top
        if bp is None:
            return

        for acc_ind in bp['$objects'][1]['NS.objects']:
            if acc_ind is None:
                break
            self.decode_account(bp['$objects'], acc_ind.Value,node.AbsolutePath)

    def decode_account(self, bp, dict_ind,source):
        values = self.get_dict_from_bplist(bp, dict_ind)
        ac = Account()
        ac.source = self.app_name
        ac.ServiceType = self.app_name
        #account.deleted = DeletedState.Intact
        try:
            ac.nickname = values['_loginAccount'].Value
            ac.account_id = values['_uin'].Value
            ac.nickname = values['_nick'].Value		            
            ac.province = values['_sProvince'].Value
            ac.city = values['_sCity'].Value
            ac.sex =  values['_sex'].Value
            ac.country = values['_sCountry'].Value 
            self.nickname[ac.account_id] = ac.nickname
        except:
            pass
        ac.source = source         
        self.accounts.append(ac)        

    def decode_friendlist(self,acc_id):
        try:
            node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQFriendList_v3.plist')
            if node is not None:
                friendPlistPath = node.PathWithMountPoint            
                self.friendsGroups,self.friendsNickname = getFriendNickName(friendPlistPath)            
            else:
                node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQFriendList_v2.plist')    
                if node is not None:
                    friendPlistPath = node.PathWithMountPoint            
                    self.friendsGroups,self.friendsNickname = getFriendNickNameV2(friendPlistPath)  
                
            for k in self.friendsNickname:
                if canceller.IsCancellationRequested:
                    return            
                friend = Friend()
                friend.type = FRIEND_TYPE_FRIEND
                friend.account_id = acc_id
                friend.friend_id = k
                friend.nickname =self.friendsNickname[k][0]
                friend.remark = self.friendsNickname[k][1]
                friend.source = node.AbsolutePath
                self.im.db_insert_table_friend(friend)
                self.friends[friend.friend_id] = friend
        except:
            pass
        self.im.db_commit()
    def decode_friends(self, acc_id):
        self.decode_friendlist(acc_id)
    def decode_group_info(self,acc_id): 
        '''
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQTroopMemo')        
        if node is  not None:
            #groups = PlistHelper.ReadPlist(node) 
            groups = NSHelpers.ReadPlist[NSDictionary](node.PathWithMountPoint)               
            if(groups is not None):                
                for data in groups:
                    try:
                        if canceller.IsCancellationRequested:
                            return
                        g = Chatroom()
                        v = data.Value
                        g.chatroom_id = data.Key
                        g.account_id = acc_id
                        g.name = v["name"].ToString()
                        g.notice = v["memo"].ToString()
                        g.source = node.AbsolutePath                        
                    except:                        
                        pass
                    self.troops[g.chatroom_id] = g
                    self.im.db_insert_table_chatroom(g)     
            self.im.db_commit()
         '''
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if  len(self.troops) > 0:
            return
        
        if node is  not None:
            d = node.PathWithMountPoint                
            db = SQLiteParser.Database.FromNode(node,canceller)
            if db is None:
                return		
            table = 'tb_troop'
            if 'tb_troop' not in db.Tables:
                table = 'tb_troop_new'
            sql = 'select GroupCode,groupname from '+ table
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():                               
                try:
                    if canceller.IsCancellationRequested:
                        return
                    g = Chatroom()
                    g.chatroom_type = CHATROOM_TYPE_GROUP
                    g.chatroom_id = str(SafeGetInt64(reader,0))
                    g.account_id = acc_id
                    g.name = SafeGetString(reader,1)
                    #g.owner_id = SafeGetString(reader,2)
                    g.source = node.AbsolutePath                        
                except:                        
                    pass
                self.troops[g.chatroom_id] = g
                self.im.db_insert_table_chatroom(g)     
            self.im.db_commit() 
    def decode_groupMember_info(self, acc_id):
        try:
            node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ_Group_CFG.db')            
            if node is not None:
                d = node.PathWithMountPoint
                conn = sqlite3.connect(d)
                sql = 'select strNick,groupCode,MemberUin,strRemark,PhoneNumber  from tb_troopRemarkNew'
                cursor = conn.execute(sql)
                for row in cursor:
                    if canceller.IsCancellationRequested:
                            return
                    chatroommem = ChatroomMember()
                    try:
                        strNick = row[0]
                        groupCode = str(row[1])
                        MemberUin = str(row[2])
                        strRemark = row[3]
                        PhoneNumber = row[4]
                        chatroommem.account_id = acc_id
                        chatroommem.chatroom_id = groupCode
                        chatroommem.member_id = MemberUin
                        chatroommem.nick_name = strNick
                        chatroommem.display_name = strRemark                                                
                        chatroommem.source = node.AbsolutePath
                        self.chatroommembers[(groupCode,MemberUin)] =  chatroommem
                    except:
                        pass                    
                    
            node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
            if node is not None:
                d = node.PathWithMountPoint                
                sql = 'select nick,GroupCode,MemUin,Age,JoinTime,LastSpeakTime,gender from tb_TroopMem'               
                datasource = "Data Source =  " + d +";ReadOnly=True"
                conn = SQLiteConnection(datasource)
                conn.Open()
                if(conn is None):
                    return
                command = SQLiteCommand(conn)                
                command.CommandText = sql
                reader = command.ExecuteReader()
                while reader.Read():               
                    if canceller.IsCancellationRequested:
                        return
                    try:
                        nick = SafeGetString(reader,0)
                        groupCode = SafeGetString(reader,1)
                        MemberUin = SafeGetString(reader,2)
                        Age = int(SafeGetString(reader,3))
                        JoinTime = int(SafeGetString(reader,4))
                        LastSpeakTime = int(SafeGetString(reader,5))
                        gender =int(SafeGetString(reader,6))
                        chatmem = self.chatroommembers[(groupCode, MemberUin)]
                        if(chatmem.source is None):
                            chatmem.source = node.AbsolutePath
                        chatmem.account_id = acc_id
                        chatmem.chatroom_id = groupCode
                        chatmem.member_id = MemberUin
                        if(chatmem.nick_name == ''):
                            chatmem.nick_name = nick
                        chatmem.age  = Age
                        if(gender == 0):
                            chatmem.gender = GENDER_MALE
                        elif(gender == 1):
                            chatmem.gender = GENDER_FEMALE
                        else:
                            chatmem.gender = GENDER_NONE
                        chatmem.JoinTime = JoinTime
                        chatmem.lastspeektime = LastSpeakTime
                    except Exception as e:
                        pass
                reader.Close()
                command.Dispose()		
                conn.Close()   
            for k in self.chatroommembers:
                self.im.db_insert_table_chatroom_member(self.chatroommembers[k])
        except Exception as e:
            pass
        self.im.db_commit()

    def decode_friend_messages(self, acc_id):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return
        #tables = set()
      
        d = node.PathWithMountPoint            
        sql = "select tbl_name from sqlite_master  where type ='table'"
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn)                
        command.CommandText = sql
        reader = command.ExecuteReader()
        while reader.Read():
                tablename = SafeGetString(reader,0)
                if(tablename.startswith("tb_c2cMsg_")):
                    self.c2cmsgtables.add(tablename)
        #c2c
        for table in self.c2cmsgtables:
            if canceller.IsCancellationRequested:
                return
            self.decode_friend_chat_table(acc_id,table)
    def decode_discussGrp_info(self, acc_id):
        try:
            node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
            if node is None:
                return
            d = node.PathWithMountPoint            
            sql = 'select uin ,name, create_time,owner_uin,member_num  from  tb_discussGrp_list'
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    chatroom = Chatroom()
                    chatroom.chatroom_type = CHATROOM_TYPE_DISCUSSION
                    chatroom.account_id = acc_id
                    chatroom.chatroom_id = str(SafeGetInt64(reader,0))
                    chatroom.name = SafeGetString(reader,1)
                    chatroom.create_time = SafeGetInt64(reader,2)
                    chatroom.owner_id = str(SafeGetInt64(reader,3))
                    chatroom.member_count = SafeGetInt64(reader,4)  
                    chatroom.source = node.AbsolutePath              
                except:
                    pass             
                self.discussGrptables.add("tb_discussGrp_"+ chatroom.chatroom_id)
                self.troops[chatroom.chatroom_id ] = chatroom
                self.im.db_insert_table_chatroom(chatroom)
            self.im.db_commit()
            reader.Close()
            command.Dispose()		
            conn.Close()  
        except:
            pass
    def decode_discussgroupMember_info(self, acc_id):
        try:
            node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')            
            if node is not None:
                d = node.PathWithMountPoint
                datasource = "Data Source =  " + d +";ReadOnly=True"
                conn = SQLiteConnection(datasource)
                conn.Open()
                if(conn is None):
                    return
                sql = "select dis_uin, uin ,interemark from tb_discussGrp_member" 
                command = SQLiteCommand(conn)                
                command.CommandText = sql
                reader = command.ExecuteReader()
                while reader.Read():
                    if canceller.IsCancellationRequested:
                        return
                    chatroommem = ChatroomMember()
                    try:                        
                        dis_uin = str(SafeGetInt64(reader,0))
                        MemberUin = str(SafeGetInt64(reader,1))
                        strNick = SafeGetString(reader,2)
                        chatroommem.account_id = acc_id
                        chatroommem.chatroom_id = dis_uin
                        chatroommem.member_id = MemberUin
                        chatroommem.display_name = strNick
                        chatroommem.source = node.AbsolutePath
                    except:
                        pass           
                    self.chatroommembers[(dis_uin,MemberUin)] = chatroommem
                    self.im.db_insert_table_chatroom_member(chatroommem)
        except:
            pass
        return    
    def decode_discussGrp_messages(self, acc_id):
        for table in self.discussGrptables:
            try:
                if canceller.IsCancellationRequested:
                    return     
                self.decode_discussGrp_chat_table(acc_id,table)                        
            except:
                pass
    def decode_discussGrp_chat_table(self,acc_id,table_name):
        group_id = table_name[table_name.rfind('_')+1:]
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')         
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return		
        if table_name not in db.Tables:
            return
        if node is not None:
            d = node.PathWithMountPoint            
            sql = 'select discussuin , senduin , msgtime, Msgtype,read,msg ,msgid,nickname,picurl from ' + table_name + ' order by msgtime'
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    if canceller.IsCancellationRequested:
                        return
                    msg = Message()
                    discussuin = str(SafeGetString(reader,0))
                    senduin = str(SafeGetString(reader,1))
                    sendtime = int(SafeGetDouble(reader,2))
                    msgtype =SafeGetInt64(reader,3)
                    bread = SafeGetInt64(reader,4)
                    content =SafeGetString(reader,5)
                    msgid = str(SafeGetInt64(reader,6))	
                    nickname = SafeGetString(reader,7)
                    msgid = SafeGetString(reader,8)
                    msg.account_id = acc_id
                    msg.talker_id = group_id
                    msg.send_id = senduin
                    try:
                       msg.talker_name = self.troops[group_id].name
                    except:
                        msg.talker_name = ""
                    if senduin == acc_id:                        
                        msg.sender_name = self.nickname[acc_id]
                        msg.is_sender = 1
                    else:
                        try:
                            msg.sender_name = self.chatroommembers[(group_id,senduin)].display_name        
                        except:
                            msg.sender_name = ""
                            pass
                        if msg.sender_name == ""or msg.sender_name is None:
                                try:
                                    msg.sender_name = self.chatroommembers[(group_id,senduin)].nick_name
                                except:
                                    pass
                        msg.is_sender = 0 
                    msg.msg_id = msgid
                    msg.timestamp = sendtime
                    msg.talker_type = CHAT_TYPE_DISCUSSION
                    if(bread == 0):
                        msg.status = MESSAGE_STATUS_UNREAD
                    else:
                        msg.status = MESSAGE_STATUS_READ
                    msg.source = node.AbsolutePath
                    msg.content = content	
                    types  = (MESSAGE_CONTENT_TYPE_TEXT,MESSAGE_CONTENT_TYPE_IMAGE,MESSAGE_CONTENT_TYPE_VOICE,
                    MESSAGE_CONTENT_TYPE_ATTACHMENT,MESSAGE_CONTENT_TYPE_VIDEO,MESSAGE_CONTENT_TYPE_LOCATION,
                    MESSAGE_CONTENT_TYPE_RED_ENVELPOE,MESSAGE_CONTENT_TYPE_TRANSFER)
                    if(msgtype == 0):
                        msg.type = MESSAGE_CONTENT_TYPE_TEXT										
                    elif(msgtype == 1):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                        msg.media_path,msg.media_thum_path = self.get_picture_attachment(acc_id,picUrl)	                        
                    elif(msgtype == 3):
                        msg.type = MESSAGE_CONTENT_TYPE_VOICE
                        msg.media_path = self.get_audio_attachment(acc_id,picUrl)						
                    elif(msgtype == 4):
                        msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
                        msg.media_path = self.get_file_attachment(acc_id,content)						
                    elif(msgtype == 181):
                        msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                        msg.media_path = self.get_video_attachment(acc_id,picUrl)
                    #maybe the system
                    elif(msgtype == 337):                        
                        loc = self.get_location(acc_id,content)
                        if(loc is not None): 
                            msg.type = MESSAGE_CONTENT_TYPE_LOCATION                            
                            msg.location_latitude = float(loc[1])
                            msg.location_longitude = float(loc[2])
                            msg.location_address = loc[0]
                            msg.location_type = LOCATION_TYPE_GOOGLE
                    elif(msgtype == 311):
                        self.get_dealinfo(acc_id,msg)   
                    if(msg.type not in types):	
                        msg.type = MESSAGE_CONTENT_TYPE_SYSTEM
                    msg.insert_db(self.im)
                except:
                    pass
            self.im.db_commit()
        return		

    def decode_group_messages(self, acc_id):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return      
        for table in db.Tables: 
            if table.startswith("tb_TroopMsg_"):
                self.troopmsgtables.add(table)       
        for table in self.troopmsgtables:
            if canceller.IsCancellationRequested:
                return     
            self.decode_group_chat_table(acc_id,table)	
    def decode_group_chat_table(self, acc_id, table_name):		
        group_id = table_name[table_name.rfind('_')+1:]
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is not None:
            d = node.PathWithMountPoint            
            sql = 'select senduin ,msgtime, sMsgtype,read,strmsg,msgid,nickname,picurl from  ' + table_name + ' order by msgtime'
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    if canceller.IsCancellationRequested:
                        return
                    senduin = str(SafeGetInt64(reader,0))
                    sendtime = int(SafeGetDouble(reader,1))
                    msgtype =SafeGetInt64(reader,2)
                    bread = SafeGetInt64(reader,3)
                    content =SafeGetString(reader,4)
                    msgid = str( SafeGetInt64(reader,5))	
                    nickname = SafeGetString(reader,6)
                    picUrl = SafeGetString(reader,7)
                    msg = Message()
                    msg.account_id = acc_id
                    msg.talker_id = group_id                                                            
                    msg.talker_type = CHAT_TYPE_GROUP
                    msg.source = node.AbsolutePath
                    try:
                        msg.talker_name = self.troops[group_id].name
                    except:
                        msg.talker_name = ""
                    if senduin == acc_id:                        
                        msg.sender_name = self.nickname[acc_id]
                        msg.is_sender = 1
                    else:
                        try:
                            msg.sender_name = self.chatroommembers[(group_id,senduin)].display_name        
                        except:
                            msg.sender_name = ""
                            pass                           
                        if msg.sender_name == "" or msg.sender_name is None:
                                try:
                                    msg.sender_name = self.chatroommembers[(group_id,senduin)].nick_name
                                except:
                                    pass
                        msg.is_sender = 0
                    if(bread == 0):
                        msg.status = MESSAGE_STATUS_UNREAD
                    else:
                        msg.status = MESSAGE_STATUS_READ					
                    msg.deleted = 0
                    msg.repeated = 0
                    msg.msg_id = msgid
                    msg.timestamp = sendtime									
                    msg.sender_id  = senduin
                    msg.content = content	
                    types  = (MESSAGE_CONTENT_TYPE_TEXT,MESSAGE_CONTENT_TYPE_IMAGE,MESSAGE_CONTENT_TYPE_VOICE,
                    MESSAGE_CONTENT_TYPE_ATTACHMENT,MESSAGE_CONTENT_TYPE_VIDEO,MESSAGE_CONTENT_TYPE_LOCATION,
                    MESSAGE_CONTENT_TYPE_RED_ENVELPOE,MESSAGE_CONTENT_TYPE_TRANSFER)
                    if(msgtype == 0):
                        msg.type = MESSAGE_CONTENT_TYPE_TEXT										
                    elif(msgtype == 1):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                        msg.media_path,msg.media_thum_path = self.get_picture_attachment(acc_id,picUrl)	                        
                    elif(msgtype == 3):
                        msg.type = MESSAGE_CONTENT_TYPE_VOICE
                        msg.media_path = self.get_audio_attachment(acc_id,picUrl)						
                    elif(msgtype == 4):
                        msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
                        msg.media_path = self.get_file_attachment(acc_id,content)						
                    elif(msgtype == 181):
                        msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                        msg.media_path = self.get_video_attachment(acc_id,picUrl)
                    #maybe the system
                    elif(msgtype == 337):                        
                        loc = self.get_location(acc_id,content)
                        if(loc is not None):                                                        
                            msg.type = MESSAGE_CONTENT_TYPE_LOCATION                            
                            msg.location_latitude = float(loc[1])
                            msg.location_longitude = float(loc[2])
                            msg.location_address = loc[0]
                            msg.location_type = LOCATION_TYPE_GOOGLE     
                    elif(msgtype == 311):
                        self.get_dealinfo(acc_id,msg)                        
                    if(msg.type not in types):	
                        msg.type = MESSAGE_CONTENT_TYPE_SYSTEM
                    #self.im.db_insert_table_message(msg)
                    msg.insert_db(self.im)
                except:
                    pass
            self.im.db_commit()			
        return	
    def decode_friend_chat_table(self, acc_id, table_name):		
        chat_id = table_name[table_name.rfind('_')+1:]
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is not None:
            d = node.PathWithMountPoint
            sql = 'select uin,time,type,read,content,msgId,flag,picUrl from ' + table_name + ' order by time'
            datasource = "Data Source =  " + d +";ReadOnly=True"
            conn = SQLiteConnection(datasource)
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    if canceller.IsCancellationRequested:
                        return                    
                    uin = SafeGetString(reader,0)   
                    sendtime = int(SafeGetDouble(reader,1))
                    msgtype = SafeGetInt64(reader,2)
                    bread = SafeGetInt64(reader,3)
                    content =SafeGetString(reader,4)
                    msgid = str(SafeGetInt64(reader,5))
                    flag = SafeGetInt64(reader,6)
                    picUrl = SafeGetString(reader,7)
                    msg = Message()
                    msg.account_id = acc_id
                    msg.talker_id = uin
                    msg.talker_type = CHAT_TYPE_FRIEND
                    msg.source = node.AbsolutePath
                    try:
                        msg.talker_name = self.friends[chat_id].nickname
                    except:
                        msg.talker_name = ""
                    if uin == acc_id:                        
                        msg.sender_name = self.nickname[acc_id]
                        msg.is_sender = 1
                    else:
                        msg.sender_name = msg.talker_name        
                        msg.is_sender = 0
                    if(bread == 0):
                        msg.status = MESSAGE_STATUS_UNREAD
                    else:
                        msg.status = MESSAGE_STATUS_READ					
                    msg.deleted = 0
                    msg.repeated = 0
                    msg.msg_id = msgid
                    msg.timestamp = sendtime
                    msg.sender_id =  uin
                    msg.content = content	
                    types  = (MESSAGE_CONTENT_TYPE_TEXT,MESSAGE_CONTENT_TYPE_IMAGE,MESSAGE_CONTENT_TYPE_VOICE,
                    MESSAGE_CONTENT_TYPE_ATTACHMENT,MESSAGE_CONTENT_TYPE_VIDEO,MESSAGE_CONTENT_TYPE_LOCATION,
                    MESSAGE_CONTENT_TYPE_RED_ENVELPOE,MESSAGE_CONTENT_TYPE_TRANSFER)
                    if(msgtype == 0):
                        msg.type = MESSAGE_CONTENT_TYPE_TEXT										
                    elif(msgtype == 1):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                        msg.media_path,msg.media_thum_path = self.get_picture_attachment(acc_id,picUrl)											
                    elif(msgtype == 3):
                        msg.type = MESSAGE_CONTENT_TYPE_VOICE
                        msg.media_path = self.get_audio_attachment(acc_id,picUrl)						
                    elif(msgtype == 4):
                        msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
                        msg.media_path = self.get_file_attachment(acc_id,content)						
                    elif(msgtype == 181):
                        msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                        msg.media_path = self.get_video_attachment(acc_id,picUrl)
                    #maybe the system
                    elif(msgtype == 337):
                        loc = self.get_location(acc_id,content)
                        if(loc is not None):     
                            msg.type = MESSAGE_CONTENT_TYPE_LOCATION                            
                            msg.location_latitude = float(loc[1])
                            msg.location_longitude = float(loc[2])
                            msg.location_address = loc[0]
                            msg.location_type = LOCATION_TYPE_GOOGLE
                    elif(msgtype == 311):
                        self.get_dealinfo(acc_id,msg)                        
                    if(msg.type not in types):	
                        msg.type = MESSAGE_CONTENT_TYPE_SYSTEM
                    msg.insert_db(self.im)
                except Exception as e:
                    pass
        self.im.db_commit()		
        reader.Close()
        command.Dispose()		
        conn.Close()   	
        return
    def get_dealinfo(self,acc_id,msg):
        try:            
            j = json.loads(msg.content)                                    
            if(j['redtype'] == 2 or j['redtype'] == 1):               
                msg.deal_description = j['subtitle'] 
                msg.deal_remark = j['title']    
                msg.type = MESSAGE_CONTENT_TYPE_RED_ENVELPOE
                msg.deal_mode = DEAL_MODE_NONE
                msg.deal_status =  DEAL_STATUS_RED_ENVELOPE_NONE      
            elif j['redtype'] == 0:            
                msg.deal_remark = j['content']
                msg.deal_money =  j['title']         
                msg.deal_description = j['subtitle']                  
                msg.type = MESSAGE_CONTENT_TYPE_TRANSFER
                msg.deal_mode = DEAL_MODE_NONE
                msg.deal_status =  DEAL_STATUS_TRANSFER_RECEIVED              
            msg.deal_create_time = msg.timestamp
        except:
            pass
        return 
    def get_video_attachment(self ,acc_id,picUrl):
        try:			
            pic_json = json.loads(picUrl)
        except:
            return ''
        fileName= ''
        if 'fileName'  in pic_json[0]:
            fileName = pic_json[0]['videoMD5']
        else:
            return ''
        acc_folder = self.root.GetByPath('/Documents/{0}/Audio/{1}.mp4'.format(acc_id,fileName))
        if(acc_folder is None):
            return ''
        return acc_folder.AbsolutePath		
    def _get_xml_msg_data(self, body, del_state, acc_id, main_key):
        return 

    def get_shared_links(self, body, deleted, acc_id):		

        return 
    def get_location(self, acc_id,content):
        try:            
            x = json.loads(content)
            if 'com.tencent.map' == x['app']:
                address	= x['meta']['Location.Search']['address']
                lat	= float(x['meta']['Location.Search']['lat'])
                lng	= float(x['meta']['Location.Search']['lng'])
                return address,lat,lng
        except:
            pass
        return None	
    def get_shared_conatct(self, body, del_state, acc_id):
        try:
            data = self._get_xml_msg_data(body, del_state, acc_id, "a_actionData")
            if not data:
                return
            root, keys, parts, mr, actionData = data
            if  'source' not in keys or parts['source'] != 'sharecard' or 'uin' not in keys:
                return
            uin = parts['uin']
            if uin in self.contacts and type(self.contacts[uin]) == Contact:
                return self.contacts[uin]
            c = Contact()
            c.Deleted = del_state
            c.Source.Value = self.app_name
            user_id = UserID()
            user_id.Deleted = del_state
            user_id.Value.Value = uin
            user_id.Value.Source = MemoryRange(actionData.Source) if self.extract_source else None
            c.Entries.Add(user_id)
            if list(XMLParserHelper.XMLParserTools.GetByXPath(root, 'msg/item/title')):
                c.Name.Value = list(XMLParserHelper.XMLParserTools.GetByXPath(root, 'msg/item/title'))[0].Value
                c.Name.Source = mr if self.extract_source else None
            self.models.append(c)
            self.contacts[uin] = c
            return c
        except :
            pass
    def get_audio_attachment(self, acc_id, js):
        try:			
            pic_json = json.loads(js)       
            fileName= ''
            if 'fileName'  in pic_json[0].keys():
                fileName = pic_json[0]['fileName']
            acc_folder = self.root.GetByPath('/Documents/{0}/Audio/{1}.amr'.format(acc_id,fileName))
            if(acc_folder is None):
                return ''
            return acc_folder.AbsolutePath
        except:
            return ''

    def get_picture_attachment(self, acc_id, js):	
        try:			
            pic_json = json.loads(js)        
            if 'md5'  in pic_json[0].keys():
                md5 = pic_json[0]['md5']
            elif 'videoMD5'  in pic_json[0].keys():
                md5 = pic_json[0]['videoMD5']
            urlOriginal = ''
            if 'urlOriginal' in pic_json[0].keys():
                urlOriginal = pic_json[0]['urlOriginal']
            
            originalPath = self.root.GetByPath('/Documents/{0}/image_original/{1}.png'.format(acc_id,md5))
            if(originalPath is None):                
                originalPath = self.root.GetByPath('/Documents/{0}/image_big/{1}.png'.format(acc_id,md5))
            originalPathAbs = ''
            if originalPath is not None:
                originalPathAbs = originalPath.AbsolutePath
            thumbnailPath = self.root.GetByPath('/Documents/{0}/image_thumbnail/{1}.png'.format(acc_id,md5))     
            thumbnailAbsPath = ''
            if thumbnailPath is not None:
                thumbnailAbsPath = thumbnailPath.AbsolutePath
            if(originalPath is None and thumbnailPath is None):
                if urlOriginal is '':
                    return  '',''
                if urlOriginal.find('/offpic_new/') == 0:
                    url  = "https://c2cpicdw.qpic.cn" + urlOriginal
                    return url,url
                elif urlOriginal.find('/gchatpic_new/') == 0:
                    url  = "http://gchat.qpic.cn" + urlOriginal
                    return url,url                    
            else:
                return originalPathAbs,thumbnailAbsPath
        except:
            return '',""
    def get_file_attachment(self, acc_id, content):
        try:			
            filename = content[0:content.find('||||||||')]       
            acc_folder = self.root.GetByPath('/Documents/{0}/FileRecv/{1}'.format(acc_id,filename))
            if(acc_folder is None):
                return ''
            return acc_folder.AbsolutePath
        except:
            return ''
        
    def get_dict_from_bplist(self, bp, dict_ind):
        values = {}
        for key in bp[dict_ind].Keys:
            val_ind = bp[dict_ind][key].Value
            values[key] = bp[val_ind]
        return values
    def get_dict_from_bplist2(self, bp, dict_ind):
        values = {}
        #keyIndex = bp['NS.keys']
        for keyindex in bp[1]['NS.keys']:            
            key = bp[keyindex].Value
            values[key] = bp[val_ind]
        return values
    def init_model_field_from_bp_field(self, src, dst, extract_source):
        if src is None:
            return
        if extract_source:
            dst.Init(src.Value, MemoryRange(src.Source))
        else:
            dst.Value = src.Value
    def decode_recover_friends(self,acc_id):
        return True
    def decode_recover_group_info(self,acc_id):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return		
        table = 'tb_troop'
        if 'tb_troop' not in db.Tables:
            table = 'tb_troop_new'
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'groupid', 4,5,6)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'groupcode', 4,5,6)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'GroupName', 13)
        for rec in db.ReadTableDeletedRecords(ts, False):
            if canceller.IsCancellationRequested:
                return
            group = Chatroom()
            groupCode = str(rec['groupcode'].Value)
            groupName = str(rec['GroupName'].Value)						
            group.name = groupName
            group.chatroom_id = groupCode	
            group.deleted = 1					
            group.source = node.AbsolutePath
            self.im.db_insert_table_chatroom(group) 		
        self.im.db_commit()
    def decode_recover_groupMember_info(self,acc_id):    		
        return True
    def decode_recover_friend_messages(self,acc_id):			
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return
        for table_name in self.c2cmsgtables:			
            chat_id = table_name[table_name.rfind('_')+1:]
            #d = node.PathWithMountPoint
            #conn = sqlite3.connect(d)
            #sql = 'select uin,time,type,read,content,msgId,flag,picUrl from ' + table_name + ' order by time'
            #cursor = conn.execute(sql)			
            ts = SQLiteParser.TableSignature(table_name)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'uin', 1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'time',2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'type', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'read', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'content', 1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgId', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'flag', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'picUrl', 1,2)
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    uin = str(rec['uin'].Value)
                    sendtime = rec['time'].Value
                    msgtype = rec['type'].Value
                    bread = rec['read'].Value
                    content = rec['content'].Value
                    msgid = str(rec['msgId'].Value)
                    flag = rec['read'].Value
                    picUrl = rec['picUrl'].Value
                    msg = Message()
                    msg.account_id = acc_id
                    msg.talker_id = uin
                    msg.talker_type = CHAT_TYPE_FRIEND
                    msg.source = node.AbsolutePath                    
                    try:
                        msg.talker_name = self.friends[chat_id].nickname					
                    except:
                        msg.talker_name = ''
                    if acc_id == uin:
                         msg.sender_name = self.nickname[acc_id]
                    else:
                        msg.sender_name = talker_name
                    if(bread == 0):
                        msg.status = MESSAGE_STATUS_UNREAD
                    else:
                        msg.status = MESSAGE_STATUS_READ					
                    msg.deleted = 1					
                    msg.msg_id = msgid
                    msg.timestamp = sendtime
                    msg.sender_id =  uin
                    msg.content = content	
                    types  = (MESSAGE_CONTENT_TYPE_TEXT,MESSAGE_CONTENT_TYPE_IMAGE,MESSAGE_CONTENT_TYPE_VOICE,
                    MESSAGE_CONTENT_TYPE_ATTACHMENT,MESSAGE_CONTENT_TYPE_VIDEO,MESSAGE_CONTENT_TYPE_LOCATION)
                    if(msgtype == 0):
                            msg.type = MESSAGE_CONTENT_TYPE_TEXT										
                    elif(msgtype == 1):
                            msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                            msg.media_path,msg.media_thum_path = self.get_picture_attachment(acc_id,picUrl)											
                    elif(msgtype == 3):
                            msg.type = MESSAGE_CONTENT_TYPE_VOICE
                            msg.media_path = self.get_audio_attachment(acc_id,picUrl)						
                    elif(msgtype == 4):
                            msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
                            msg.media_path = self.get_file_attachment(acc_id,content)						
                    elif(msgtype == 181):
                            msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                            msg.media_path = self.get_video_attachment(acc_id,picUrl)
                    #maybe the system
                    elif(msgtype == 337):
                        loc = self.get_location(acc_id,content)
                        if(loc is not None):                  
                            msg.type = MESSAGE_CONTENT_TYPE_LOCATION                            
                            msg.location_latitude = float(loc[1])
                            msg.location_longitude = float(loc[2])
                            msg.location_address = loc[0]
                            msg.location_type = LOCATION_TYPE_GOOGLE  
                    if(msg.type not in types):	
                        msg.type = MESSAGE_CONTENT_TYPE_SYSTEM
                    msg.insert_db(self.im)
                except:
                    pass
            self.im.db_commit()			
        return
    def decode_recover_group_messages(self,acc_id):    
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return
        for table_name in self.troopmsgtables:			
            group_id = table_name[table_name.rfind('_')+1:]	
            #sql = 'select senduin ,msgtime, sMsgtype,read,strmsg,msgid,nickname,picurl from  ' + table_name + ' order by msgtime'			
            ts = SQLiteParser.TableSignature(table_name)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'SendUin', 1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'MsgTime',2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'sMsgType',2 ,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'read', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'strMsg', 1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgId', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'nickName',1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'picUrl', 1,2)
            for row in  db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    senduin = str(row['SendUin'].Value)
                    sendtime = row['MsgTime'].Value
                    msgtype = row['sMsgType'].Value
                    bread = row['read'].Value
                    content = row['strMsg'].Value
                    msgid = str(row['msgId'].Value)	
                    nickname = 	row['nickName'].Value		
                    picUrl = row['picUrl'].Value
                    msg = Message()
                    msg.account_id = acc_id
                    msg.talker_id = group_id
                    msg.sender_id = senduin                                        
                    msg.talker_type = CHAT_TYPE_GROUP
                    msg.source = node.AbsolutePath
                    try:
                        msg.talker_name = self.troops[group_id].name
                    except:
                        msg.talker_name = ""
                    if senduin == acc_id:                        
                        msg.sender_name = self.nickname[acc_id]
                        msg.is_sender = 1
                    else:
                        try:
                            msg.sender_name = self.chatroommembers[(group_id,senduin)].display_name                                                        
                        except:
                            msg.sender_name = ""
                        if msg.sender_name == ""or msg.sender_name is None:
                            try:
                                msg.sender_name = self.chatroommembers[(group_id,senduin)].nick_name
                            except:
                                pass                           
                        msg.is_sender = 0 
                    if(bread == 0):
                        msg.status = MESSAGE_STATUS_UNREAD
                    else:
                        msg.status = MESSAGE_STATUS_READ					
                    msg.deleted = 1					
                    msg.msg_id = msgid
                    msg.timestamp = sendtime									
                    msg.content = content	
                    types  = (MESSAGE_CONTENT_TYPE_TEXT,MESSAGE_CONTENT_TYPE_IMAGE,MESSAGE_CONTENT_TYPE_VOICE,
                    MESSAGE_CONTENT_TYPE_ATTACHMENT,MESSAGE_CONTENT_TYPE_VIDEO,MESSAGE_CONTENT_TYPE_LOCATION)
                    if(msgtype == 0):
                        msg.type = MESSAGE_CONTENT_TYPE_TEXT										
                    elif(msgtype == 1):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                        msg.media_path,msg.media_thum_path = self.get_picture_attachment(acc_id,picUrl)	     
                    elif(msgtype == 3):
                        msg.type = MESSAGE_CONTENT_TYPE_VOICE
                        msg.media_path = self.get_audio_attachment(acc_id,picUrl)						
                    elif(msgtype == 4):
                        msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
                        msg.media_path = self.get_file_attachment(acc_id,content)						
                    elif(msgtype == 181):
                        msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                        msg.media_path = self.get_video_attachment(acc_id,picUrl)
                    #maybe the system
                    elif(msgtype == 337):
                        loc = self.get_location(acc_id,content)
                        if(loc is not None):                       
                            msg.type = MESSAGE_CONTENT_TYPE_LOCATION                            
                            msg.location_latitude = float(loc[1])
                            msg.location_longitude = float(loc[2])
                            msg.location_address = loc[0]
                            msg.location_type = LOCATION_TYPE_GOOGLE
                    if(msg.type not in types):	
                        msg.type = MESSAGE_CONTENT_TYPE_SYSTEM
                    msg.insert_db(self.im)
                except:
                    pass
            self.im.db_commit()
        return			
    def decode_recover_fts_messages(self,acc_id):
        self.decode_recover_fts_chat_table(acc_id,'tb_Index_c2cMsg_content')
        self.decode_recover_fts_group_table(acc_id,'tb_Index_TroopMsg_content')
        self.decode_recover_fts_discussGrp_table(acc_id,'tb_Index_discussGrp_content')
        
    def decode_recover_fts_chat_table(self,acc_id,table):    		
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return
        #sql = 'select c1uin,c2time,c3type,c4flag,c7content,c8conversationuin,a.msgId from ' + table  +  ' a,tb_Index_c2cMsg_content b  where a.docid = b.docid'			
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c0msgId',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c1uin',1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c2time',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c3type', 2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c4flag', 2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c7content', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c8conversationUin', 1,2)
        for row in  db.ReadTableDeletedRecords(ts, False):
            try:
                if canceller.IsCancellationRequested:
                    return
                msg = Message()
                msg.id = str(row['c0msgId'].Value)
                uin = str(row['c1uin'].Value)
                sendtime = row['c2time'].Value
                msgtype = row['c3type'].Value
                flag = row['c4flag'].Value
                content = row['c7content'].Value	
                msg.account_id = acc_id
                msg.talker_id = row['c8conversationUin'].Value
                msg.deleted = 1
                msg.source = node.AbsolutePath
                try:
                    msg.talker_name = self.friends[msg.talker_id].nickname
                except:
                    msg.talker_name = ''
                msg.sender_id =  uin  
                if uin == acc_id:
                    msg.sender_name = self.nickname[acc_id]
                else:
                    try:
                        msg.sender_name =  self.friends[uin].nickname 
                    except:
                        msg.sender_name = ""
                msg.type = MESSAGE_CONTENT_TYPE_TEXT
                msg.content = content
                msg.timestamp = sendtime
                msg.talker_type = CHAT_TYPE_FRIEND				
                self.im.db_insert_table_message(msg)
            except:					
                pass
        self.im.db_commit()																							
        return 
    def decode_recover_fts_group_table(self,acc_id,table):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return	
        #sql = 'select c1uin,c2time,c3type,c4flag,c5nickname,c7content,c8conversationuin,c0msgId from ' + table  +  ' a,tb_Index_TroopMsg_content b  where a.docid = b.docid'	
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c0msgId',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c1uin', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c2time',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c3type',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c4flag', 2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c5nickName', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c7content', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c8conversationUin', 1,2)
        for row in db.ReadTableDeletedRecords(ts, False):
            try:
                if canceller.IsCancellationRequested:
                    return
                msg = Message()
                
                senduin = str(row['c1uin'].Value)
                sendtime = row['c2time'].Value
                msgtype =row['c3type'].Value
                flag = row['c4flag'].Value
                content = row['c7content'].Value
                nickname = row['c5nickName'].Value
                msgid =str(row['c0msgId'].Value)
                group_id = str(row['c8conversationUin'].Value)
                msg.account_id = acc_id
                msg.talker_id = group_id
                msg.source = node.AbsolutePath
                try:
                    msg.talker_name = self.troops[group_id].name
                except:
                    msg.talker_name = ""
                if senduin == acc_id:                        
                    msg.sender_name = self.nickname[acc_id]
                    msg.is_sender = 1
                else:
                    try:
                        msg.sender_name = self.chatroommembers[(group_id,senduin)].display_name                                                        
                    except:
                        msg.sender_name = ""
                        pass
                    if msg.sender_name == ""or msg.sender_name is None:
                        try:
                            msg.sender_name = self.chatroommembers[(group_id,senduin)].nick_name
                        except:
                            pass   
                    msg.is_sender = 0 
                msg.deleted = 1
                msg.sender_id = senduin                    
                msg.id = msgid
                msg.type = MESSAGE_CONTENT_TYPE_TEXT
                msg.content = content
                msg.timestamp = sendtime
                msg.talker_type = CHAT_TYPE_GROUP
                self.im.db_insert_table_message(msg)
            except:					
                pass
        self.im.db_commit()																							
        return 
    def decode_recover_fts_discussGrp_table(self,acc_id,table):
        node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return	
        #sql = 'select c1uin,c2time,c3type,c4flag,c5nickname,c7content,c8conversationuin,c0msgId from ' + table  +  ' a,tb_Index_TroopMsg_content b  where a.docid = b.docid'	
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c0msgId',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c1uin', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c2time',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c3type',2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c4flag', 2,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c5nickName', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c7content', 1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c8conversationUin', 1,2)
        for row in db.ReadTableRecords(ts, True):
            try:
                if canceller.IsCancellationRequested:
                    return
                msg = Message()				
                senduin = (row['c1uin'].Value)
                sendtime = row['c2time'].Value
                #something not same
                flag =row['c3type'].Value
                msgtype = row['c4flag'].Value
                content = row['c7content'].Value
                nickname = row['c5nickName'].Value
                msgid =str(row['c0msgId'].Value)
                group_id = (row['c8conversationUin'].Value)
                msg.account_id = acc_id
                msg.talker_id = group_id
                msg.source = node.AbsolutePath
                try:
                    msg.talker_name = self.troops[group_id].name
                except:
                    msg.talker_name = ""
                if senduin == acc_id:                        
                    msg.sender_name = self.nickname[acc_id]
                    msg.is_sender = 1
                else:
                    try:
                        msg.sender_name = self.chatroommembers[(group_id,senduin)].display_name        
                    except:
                        msg.sender_name = ""
                    if msg.sender_name == ""or msg.sender_name is None:
                        try:
                            msg.sender_name = self.chatroommembers[(group_id,senduin)].nick_name
                        except:
                            pass   
                    msg.is_sender = 0     
                msg.deleted = 1
                msg.sender_id = senduin
                msg.id = msgid
                msg.type = MESSAGE_CONTENT_TYPE_TEXT
                msg.content = content
                msg.timestamp = sendtime
                msg.talker_type = CHAT_TYPE_GROUP
                self.im.db_insert_table_message(msg)
            except:					
                pass
        self.im.db_commit()																							
        return 