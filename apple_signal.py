# coding=utf-8
import clr
import threading
clr.AddReference('System.Web')
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')

try:
    clr.AddReference('SqliteExp')
    clr.AddReference('PNFA.InfraLib.Exts')
    clr.AddReference("PNFA.Formats.NextStep")
    clr.AddReference('model_im')
    clr.AddReference('bcp_im')    
 
except:
    pass
del clr
from System.Data.SQLite import *
from System.Text import Encoding
from System.Xml.Linq import *
from PA_runtime import *
import PA_runtime
from bcp_im import *
from  model_im import *
import bcp_im
import hashlib
from xml.dom.minidom import parse
from  ctypes import * 
import shutil
from SqliteExp import NativeMethod
from System import Array,Byte
from PA.Formats.NextStep import *
from PA.InfraLib.Extensions import PlistHelper
from collections import defaultdict
import collections
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

hitdict =  {'(?i)database/Signal.sqlite$':('Signal',ParserResults())}
         
def checkhit(root):
    nodes = []
    global hitdict
    for re in hitdict.keys():                 
        node = root.FileSystem.Search(re)
        if(len(list(node)) != 0):
            nodes.append((node,hitdict[re]))
    return nodes
   
def startthread(root,extdata,extract_deleted,extract_source):        
    try:
        sourceApp = extdata[0]
        pr = extdata[1]
        pr.Models.AddRange(signal(root, extract_deleted, extract_source).parse())
        pr.Build(sourceApp)    
    except Exception as e:
        print(e)
def analyze_apple_signal(root, extract_deleted, extract_source):
    try:     
        pr = ParserResults()    
        nodes = checkhit(root)
        if len(nodes) != 0:
            progress.Start()
        threads = []
        for node in nodes:
            for root in node[0]:
                try:
                    global hitdict
                    arg = (root,node[1],extract_deleted,extract_source)                    
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
    return pr
#singal just one user
class signal(object):
    def __init__(self, app_root_dir, extract_deleted, extract_source):        
        self.root = app_root_dir
        self.nickname = ''
        self.models = []
        self.accounts = []
        self.friends= collections.defaultdict(str)
        self.contacts =collections.defaultdict(str) # uin to contact
        self.groups = {}
        self.groupMeminfo ={}
        self.im = IM()
        self.userid = ""
        self.cachepath = ds.OpenCachePath("Signal") 
        self.bcppath = ds.OpenCachePath("tmp") 
        m = hashlib.md5()
        m.update(self.root.AbsolutePath)        
        self.cachedb =  self.cachepath  + '/' + m.hexdigest().upper() + ".db"  
        self.signaldb =  self.cachepath  + "/signal.sqlite"  
        self.designaldb =  self.cachepath  + "/designal.sqlite"  
        self.dbAbsolutePath =  self.root.AbsolutePath
        self.VERSION_APP_VALUE = 10000
        self.keychain  = 'private/var/tmp/keychain.plist'
        self.initsuccess = 0
        node = self.root.FileSystem.Search(self.keychain)
        self.sqlitekey = None
        if len(list(node)) == 0:
            return 
        for k in node:
            x = k.PathWithMountPoint
            plist = PlistHelper.ReadPlist(x)            
            for d in  plist['genp']:
                try:
                    if ('acct' in d.Keys and 'svce' in d.Keys):                        
                        if (str(d['acct']) == 'OWSDatabaseCipherKeySpec' and str(d['svce']) =='TSKeyChainService'):
                            self.sqlitekey = d['v_Data']
                            break
                except Exception as e:
                    print (e)
            break
        if self.sqlitekey is None :
            return []
        if(os.path.exists(self.designaldb)):
            os.remove(self.designaldb )
        shutil.copyfile(self.root.PathWithMountPoint, self.signaldb )   
        self.ininsuccess = NativeMethod.ExportSqlCipherDatabase(self.signaldb,self.designaldb,self.sqlitekey.Bytes ,32)
    def parse(self):
        if self.initsuccess == 0 :
            return []
        if self.im.need_parse(self.cachedb, self.VERSION_APP_VALUE):           
            try:
                self.im.db_create(self.cachedb)            
                self.userinfo()
                if self.userid == "":
                    os.remove(self.cachedb)
                    return 
                self.friendinfo()
                self.groupinfo()             
                self.messages()         
                if canceller.IsCancellationRequested:
                    os.remove(self.cachedb)
                    return
                self.im.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
                self.im.db_insert_table_version(VERSION_KEY_APP, self.VERSION_APP_VALUE)
                self.im.db_commit()
                self.im.db_close()
            except:
                os.remove(self.cachedb)
        PA_runtime.save_cache_path(bcp_im.CONTACT_ACCOUNT_TYPE_IM_QQ,self.cachedb,self.bcppath)
        gen = GenerateModel(self.cachedb)
        return gen.get_models()    
    def userinfo(self):
        #acture file
        d = self.designaldb 
        sql = "select collection,key,data from database2  where collection == 'UserProfile' order by rowid"
        datasource = "Data Source =  " + d +";ReadOnly=True;Charset=utf8;"
        conn = SQLiteConnection(datasource)
        try:
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    collection = SafeGetString(reader,0)
                    key = SafeGetString(reader,1)
                    data = SafeGetBlob(reader,2)
                    self.processdata(collection,key,data)
                except Exception as e:
                    print(e)      
            self.im.db_commit()        
            reader.Close()
            command.Dispose()
            conn.Close()                 
        except Exception as e:
            pass
        return 
    def friendinfo(self):
        d = self.designaldb 
        sql = "select collection,key,data from database2  where collection == 'SignalAccount' order by rowid"
        datasource = "Data Source =  " + d +";ReadOnly=True;Charset=utf8;"
        conn = SQLiteConnection(datasource)
        try:
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                try:
                    collection = SafeGetString(reader,0)
                    key = SafeGetString(reader,1)
                    data = SafeGetBlob(reader,2)
                    self.processdata(collection,key,data)
                except Exception as e:
                    print(e)  
            self.im.db_commit()          
            reader.Close()
            command.Dispose()
            conn.Close()   
        except:
            pass           
        return 
    def groupinfo(self):
        d = self.designaldb 
        sql = "select collection,key,data from database2  where  collection== 'TSThread'  order by rowid"
        datasource = "Data Source =  " + d +";ReadOnly=True;Charset=utf8;"
        conn = SQLiteConnection(datasource)
        try:
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                collection = SafeGetString(reader,0)
                key = SafeGetString(reader,1)
                data = SafeGetBlob(reader,2)
                self.processdata(collection,key,data);
            self.im.db_commit()        
        except Exception as e:
            print (e)
    def processcontact(self,tree):
        try:
            friend = Friend()
            uniqueId = tree['uniqueId'].Value
            fullname = tree['contact']['fullName'].Value  
            friend.account_id = self.userid 
            friend.friend_id = uniqueId
            friend.nickname  = fullname
            friend.source = self.dbAbsolutePath
            self.friends[uniqueId] = fullname
            self.im.db_insert_table_friend(friend)            
        except:
            pass
       
    def processmsg(self,tree):
        try:
            msg = Message() 
            msg.source  = self.dbAbsolutePath
            msg.account_id = self.userid
            receivetime = tree['receivedAtTimestamp'].Value
            msg.send_time = receivetime /1000
            time = tree['timestamp'].Value
            msg.send_time = receivetime /1000
            attachmentIds = tree['attachmentIds'].Value 
            if 'isVoiceMessage' in  tree.Keys:
                isVoiceMessage = tree['isVoiceMessage'].Value
            uniqueThreadId = tree['uniqueThreadId'].Value
            if uniqueThreadId in self.groups.keys():
                msg.talker_type  = CHAT_TYPE_GROUP
                msg.talker_id = uniqueThreadId
            else:
                msg.talker_type  = CHAT_TYPE_FRIEND
                msg.talker_id = self.contacts[uniqueThreadId]            
                      
            if msg.talker_type == CHAT_TYPE_GROUP:  
                recepientMap = tree['recipientStateMap'].Value 
                receives = set()
                for recepient in recepientMap.Keys:                
                    receives.add(recepient)
                if len(recepientMap) == 0:
                    msg.sender_id = self.userid
                else:
                    msg.sender_id = list(self.groupMeminfo[uniqueThreadId] - receives)[0]
                if msg.sender_id == self.userid:
                    msg.is_sender = 1
                msg.sender_name = self.friends[msg.sender_id]
            else:
                recepientMap = tree['recipientStateMap'].Value 
                receive = ""
                for recepient in recepientMap.Keys: 
                    receive = recepient    
                if receive == self.userid:
                    msg.is_sender = 0
                    msg.sender_id = msg.talker_id
                else:
                    msg.is_sender = 1
                    msg.sender_id = self.userid      
                msg.sender_name = self.friends[msg.sender_id]
            if isVoiceMessage == False:
                if 'body' in tree.Keys:                                                                                                                                                                                                                                                                                                                                     
                    boby = tree['body'].Value                     
                    msg.content = boby
                    msg.type = MESSAGE_CONTENT_TYPE_TEXT
                    msg.insert_db(self.im)
            else:
                 attachmentFilenameMap = tree['attachmentFilenameMap'].Value
                 for attach in attachmentFilenameMap.Keys:               
                    re = attach + '/' + attachmentFilenameMap[attach].Value
                    media_path = self.root.FileSystem.Search(re)
                    for node in media_path:
                        msg.media_path = node.AbsolutePath
                        break
                    msg.type = MESSAGE_CONTENT_TYPE_VOICE
                    msg.insert_db(self.im)
                    return
            #share
            if 'contactShare' in  tree.Keys:
                contactShare =  tree['contactShare'].Value                      
                name = contactShare['name'].Value            
                phoneNumbers = contactShare['phoneNumbers'].Value
                phoneNumber = phoneNumbers[0]['phoneNumber'].Value
                familyname = name['familyName'].Value
                displayName = name['displayName'].Value                 
                link = msg.create_link()
                link.content = displayName + 'phone: ' + phoneNumber
                msg.insert_db(self.im)     
                return    
            attachmentFilenameMap = tree['attachmentFilenameMap'].Value
            for attach in attachmentFilenameMap.Keys:
                re = attach  + '/' + attachmentFilenameMap[attach].Value
                media_path = self.root.FileSystem.Search(re)
                for node in media_path:
                    msg.media_path = node.AbsolutePath
                    break                                  
                if msg.media_path.endswith('.mp4'):
                    msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                elif msg.media_path.endswith('.jpg'):
                    msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                elif msg.media_path.endswith('.png'):
                    msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                elif msg.media_path.endswith('.jpeg'):
                    msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                elif msg.media_path.endswith('.gif'):
                    msg.type = MESSAGE_CONTENT_TYPE_IMAGE    
                else:
                    msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT           
                msg.insert_db(self.im)
        except Exception as e:
            print(e)
         
    def processdata(self,collection,key,data):
        try:
            mem = MemoryRange.FromBytes(data)
            tree = BPReader.GetTree(mem)  
            if collection == 'UserProfile':                
                if key == 'kLocalProfileUniqueId':                       
                    self.nickname = tree['profileName'].Value                                        
                else:                              
                    ac = Account()
                    try:      
                        ac.account_id = tree['uniqueId'].Value                                           
                        ac.source  = self.dbAbsolutePath    
                        self.userid = ac.account_id               
                        if self.nickname == '':
                            self.nickname = ac.nickname                        
                        ac.nickname =  tree['profileName'].Value 
                        self.friends[self.userid] = ac.nickname 
                    except:
                        ac.account_id = key
                        ac.nickname = 'unknown'
                        self.friends[self.userid] = ac.nickname 
                        pass
                    self.im.db_insert_table_account(ac)                                          
            elif collection =='SignalAccount':           
                self.processcontact(tree)
            elif collection == 'TSInteraction':
                self.processmsg(tree)
            elif collection == 'TSThread':
                self.processgroup(tree)                
        except Exception as e:
            print (e)
        return
    
    def processgroup(self,tree):
        try:
            if 'groupModel' in tree.Keys:  
                g = Chatroom()
                g.account_id = self.userid                
                g.source = self.dbAbsolutePath          
                groupModel = tree['groupModel'].Value
                uniqueId = tree['uniqueId'].Value            
                g.chatroom_id = uniqueId  
                groupName = groupModel['groupName'].Value
                g.name = groupName
                groupId = groupModel['groupId']                 
                groupMemberIds = groupModel['groupMemberIds']
                self.groups[uniqueId] = g
                self.im.db_insert_table_chatroom(g)
                memberset = set()
                for memid in groupMemberIds:
                    member = ChatroomMember()
                    member.account_id =self.userid
                    member.chatroom_id = g.chatroom_id
                    member.member_id = memid.Value
                    member.source = self.dbAbsolutePath
                    memberset.add(member.member_id)     
                    self.im.db_insert_table_chatroom_member(member)
                self.groupMeminfo[uniqueId] = memberset
            else:
                uniqueId = tree['uniqueId'].Value
                self.contacts[uniqueId] = uniqueId[1:]
                return                                                                  
        except:
            pass    
    def messages(self):        
        #node = self.root.GetByPath('/Documents/contents/' + acc_id + '/FTSMsg.db')        
        d =  self.designaldb    
        sql = "select collection,key,data from database2  where  collection== 'TSInteraction'  order by rowid"
        datasource = "Data Source =  " + d +";ReadOnly=True;Charset=utf8;"
        conn = SQLiteConnection(datasource)
        try:
            conn.Open()
            if(conn is None):
                return
            command = SQLiteCommand(conn)                
            command.CommandText = sql
            reader = command.ExecuteReader()
            while reader.Read():
                collection = SafeGetString(reader,0)
                key = SafeGetString(reader,1)
                data = SafeGetBlob(reader,2)
                self.processdata(collection,key,data);
            self.im.db_commit()
        except Exception as e:
            print (e)
