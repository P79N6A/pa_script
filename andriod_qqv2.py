#coding=utf-8
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
    clr.AddReference('model_qq')
    clr.AddReference('QQ_struct')
    clr.AddReference('bcp_im')
    clr.AddReference('qq_pic_message_pb2')
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
from  qq_pic_message_pb2 import *
from model_qq import *
import uuid
from QQ_struct import tencent_struct
import bcp_im
import json
#import bcp_im
import base64
import threading
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

def decode_blob(imei_bytes, buffers):
    ret = bytearray()
    for i in range(len(buffers)):
        c = ((buffers[i])^ord(imei_bytes[i % len(imei_bytes)]))
        ret.append(c)
    try:
        return ret
    except:
        return None
def decode_text(imei_bytes, text):
    if text is None:
        return ''
    ret = ''
    for i in range(len(text)):
        try:
            c = bytearray(text[i].encode('utf8'))
            c[len(c)-1] = (c[len(c)-1]) ^ ord(imei_bytes[i % len(imei_bytes)])
            ret += c.decode('utf-8',"ignore")
        except Exception as e:
            pass
    return ret
def readVarInt(data):
        i = 0
        j = 0
        l = 0
        k = 0
        while True:
            try:
                j, = struct.unpack('B',data[k])
                k = k+1
            except Exception as e:
                pass
            l = l | (j & 0x7f) << i
            if((j & 0x80) == 0):
                return l
            i = i + 7

hitdict =  { '(?i)com.tencent.mobileqq/.*databases$':['QQ','mobileqq'],
            '(?i)com.tencent.qqlite/.*databases$':['QQ轻聊版','qqlite'],           
            '(?i)com.tencent.mobileqqi/.*databases$':['QQ国际版','mobileqqi'],
            '(?i)com.tencent.tim/.*databases$':['QQ TIM','tim'],
            '(?i)com.tencent.minihd.qq/.*databases$':['平板QQ','minihd']
            }    
def checkhit(root):
    nodes = []
    global hitdict
    for re in hitdict.keys():                 
        node = root.FileSystem.Search(re)
        if(len(list(node)) != 0):
            if len(node) > 1 : 
                i = 1
                for d in node:                                                             
                    data = [hitdict[re][0] +  "-分身版本-" + str(i), hitdict[re][1]]
                    i = i+ 1
                    nodes.append((d,data))                    
            else:
                nodes.append((node[0],hitdict[re]))

    return nodes
   
def startthread(root,extdata,extract_deleted,extract_source):        
    try:
        sourceApp = extdata[0]
        resFloder = extdata[1]
        Andriod_QQParser(root,sourceApp,resFloder, extract_deleted, extract_source).parse()   
    except Exception as e:
        pass
    
def analyze_andriod_qq(root, extract_deleted, extract_source):
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
    return pr
class Andriod_QQParser(object):
    def __init__(self, app_root_dir, sourceApp,resFloder,extract_deleted, extract_source):
        self.root = app_root_dir.Parent
        self.extract_source = extract_source
        self.extract_deleted = extract_deleted
        self.app_name = 'QQ'
        self.friendsGroups = collections.defaultdict()
        self.friendsNickname = collections.defaultdict()
        self.groupContact = collections.defaultdict()
        self.nickname = ''
        self.models = []
        self.accounts = []
        self.contacts = {}  # uin to contact
        self.accounttables = []
        self.friendmsgtables =set()
        self.troopmsgtables =set()

        self.friendhash = collections.defaultdict(str)
        self.accinfo = collections.defaultdict(list)
        self.troops = collections.defaultdict(Chatroom)
        self.im = IM()
        self.cachepath = ds.OpenCachePath("QQ")
        self.bcppath = ds.OpenCachePath("tmp")
        self.imei = ''
        self.imeilen = 15
        self.VERSION_APP_VALUE = 10000
        self.sourceApp = sourceApp
        self.resFolder = resFloder
        self.cachedb = collections.defaultdict()        
        self.accountsProg = collections.defaultdict()
    def set_progress(self, acc_id,value):
        try:
            if self.accountsProg[acc_id] is not None and value != self.accountsProg[acc_id].Value:
                self.accountsProg[acc_id].Value = value
                print('set_progress() %d' % value)
        except Exception as e:
            pass
    def insertaccount(self,ac):
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
        if(len(self.accounts) != 0):                    
            progress.Start()
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
                self.getImei()                                
                self.im.db_create(cachedb)
                self.insert_account(acc)               
                try:
                    if canceller.IsCancellationRequested:
                        return
                    self.friendsNickname.clear()
                    self.friendsGroups.clear()
                    self.groupContact.clear()
                    self.accounttables = []
                    self.troops.clear()
                    #self.nickname = self.accinfo[acc_id][1]
                    self.contacts = {}
                    self.friendhash.clear()
                    self.friendmsgtables =set()
                    self.troopmsgtables =set()
                    self.set_progress(acc_id,1)
                    self.decode_accounttables(acc_id)
                    self.set_progress(acc_id,2)
                    self.decode_friends(acc_id)
                    self.set_progress(acc_id,3)
                    self.decode_group_info(acc_id)
                    self.set_progress(acc_id,6)
                    self.decode_groupMember_info(acc_id)
                    self.set_progress(acc_id,10)
                    self.decode_friend_messages(acc_id)
                    self.set_progress(acc_id,18)
                    self.decode_group_messages(acc_id)
                    self.set_progress(acc_id,25)
                    self.recover_msg_from_friendtable(acc_id)
                    self.set_progress(acc_id,28)
                    self.recover_msg_from_trooptable(acc_id)
                    self.set_progress(acc_id,30)
                    self.decode_msg_ftstable(acc_id)
                    self.set_progress(acc_id,32)
                    self.recover_msg_ftstable(acc_id)              
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
            acc_id = acc.account_id
            try:
                PA_runtime.save_cache_path(bcp_im.CONTACT_ACCOUNT_TYPE_IM_QQ,self.cachedb[acc_id],self.bcppath)
                gen = GenerateModel(self.cachedb[acc_id],self.sourceApp,self.accountsProg[acc_id])                     
                gen.get_models()
                self.set_progress(acc_id,100)
                self.accountsProg[acc_id].Finish(True)
            except:
                pass
        try:
            gen.ar.set_unique_id(self.root.AbsolutePath)
            self.get_qq_res(gen.ar)
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, e)  
        prog.Finish(True)
        return

    def get_qq_res(self, ar):
        dicts = {}
        searchkey = 'tencent/'+ self.resFolder + "$"
        resnode = self.root.FileSystem.Search(searchkey)
        for node in resnode:
            ar.save_res_folder(node, "Other")
        else:
            return      

    def decode_accounttables(self,acc_id):
        node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        sql = 'select tbl_name from sqlite_master where type ="table"'
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn)
        command.CommandText = sql
        reader = command.ExecuteReader()
        while reader.Read():
            self.accounttables.append(SafeGetString(reader,0))
        reader.Close()
        command.Dispose()
        conn.Close()
        for table in self.accounttables:
            if table.find('mr_friend_') != -1:
                self.friendmsgtables.add(table)
            elif table.find('mr_troop_') != -1:
                self.troopmsgtables.add(table)

    def getImei(self):
        path = self.root.GetByPath('/files/imei')
        if path is None:
            return
        try:
            d = path.PathWithMountPoint
            f = open(d,"rb")
            l = f.readlines()
            for x in l:
                pos = x.find('imei=')
                if(pos != -1):
                    self.imei = x[5:len(x)-1]
                    self.imeilen = len(self.imei)
        except Exception as e:
            pass
        return
    def decode_accounts(self):
        try:
            dblist = []
            pattern = r"^([0-9]+).db$"
            path =  self.root.GetByPath('/databases/')
            if path is None:
                return
            d =  path.PathWithMountPoint
            for root, dirs, files in os.walk(d):
                for f in files:
                    if(re.match(pattern,f)):
                        dblist.append(f)
            #account
            accounts = []
            for db in dblist:
                acc_id = db[0:db.find('.db')]
                accounts.append(acc_id)
            #nick
            nickfile =  self.root.GetByPath('/files/Properties')
            nickfilepath = nickfile.PathWithMountPoint
            f = open(nickfilepath,'rb')
            nickdata = f.readlines()
            f.close()
            nickdata = sorted(nickdata)
            for acc_id in accounts:
                for line in nickdata:
                    name = 'nickName'+ acc_id+ '='
                    t = acc_id +'_logintime='
                    pos = line.find(name)
                    if(pos != -1):
                        x = line[pos+len(name):len(line)-1]
                        nickname = unicode(x).decode('unicode-escape')
                        #nickname  = nickname.encode('utf-8')                        
                        self.accinfo[acc_id].append(nickname)
                    postime = line.find(t)
                    if(postime != -1):
                        time = line[postime+len(t):len(line)-1]
                        time = time[:len(time) -3]
                        self.accinfo[acc_id].append(time)      
            for acc in self.accinfo:
                try:
                    ac = Account()
                    ac.ServiceType = self.sourceApp
                    ac.account_id = acc
                    ac.source = nickfile.AbsolutePath
                    ac.nickname = self.accinfo[acc][1]                    
                    self.accounts.append(ac)
                except Exception as e:
                    pass
        except Exception as e:
            pass
        return
    def insert_account(self,account):
        try:
            self.im.db_insert_table_account(account)
            self.im.db_commit()
        except:
            pass
    def decode_friends(self,acc_id):
        node = self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        if(conn is None):
            return
        command = SQLiteCommand(conn)
        command.CommandText = 'select uin ,remark,name,datetime from friends'
        reader = command.ExecuteReader()
        while reader.Read():
            try:
                if canceller.IsCancellationRequested:
                    return
                friend = Friend()
                friend.friend_id = decode_text(self.imei,SafeGetString(reader,0))
                m = hashlib.md5()
                m.update(friend.friend_id)
                self.friendhash[friend.friend_id] = m.hexdigest().upper()
                friend.type = FRIEND_TYPE_FRIEND
                friend.account_id = acc_id
                friend.nickname = decode_text(self.imei,SafeGetString(reader,2))
                friend.source  = node.AbsolutePath                
                friend.remark = decode_text(self.imei,SafeGetString(reader,1))
            except Exception as e:
                pass
            self.im.db_insert_table_friend(friend)
        self.im.db_commit()
        reader.Close()
        command.Dispose()
        conn.Close()
    def decode_group_info(self,acc_id):
        node = self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        if 'TroopInfo' in self.accounttables:
            sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo,troopCreateTime,wmemberNum from TroopInfo'
        else:
            sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo, troopCreatetime,wmemberNum from TroopInfoV2'
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
                g.source = node.AbsolutePath
                g.account_id = acc_id               
                g.chatroom_id = decode_text(self.imei,SafeGetString(reader,0))                
                g.owner_id = decode_text(self.imei,SafeGetString(reader,2))
                g.name = decode_text(self.imei,SafeGetString(reader,3))
                g.notice = decode_text(self.imei,SafeGetString(reader,4))
                g.create_time = SafeGetInt64(reader,5)
                g.member_count  = SafeGetInt64(reader,6)               
            except Exception as e:
                pass
            self.troops[g.chatroom_id] = g
            self.im.db_insert_table_chatroom(g)
        reader.Close()
        command.Dispose()
        conn.Close()
        self.im.db_commit()

    def decode_groupMember_info(self,acc_id):
        node = self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        sql ='''
               select troopuin, memberuin,friendnick,
            autoremark,age,join_time,last_active_time,troopnick
            from TroopMemberInfo order by troopuin
            '''
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
                mem = ChatroomMember()
                mem.account_id = acc_id
                mem.chatroom_id = decode_text(self.imei,SafeGetString(reader,0))
                mem.member_id = decode_text(self.imei,SafeGetString(reader,1))
                nickname = SafeGetString(reader,2)
                if nickname ==  '':
                    nickname = SafeGetString(reader,7)
                mem.display_name = decode_text(self.imei,nickname)
                mem.nick_name = mem.display_name
                mem.signature = decode_text(self.imei,SafeGetString(reader,3))
                mem.age = SafeGetInt64(reader,4)
                mem.joinTime = SafeGetInt64(reader,5)
            except Exception as e:
                pass
            self.im.db_insert_table_chatroom_member(mem)
        reader.Close()
        command.Dispose()
        conn.Close()
        self.im.db_commit()


    def decode_friend_messages(self,acc_id):
        for table in self.friendmsgtables:
            self.decode_msg_from_friendtbale(acc_id ,table)

    def processmedia(self,msg,path):
        try:
            picheader = {'/offpic_new/':'https://c2cpicdw.qpic.cn', '/gchatpic_new/': 'http://gchat.qpic.cn'}
            sdcard = '/storage/emulated/0/'
            searchkey = ''
            nodes = list()
            if path.find(sdcard) != -1 :
                searchkey = path[path.find(sdcard) +len(sdcard):]
                nodes = self.root.FileSystem.Search(searchkey+'$')
                if len(list(nodes)) == 0:
                    searchkey = path[path.rfind('/')+1:]
                    nodes = self.root.FileSystem.Search(searchkey+ '$')                        
                for node in nodes:
                    msg.media_path = node.AbsolutePath
                    if msg.media_path.endswith('.mp3') :
                        msg.type = MESSAGE_CONTENT_TYPE_VOICE
                    elif msg.media_path.endswith('.amr'):
                        msg.type = MESSAGE_CONTENT_TYPE_VOICE
                    elif msg.media_path.endswith('.slk') :
                        msg.type = MESSAGE_CONTENT_TYPE_VOICE
                    elif msg.media_path.endswith('.mp4'):
                        msg.type = MESSAGE_CONTENT_TYPE_VIDEO
                    elif msg.media_path.endswith('.jpg'):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                    elif msg.media_path.endswith('.gif'):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                    elif msg.media_path.endswith('.jpge'):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                    elif msg.media_path.endswith('.png'):
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                    else:
                        msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
                    return True
            else:                
                for header in picheader.keys():
                    if(path.find(header) == 0):
                        msg.media_path = picheader[header] + path
                        msg.type = MESSAGE_CONTENT_TYPE_IMAGE                   
                        return True
        except Exception as e:
            pass
        return False
    def decode_msg_from_friendtbale(self,acc_id,table):
        node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        command = SQLiteCommand(conn)
        command.CommandText = 'select msgseq,extstr,frienduin,msgdata,senderuin,time,msgtype ,_id from ' + table + ' order by time'
        reader = command.ExecuteReader()
        hash = table[10:42]
        m = hashlib.md5()
        talker_id = ''
        while reader.Read():
            try:
                if canceller.IsCancellationRequested:
                    return
                msg = Message()
                msg.msg_id = SafeGetInt64(reader,0)
                msg.account_id = acc_id
                frienduin = decode_text(self.imei, SafeGetString(reader,2))
                blobdata = SafeGetBlob(reader,3)
                msgdata = decode_blob(self.imei, blobdata)
                senderuin = decode_text(self.imei, SafeGetString(reader,4))
                msg.timestamp = SafeGetInt64(reader,5)
                msgtype = SafeGetInt64(reader,6)
                msg.status = msgtype
                msg.source = node.AbsolutePath
                msg.talker_type = CHAT_TYPE_FRIEND
                if talker_id is '':
                    m.update(frienduin)
                    digest = m.hexdigest().upper()
                    if digest == hash:
                        talker_id =  frienduin
                    else:
                        for friend in self.friendhash:
                            if(hash == self.friendhash[friend]):
                                talker_id = friend
                                break
                if talker_id is '':
                    msg.talker_id = hash
                else:
                    msg.talker_id = talker_id                
                if senderuin == acc_id:                    
                    msg.sender_id = acc_id
                else:                    
                    msg.sender_id = msg.talker_id
                msg.type = MESSAGE_CONTENT_TYPE_TEXT
                self.processQQMsg(msg,msgdata,msgtype)
            except Exception as e:
                pass
        reader.Close()
        command.Dispose()
        conn.Close()
        self.im.db_commit()

    def decode_group_messages(self,acc_id):
        for table in self.troopmsgtables:
            self.decode_msg_from_trooptbale(acc_id ,table)

    def decode_tencentmap(self,msg,msgstruct):
        try:
            msg.type = MESSAGE_CONTENT_TYPE_LOCATION
            msg.address = msgstruct['meta']['Location.Search']['address'].decode("utf8","ignore")
            msg.latitude = msgstruct['meta']['Location.Search']['lat']
            msg.longitude = msgstruct['meta']['Location.Search']['lng']
            msg.location_type = LOCATION_TYPE_GOOGLE
        except:
            pass
        
    def decode_msg_from_trooptbale(self,acc_id ,table):
        node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        command = SQLiteCommand(conn)
        command.CommandText = 'select msgseq,frienduin,msgdata,senderuin,time,msgtype from ' + table + ' order by time'
        reader = command.ExecuteReader()
        hash = table[9:41]
        m = hashlib.md5()
        talker_id = ''
        while reader.Read():
            try:
                if canceller.IsCancellationRequested:
                    return
                msg = Message()
                msg.msg_id = SafeGetInt64(reader,0)
                msg.account_id = acc_id
                frienduin = decode_text(self.imei, SafeGetString(reader,1))
                blobdata = SafeGetBlob(reader,2)
                msgdata = decode_blob(self.imei, blobdata)
                senderuin = decode_text(self.imei, SafeGetString(reader,3))
                msg.timestamp = SafeGetInt64(reader,4)
                msgtype = SafeGetInt64(reader,5)
                msg.talker_type = CHAT_TYPE_GROUP
                if talker_id == '':
                    m.update(frienduin)
                    digest = m.hexdigest().upper()
                    if digest == hash:
                        talker_id =  frienduin
                    else:
                        for troopuin in self.troops:
                            m.update(troopuin)
                            digest = m.hexdigest().upper()
                            if(hash == digest):
                                talker_id = troopuin
                                break
                if talker_id == '':
                    msg.talker_id = hash
                else:
                    msg.talker_id = talker_id
                if senderuin == acc_id:                    
                    msg.sender_id = acc_id
                else:                    
                    msg.sender_id = senderuin
                msg.source = node.AbsolutePath
                msg.status = msgtype
                msg.type = MESSAGE_CONTENT_TYPE_TEXT
                self.processQQMsg(msg,msgdata,msgtype)
            except Exception as e:
                pass
        reader.Close()
        command.Dispose()
        conn.Close()
        self.im.db_commit()
    def recover_msg_from_friendtable(self,acc_id):
        node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return
        m = hashlib.md5()
        talker_id = ''
        for table_name in self.friendmsgtables:
            hash = table_name[10:42]
            ts = SQLiteParser.TableSignature(table_name)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'senderuin',1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'frienduin',1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'time',2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgtype', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgdata', 3,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgseq', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'flag', 2,2)
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    msg = Message()
                    msg.msg_id = str(rec['msgseq'].Value)
                    msg.account_id = acc_id
                    frienduin = decode_text(self.imei ,rec['frienduin'].Value)
                    blobdata =rec['msgdata'].Value
                    msgdata = decode_blob(self.imei, blobdata)
                    senderuin = decode_text(self.imei,rec['senderuin'].Value)
                    msg.timestamp = rec['time'].Value
                    msgtype = rec['msgtype'].Value
                    msg.status = msgtype
                    msg.source = node.AbsolutePath
                    msg.talker_type = CHAT_TYPE_FRIEND
                    msg.deleted = 1
                    if talker_id is '':
                        m.update(frienduin)
                        digest = m.hexdigest().upper()
                        if digest == hash:
                            talker_id =  frienduin
                        else:
                            for friend in self.friendhash:
                                if(hash == self.friendhash[friend]):
                                    talker_id = friend
                                    break
                    if talker_id is '':
                        msg.talker_id = hash
                    else:
                        msg.talker_id = talker_id
                    if senderuin == acc_id:                        
                        msg.sender_id = acc_id
                    else:                        
                        msg.sender_id = msg.talker_id
                    msg.type = MESSAGE_CONTENT_TYPE_TEXT
                    self.processQQMsg(msg,msgdata,msgtype)
                except Exception as e:
                    pass
            self.im.db_commit()
    def recover_msg_from_trooptable(self,acc_id):
        node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
        if node is None:
            return
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return
        m = hashlib.md5()
        talker_id = ''
        for table_name in self.troopmsgtables:
            hash = table_name[10:42]
            ts = SQLiteParser.TableSignature(table_name)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'senderuin',1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'frienduin',1,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'time',2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgtype', 2,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgdata', 3,2)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'msgseq', 2,2)
            for rec in db.ReadTableDeletedRecords(ts, False):
                try:
                    if canceller.IsCancellationRequested:
                        return
                    msg = Message()
                    msg.msg_id = str(rec['msgseq'].Value)
                    msg.account_id = acc_id
                    frienduin = decode_text(self.imei ,rec['frienduin'].Value)
                    blobdata =rec['msgdata'].Value
                    msgdata = decode_blob(self.imei, blobdata)
                    senderuin = decode_text(self.imei,rec['senderuin'].Value)
                    msg.timestamp = rec['time'].Value
                    msgtype = rec['msgtype'].Value
                    msg.source = node.AbsolutePath
                    msg.talker_type = CHAT_TYPE_GROUP
                    msg.deleted = 1
                    if talker_id is '':
                        m.update(frienduin)
                        digest = m.hexdigest().upper()
                        if digest == hash:
                            talker_id =  frienduin
                        else:
                            for friend in self.friendhash:
                                if(hash == self.friendhash[friend]):
                                    talker_id = friend
                                    break
                    if talker_id is '':
                        msg.talker_id = hash
                    else:
                        msg.talker_id = talker_id
                    if senderuin == acc_id:                        
                        msg.sender_id = acc_id
                    else:                        
                        msg.sender_id = msg.talker_id
                    msg.type = MESSAGE_CONTENT_TYPE_TEXT
                    self.processQQMsg(msg,msgdata,msgtype)
                except Exception as e:
                    pass
        self.im.db_commit()
    def processQQMsg(self,msg,msgdata,msgtype):
        try:
            qqmsgstruct = tencent_struct()
            msgstruct = qqmsgstruct.getQQMessage(msgtype,bytes(msgdata))
            if(msgtype == -2018 or msgtype == -2050):
                msg.content = json.dumps(msgstruct[50])
            elif(msgtype == -2011 or msgtype == -2054 or msgtype == -2059):
                msg.content = msgstruct['mMsgBrief'].decode('utf-8',"ignore")
                msgItems = msgstruct['mStructMsgItemLists']
                for l in msgItems:
                    try:
                        msg.type = MESSAGE_CONTENT_TYPE_LINK
                        linkurl  = l['b']
                        if linkurl == '':
                            continue                                                                 
                        msg.url = linkurl           
                        msg.insert_db(self.im)    
                    except Exception as e:
                        pass
                return
            elif(msgtype == -5003):
                pass
            elif(msgtype == -1000):
                msg.content = msgstruct.decode("utf8","ignore")
            elif(msgtype == -3006):
                pass
            elif(msgtype == -5040 or msgtype == -5020 or msgtype == -5021 or msgtype == -5022 or msgtype == -5023):
                content =msgstruct[5][1]
                msg.content = content
                pass
            elif(msgtype == -1034):
                pass
            elif(msgtype == -1035):
                msg1035 = msg_1035()
                msg1035.ParseFromString(str(msgdata))
            elif(msgtype == -5008 or msgtype == -2007):
                try:
                    if msgstruct['app'] == 'com.tencent.map':
                        self.decode_tencentmap(msg,msgstruct)
                    else:
                        msg.content  = str(msgstruct['meta']).encode('utf-8',"ignore")
                except Exception as e:
                        pass
                try:
                    if(msgdata[0:4] == '\xac\xed\x00\05' and msgdata[4] ==  0x74):
                        lens, = struct.unpack('>H',msgdata[5:7])
                        msgstruct =json.loads(str(msgdata[7:7+lens]))
                        if msgstruct['app'] == 'com.tencent.map':
                            self.decode_tencentmap(msg,msgstruct)
                        else:
                            msg.content  = str(msgstruct['meta']).encode('utf-8',"ignore")
                except:
                    msg.content  = str(msgstruct['meta']).encode('utf-8',"ignore")
                    pass
            elif(msgtype == -2000):
                url = str(msgstruct['rawMsgUrl'])
                content= str(msgstruct['localPath'])
                thumb = str(msgstruct['thumbMsgUrl'])
                path = str(content)
                if(self.processmedia(msg,path) == False):                    
                    self.processmedia(msg,url)
            elif(msgtype == -2006):
                msg.content	= msgstruct.decode("utf8","ignore")
                pass
            elif(msgtype == -2022):
                #mp4
                filename = msgstruct[3][1]
                path = str(filename)
                self.processmedia(msg,path)
            elif(msgtype == -2053):
                pass
            elif(msgtype == -1049):
                pass
            elif(msgtype == -2025):
                pass
            elif(msgtype == -5012):
                if(msgstruct is None):
                    msgstruct=json.loads(str(msgdata))
                    msg.content = msgstruct["msg"].decode('utf-8',"ignore")
                pass
            elif(msgtype == -2038):
                pass
            elif(msgtype == -5040):
                msg.content = str(msgstruct["content"].decode('utf-8',"ignore"))
                pass
            elif(msgtype == -2005):
                sig = 0x16
                if(msgdata[0] == sig):
                    path = str(msgdata[1:msgdata.find("|")]).decode('utf-8',"ignore")
                    self.processmedia(msg,path)
            elif(msgtype == -2002):
                if msgstruct is None:
                    sdcarad = '/storage/emulated/0/'
                    pos = msgdata.find(sdcarad)
                    if( pos != -1):
                        strlens = msgdata[pos -2]
                        lenpos = pos -2
                        if(strlens & 0x80 == 0):
                            strlens = msgdata[pos -1]
                            lenpos = pos -1
                        lens = readVarInt(str(msgdata[lenpos:]))
                        path = str(msgdata[pos:pos+ lens])
                        self.processmedia(msg,path)
            else:
                msg.content = msgdata.decode('utf-8',"ignore")
        except Exception as e:
            pass
        msg.insert_db(self.im)        
    def decode_msg_ftstable(self,acc_id):
        node =  self.root.GetByPath('/databases/'+ acc_id + '-IndexQQMsg.db')
        if node is None:
            return
        d = node.PathWithMountPoint
        datasource = "Data Source =  " + d +";ReadOnly=True"
        conn = SQLiteConnection(datasource)
        conn.Open()
        command = SQLiteCommand(conn)
        command.CommandText = 'select c1content,c4ext1,c5ext2,c6ext3 from IndexContent_content  order by c4ext1'
        reader = command.ExecuteReader()
        while reader.Read():
            try:
                msg = Message()
                msg.account_id = acc_id
                msg.source = node.AbsolutePath
                content = SafeGetString(reader,0)
                msg.content = base64.b64decode(content).decode('utf-8')
                talker_id = base64.b64decode(SafeGetString(reader,1))
                msg.type = MESSAGE_CONTENT_TYPE_TEXT
                if(talker_id.find('ZzZ1')!= -1):
                    msg.talker_id = talker_id.replace('ZzZ1','')
                    msg.talker_type = CHAT_TYPE_GROUP
                else:
                    msg.talker_id = talker_id.replace('ZzZ0','')
                    msg.talker_type = CHAT_TYPE_FRIEND
                msg.sender_id = base64.b64decode(SafeGetString(reader,2))
                msg.deleted = 1
            except Exception as e:
                pass
            self.im.db_insert_table_message(msg)
        self.im.db_commit()
    def recover_msg_ftstable(self,acc_id):
        node =  self.root.GetByPath('/databases/'+ acc_id + '-IndexQQMsg.db')
        if node is None:
            return
        table = 'IndexContent_content'
        db = SQLiteParser.Database.FromNode(node,canceller)
        if db is None:
            return
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c1content',1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c4ext1',1,2)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'c5ext2',2,2)
        for rec in db.ReadTableDeletedRecords(ts, False):
            try:
                msg = Message()
                msg.account_id = acc_id
                msg.source =  node.AbsolutePath
                msg.content = base64.b64decode(rec['c1content'].Value).decode("utf-8")
                talker_id = base64.b64decode(rec['c4ext1'].Value)
                msg.type = MESSAGE_CONTENT_TYPE_TEXT
                if(talker_id.find('ZzZ1')!= -1):
                    msg.talker_id = talker_id.replace('ZzZ1','')
                    msg.talker_type = CHAT_TYPE_GROUP
                else:
                    msg.talker_id = talker_id.replace('ZzZ0','')
                    msg.talker_type = CHAT_TYPE_FRIEND
                msg.sender_id = base64.b64decode((rec['c5ext2'].Value))
                msg.deleted = 1
            except Exception as e:
                pass
            self.im.db_insert_table_message(msg)
        self.im.db_commit()
