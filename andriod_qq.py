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
    clr.AddReference('model_im')
    clr.AddReference('QQ_struct')
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
from QQ_struct import tencent_struct
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
def analyze_andriod_qq(root, extract_deleted, extract_source):
    try:
        pr = ParserResults()
        pr.Models.AddRange(Andriod_QQParser(root, extract_deleted, extract_source).parse())
        pr.Build('QQ')
        return pr
    except:
        pass

class Andriod_QQParser(object):
    def __init__(self, app_root_dir, extract_deleted, extract_source):
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
        m = hashlib.md5()
        m.update(self.root.AbsolutePath)
        self.cachedb =  self.cachepath  + '/' + m.hexdigest().upper() + ".db"
        self.imei = ''
        self.imeilen = 15
        self.VERSION_APP_VALUE = 10000

    def parse(self):
        #self.root = r'D:\com.tencent.mobileqq'
        if self.im.need_parse(self.cachedb, self.VERSION_APP_VALUE):
            self.getImei()
            self.decode_accounts()
            if len(self.accinfo) != 0 :
                self.im.db_create(self.cachedb)
                self.insert_account()
            else:
                return
            for acc_id in self.accounts:
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
                    self.decode_accounttables(acc_id)
                    self.decode_friends(acc_id)
                    self.decode_group_info(acc_id)
                    self.decode_groupMember_info(acc_id)
                    self.decode_friend_messages(acc_id)
                    self.decode_group_messages(acc_id)
                    self.recover_msg_from_friendtable(acc_id)
                    self.recover_msg_from_trooptable(acc_id)
                    self.decode_msg_ftstable(acc_id)
                    self.recover_msg_ftstable(acc_id)
                except Exception as e:
                    print(e)
            if canceller.IsCancellationRequested:
                return
            self.im.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
            self.im.db_insert_table_version(VERSION_KEY_APP, self.VERSION_APP_VALUE)
            self.im.db_commit()
            self.im.db_close()
        PA_runtime.save_cache_path(bcp_im.CONTACT_ACCOUNT_TYPE_IM_QQ,self.cachedb,self.bcppath)
        gen = GenerateModel(self.cachedb)
        return gen.get_models()
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
                pos = x.find('imei')
                if(pos != -1):
                    self.imei = x[-16:-1]
                    self.imeilen = 15
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
            for db in dblist:
                acc_id = db[0:db.find('.db')]
                self.accounts.append(acc_id)
            #nick
            nickfile =  self.root.GetByPath('/files/Properties')
            nickfilepath = nickfile.PathWithMountPoint
            f = open(nickfilepath,'rb')
            nickdata = f.readlines()
            f.close()
            nickdata = sorted(nickdata)
            for acc_id in self.accounts:
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
        except Exception as e:
            pass
        return
    def insert_account(self):
        nickfile =  self.root.GetByPath('/files/Properties')
        for acc in self.accinfo:
            try:
                ac = Account()
                ac.ServiceType = self.app_name
                ac.account_id = acc
                ac.source = nickfile.AbsolutePath
                ac.nickname = self.accinfo[acc][1]
            except Exception as e:
                pass
            self.im.db_insert_table_account(ac)
        self.im.db_commit()
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
        command.CommandText = 'select uin ,remark,name,datetime,age,gender from friends'
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
                friend.age = SafeGetInt64(reader,4)
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
            sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo from TroopInfo'
        else:
            sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo, troopCreatetime,wmemberNum,administrator from TroopInfoV2'
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
                g.chatroom_id = decode_text(self.imei,SafeGetString(reader,1))
                self.troops[g.chatroom_id] = g
                g.owner_id = decode_text(self.imei,SafeGetString(reader,2))
                g.name = decode_text(self.imei,SafeGetString(reader,3))
                g.notice = decode_text(self.imei,SafeGetString(reader,4))
                g.create_time = SafeGetInt64(reader,5)
                g.member_count  = SafeGetInt64(reader,6)
            except Exception as e:
                pass
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
               select troopuin, memberuin,friendnick
            ,autoremark,age,join_time,last_active_time
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
                mem.display_name = decode_text(self.imei,SafeGetString(reader,2))
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

    def processmedia(self,msg):
        try:
            sdcard = '/storage/emulated/0/'
            searchkey = ''
            nodes = list()
            if msg.content.find(sdcard) != -1 :
                searchkey = msg.content[msg.content.find(sdcard) +len(sdcard):]
                nodes = self.root.FileSystem.Search(searchkey+'$')
                if len(list(nodes)) == 0:
                    searchkey = msg.content[msg.content.rfind('/')+1:]
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
                elif msg.media_path.endswith('.png'):
                    msg.type = MESSAGE_CONTENT_TYPE_IMAGE
                else:
                    msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
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
                msg.send_time = SafeGetInt64(reader,5)
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
                    msg.is_sender = MESSAGE_TYPE_SEND
                    msg.sender_id = acc_id
                else:
                    msg.is_sender = MESSAGE_TYPE_RECEIVE
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
        address = msgstruct['meta']['Location.Search']['address'].decode("utf8","ignore")
        lat = msgstruct['meta']['Location.Search']['lat']
        lng = msgstruct['meta']['Location.Search']['lng']
        locat = msg.create_location()        
        locat.latitude = float(lat)
        locat.longitude = float(lng)
        locat.address = address       
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
                msg.send_time = SafeGetInt64(reader,4)
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
                    msg.is_sender = MESSAGE_TYPE_SEND
                    msg.sender_id = acc_id
                else:
                    msg.is_sender = MESSAGE_TYPE_RECEIVE
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
                    msg.send_time = rec['time'].Value
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
                        msg.is_sender = MESSAGE_TYPE_SEND
                        msg.sender_id = acc_id
                    else:
                        msg.is_sender = MESSAGE_TYPE_RECEIVE
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
                    msg.send_time = rec['time'].Value
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
                        msg.is_sender = MESSAGE_TYPE_SEND
                        msg.sender_id = acc_id
                    else:
                        msg.is_sender = MESSAGE_TYPE_RECEIVE
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
                        linkurl  = l['b']
                        if linkurl == '':
                            continue                                         
                        link = msg.create_link()
                        link.url = linkurl
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
                pass
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
                msg.content = str(content)
                if(self.processmedia(msg) == False):
                    msg.content = thumb
                    self.processmedia(msg)
            elif(msgtype == -2006):
                msg.content	= msgstruct.decode("utf8","ignore")
                pass
            elif(msgtype == -2022):
                #mp4
                filename = msgstruct[3][1]
                msg.content = str(filename)
                self.processmedia(msg)
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
                    msg.content = str(msgdata[1:msgdata.find("|")])
                    self.processmedia(msg)
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
                        msg.content = str(msgdata[pos:pos+ lens])
                        self.processmedia(msg)
            else:
                msg.content = msgdata.decode('utf-8',"ignore")
        except Exception as e:
            pass
        msg.insert_db(im)        
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
