# coding=utf-8
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

def SafeGetString(reader,i):
	if not reader.IsDBNull(i):
		return reader.GetString(i)
	else:
		return ""

def SafeGetInt64(reader,i):
	if not reader.IsDBNull(i):
		return reader.GetInt64(i)
	else:
		return 0

def SafeGetBlob(reader,i):
	if not reader.IsDBNull(i):
		obj = reader.GetValue(i)
		return obj #byte[]
	else:
		return None

def SafeGetValue(reader,i):
	if not reader.IsDBNull(i):
		obj = reader.GetValue(i)
		return obj 
	else:
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
		c = bytearray(text[i].encode('utf8'))
		c[len(c)-1] = (c[len(c)-1]) ^ ord(imei_bytes[i % len(imei_bytes)])
		ret += c.decode('utf-8',"ignore")
	return ret
def readVarInt(data):
		i = 0
		j = 0
		l = 0
		k = 0
		while True:
			assert(i < 64)
			try:              
				j, = struct.unpack('B',data[k])
				k = k+1
			except:
				raise
			l = l | (j & 0x7f) << i
			if((j & 0x80) == 0):
				return l
			i = i + 7
def analyze_andriod_qq(root, extract_deleted, extract_source):	
	pr = ParserResults()
	pr.Models.AddRange(Andriod_QQParser(root, extract_deleted, extract_source).parse())
	pr.Build('QQ')
	return pr

class Andriod_QQParser(object):
	def __init__(self, app_root_dir, extract_deleted, extract_source):
		self.root = app_root_dir
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
		self.friendmsgtables =set()
		self.troopmsgtables =set()
		self.friendhash = collections.defaultdict(str)            
		self.accinfo = collections.defaultdict(list)
		self.troops = collections.defaultdict(Chatroom)
		self.im = IM()
		self.cachepath = ds.OpenCachePath("QQ")
		self.cachedb =  self.cachepath  + "/QQ.db"
		self.im.db_create(self.cachedb)
		self.imei = ''
		self.imeilen = 15
		self.VERSION_APP_VALUE = 10000
		
	def parse(self):  
		#self.root = r'D:\com.tencent.mobileqq'
	
		self.getImei()
		self.decode_accounts()
		try:
			for acc_id in self.accounts:		
				self.friendsNickname.clear()
				self.friendsGroups.clear()
				self.groupContact.clear()
				self.troops.clear()
				self.nickname = ''
				self.contacts = {}
				self.friendhash.clear()
				self.friendmsgtables =set()
				self.troopmsgtables =set()
				
				self.decode_friends(acc_id)
		
				self.decode_group_info(acc_id)
				self.decode_groupMember_info(acc_id)
				self.decode_friend_messages(acc_id)            
				self.decode_group_messages(acc_id)	                                     
			'''
				self.decode_recover_friends(acc_id)
				self.decode_recover_group_info(acc_id)
				self.decode_recover_groupMember_info(acc_id)
				self.decode_recover_friend_messages(acc_id)
				self.decode_recover_group_messages(acc_i)
			'''
		except Exception as e:
			print e
		self.im.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
		self.im.db_insert_table_version(VERSION_KEY_APP, self.VERSION_APP_VALUE)
		self.im.db_commit()
		self.im.db_close()			
		gen = GenerateModel(self.cachedb)        
		return gen.get_models()
	
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
		except: 
			#log("imie cant get decode will be failded")           
			pass                
	def decode_accounts(self):
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
					self.accinfo[acc_id].append(line[pos+len(name):len(line)-1])
				postime = line.find(t)
				if(postime != -1):
					time = line[postime+len(t):len(line)-1]
					time = time[:len(time) -3]
					self.accinfo[acc_id].append(time)
		for acc in self.accinfo:
			ac = Account()
			ac.ServiceType = self.app_name
			#account.deleted = DeletedState.Intact
			ac.nickname = self.accinfo[acc][1]
			ac.account_id = acc
			ac.source = nickfile.AbsolutePath           
			self.nickname = ac.nickname
			self.im.db_insert_table_account(ac)
		self.im.db_commit()
		return
	def decode_friends(self,acc_id):
		
		node = self.root.GetByPath('/databases/'+ acc_id + '.db')		
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = connect(d)
		db = SQLiteParser.Database.FromNode(node)
		if db is None:
			return		
		sql = 'select uin ,remark,name,datetime,age,gender from friends'
		cursor = conn.execute(sql)
		for row in cursor:        
			try:
				friend = Friend()
				friend.account_id = acc_id
				friend.friend_id = decode_text(self.imei,row[0])
				friend.remark = decode_text(self.imei,row[1])
				friend.nickname = decode_text(self.imei,row[2])
				friend.source  = node.AbsolutePath
				friend.age = row[4]                
				m = hashlib.md5()
				m.update(friend.friend_id)
				self.friendhash[friend.friend_id] = m.hexdigest().upper()
				self.im.db_insert_table_friend(friend)
			except:
				pass
		self.im.db_commit()
	def decode_group_info(self,acc_id):
		node = self.root.GetByPath('/databases/'+ acc_id + '.db')		
		if node is None:
			return
		d = node.PathWithMountPoint
		db = SQLiteParser.Database.FromNode(node)
		conn = connect(d)		
		sql = ""
		if 'TroopInfo' in db.Tables:
			sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo, oldtroopname from TroopInfo'
		else:
			sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo, troopCreatetime,wmemberNum,administrator,dwcmduinjointime,oldtroopname from TroopInfoV2'            
		cursor = conn.execute(sql)        
		for row in cursor:         
			try:
				g = Chatroom()
				g.account_id = acc_id
				g.chatroom_id = decode_text(self.imei,row[1])            
				g.owner_id = decode_text(self.imei,row[2])
				g.name = decode_text(self.imei,row[3])
				g.notice = decode_text(self.imei,row[4])
				g.create_time = row[5]/1000
				g.member_count  = row[6]
				g.source = node.AbsolutePath
				self.troops[g.chatroom_id] = g
				self.im.db_insert_table_chatroom(g)
			except:
				pass
		self.im.db_commit()
	
	def decode_groupMember_info(self,acc_id):
		node = self.root.GetByPath('/databases/'+ acc_id + '.db')		
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = connect(d)
		sql ='''
			   select troopuin, memberuin,friendnick
			,autoremark,age,join_time,last_active_time
			from TroopMemberInfo order by troopuin
			'''         
		cursor = conn.execute(sql)  
		for row in cursor:
			try:
				mem = ChatroomMember()
				mem.account_id = acc_id
				mem.chatroom_id = decode_text(self.imei,row[0])
				mem.member_id = decode_text(self.imei,row[1])
				mem.display_name = decode_text(self.imei,row[2])
				mem.signature = decode_text(self.imei,row[3])
				mem.age = row[4]
				mem.joinTime = row[5]/1000                
				self.im.db_insert_table_chatroom_member(mem)
			except:
				pass
		self.im.db_commit()
	

	def decode_friend_messages(self,acc_id):
		node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = sqlite3.connect(d)
		sql = 'select tbl_name from sqlite_master where type ="table" and tbl_name like "mr_friend/_%" escape "/"'
		cursor = conn.execute(sql)
		for row in cursor:
			self.friendmsgtables.add(row[0])
		for table in self.friendmsgtables:
			self.decode_msg_from_friendtbale(acc_id ,table)
		
	def processmedia(self,msg):
		sdcard = '/storage/emulated/0/'
		searchkey = ''
		nodes = None
		print msg.content
		if msg.content.find(sdcard) != -1 :
			searchkey = msg.content[msg.content.find(sdcard) +len(sdcard):]						 					
			nodes = self.root.FileSystem.Search(searchkey+'$')
		if(nodes is not  None):
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
		return False
	def decode_msg_from_friendtbale(self,acc_id,table): 
		#f table != 'mr_friend_373B750958FA49CDF32A08407A21CEDC_New':
			#return 
		return
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
		while reader.Read():
			try:
				i = 0 
				msg = Message()
				msg.msg_id = SafeGetInt64(reader,i)
				msg.account_id = acc_id             
				#i =i +1           
				#extstr = decode_text(self.imei, SafeGetString(reader,i))
				i =i +2           
				frienduin = decode_text(self.imei, SafeGetString(reader,i)) 
				i =i +1           
				msgdata = decode_blob(self.imei, SafeGetBlob(reader,i))
				i =i +1           
				senderuin = decode_text(self.imei, SafeGetString(reader,i))
				i =i +1           
				msg.send_time = SafeGetInt64(reader,i)
				i =i +1           
				msgtype = SafeGetInt64(reader,i)          
				msg.talker_type = CHAT_TYPE_FRIEND			
				if msg.talker_id is None:
					m.update(frienduin)
					digest = m.hexdigest().upper()
					if digest == hash:
						msg.talker_id =  frienduin
					else:
						for friend in self.friendhash:
							if(hash == self.friendhash[friend]):
								msg.talker_id = friend
								break
				if msg.talker_id is None:
					msg.talker_id = hash
				if senderuin == acc_id:
					msg.is_sender = MESSAGE_TYPE_SEND
					msg.sender_id = acc_id
				else:
					msg.is_sender = MESSAGE_TYPE_RECEIVE
					msg.sender_id = msg.talker_id			
				msg.type = MESSAGE_CONTENT_TYPE_TEXT
				msgcontent = ''
				qqmsgstruct = tencent_struct()									
				msgstruct = qqmsgstruct.getQQMessage(msgtype,bytes(msgdata))					
				if(msgtype == -2018 or msgtype == -2050):
					msg.content = json.dumps(msgstruct[50])					
					pass
				elif(msgtype == -2011 or msgtype == -2054 or msgtype == -2059):
					msg.content = msgstruct['mMsgBrief'].decode('utf-8')							
					msgItems = msgstruct['mStructMsgItemLists']					
					for l in msgItems:
						try:
							print l['b']
							link  = l['b']
							if link == '':
								continue
							msg.content = str(link)
							msg.type = MESSAGE_CONTENT_TYPE_LINK
							self.im.db_insert_table_message(msg)	
						except:
							pass			
				elif(msgtype == -5003):
					pass
				elif(msgtype == -1000):
					msg.content =msgstruct.decode("utf8","ignore")  
					
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
					except:
						pass
					try:
						sig ='\xac\xed\x00\05'
						type = 0x74
						s = msgdata[0:4]
						d = msgdata[4]
						if(sig == s and type == d):
							lens, = struct.unpack('>H',msgdata[5:7])
							msgstruct =json.loads(str(msgdata[7:7+lens]))
							if msgstruct['app'] == 'com.tencent.map':																					
								self.decode_tencentmap(msg,msgstruct)
							else:    						
								msg.content  = str(msgstruct['meta']).encode('utf-8',"ignore")								
					except:
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
					print msg.content						
					self.im.db_insert_table_message(msg)			
			except Exception as e:		
				print (e)
		command.Dispose()		
		conn.Close()
		self.im.db_commit()
		
	def decode_group_messages(self,acc_id):
		node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = sqlite3.connect(d)
		sql = 'select tbl_name from sqlite_master where type ="table" and tbl_name like "mr_troop/_%" escape "/"'
		cursor = conn.execute(sql)
		for row in cursor:
			self.troopmsgtables.add(row[0])
		for table in self.troopmsgtables:
			self.decode_msg_from_trooptbale(acc_id ,table) 
					
	def decode_tencentmap(self,msg,msgstruct):
		address = msgstruct['meta']['Location.Search']['address'].decode("utf8","ignore")                     
		lat = msgstruct['meta']['Location.Search']['lat']
		lng = msgstruct['meta']['Location.Search']['lng']                    
		msg.extra_id  =  str(uuid.uuid1())
		msg.type = MESSAGE_CONTENT_TYPE_LOCATION
		locat = Location()
		locat.location_id = msg.extra_id 
		locat.latitude = lat
		locat.longitude = lng
		locat.address = address	
		self.im.db_insert_table_location(locat)			
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
		command.CommandText = 'select msgseq,extstr,frienduin,msgdata,senderuin,time,msgtype from ' + table + ' order by time'
		reader = command.ExecuteReader()	
		hash = table[9:41]   
		m = hashlib.md5()     		
		while reader.Read():
			try:
				i = 0 
				msg = Message()
				msg.msg_id = SafeGetInt64(reader,i)
				msg.account_id = acc_id             
				#i =i +1 
				#strextstr = SafeGetBlob(reader,i)
				#extstr = decode_text(self.imei, bytes(strextstr))
				i =i +2          
				frienduin = decode_text(self.imei, SafeGetString(reader,i)) 
				i =i +1           
				msgdata = decode_blob(self.imei, SafeGetBlob(reader,i))
				i =i +1           
				senderuin = decode_text(self.imei, SafeGetString(reader,i))
				i =i +1           
				msg.send_time = SafeGetInt64(reader,i)
				i =i +1           
				msgtype = SafeGetInt64(reader,i)          
				msg.talker_type = CHAT_TYPE_GROUP			
				if msg.talker_id is None:
					m.update(frienduin)
					digest = m.hexdigest().upper()
					if digest == hash:
						msg.talker_id =  frienduin
					else:
						for troopuin in self.troops:
							m.update(troopuin)
							digest = m.hexdigest().upper()
							if(hash == digest):
								msg.talker_id = troopuin
								break
				if msg.talker_id is None:
					msg.talker_id = hash
				if senderuin == acc_id:
					msg.is_sender = MESSAGE_TYPE_SEND
					msg.sender_id = acc_id
				else:
					msg.is_sender = MESSAGE_TYPE_RECEIVE
					msg.sender_id = senderuin	
				msg.type = MESSAGE_CONTENT_TYPE_TEXT
				msgcontent = ''
				qqmsgstruct = tencent_struct()								
				msgstruct = qqmsgstruct.getQQMessage(msgtype,bytes(msgdata))		
				if(msgtype == -2018 or msgtype == -2050):
					msg.content = json.dumps(msgstruct[50])					
					pass
				elif(msgtype == -2011 or msgtype == -2054 or msgtype == -2059):
					msg.content = msgstruct['mMsgBrief'].decode('utf-8',"ignore")							
					msgItems = msgstruct['mStructMsgItemLists']					
					for l in msgItems:
						try:
							print l['b']
							link  = l['b']
							if link == '':
								continue
							msg.content = str(link)
							msg.type = MESSAGE_CONTENT_TYPE_LINK
							self.im.db_insert_table_message(msg)	
						except:
							pass			
				elif(msgtype == -5003):
					pass
				elif(msgtype == -1000):
					msg.content =msgstruct.decode("utf8","ignore")  
					
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
					except:
						pass
					try:			
						if(msgdata[0:4] == '\xac\xed\x00\05' and msgdata[4] == d):
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
				print msg.content
				self.im.db_insert_table_message(msg)				
			except Exception as e:		
				print e				
		command.Dispose()		
		conn.Close()		
		self.im.db_commit()		