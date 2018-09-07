# coding=utf-8
import PA_runtime
import clr
import json
from sqlite3 import *
import hashlib
clr.AddReference('System.Web')
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr
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
#from System.Collections.Generic import *
from collections import defaultdict
import logging
from  model_im import *
import uuid 
#just msgdata
def decode_blob(imei_bytes, buffers):
	ret = bytearray()      
	for i in range(len(buffers)):
		#a = struct.unpack('<B',buffers[i])
		#b = struct.unpack('<B',imei_bytes[i % len(imei_bytes)])
		#c = a[0] ^ b[0]
		c = chr(ord(buffers[i])^ord(imei_bytes[i % len(imei_bytes)]))
		ret.append(c)
	try:     
		return ret.decode('utf-8','ignore')
	except:
		return None        
def decode_text(imei_bytes, text):
	if text is None:
		return ''
	ret = ''
	for i in range(len(text)):
		c = bytearray(text[i].encode('utf8'))
		c[len(c)-1] = (c[len(c)-1]) ^ ord(imei_bytes[i % len(imei_bytes)])
		ret += c.decode()
	return ret
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
		return
	def parse(self):  
			#self.root = r'D:\com.tencent.mobileqq'
			self.getImei()
			self.decode_accounts()
			for acc_id in self.accounts:
				self.friendsNickname.clear()
				self.friendsGroups.clear()
				self.groupContact.clear()
				self.troops.clear()
				self.nickname = ''
				self.contacts = {}			
				self.troopmsgtables =[]
				self.c2cmsgtables =set()
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
				self.decode_recover_group_messages(acc_i
			self.im.db_insert_table_version(VERSION_KEY_DB, VERSION_VALUE_DB)
			self.im.db_insert_table_version(VERSION_KEY_APP, self.VERSION_APP_VALUE)
			self.im.db_commit()
			self.im.db_close()
			
		gen = GenerateModel(self.cachedb,self.root.FileSystem.MountPoint)        
		return gen.get_models()
	'''
	def getImei(self):
		path = self.root + '\\files\\imei'
		try:
			f = open(path)               
			l = f.readlines()
			for x in l:
				pos = x.find('imei')
				if(pos != -1):
					self.imei = x[-16:-1]
					self.imeilen = 15
		except:            
			pass                
	def decode_accounts(self):
		dblist = []
		pattern = r"^([0-9]+).db$" 
		path =  self.root + '/databases/'
		for root, dirs, files in os.walk(path):  
			for f in files:
				if(re.match(pattern,f)):
					dblist.append(f)
		#account
		for db in dblist:
			acc_id = db[0:db.find('.db')]
			self.accounts.append(acc_id)   
		#nick
		nickfile = self.root + '/files/Properties'
		f = open(nickfile,'rb')
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
			ac.source = self.app_name
			ac.ServiceType = self.app_name
			#account.deleted = DeletedState.Intact
			ac.nickname = acc[acc][1]
			ac.account_id = acc
			ac.source = nickfile            
			self.nickname = ac.nickname
			self.im.db_insert_table_account(ac)
			self.im.db_commit()
		return
	def decode_friends(self,acc_id):
		
		dbpath = self.root + '/databases/'+ acc_id + '.db'
		node = self.root.GetByPath(dbpath)
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = connect(d)
		db = SQLiteParser.Database.FromNode(node)
		if db is None:
			return		
		sql = 'select uin ,remark,name,datatime,age,gender from friends'
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
		dbpath = self.root + '/databases/'+ acc_id + '.db'
		node = self.root.GetByPath(dbpath)
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = connect(d)
		db = SQLiteParser.Database.FromNode(node)
		if db is None:
			return	
		sql = ""
		if 'TroopInfo' in db.Tables:
			sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo, oldtroopname from TroopInfo'
		else:
			sql = 'select troopuin,troopcode,troopowneruin,troopname,fingertroopmemo, troopCreattime,wmemberNum,administrator,dwcmduinjointime,oldtroopname from TroopInfoV2'            
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
				self.im.db_insert_table_chatroom(g)
			except:
				pass
		self.im.db_commit()
	
	def decode_groupMember_info(self,acc_id):
		dbpath = self.root + '/databases/'+ acc_id + '.db'
		node = self.root.GetByPath(dbpath)
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = connect(d)
		db = SQLiteParser.Database.FromNode(node)
		if db is None:
			return	
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
		
	def decode_msg_from_friendtbale(self,acc_id,table):         
		node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = sqlite3.connect(d)
		hash = table[10:42]   
		m = hashlib.md5()     
		sql = 'select msgseq,extstr,frienduin,msgdata,senderuin,time,msgtype from ' + table + ' order by _id'
		cursor = conn.execute(sql)
		for row in cursor:
			msg = Message()
			msg.msg_id = row[0]
			msg.account_id = acc_id                        
			extstr = decode_text(self.imei,row[1])
			frienduin = decode_text(self.imei,row[2]) 
			msgdata = decode_blob(self.imei,row[3])
			senderuin = decode_text(self.imei,row[4])
			msg.send_time = row[5]
			msgtype = row[6]          
			#defalut talker
			msg.talker_id = hash
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
	
			if senderuin == acc_id:
				msg.is_sender = 1
			else:
				msg.is_sender = 0
			content = collections.defaultdict(str)
			msg.type = MESSAGE_CONTENT_TYPE_TEXT
			msgcontent = ''
			try:
				if msgtype == -1000:
					data = str(msgdata)	
					msgcontent = data
				if msgtype == -1043:
					data = str(msgdata)	
					msgcontent = data			
				elif msgtype == -2009 or msgtype == -2005:
					tp = (msgdata[0])
					pos = msgdata.find('\x7c')
					data = msgdata[1:pos]
					content[tp] = data	
					msgcontent = content[22]     
				elif msgtype == -2002 or msgtype == -2005:
					tp =  (msgdata[0])
					length = msgdata[1]
					content[tp] = msgdata[2:2+length]
					msgcontent = content[10]
					if msgcontent.endswith('.mp3') :
						msg.type = MESSAGE_CONTENT_TYPE_VOICE                    
					elif msgcontent.endswith('.amr'):
						msg.type = MESSAGE_CONTENT_TYPE_VOICE     
					elif msgcontent.endswith('.slk') :
						msg.type = MESSAGE_CONTENT_TYPE_VOICE                    
					elif msgcontent.endswith('.mp4'):
						msg.type = MESSAGE_CONTENT_TYPE_VIDEO
					else:
						msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
					msg.media_path = msgcontent
				elif msgtype == -2022:
					offset = 0
					try:
						while offset < len(msgdata):
							tp = (msgdata[offset])
							offset = offset +1
							length = msgdata[offset]
							offset = offset + 1
							#skip 0x1
							if msgdata[offset] == 1:
								offset =offset + 1
							data = msgdata[offset:offset+length]
							offset = offset+length
							content[tp] = data		
							#print content
					except:
						pass
					msgcontent = content[26]
					msg.type = MESSAGE_CONTENT_TYPE_VIDEO
					msg.media_path = msgcontent
					
				elif msgtype == -2000:
					offset = 0
					try:
						while offset < len(msgdata):
							tp = (msgdata[offset])
							offset = offset +1
							length = msgdata[offset]
							offset = offset + 1
							#skip 0x1
							if msgdata[offset] == 1:
								offset =offset + 1
							data = msgdata[offset:offset+length]
							offset = offset+length
							content[tp] = data	
					except:
						pass                                                
					msgcontent = content[10]					
					msg.type = MESSAGE_CONTENT_TYPE_IMAGE
					msg.media_path = msgcontent
					
				elif msgtype == -5008:
					sig = msgdata[0:4]
					offset = 4
					tp = msgdata[offset]
					offset = offset+1
					#2 bytes  size
					length = struct.unpack('>H',msgdata[offset:offset+2])
					offset = offset +2
					data = str(msgdata[offset:offset+ length[0]])
					j = json.loads(data)   
					if j['app'] == 'com.tencent.map':                 	
						#name = j['meta']['Location.Search']['name']
						address = j['meta']['Location.Search']['address']                    
						lat = j['meta']['Location.Search']['lat']
						lng = j['meta']['Location.Search']['lng']                    
						msg.extra_id  = uuid.uuid1()
						msg.type = MESSAGE_CONTENT_TYPE_LOCATION
						locat = Location()
						locat.location_id = msg.extra_id 
						locat.latitude = lat
						locat.longitude = lng
						locat.address = address
					self.im.db_insert_table_location(locat)                                                     
				else:
					msgcontent = str(msgdata)
				self.im.db_insert_table_message(msg)
			except:
				pass
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
		
	def decode_msg_from_trooptbale(self,acc_id ,table):
		node =  self.root.GetByPath('/databases/'+ acc_id + '.db')
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = sqlite3.connect(d)
		hash = table[10:42]   
		m = hashlib.md5()     
		sql = 'select msgseq,extstr,frienduin,msgdata,senderuin,time,msgtype from ' + table + ' order by _id'
		cursor = conn.execute(sql)
		for row in cursor:
			msg = Message()
			msg.msg_id = row[0]
			msg.account_id = acc_id                        
			extstr = decode_text(self.imei,row[1])
			frienduin = decode_text(self.imei,row[2]) 
			msgdata = decode_blob(self.imei,row[3])
			senderuin = decode_text(self.imei,row[4])
			msg.send_time = row[5]
			msgtype = row[6]          
			#defalut talker
			msg.talker_id = hash
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
			#sender
		 
			if senderuin == acc_id:
				msg.is_sender = 1
			else:
				msg.is_sender = 0
			content = collections.defaultdict(str)
			msg.type = MESSAGE_CONTENT_TYPE_TEXT
			msgcontent = ''
			try:
				if msgtype == -1000:
					data = str(msgdata)	
					msgcontent = data
				if msgtype == -1043:
					data = str(msgdata)	
					msgcontent = data			
				elif msgtype == -2009 or msgtype == -2005:
					tp = (msgdata[0])
					pos = msgdata.find('\x7c')
					data = msgdata[1:pos]
					content[tp] = data	
					msgcontent = content[22]     
				elif msgtype == -2002 or msgtype == -2005:
					tp =  (msgdata[0])
					length = msgdata[1]
					content[tp] = msgdata[2:2+length]
					msgcontent = content[10]
					if msgcontent.endswith('.mp3') :
						msg.type = MESSAGE_CONTENT_TYPE_VOICE                    
					elif msgcontent.endswith('.amr'):
						msg.type = MESSAGE_CONTENT_TYPE_VOICE     
					elif msgcontent.endswith('.slk') :
						msg.type = MESSAGE_CONTENT_TYPE_VOICE                    
					elif msgcontent.endswith('.mp4'):
						msg.type = MESSAGE_CONTENT_TYPE_VIDEO
					else:
						msg.type = MESSAGE_CONTENT_TYPE_ATTACHMENT
					msg.media_path = msgcontent
				elif msgtype == -2022:
					offset = 0
					while offset < len(msgdata):
						tp = (msgdata[offset])
						offset = offset +1
						length = msgdata[offset]
						offset = offset + 1
						#skip 0x1
						if msgdata[offset] == 1:
							offset =offset + 1
						data = msgdata[offset:offset+length]
						offset = offset+length
						content[tp] = data		
						#print content
					msgcontent = content[26]
					msg.type = MESSAGE_CONTENT_TYPE_VIDEO
					msg.media_path = msgcontent
				elif msgtype == -2000:
					offset = 0
					while offset < len(msgdata):
						tp = (msgdata[offset])
						offset = offset +1
						length = msgdata[offset]
						offset = offset + 1
						#skip 0x1
						if msgdata[offset] == 1:
							offset =offset + 1
						data = msgdata[offset:offset+length]
						offset = offset+length
						content[tp] = data	                                                
					msgcontent = content[10]
					msg.type = MESSAGE_CONTENT_TYPE_IMAGE
					msg.media_path = msgcontent
				elif msgtype == -5008:
					sig = msgdata[0:4]
					offset = 4
					tp = msgdata[offset]
					offset = offset+1
					#2 bytes  size
					length = struct.unpack('>H',msgdata[offset:offset+2])
					offset = offset +2
					data = str(msgdata[offset:offset+ length[0]])
					j = json.loads(data)                    	
					name = j['meta']['Location.Search']['name']
					address = j['meta']['Location.Search']['address']                    
					lat = j['meta']['Location.Search']['lat']
					lng = j['meta']['Location.Search']['lng']                    
					msg.extra_id  = uuid.uuid1()
					msg.type = MESSAGE_CONTENT_TYPE_LOCATION
					locat = Location()
					locat.location_id = msg.extra_id 
					locat.latitude = lat
					locat.longitude = lng
					locat.address = address
					self.im.db_insert_table_location(locat)                                                     
				else:
					msgcontent = str(msgdata)
				self.im.db_insert_table_message(msg)
			except:
				pass
		self.im.db_commit()
			
			
				

				
			




		
				
			
			
			



			
			


			 
		
  

