# coding=utf-8
import PA_runtime
import clr
import json
from sqlite3 import *

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
from PA.InfraLib.Models.Common import *
from QQFriendNickName import *
from PA.InfraLib.Utils import PList
from PA.InfraLib.Extensions import PlistHelper
#from System.Collections.Generic import *
from collections import defaultdict
import logging
from  model_im import *
class QqParser(object):
	def __init__(self, app_root_dir, extract_deleted, extract_source):
		self.root = app_root_dir
		self.extract_source = extract_source
		self.extract_deleted = extract_deleted
		self.app_name = 'QQ'
		self.friendsGroups = collections.defaultdict()
		self.friendsNickname = collections.defaultdict()
		self.groupContact = collections.defaultdict()
		self.models = []
		self.accounts = []
		self.contacts = {}  # uin to contact
		self.troops = collections.defaultdict()
		self.im = IM()
		self.cachepath = ds.OpenCachePath("QQ")
		self.cachedb =  self.cachepath  + "/QQ.db"
		self.im.db_create(self.cachedb)
	def parse(self):
		self.decode_accounts()
		for acc_id in self.accounts:
			self.decode_friends(acc_id)
			self.decode_group_info(acc_id)
			self.decode_groupMember_info(acc_id)
			self.decode_friend_messages(acc_id)
			self.decode_group_messages(acc_id)			
			self.decode_db_calls(acc_id)		

	def decode_db_calls(self, acc_id):
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ_Mix.db')
		if node is None:
			return
		db = SQLiteParser.Database.FromNode(node)
		if db is None or 'tb_callRecord' not in db.Tables:
			return
		ts = SQLiteParser.TableSignature('tb_callRecord')
		SQLiteParser.Tools.AddSignatureToTable(ts, 'uin', 5)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'duration', 1, 2, 3, 8, 9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'netType', 1, 2, 3, 8, 9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'accType', 1, 2, 3, 8, 9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'msgtype', 1, 2, 3, 8, 9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'recordType', 1, 2, 3, 8, 9)
		for rec in db.ReadTableRecords(ts, self.extract_deleted):
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
			self.decode_account(bp['$objects'], acc_ind.Value)
	def decode_account(self, bp, dict_ind):
		values = self.get_dict_from_bplist(bp, dict_ind)
		ac = Account()
		ac.source = self.app_name
		ac.ServiceType = self.app_name
		#account.deleted = DeletedState.Intact
		ac.nickname = values['_loginAccount'].Value
		ac.account_id = values['_uin'].Value
		ac.nickname = values['_nick'].Value
		ac.city = values['_sCountry'].Value +values['_sProvince'].Value+ values['_sCity'].Value
		ac.sex =  values['_sex'].Value
		self.accounts.append(ac.account_id)
		self.im.db_insert_table_account(ac)
		self.im.db_commit()
	def decode_friendlist_v3(self,acc_id):
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQFriendList_v3.plist')
		if node is not None:
			#friendPlistPath =  self.cachepath  + "/QQFriendList_v3.plist"
			#node.SaveToFile(friendPlistPath)
			friendPlistPath = node.PathWithMountPoint
			#this get friend nickname
			self.friendsGroups,self.friendsNickname = getFriendNickName(friendPlistPath)
			for k in self.friendsNickname:
				friend = Friend()
				friend.account_id = acc_id
				friend.friend_id = k
				friend.nickname =self.friendsNickname[k][0]
				friend.remark = self.friendsNickname[k][1]
				self.im.db_insert_table_friend(friend)
		self.im.db_commit()
	def decode_friends(self, acc_id):
		self.decode_friendlist_v3(acc_id)
	def decode_group_info(self,acc_id):
		plist =  None
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQTroopMemo')
		if node is None:
			return
		if node is  not None:
				plist = PlistHelper.ReadPlist(node)
				groups =  plist
				for data in groups:
					try:
						g = Chatroom()
						v = data.Value
						g.chatroom_id = data.Key
						g.account_id = acc_id
						g.name = v["name"]
						g.notice = v["memo"]
						self.troops[g.chatroom_id] = g
					except:
						pass
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
		if node is None:
			return
		d = node.PathWithMountPoint
		conn = connect(d)
		db = SQLiteParser.Database.FromNode(node)
		if db is None:
			return		
		sql = ''
		if 'tb_troop' in db.Tables:
			sql = 'select groupCode, groupName , groupMemNum  from tb_troop'
		else:
			sql = 'select groupCode, groupName , groupMemNum  from tb_troop_new'
		cursor = conn.execute(sql)
		for row in cursor:
			groupCode = str(row[0])
			groupName = str(row[1])
			groupMemNum = row[2]
			group = self.troops[groupCode]
			group.member_count = groupMemNum
			group.name = groupName
			self.im.db_insert_table_chatroom(group)
		self.im.db_commit()

	def decode_groupMember_info(self, acc_id):
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ_Group_CFG.db')
		chatroommmebers = collections.defaultdict(ChatroomMember)
		if node is not None:
			d = node.PathWithMountPoint
			conn = sqlite3.connect(d)
			sql = 'select strNick,groupCode,MemberUin,strRemark,PhoneNumber  from tb_troopRemarkNew'
			cursor = conn.execute(sql)
			for row in cursor:
				chatroommem = ChatroomMember()
				strNick = row[0]
				groupCode = str(row[1])
				MemberUin = str(row[2])
				strRemark = row[3]
				PhoneNumber = row[4]
				chatroommem.account_id = acc_id
				chatroommem.chatroom_id = groupCode
				chatroommem.member_id = MemberUin
				chatroommem.display_name = strNick
				chatroommem.telephone  = PhoneNumber
				chatroommem.signature = strRemark
				chatroommmebers[(groupCode,MemberUin)] =  chatroommem
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
		if node is not None:
			d = node.PathWithMountPoint
			conn = sqlite3.connect(d)
			sql = 'select nick,GroupCode,MemUin,Age,JoinTime,LastSpeakTime,gender from tb_TroopMem'
			cursor = conn.execute(sql)
			for row in cursor:
					nick = row[0]
					groupCode = str(row[1])
					MemberUin = str(row[2])
					Age = row[3]
					JoinTime = row[4]
					LastSpeakTime =  row[5]
					gender = row[6]
					chatmem = chatroommmebers[(groupCode, MemberUin)]
					chatmem.account_id = acc_id
					chatmem.chatroom_id = groupCode
					chatmem.member_id = MemberUin
					if(chatmem.display_name == ''):
						chatmem.display_name = nick
					chatmem.age  = Age
					if(gender == 0):
						chatmem.gender = GENDER_MALE
					elif(gender == 1):
						chatmem.gender = GENDER_FEMALE
					else:
						chatmem.gender = GENDER_OTHER
					chatmem.jiontime = JoinTime
					chatmem.lastspeektime = LastSpeakTime
		for k in chatroommmebers:
			self.im.db_insert_table_chatroom_member(chatroommmebers[k])
		self.im.db_commit()

	def decode_friend_messages(self, acc_id):
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
		if node is None:
			return
		db = SQLiteParser.Database.FromNode(node)
		if db is None:
			return
		tables = set()
		if 'tb_message' in db.Tables:
			tables.add('tb_message')
		if 'tb_c2cTables' in db.Tables:
			ts = SQLiteParser.TableSignature('tb_c2cTables')
			for rec in db.ReadTableRecords(ts, True):
				if not IsDBNull(rec['uin'].Value) and rec['uin'].Value.startswith('tb_c2cMsg_') and rec['uin'].Value in db.Tables:
					tables.add(rec['uin'].Value)
		#c2c
		for table in tables:
			self.decode_friend_chat_table(acc_id,table)
	def decode_group_messages(self, acc_id):
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
		if node is None:
			return
		d = node.PathWithMountPoint
		tables = []
		conn = sqlite3.connect(d)
		sql = 'select tbl_name from sqlite_master where type ="table" and tbl_name like "tb_TroopMsg/_%" escape "/"'
		cursor = conn.execute(sql)
		for row in cursor:
			tables.append(row[0])
		for table in tables:
			self.decode_group_chat_table(acc_id,table)	
	def decode_group_chat_table(self, acc_id, table_name):		
		group_id = table_name[table_name.rfind('_')+1:]
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
		if node is not None:
			d = node.PathWithMountPoint
			conn = sqlite3.connect(d)
			sql = 'select senduin ,msgtime, sMsgtype,read,strmsg,msgid,nickname,picurl from  ' + table_name + ' order by msgtime'
			cursor = conn.execute(sql)
			for row in cursor:
				try:
					uin = str(row[0])
					sendtime = row[1]
					msgtype = row[2]
					bread = row[3]
					content = row[4]
					msgid = str(row[5])				
					picUrl = row[7]
					msg = Message()
					msg.account_id = acc_id
					msg.talker_id = group_id
					msg.sender_id = uin
					msg.talker_type = USER_TYPE_CHATROOM
					if(bread == 0):
						msg.status = MESSAGE_STATUS_UNREAD
					else:
						msg.status = MESSAGE_STATUS_READ
					msg.source =  self.app_name + acc_id + " QQ.db " + table_name
					msg.deleted = 0
					msg.repeated = 0
					msg.msg_id = msgid
					msg.send_time = sendtime									
					msg.is_sender = int(msg.sender_id == msg.account_id)
					msg.content = content	
					types  = (MESSAGE_CONTENT_TYPE_TEXT,MESSAGE_CONTENT_TYPE_IMAGE,MESSAGE_CONTENT_TYPE_VOICE,
					MESSAGE_CONTENT_TYPE_ATTACHMENT,MESSAGE_CONTENT_TYPE_VIDEO,MESSAGE_CONTENT_TYPE_LOCATION)
					if(msgtype == 0):
						msg.type = MESSAGE_CONTENT_TYPE_TEXT										
					elif(msgtype == 1):
						msg.type = MESSAGE_CONTENT_TYPE_IMAGE
						msg.media_path = self.get_picture_attachment(acc_id,picUrl)	
						if(msg.media_path is not None):
							print msg.media_path
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
							msg.location_name = loc[0]
							msg.location_lat = loc[1]
							msg.location_lng = loc[2]
					if(msg.type not in types):	
						msg.type = MESSAGE_CONTENT_TYPE_SYSTEM
					self.im.db_insert_table_message(msg)
				except:
					pass
			self.im.db_commit()			
		return	
	def decode_friend_chat_table(self, acc_id, table_name):		
		chat_id = table_name[table_name.rfind('_')+1:]
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
		if node is not None:
			d = node.PathWithMountPoint
			conn = sqlite3.connect(d)
			sql = 'select uin,time,type,read,content,msgId,flag,picUrl from ' + table_name + ' order by time'
			cursor = conn.execute(sql)
			for row in cursor:
				try:
					uin = str(row[0])
					sendtime = row[1]
					msgtype = row[2]
					bread = row[3]
					content = row[4]
					msgid = str(row[5])
					flag = row[6]
					picUrl = row[7]
					msg = Message()
					msg.account_id = acc_id
					msg.talker_id = uin
					msg.talker_type = USER_TYPE_FRIEND
					if(bread == 0):
						msg.status = MESSAGE_STATUS_UNREAD
					else:
						msg.status = MESSAGE_STATUS_READ
					msg.source =  self.app_name + acc_id + " QQ.db " + table_name
					msg.deleted = 0
					msg.repeated = 0
					msg.msg_id = msgid
					msg.send_time = sendtime
					if(flag):
						msg.is_sender = 1
						msg.sender_id =  msg.account_id
					else:
						msg.is_sender = 0	
						msg.sender_id =  uin			
					msg.content = content	
					types  = (MESSAGE_CONTENT_TYPE_TEXT,MESSAGE_CONTENT_TYPE_IMAGE,MESSAGE_CONTENT_TYPE_VOICE,
					MESSAGE_CONTENT_TYPE_ATTACHMENT,MESSAGE_CONTENT_TYPE_VIDEO,MESSAGE_CONTENT_TYPE_LOCATION)
					if(msgtype == 0):
							msg.type = MESSAGE_CONTENT_TYPE_TEXT										
					elif(msgtype == 1):
							msg.type = MESSAGE_CONTENT_TYPE_IMAGE
							msg.media_path = self.get_picture_attachment(acc_id,picUrl)											
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
								msg.location_name = loc[0]
								msg.location_lat = loc[1]
								msg.location_lng = loc[2]
					if(msg.type not in types):	
						msg.type = MESSAGE_CONTENT_TYPE_SYSTEM
					self.im.db_insert_table_message(msg)
				except:
					pass
			self.im.db_commit()
			
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
			if('com.tencent.map'in x):
				address	= x['meta']['Location.Search']['address']
				lat	= x['meta']['Location.Search']['lat']
				lng	= x['meta']['Location.Search']['lng']			
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
		except Exception, e:
			pass
	def get_audio_attachment(self, acc_id, js):
		try:			
			pic_json = json.loads(js)
		except:
			return ''
		fileName= ''
		if 'fileName'  in pic_json[0]:
			fileName = pic_json[0]['fileName']
		elif 'fileId'  in pic_json[0]:
			fileName = pic_json[0]['fileId']
		else:
			return ''
		acc_folder = self.root.GetByPath('/Documents/{0}/Audio/{1}.arm'.format(acc_id,fileName))
		if(acc_folder is None):
			return ''
		return acc_folder.AbsolutePath

	def get_picture_attachment(self, acc_id, js):	
		try:			
			pic_json = json.loads(js)
		except:
			return ''
		if 'md5'  in pic_json[0]:
			md5 = pic_json[0]['md5']
		elif 'videoMD5'  in pic_json[0]:
			md5 = pic_json[0]['videoMD5']
		else:
			return ''
		acc_folder = self.root.GetByPath('/Documents/{0}/image_original/{1}.png'.format(acc_id,md5))
		if(acc_folder is None):
			return None
		return acc_folder.AbsolutePath

	def get_file_attachment(self, acc_id, content):
		try:			
			filename = content[0:content.find('||||||||')]
		except:
			return ''
		acc_folder = self.root.GetByPath('/Documents/{0}/FileRecv/{1}'.format(acc_id,filename))
		if(acc_folder is None):
			return ''
		return acc_folder.AbsolutePath
		
	def get_dict_from_bplist(self, bp, dict_ind):
		values = {}
		for key in bp[dict_ind].Keys:
			val_ind = bp[dict_ind][key].Value
			values[key] = bp[val_ind]
		return values

	def init_model_field_from_bp_field(self, src, dst, extract_source):
		if src is None:
			return
		if extract_source:
			dst.Init(src.Value, MemoryRange(src.Source))
		else:
			dst.Value = src.Value
def analyze_qq(root, extract_deleted, extract_source):
	"""
	QQ 应用分析
	user accounts, contacts, chats , attachments.
	"""
	pr = ParserResults()
	pr.Models.AddRange(QqParser(root, extract_deleted, extract_source).parse())
	return pr
