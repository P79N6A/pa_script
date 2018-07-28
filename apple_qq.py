#coding=utf-8
import PA_runtime
import clr
import json
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

class QqParser(object):
	def __init__(self, app_root_dir, extract_deleted, extract_source):
		self.root = app_root_dir
		self.extract_source = extract_source
		self.extract_deleted = extract_deleted
		self.app_name = 'QQ'

		self.models = []

		self.accounts = []
		self.contacts = {} # uin to contact
		self.parties = {} #{contact id : contact name}
		self.att_content_types = {
			1 : 'image',
			3 : 'sound',
		}
	
	def parse(self):
		self.decode_accounts()
		for acc_id in self.accounts:
			self.decode_contacts_and_init_parties(acc_id)
			self.decode_all_chats(acc_id)
			self.decode_db_contacts(acc_id)
			self.decode_db_calls(acc_id)
		return self.models

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
				other_party.Role.Init(PartyRole.From, MemoryRange(rec['type'].Source) if self.extract_source else None)
				main_party.Role.Init(PartyRole.To, MemoryRange(rec['type'].Source) if self.extract_source else None)
				if rec['type'].Value == 1:
					c.Type.Init(CallType.Incoming, MemoryRange(rec['type'].Source) if self.extract_source else None)
				elif rec['type'].Value == 2:
					c.Type.Init(CallType.Missed, MemoryRange(rec['type'].Source) if self.extract_source else None)
			if 'type' in rec and rec['type'].Value == 0:
				other_party.Role.Init(PartyRole.To, MemoryRange(rec['type'].Source) if self.extract_source else None)
				main_party.Role.Init(PartyRole.From, MemoryRange(rec['type'].Source) if self.extract_source else None)
				c.Type.Init(CallType.Outgoing, MemoryRange(rec['type'].Source) if self.extract_source else None)
			if main_party.HasContent:
				c.Parties.Add(main_party)
			if other_party.HasContent:
				c.Parties.Add(other_party)
			
			SQLiteParser.Tools.ReadColumnToField[str](rec, "uin", other_party.Identifier, self.extract_source, lambda x: str(x))
			SQLiteParser.Tools.ReadColumnToField(rec, "nickname", other_party.Name, self.extract_source)
			SQLiteParser.Tools.ReadColumnToField[TimeSpan](rec, "duration", c.Duration, self.extract_source, lambda x:TimeSpan.FromSeconds(x))
			SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, "time", c.TimeStamp, self.extract_source, lambda x: TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan1970(x), True))
			if c.TimeStamp.Value and not c.TimeStamp.Value.IsValidForSmartphone():
				c.TimeStamp.Init(None, None)
			if c.Duration.Value == 0 and c.Type.Value == c.Type.Incoming:
				c.Type.Value = CallType.Missed
			if 'msgtype' in rec and rec['msgtype'].Value == 2:
				c.VideoCall.Init(True, MemoryRange(rec['msgtype'].Source) if self.extract_source else Non)
			if c.HasContent:
				self.models.append(c)
			
	# account decoding
	def decode_accounts(self):
		node = self.root.GetByPath('/Documents/contents/QQAccountsManager')
		if node is None:
			for account_id, deleted in [(n.Parent.Name, n.Parent.Deleted) for n in self.root.Search('/QQ\.db$') if n.Parent and n.Parent.Name.isdigit()]:
				ua = UserAccount()
				ua.Deleted = deleted
				ua.ServiceType.Value = self.app_name
				ua.Username.Value = account_id
				self.models.append(ua)
				self.accounts.append(account_id)
				self.parties[account_id] = account_id
			return
		bp = BPReader(node.Data).top
		if bp is None:
			return

		for acc_ind in bp['$objects'][1]['NS.objects']:
			if acc_ind is None:
				break
			self.models.append(self.decode_account(bp['$objects'], acc_ind.Value))

	def decode_account(self, bp, dict_ind):
		values = self.get_dict_from_bplist(bp, dict_ind)

		ua = UserAccount()
		ua.Deleted = DeletedState.Intact
		ua.ServiceType.Value = self.app_name
		ua.Username.Value = values['_loginAccount'].Value 
		ua.Name.Value = values['_nick'].Value

		self.parties[values['_loginAccount'].Value] = self.parties[values['_uin'].Value] = values['_nick'].Value

		# decode password? how do you save the password?

		self.accounts.append(values['_uin'].Value)

		return ua

	def decode_contacts_v3(self, acc_id):
		"""
		In new versions of QQ, the old QQFriendList.plist file is replaced by QQFriendList_V3.plist.
		However, weirdly, this file doesn't contain any relevant info. Instead, we get the contacts from another file
		(QQContacts.data). This new file doesn't contain the contacts' UIN, so we can't add them as parties to the
		chats.

		"""
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/Contacts/QQContacts.data')
		if node is None:
			return
		bp = BPReader(node.Data).top
		if bp is None:
			return
		for contact_ind in bp['$objects'][1]['NS.objects']:
			if contact_ind is None:
				break
			contact_dict = {}
			for k in bp['$objects'][contact_ind.Value].Keys:
				contact_dict[k] = bp['$objects'][bp['$objects'][contact_ind.Value][k].Value]
			c = Contact()
			c.Deleted = DeletedState.Intact
			c.Source.Value = self.app_name + ": " + acc_id
			self.init_model_field_from_bp_field(contact_dict['Contact_name'], c.Name, self.extract_source)
			if ('Contact_nickName' in contact_dict and contact_dict['Contact_nickName'].Value != ""):
				nickname = values['Contact_nickName']
				if self.extract_source:
					c.Notes.Add('Nickname: ' + nickname.Value, MemoryRange(nickname.Source))
				else:
					c.Notes.Add('Nickname: ' + nickname.Value)

			if 'Contact_phoneCode' in contact_dict and contact_dict['Contact_phoneCode'].Value != "":
				pn = PhoneNumber(contact_dict['Contact_phoneCode'].Value)
				if self.extract_source:
				    pn.Value.Source = MemoryRange(contact_dict['Contact_phoneCode'].Source)
				pn.Deleted = c.Deleted
				c.Entries.Add(pn)
			self.models.append(c)

	# contacts decoding
	def decode_contacts_and_init_parties(self, acc_id):
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQFriendList.plist')
		if node is None:
			# This file contains much less info, so only parse it if the more detailed file doesn't exist
			self.decode_contacts_v3(acc_id)
			return
		bp = BPReader(node.Data).top
		if bp == None:
			return
		for group_ind in bp['$objects'][1]['NS.objects']:
			if group_ind is None:
				break
			self.decode_group_contacts(bp['$objects'], group_ind.Value, acc_id)

	def decode_group_contacts(self, bp, dict_ind, acc_id):
		values = self.get_dict_from_bplist(bp, dict_ind)
		group_name = values['_groupName']

		for con_ind in values['_friendList']['NS.objects']:
			if con_ind is None:
				break
			self.decode_contact_from_bplist(bp, con_ind.Value, group_name, acc_id)
		
	def decode_contact_from_bplist(self, bp, dict_ind, group_name, acc_id):
		values = self.get_dict_from_bplist(bp, dict_ind)
		con_id = values['_fuin']
		if '_realNickName' in values:
			con_name = values['_realNickName']
			self.parties[con_id.Value] = con_name.Value
		else:
			con_name = None

		# init contact
		c = Contact()
		c.Deleted = DeletedState.Intact
		c.Source.Value = self.app_name + ': ' + acc_id
		self.init_model_field_from_bp_field(group_name, c.Group, self.extract_source)
		self.init_model_field_from_bp_field(con_name, c.Name, self.extract_source)
		if (('_isMatchRealNick' in values and values['_isMatchRealNick'].Value != 0) or 
			(con_name is None and '_nick' in values)):
			nickname = values['_nick']
			if self.extract_source:
				c.Notes.Add('Nickname: ' + nickname.Value, MemoryRange(nickname.Source))
			else:
				c.Notes.Add('Nickname: ' + nickname.Value)
		uid = UserID()
		uid.Deleted = DeletedState.Intact
		self.init_model_field_from_bp_field(values['_fuin'], uid.Value, self.extract_source)
		c.Entries.Add(uid)
		self.models.append(c)

	def decode_db_contacts(self, acc_id):
		node = self.root.GetByPath('/Documents/contents/' + acc_id + '/QQ.db')
		if node is None:
			return
		db = SQLiteParser.Database.FromNode(node)
		if db is None or 'tb_userSummary' not in db.Tables:
			return
		ts = SQLiteParser.TableSignature('tb_userSummary')
		SQLiteParser.Tools.AddSignatureToTable(ts, 'uin', 5)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'isShowlog', 1,8,9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'isNative', 1,8,9)
		for rec in db.ReadTableRecords(ts, self.extract_deleted):
			c = Contact()
			c.Source.Value = self.app_name
			uid =UserID()
			remark = UserID()
			phone = PhoneNumber()
			sa = StreetAddress()
			c.Deleted = uid.Deleted = sa.Deleted = phone.Deleted = remark.Deleted= rec.Deleted
			SQLiteParser.Tools.ReadColumnToField(rec, "nick", c.Name, self.extract_source)
			if not  c.Name.Value:
				SQLiteParser.Tools.ReadColumnToField(rec, "contactName", c.Name, self.extract_source)
			SQLiteParser.Tools.ReadColumnToField[str](rec, "uin", uid.Value, self.extract_source, lambda x: str(x))
			SQLiteParser.Tools.ReadColumnToField(rec, "remark", remark.Value, self.extract_source)
			SQLiteParser.Tools.ReadColumnToField(rec, "mobileNum", phone.Value, self.extract_source)
			SQLiteParser.Tools.ReadColumnToField(rec, "country", sa.Country, self.extract_source)
			SQLiteParser.Tools.ReadColumnToField(rec, "city", sa.City, self.extract_source)
			SQLiteParser.Tools.ReadColumnToMultiField[str](rec, "latestPicUrl1", c.Notes, self.extract_source, lambda x: "pic url: {0}".format(x))
			SQLiteParser.Tools.ReadColumnToMultiField[str](rec, "latestPicUrl2", c.Notes, self.extract_source, lambda x: "pic url: {0}".format(x))
			SQLiteParser.Tools.ReadColumnToMultiField[str](rec, "latestPicUrl2", c.Notes, self.extract_source, lambda x: "pic url: {0}".format(x))
			if uid.Value.Value:
				uid.Category.Value = "uin"
				c.Entries.Add(uid)
			if remark.Value.Value:
				remark.Category.Value = "remark"
				c.Entries.Add(remark)
			if phone.Value.Value:
				phone.Category.Value = "phone"
				c.Entries.Add(phone)
			if sa.HasContent:
				phone.Category.Value = "Street address"
				c.Addresses.Add(sa)
			if c.HasContent:
				self.models.append(c)
				if uid.HasLogicalContent and uid.Category.Value not in self.contacts:
					self.contacts[uid.Category.Value] = c

	# chats decoding
	def decode_all_chats(self, acc_id):
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

		for table in tables:
			self.decode_chat_table(acc_id, db, table)

	def decode_chat_table(self, acc_id, db, table_name):
		chat_source = self.app_name + ': ' + acc_id
		chats = {} # {contact_id : Chat()}
		chat_id = table_name[table_name.rfind('_')+1:]

		ts = SQLiteParser.TableSignature(table_name)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'type', 1,8,9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'flag', 8,9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'time', 4)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'read', 8,9)
		SQLiteParser.Tools.AddSignatureToTable(ts, 'content', SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)

		for rec in db.ReadTableRecords(ts, self.extract_deleted, True):
			contact_id = rec['uin'].Value
			if chat_id != 'message' and contact_id != chat_id:
				continue
			if contact_id not in chats:
				chats[contact_id] = self.init_chat_object(acc_id, contact_id, rec)
			message = self.decode_im_from_rec(rec, acc_id, contact_id)
			if message is not None:
				chats[contact_id].Messages.Add(message)

		for chat in chats.values():
			self.complete_chat_fields(chat)
		self.models += chats.values()

	def init_chat_object(self, acc_id, contact_id, rec):
		c = Chat()
		c.Deleted = rec.Deleted
		c.Source.Value = self.app_name + ': ' + acc_id
		SQLiteParser.Tools.ReadColumnToField(rec, 'uin', c.ChatId, self.extract_source)

		con_p = Party()
		con_p.Deleted = DeletedState.Intact
		SQLiteParser.Tools.ReadColumnToField(rec, 'uin', con_p.Identifier, self.extract_source)
		if contact_id in self.parties:
			con_p.Name.Value = self.parties[contact_id]

		acc_p = Party()
		acc_p.Deleted = DeletedState.Intact
		acc_p.Identifier.Value = acc_id
		acc_p.Name.Value = self.parties[acc_id]

		c.Participants.AddRange([con_p, acc_p])

		return c

	def decode_im_from_rec(self, rec, acc_id, contact_id):
		im = InstantMessage()
		deleted_state = rec.Deleted
		#ticket 1184447
		if rec['type'].Value == 332:
			deleted_state = DeletedState.Unknown
		im.Deleted = deleted_state
		im.SourceApplication.Value = self.app_name + ': ' + acc_id
		SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, 'time', im.TimeStamp, self.extract_source, lambda x: TimeStamp.FromUnixTime(x))
		rec_content_field = Field[str]('')
		SQLiteParser.Tools.ReadColumnToField(rec, 'content', rec_content_field, self.extract_source)
		if rec['type'].Value not in [1, 141, 7141]:
			im.Body.Init(rec_content_field)

		if rec_content_field.HasLogicalContent:
			loc = self.get_location(rec_content_field, deleted_state, acc_id)
			if loc and loc.HasLogicalContent:
				if im.TimeStamp.HasLogicalContent:
					loc.TimeStamp.Init(im.TimeStamp)
				self.models.append(loc)
				loc.LinkModels(im)
				if loc.Position.HasLogicalContent:
					im.Position.Value = loc.Position.Value
			shared_contact =  self.get_shared_conatct(rec_content_field, deleted_state, acc_id)
			if shared_contact and shared_contact.HasLogicalContent:
				im.SharedContacts.Add(shared_contact)
			shared_links = self.get_shared_links(rec_content_field, deleted_state, acc_id)
			if shared_links is not None:
				for att in shared_links:
					im.Attachments.Add(att)

		if rec['type'].Value == 3:
			if not IsDBNull(rec['content'].Value):
				audio, audio_node = self.get_audio_attachment(acc_id, rec['content'].Value)
				if audio is not None:
					audio.Deleted = deleted_state
					CreateSourceEvent(audio_node, im)
					im.Attachments.Add(audio)

		elif rec['type'].Value != 0:
			if not IsDBNull(rec['content'].Value):
				att, att_node = self.get_attachment(acc_id, rec['content'].Value, rec['type'].Value)
			else:
				att = None
			if att is not None:
				att.Deleted = deleted_state
				CreateSourceEvent(att_node, im)
				im.Attachments.Add(att)
			for pic, pic_node in self.get_picture_attachment(acc_id, rec):
				if pic is not None:
					pic.Deleted = deleted_state
					CreateSourceEvent(pic_node, im)
					im.Attachments.Add(pic)

		from_p = Party()
		from_p.Deleted = DeletedState.Intact
		from_p.Role.Value = PartyRole.From
		to_p = Party()
		to_p.Deleted = DeletedState.Intact
		to_p.Role.Value = PartyRole.To

		if rec['flag'].Value == 0: #outgoing
			from_p.Identifier.Value = acc_id
			if acc_id in self.parties:
				from_p.Name.Value = self.parties[acc_id]

			to_p.Identifier.Value = contact_id
			if contact_id in self.parties:
				to_p.Name.Value = self.parties[contact_id]
		
		elif rec['flag'].Value == 1: #incoming
			to_p.Identifier.Value = acc_id
			if acc_id in self.parties:
				to_p.Name.Value = self.parties[acc_id]
		
			from_p.Identifier.Value = contact_id
			if contact_id in self.parties:
				from_p.Name.Value = self.parties[contact_id]

		im.From.Value = from_p
		im.To.Add(to_p)
		return im

	def _get_xml_msg_data(self, body, del_state, acc_id, main_key):
		if not body.Value or body.Value[0] != '<':
			return
		should_dispose = False
		if body.Source:
			mr = body.Source
		else:
			data = Encoding.UTF8.GetBytes(body.Value)
			ms = MemoryStream(data)            
			mr = MemoryRange(Chunk(ms,0,data.Length))
			should_dispose = True
		try:
			doc = XSDocument.Load(mr)
			if not doc or not doc.RootElements.Length:
				return
			actionData = None
			for root in doc.RootElements:
				if root.Name.Value != "msg":
					continue
				actionData = root.GetAttribute(main_key)
				if not actionData:
					continue
				succeded, uri = System.Uri.TryCreate(actionData.Value, System.UriKind.Absolute)
				if succeded:
					parts = System.Web.HttpUtility.ParseQueryString(uri.Query)
					keys = parts.AllKeys
					return root, keys, parts, mr, actionData
			return doc, [], {}, mr, actionData
		except:
			pass
		finally:
			if should_dispose:
				mr.Dispose()

	def get_shared_links(self, body, deleted, acc_id):
		try:
			data = self._get_xml_msg_data(body, deleted, acc_id, "url")
		except:
			return None
		
		if data is None:
			return None

		root, keys, parts, mr, actionData = data

		if type(root) is XSDocument:
			for rootElem in root.RootElements:
				if rootElem.Name.Value == "msg":
					root = rootElem
		try:
		    items = list(XMLParserHelper.XMLParserTools.GetByXPath(root, 'msg/item'))
		except:
			return None

		if items is None:
			return None
		
		atts = []
		for item in items:
			title = None
			summary = None
			url = None

			title_elems = list(XMLParserHelper.XMLParserTools.GetByXPath(item, 'item/title'))
			if title_elems is not None and len(title_elems) > 0:
				title = title_elems[0]

			summary_elems = list(XMLParserHelper.XMLParserTools.GetByXPath(item, 'item/summary'))
			if summary_elems is not None and len(summary_elems) > 0:
				summary = summary_elems[0]

			audio_elems = list(XMLParserHelper.XMLParserTools.GetByXPath(item, 'item/audio'))
			if audio_elems is not None and len(audio_elems) > 0:
				url = audio_elems[0].GetAttribute('src')
			else:
				audio_elems = list(XMLParserHelper.XMLParserTools.GetByXPath(item, 'item/picture'))
				if audio_elems is not None and len(audio_elems) > 0:
					url = audio_elems[0].GetAttribute('cover')

			if title is None and summary is None and url is None:
				continue

			att = Attachment()
			att.Deleted = deleted
			if title is not None or summary is not None:
				att.Title.Init('{0} - {1}'.format(title.Value if title is not None else '', summary.Value if summary is not None else ''),
                                MemoryRange(list(title.Source) if title is not None else [] + list(summary.Source)if title is not None else []) if self.extract_source else None)
			if url is not None:
				att.URL.Init(url.Value, MemoryRange(url.Source) if self.extract_source else None)
			atts.append(att)

		return atts


	def get_location(self, body, del_state, acc_id):
		try:
			data = self._get_xml_msg_data(body, del_state, acc_id, "actionData")
			if not data:
				return
			root, keys, parts, mr, actionData = data
			if  'loc' not in keys:
				return
			coor = Coordinate()
			coor.Deleted = del_state
			loc = Location(coor)
			loc.Category.Value = self.app_name + ': ' + acc_id
			loc.Deleted = del_state
			if "lat" in keys:
				coor.Latitude.Value = float(parts["lat"])
				if self.extract_source:
					coor.Latitude.Source = MemoryRange(actionData.Source)
			if "lon" in keys:
				coor.Longitude.Value = float(parts["lon"])
				if self.extract_source:
					coor.Longitude.Source = MemoryRange(actionData.Source)
			if "title" in keys:
				loc.Name.Value = parts["title"]
				if self.extract_source:
					loc.Name.Source = MemoryRange(actionData.Source)
			if "loc" in keys:
				loc.PositionAddress.Value = parts["loc"]
				coor.PositionAddress.Value = parts["loc"]
				if self.extract_source:
					loc.PositionAddress.Source = MemoryRange(actionData.Source)
					coor.PositionAddress.Source = MemoryRange(actionData.Source)
			brief = root.GetAttribute("brief")
			if brief:
				loc.Description.Value = brief.Value
				if self.extract_source:
					loc.Description.Source = MemoryRange(brief.Source)
			if loc.HasLogicalContent:
				return loc
		except Exception, e:
			pass

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


	def get_audio_attachment(self, acc_id, im_content):
		folder = self.root.GetByPath('/Documents/{0}/Audio'.format(acc_id))
		if folder is None:
			return None, None
		audio_node = folder.GetByPath(im_content[:im_content.rfind('+')] + '.amr')
		if audio_node is not None:
			audio = Attachment()
			audio.Filename.Value = audio_node.Name
			audio.Data.Source = audio_node.Data
			return audio, audio_node
		return None, None

	def get_picture_attachment(self, acc_id, rec):
		if 'picUrl' not in rec or IsDBNull(rec['picUrl'].Value):
			return
			url =''
		try:
			url = rec['picUrl'].Value
			print(url)
			pic_json = json.loads(url)
		except:
			return
		if pic_json is None or len(pic_json) == 0 or not isinstance(pic_json,list):
			return
		if 'md5'  in pic_json[0]:
			md5 = pic_json[0]['md5']
		elif 'videoMD5'  in pic_json[0]:
			md5 = pic_json[0]['videoMD5']
		else:
			return
		acc_folder = self.root.GetByPath('/Documents/{0}'.format(acc_id))
		for pic_node in acc_folder.Search(md5):
			pic = Attachment()
			pic.Filename.Value = pic_node.Name
			pic.Data.Source = pic_node.Data
			yield pic, pic_node
		return

	def get_attachment(self, acc_id, im_content, att_type):
		att_guid = re.findall('[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}', im_content)
		if len(att_guid) == 0:
			return None, None
		nodes = list(self.root.GetByPath('/Documents/' + acc_id).Search(att_guid[0], True))
		if len(nodes) == 0:
			return None, None
		att_node = nodes[0]

		att = Attachment()
		att.Filename.Value = att_node.Name
		if att_type in self.att_content_types:
			att.ContentType.Value = self.att_content_types[att_type]
		att.Data.Source = att_node.Data
		return att, att_node

	def complete_chat_fields(self, chat):
		if chat.Messages.Count == 0:
			return
		tss = []
		map(lambda x: tss.append(x.TimeStamp.Value), chat.Messages)
		tss.sort()
		chat.StartTime.Value = tss[0]
		chat.LastActivity.Value = tss[-1]

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