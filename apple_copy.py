#coding:utf-8

import struct
import traceback
import re
from PA_runtime import *
from System import Convert

class CopyParser(object):
	def __init__(self, node, extractDeleted, extractSource):
		self.node = node.Parent.Parent.Parent		
		self.extract_deleted = extractDeleted
		self.extract_source = extractSource
		self.source = 'Copy'	#/Applications/com.copy.agent
		self.db=SQLiteParser.Database.FromNode(self.node.GetByPath('/Library/Application Support/copy.db'))		
		self.config_file = self.node.GetByPath('/Library/Application Support/config.ini')
		self.passwords = {}
		self.user_accounts = {}
		self.results = []


	def make_file(self,rec):
		if rec['attributes'].Value == 16:	# Directory
			f = Directory()
		elif rec['attributes'].Value == 0:	# File
			f = File()		
		f.Name = rec['name'].Value
		f.CreationTime = TimeStamp.FromUnixTime(rec['ctime'].Value)
		f.Deleted = rec.Deleted
		return f

	def create_fs(self):		
		fs = FileSystem(self.source)
		file_dictionary={}
		parent_to_children={}
		cached_files=set([])		
		ts=SQLiteParser.TableSignature("file")
		for rec in self.db.ReadTableRecords(ts,self.extract_deleted):
			if not IsDBNull(rec['OID'].Value) and (rec['OID'].Value not in file_dictionary or rec.Deleted == DeletedState.Intact):
				file_dictionary[rec['OID'].Value] = self.make_file(rec)
				if rec['parentOID'].Value in parent_to_children:
					parent_to_children[rec['parentOID'].Value].add(rec['OID'].Value)
				else:
					parent_to_children[rec['parentOID'].Value] = set([rec['OID'].Value])

		ts=SQLiteParser.TableSignature("linked_file")
		for rec in self.db.ReadTableRecords(ts,self.extract_deleted):
			if not IsDBNull(rec['fileOID'].Value):
				cached_files.add(rec['fileOID'].Value)

		ts=SQLiteParser.TableSignature("transfer")	
		for rec in self.db.ReadTableRecords(ts,self.extract_deleted):
			if not IsDBNull(rec['localPath'].Value) and not IsDBNull(rec['fileOID'].Value):	
				try:					
					path = "/Library" +rec['localPath'].Value.split('Library')[1]						
					if self.node.GetByPath(path):					
						file_data = self.node.GetByPath(path).Data
						if rec['fileOID'].Value in file_dictionary:						
							f = file_dictionary[rec['fileOID'].Value]							
							if f.Data != None:
								continue
							if file_data != None:
								f.Data = file_data
							else:					
								cached_file_list = list(self.node.GetByPath('/Library/Application Support/download/favorite_cache/').\
									Search(file_dictionary[rec['fileOID'].Value].Name))	
								for cached_file in cached_file_list:
									if cached_file.Size > 0:						
										f.Data = cached_file.Data									
					else:
						continue	
				except:
					continue						
		
		for file_id in file_dictionary:
			f = file_dictionary[file_id]
			if f.Type == Data.Files.NodeType.File:
				if f.Data is None:	
					f.MetaData.Add("File saved on device: ","False")
				else:	
					f.MetaData.Add("File saved on device: ","True")
			elif f.Type == Data.Files.NodeType.Directory:
				if f.CreationTime is None:
					f.MetaData.Add("Folder appears on device: ","False")
				else:	
					f.MetaData.Add("Folder appears on device: ","True")			
			if file_id in parent_to_children:
				for child_id in parent_to_children[file_id]:
					child = file_dictionary[child_id]
					f.Children.Add(child)	
		fs.Children.Add(file_dictionary[1])
		return fs		
		
	def parse_search_items(self):
		searched_items = []
		ts=SQLiteParser.TableSignature("search")	
		for rec in self.db.ReadTableRecords(ts,self.extract_deleted):
		    if rec.Deleted == DeletedState.Intact: 
		        searched_item = SearchedItem()
		        searched_item.Deleted = rec.Deleted
		        searched_item.Value.Init(rec['pattern'].Value, MemoryRange(rec['pattern'].Source) if self.extract_source else None)
		        searched_item.Source.Value = self.source
		        searched_items.append(searched_item)
		return searched_items

	def parse_config_file(self):
		config = ConfigParser.ConfigParser()
		config.readfp(StringIO.StringIO(self.config_file.read()))	
		config_file_source = MemoryRange(self.config_file) if self.extract_source else None

		if 'credentials.PhoneSync' in config.sections():
			auth = config.get('credentials.PhoneSync','authToken')
			push = config.get('credentials.PhoneSync','pushToken')
			user_id = config.get('credentials.PhoneSync','userId')
			first_name = config.get('credentials.PhoneSync','firstName')
			last_name = config.get('credentials.PhoneSync','lastName')
			email = config.get('credentials.PhoneSync','userEmails')
			
			email_address = EmailAddress()
			email_address.Deleted = self.config_file.Deleted
			email_address.Value.Init(email, config_file_source)
			email_address.Domain.Value = "email address"

			auth_token = Password()
			push_token = Password()		
			auth_token.Deleted = push_token.Deleted = self.config_file.Deleted
			auth_token.Service.Value = push_token.Service.Value  = self.source
			auth_token.Type.Value = push_token.Type.Value = PasswordType.Token
			auth_token.Data.Init(auth, config_file_source)
			push_token.Data.Init(push, config_file_source)

			self.passwords[user_id] = [auth_token,push_token]

			ua = UserAccount()
			ua.Deleted = self.config_file.Deleted
			ua.ServiceType.Value = self.source
			ua.Name.Init(first_name +' ' +last_name,config_file_source)
			ua.Username.Init(user_id, config_file_source)
			ua.Entries.Add(email_address)

			self.user_accounts[user_id] = ua						

		if 'PhoneSync' in config.sections():
			passcode_md5 = config.get('PhoneSync','passcode')
			passcode = Password()
			passcode.Service.Value = self.source				
			passcode.Data.Init(passcode_md5, config_file_source)
			passcode.GenericAttribute.Value = "User passcode after MD5 digest"
			self.passwords[user_id].append(passcode)	
			

	def parse(self):
		self.parse_config_file() 
		self.results.extend(self.user_accounts.values())		
		for password_list in self.passwords.values():
		    self.results.extend(password_list)    		
		self.results.extend(self.parse_search_items()) 		
		return self.results

def analyze_copy(node, extractDeleted, extractSource):
	pr = ParserResults()
	copy_parser = CopyParser(node, extractDeleted, extractSource)
	if copy_parser.db:
	    pr.FileSystems.Add(copy_parser.create_fs())
	    results=copy_parser.parse()
	    pr.Models.AddRange(results)
        pr.Build("复制/粘贴")
	return pr