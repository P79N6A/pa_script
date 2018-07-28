#coding=utf-8
import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *

def analyze_wechat(root, extract_deleted, extract_source):
    """
    微信 (/DB/MM.sqlite)
    解析 Account, Contacts, Chats (Attachments)
    """
    pr = ParserResults()
    pr.Categories = DescripCategories.Wechat #声明这是微信应用解析的数据集
    models = WeChatParser(root, extract_deleted, extract_source).parse()
    mlm = ModelListMerger()
    
    pr.Models.AddRange(list(mlm.GetUnique(models)))
    return pr

class WeChatParser:
    
    def __init__(self, node, extract_deleted, extract_source):
        self.APP_NAME = "微信"
        self.root = node.Parent.Parent
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.user_account = UserAccount()
        self.chat_participants = defaultdict(set)
        self.multi_chatrooms_ids = set()
        self.calls = set()
        self.contacts = {}
        self.chats = {}
        self.root_files = defaultdict(list)
        self.all_files = defaultdict(list)
        for node in self.root.GetAllNodes(NodeType.File):
                self.root_files[node.Name].append(node)
        for node in self.root.Parent.Parent.GetAllNodes(NodeType.File):
                self.all_files[node.Name].append(node)
        self.unknown_chat_counter = 0

    def parse(self):
        models = []
        self.covert_silk_and_amr()
        user_plists = self.root_files["mmsetting.archive"]
        for user_plist in user_plists:
            if user_plist.Deleted == DeletedState.Intact:
                self.parse_user(user_plist)
        if self.user_account.HasContent and self.user_account.Username.HasContent:
            self.APP_NAME += ": " + self.user_account.Username.Value

        models.append( self.user_account )
        self.parse_session_files()
        session_dbs = list(self.root.Parent.SearchNodesExactPath("session/session.db"))
        for session_db in session_dbs:
            if session_db.Deleted == DeletedState.Intact:
                self.parse_chat_praticipants(session_db)
                break
        
        db_nodes = self.root_files["MM.sqlite"]
        for node in db_nodes:    
            self.parse_contacts_from_db_node(node)
            models += self.decode_chat(node)
        db_nodes = self.root_files["wc005_008.db"]
        for node in db_nodes:   
            self.parse_messages_from_db_node(node)
        if self.calls:
            models.extend(self.calls)
        if self.contacts:
            models.extend(self.contacts.values())
        if self.chats:
            models.extend(self.chats.values())
        return models

    def parse_messages_from_db_node(self, node):
        db = SQLiteParser.Database.FromNode(node)
        if not db:
            return
        tables = [t for t in db.Tables if t.startswith('MyWC01_')]
        ids = set()
        for table in tables:
            ts = SQLiteParser.TableSignature(table)
            SQLiteParser.Tools.AddSignatureToTable(ts, "MALocalId", SQLiteParser.FieldType.Int)
            SQLiteParser.Tools.AddSignatureToTable(ts, "GroupHint", 1,8,9)
            SQLiteParser.Tools.AddSignatureToTable(ts, "Id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "FromUser", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "Buffer", SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            chat_id = table[7:]
            
            if not chat_id in self.chats:
                chat = Chat()
                chat.Deleted = node.Deleted
                is_new_chat = True
            else:
                chat = self.chats[chat_id]
                is_new_chat = False
            chat_messages = []
            for rec in db.ReadTableRecords(ts, self.extract_deleted, False):
                if 'Id' in rec and rec['Id'].Value in ids:
                    continue
                elif 'Id' in rec:
                    ids.add(rec['Id'].Value)
                if not rec["Buffer"].IsDBNull:
                    root_mr = MemoryRange(rec["Buffer"].Source)
                    if root_mr.Length < 8 or root_mr.read(8) != "bplist00":
                        continue
                    root_mr.seek(0)
                    try:
                        root = BPReader.GetTree(root_mr)
                    except:
                        continue
                    if not root or not root.Children:
                        continue
                    message = InstantMessage()
                    message.Deleted = rec.Deleted
                    KNodeTools.TryReadToField(root, "contentDesc", message.Body, self.extract_source)
                    party = Party()
                    party.Deleted = root.Deleted
                    KNodeTools.TryReadToField(root, "username", party.Identifier, self.extract_source)
                    KNodeTools.TryReadToField(root, "nickname", party.Name, self.extract_source)
                    if party.HasLogicalContent:
                        message.From.Value = party
                    if 'createtime' in root.Children and root.Children["createtime"].Type in [KType.Integer, KType.Long, KType.FloatingPoint]:
                        message.TimeStamp.Init(TimeStamp.FromUnixTime(root.Children["createtime"].Value, False), MemoryRange(root.Children["createtime"].Source))
                        if not message.TimeStamp.Value.IsValidForSmartphone():
                            message.TimeStamp.Value = None
                            message.TimeStamp.Source = None
                    if 'locationInfo' in root.Children:
                        location = self.parse_location(root.Children['locationInfo'])
                        if location:
                            message.Position.Value = location
                    KNodeTools.TryReadToField(root, "title", message.Subject, self.extract_source)
                    if 'contentObj' in root.Children:
                        atts = self.parse_attachment(root.Children['contentObj'])
                        if atts:
                            message.Attachments.AddRange(atts)
                    if message.HasLogicalContent:
                        chat_messages.append(message)
            if chat_messages:
                mlm = ModelListMerger()
                chat.Messages.AddRange(mlm.GetUnique(chat_messages))
            if chat.HasLogicalContent:
                chat.SetTimesByMessages()
                chat.SetParticipantsByMessages()
                if is_new_chat:
                    self.chats[chat_id] = chat                    
                    chat.Source.Value = self.APP_NAME
                    chat.ChatId.Value = chat_id

    def parse_location(self, locationBPNode):
        coord = Coordinate()
        if not locationBPNode.Children:
            return 
        coord.Deleted = locationBPNode.Deleted
        KNodeTools.TryReadToField(locationBPNode, "location_latitude", coord.Latitude, self.extract_source)
        KNodeTools.TryReadToField(locationBPNode, "location_longitude", coord.Longitude, self.extract_source)
        KNodeTools.TryReadToField(locationBPNode, "poiAddress", coord.PositionAddress, self.extract_source)
        if not coord.PositionAddress.HasLogicalContent:
            KNodeTools.TryReadToField(locationBPNode, "city", coord.PositionAddress, self.extract_source)
        else:
                coord.PositionAddress.Value += '\n' + locationBPNode.Children['city'].Value
                chunks = System.Collections.Generic.List[Utils.Streams.Chunk](coord.PositionAddress.Source.Chunks)
                chunks.AddRange(locationBPNode.Children['city'].Source)
                coord.PositionAddress.Source = MemoryRange(chunks)
        KNodeTools.TryReadToField(locationBPNode, "poiName", coord.Comment, self.extract_source)
        if 'poiInfoUrl' in locationBPNode.Children and locationBPNode.Children['poiInfoUrl'].Value and  locationBPNode.Children['poiInfoUrl'].Type == KType.String:
            if coord.PositionAddress.HasLogicalContent:
                coord.Comment.Value += '\n' + locationBPNode.Children['poiInfoUrl'].Value
                chunks = System.Collections.Generic.List[Utils.Streams.Chunk](coord.PositionAddress.Source.Chunks)
                chunks.AddRange(locationBPNode.Children['poiInfoUrl'].Source)
                coord.Comment.Source  = MemoryRange(chunks)
            else:
                coord.Comment.Init(locationBPNode.Children['poiInfoUrl'].Value, MemoryRange(locationBPNode.Children['city'].Source)) 
        if coord.HasLogicalContent:
            return coord

    def parse_attachment(self, contentBPNode):
        attachments = []
        if 'mediaList' in contentBPNode.Children and contentBPNode.Children['mediaList'].Values:
            for item in contentBPNode.Children['mediaList'].Values:
                att = Attachment()
                att.Deleted = item.Deleted
                KNodeTools.TryReadToField(item, "dataUrl/url", att.URL, self.extract_source)
                if att.HasLogicalContent:
                    attachments.append(att)
        return attachments

    def _is_silk(self, node):
        if not node or not node.Data or node.Data.Length < 0xa:
            return False
        node.Data.seek(0)
        header = node.read(0x9)
        if header == '#!SILK_V3':
            return True
        return False

    def covert_silk_and_amr(self):
        for audio_file in self.root.Search("/Audio/.*\.aud$"):
            if not audio_file or not audio_file.Data or audio_file.Data.Length < 0xa: continue
            audio_file.Data.seek(0)
            header = audio_file.read(0xa)
            #silk
            if header == '\x02#!SILK_V3':
                child = Node(NodeType.File|NodeType.Embedded)
                child.Deleted = audio_file.Deleted
                fs = ParserHelperTools.CreateIsolatedFileStream(self.APP_NAME)
                with audio_file.Data.GetSubRange(1, audio_file.Data.Length - 1) as temp_stream:
                    temp_stream.CopyTo(fs)
                child.Data = MemoryRange(Chunk(fs, 0, fs.Length))
                child.Labels = Labels.Silk
                child.Tags.Add(MediaTags.Audio)
                child.Name = audio_file.Name + ".silk"
                child.MetaData.Add(MetaDataField("Channels", "1"))
                child.MetaData.Add(MetaDataField("Rate", "24000"))
                child.MetaData.Add(MetaDataField("Bit", "16"))
                audio_file.Children.Add(child)
            #AMR
            elif header[0] == '\x0c':
                child = Node(NodeType.File|NodeType.Embedded)
                child.Deleted = audio_file.Deleted
                chunks = []
                file_header = Chunk(ThreadSafeMemoryStream(to_byte_array( '#!AMR\n'), 0, 6),0,6)
                child.Tags.Add(MediaTags.Audio)
                child.Name = audio_file.Name + ".amr"
                chunks.append(file_header)
                chunks.extend(audio_file.Data.GetSubRange(0, audio_file.Data.Length).Chunks)
                fs = ParserHelperTools.CreateIsolatedFileStream(self.APP_NAME)
                with MemoryRange(chunks) as temp_stream:
                    temp_stream.CopyTo(fs)
                child.Data = MemoryRange(Chunk(fs, 0, fs.Length))
                audio_file.Children.Add(child)

    def parse_user(self, user_plist):
        root = BPReader(user_plist.Data).top
        if not root:
            return

        objs = root['$objects']
        info = objs[1]

        self.user_account.Deleted = user_plist.Deleted
        self.user_account.ServiceType.Value = self.APP_NAME

        val, src = self.getValSrcFromInfo('UsrName', objs, info)
        if val:
            self.user_account.Username.Value = val
            if self.extract_source:
                self.user_account.Username.Source = src

        val, src = self.getValSrcFromInfo('NickName', objs, info)
        if val:
            self.user_account.Name.Value = val
            if self.extract_source:
                self.user_account.Name.Source = src
        
        val, src = self.getValSrcFromInfo('Email', objs, info)
        if val:
            email_address = EmailAddress()
            email_address.Deleted = self.user_account.Deleted
            email_address.Value.Value =  val
            if self.extract_source:
                email_address.Value.Source =  src
            self.user_account.Entries.Add(email_address)

        val, src = self.getValSrcFromInfo('Country', objs, info)
        if val:
            country = UserID()
            country.Deleted = self.user_account.Deleted
            country.Category.Value = 'Country'
            country.Value.Init(val,src if self.extract_source else None)
            self.user_account.Entries.Add(country)

        #adding ids
        for field, id in [('facebook_id', 'facebook id'), ('LastUUID', 'LastUUID'), ('Mobile', 'Phone Number'), ('LINKEDIN_ID', 'LINKEDIN ID') ]:
            val, src = self.getValSrcFromInfo(field, objs, info)
            if val:
                userID = UserID()
                userID.Deleted = self.user_account.Deleted
                userID.Category.Value = id
                userID.Value.Value =  val
                if self.extract_source:
                    userID.Value.Source =  src
                self.user_account.Entries.Add(userID)


    
        val, src = self.getValSrcFromInfo('Signature', objs, info)   
        if val:
            self.user_account.Notes.Add( val )

        bplist = BPReader.GetTree(user_plist)
        if "new_dicsetting" in bplist and "headimgurl" in bplist["new_dicsetting"]:
            self.user_account.Notes.Add("Profile pic URL {0}".format(bplist["new_dicsetting"]["headimgurl"]))

    def parse_session_files(self):
        parser = ProtobufParser()
        option =  self._create_session_proto_options()
        for session_file in self.root.Search("/session/data/.*/\w+$"):
            if not session_file.Data: continue
            try:
                obj = parser.Parse(session_file.Data, option)
            except:
                if session_file.Deleted == DeletedState.Intact:
                    ServiceLog.Warning("WechatParser failed to parse: {0}".format(session_file.AbsolutePath))
                    continue
                obj = None
            if obj:
                self._parse_contact_from_session_buff(obj, session_file) 
                self._parse_multi_chat_parties_from_session_buff(obj, session_file)

    def _try_get_protobuff_path(self, buffer, *path):
        try:
            current = buffer
            for tag in path:
                if current.ContainsPath(tag):
                    current = current.GetByPath(tag)[0]
                    self.current = current                    
                else:
                    return None
            return current.Value, MemoryRange(current.Source) if self.extract_source else None
        except Exception , e:
            return None

    def _create_session_proto_options(self):
        chatInnerOptions =  ProtobufComplexOptions();#in 1/1
        chatInnerOptions.AddOption(CombinedTag(1, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, "chat_user_id"))
        chatInnerOptions.AddOption(CombinedTag(2, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, "chat_username"))
        chatInnerOptions.AddOption(CombinedTag(4, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, "user_full_name"))
        chatInnerOptions.AddOption(CombinedTag(5, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, "user_full_name_no_space"))
        chatInnerOptions.AddOption(CombinedTag(14, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, "img_url_small"))
        chatInnerOptions.AddOption(CombinedTag(15, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, "img_url_big"))
        chatInnerOptions.AddOption(CombinedTag(22, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String))
        chatOptions =  ProtobufComplexOptions();#in 1/
        chatOptions.AddOption(CombinedTag(1, ProtobufType.LengthValue), chatInnerOptions)
        chatOptions.AddOption(CombinedTag(3, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, "multi_chat_participants_ids"))
        chatOptions.AddOption(CombinedTag(4, ProtobufType.Varint),  NumericOptions())
        chatOptions.AddOption(CombinedTag(5, ProtobufType.LengthValue),  LengthValueOptions(LengthValueOptions.InnerDataType.String, ))
        chatOptions.AddOption(CombinedTag(6, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String))
        chatOptions.AddOption(CombinedTag(7, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String))
        chatOptions.AddOption(CombinedTag(8, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String))
        chatOptions.AddOption(CombinedTag(9, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String))
        chatOptions.AddOption(CombinedTag(10, ProtobufType.Varint), NumericOptions())
        chatOptions.AddOption(CombinedTag(11, ProtobufType.Varint), NumericOptions())
        chatOptions.AddOption(CombinedTag(12, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String))
        chatOptions.AddOption(CombinedTag(14, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String))
        chatOptions.AddOption(CombinedTag(17, ProtobufType.Varint), NumericOptions())
        chatOptions.AddOption(CombinedTag(20, ProtobufType.Varint), NumericOptions())
        chatOptions.AddOption(CombinedTag(14, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String));
        lastMessageOptions = ProtobufComplexOptions()
        lastMessageOptions.AddOption(CombinedTag(1, ProtobufType.Varint), NumericOptions())
        lastMessageOptions.AddOption(CombinedTag(2, ProtobufType.Varint), NumericOptions())
        lastMessageOptions.AddOption(CombinedTag(3, ProtobufType.Varint), NumericOptions())
        lastMessageOptions.AddOption(CombinedTag(4, ProtobufType.Varint), NumericOptions())
        lastMessageOptions.AddOption(CombinedTag(5, ProtobufType.Varint), NumericOptions())
        lastMessageOptions.AddOption(CombinedTag(6, ProtobufType.Varint), NumericOptions())
        lastMessageOptions.AddOption(CombinedTag(7, ProtobufType.Varint), NumericOptions("recvtime"))
        lastMessageOptions.AddOption(CombinedTag(8, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String, "from_adress"))
        lastMessageOptions.AddOption(CombinedTag(9, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String, "to_address"))
        lastMessageOptions.AddOption(CombinedTag(10, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String, "sender_in_multi_chat"))
        lastMessageOptions.AddOption(CombinedTag(11, ProtobufType.LengthValue), LengthValueOptions(LengthValueOptions.InnerDataType.String, "last_message"))
        lastMessageOptions.AddOption(CombinedTag(12, ProtobufType.Varint), NumericOptions())
        mainOptions = ProtobufComplexOptions()
        mainOptions.AddOption(CombinedTag(1, ProtobufType.LengthValue), chatOptions )
        mainOptions.AddOption(CombinedTag(2, ProtobufType.LengthValue), lastMessageOptions)
        mainOptions.AddOption(CombinedTag(12, ProtobufType.Varint), NumericOptions())
        return mainOptions
    
    #session files (protobuf) 包含 contact 详情和照片 url + the last message from or to that contact
    def _parse_contact_from_session_buff(self, obj, session_file):
        user_id = self._try_get_protobuff_path(obj, 1, 1, 1) #userID
        username = self._try_get_protobuff_path(obj, 1, 1, 2) #username
        printed_name = self._try_get_protobuff_path(obj, 1, 1, 4) #full name
        printed_name_without_space = self._try_get_protobuff_path(obj, 1, 1, 5) #fullname
        small_pic_url = self._try_get_protobuff_path(obj, 1, 1, 0x0E) 
        large_pic_url = self._try_get_protobuff_path(obj, 1, 1, 0x0F)
        if not user_id: return
        c = Contact()
        c.Source.Value = self.APP_NAME
        uid = UserID()
        uid.Category.Value = "username"
        c.Deleted = uid.Deleted = session_file.Deleted
        uid.Value.Init(*user_id)
        c.Entries.Add(uid)
        if printed_name and printed_name[0]:
            c.Name.Init(*printed_name)
        if small_pic_url and small_pic_url[0]:
            c.Notes.Add(*small_pic_url)
        if large_pic_url and large_pic_url[0]:
            c.Notes.Add(*large_pic_url)
        #查找用户的头像照片, 找到的话更新Contact对象
        for photo_node in  (self.all_files[session_file.Name + ".pic_hd"] + self.all_files[session_file.Name + ".pic_usr"]):
            cp = ContactPhoto()
            cp.Deleted = photo_node.Deleted
            cp.PhotoNode.Value = photo_node
            c.Photos.Add(cp)
        if uid.Value.Value in self.contacts:
            ParserHelperTools.MergeContacts(self.contacts[uid.Value.Value], c)
        else:
            self.contacts[uid.Value.Value] = c
            
    def _parse_last_msg_from_session_buff(self, obj):
        unknown = self._try_get_protobuff_path(obj, 1, 2, 1)
        rec_MesLocalID = self._try_get_protobuff_path(obj, 1, 2, 2)
        rec_Type = self._try_get_protobuff_path(obj, 1, 2, 3)
        rec_Status = self._try_get_protobuff_path(obj, 1, 2, 4)
        rec_ImgStatus = self._try_get_protobuff_path(obj, 1, 2, 5)
        unknown = self._try_get_protobuff_path(obj, 1, 2, 6)
        rec_CreateTime = self._try_get_protobuff_path(obj, 1, 2, 7)
        from_user_id = self._try_get_protobuff_path(obj, 1, 2, 8)
        to_user_id = self._try_get_protobuff_path(obj, 1, 2, 9)
        sender_in_multi_chat = self._try_get_protobuff_path(obj, 1, 2, 0xA)
        rec_Message = self._try_get_protobuff_path(obj, 1, 2, 0xB)
        rec_MesSvrID = self._try_get_protobuff_path(obj, 1, 2, 0xC)

    def _parse_multi_chat_parties_from_session_buff(self, obj, session_file):
        parties_string = self._try_get_protobuff_path(obj, 1, 3)
        chat_table_id = "".join(session_file.AbsolutePath.split('/')[-2:])
        if not parties_string or not type(parties_string[0]) == str: return
        for party_id in parties_string[0].split(';'):
            if not party_id: continue
            p = Party()
            p.Deleted = session_file.Deleted
            p.Identifier.Init(party_id, parties_string[1])
            self.chat_participants[chat_table_id].add(p)

    def parse_chat_praticipants(self, node):
        db = SQLiteParser.Database.FromNode(node)
        res = {}
        if db is None:
            return res
        if "SessionAbstract" in db.Tables:
            ts = SQLiteParser.TableSignature("SessionAbstract")
            for record in db.ReadTableRecords(ts, self.extract_deleted):
                if 'ConStrRes1' in record and not IsDBNull(record['ConStrRes1'].Value) and 'UsrName' in record and not IsDBNull(record['UsrName'].Value) and type(record['UsrName'].Value) == str:
                    chat_table_id = record['ConStrRes1'].Value
                    chat_table_id = "".join(chat_table_id.split('/')[-2:])
                    #添加聊天群参与者
                    if type(record["ConStrRes1"].Value) == str and  record["UsrName"].Value.endswith("@chatroom") and record["ConStrRes1"].Value != '':
                        self.multi_chatrooms_ids.add(chat_table_id)
                        participants_details = self.root.GetByPath(record["ConStrRes1"].Value)
                        if not participants_details: continue
                        chat_parties = self._get_parties_from_room_data(participants_details)
                        for party in chat_parties:
                            self.chat_participants[chat_table_id].add(party)
                    else:
                        p = Party()
                        c = Contact()
                        c.Source.Value = self.APP_NAME
                        uid = UserID()
                        uid.Category.Value = "username"
                        c.Deleted = uid.Deleted = p.Deleted = record.Deleted
                        SQLiteParser.Tools.ReadColumnToField(record, 'UsrName', p.Identifier, self.extract_source)
                        SQLiteParser.Tools.ReadColumnToField(record, 'UsrName', uid.Value, self.extract_source)
                        c.Entries.Add(uid)
                        #if chat_table_id not in self.chat_participants:
                        self.chat_participants[chat_table_id].add(p)
                        #if the contact already exists merging them
                        if uid.Value.Value in self.contacts: 
                            ParserHelperTools.MergeContacts(self.contacts[uid.Value.Value], c)
                        else:
                            self.contacts[uid.Value.Value] = c

    def _get_parties_from_room_data(self, participants_details):
        result = set()
        if not participants_details or not participants_details.Data:
            return result
        participants_details.Data.seek(0)
        rd = participants_details.Data.read()
        #roomData xml
        room_data = rd[rd.find('<RoomData'):rd.find(r'</RoomData>')+len('</RoomData>')]
        try:
            ms = MemoryStream(Encoding.UTF8.GetBytes(room_data))
            soup = XElement.Load(ms) 
            xs = XPathExtensions.XPathSelectElements(soup,"Member[@UserName]")
            xs=Enumerable.ToList[XElement](xs)
        except Exception, e:# 错误的xml?
            return result
#xe.Element("DisplayName").Value
        for username, display_name in [(tg.Attribute('UserName').Value, tg.Element("DisplayName").Value if tg.Element("DisplayName") else None) for tg in xs]:
            p = Party()
            p.Deleted = participants_details.Deleted
            if username:
                p.Identifier.Init(username, participants_details.Data if self.extract_source else None)
                result.add(p)
        return result

    def getValSrcFromInfo(self, key, objs, info):
        if key in info.Keys and objs[info[key].Value].Value:
            return objs[info[key].Value].Value, MemoryRange(objs[info[key].Value].Source)
        return None, None
    
    def is_message_from_main_user(self, rec):
        return rec['Des'].Value == 0
        
    def set_message_party(self, account_owner, chatid, im, rec):
        is_party_in_msg = False
        if rec['Type'].Value == 10000: return is_party_in_msg
        if self.is_message_from_main_user(rec) and account_owner.HasContent: # message from the mainUser
            im.From.Value = account_owner
            return is_party_in_msg
        if chatid not in self.chat_participants:
            return is_party_in_msg
        #if multi chat: the participant id is in the message body
        if chatid in self.multi_chatrooms_ids or len(self.chat_participants[chatid]) > 2:
            is_party_in_msg = True
            if 'Message' in rec and type(rec['Message'].Value) == str and rec['Message'].Value[:rec['Message'].Value.find(":")] in [p.Identifier.Value for p in self.chat_participants[chatid]]:
                username = rec['Message'].Value[:rec['Message'].Value.find(":")]
                party = filter(lambda x: username == x.Identifier.Value, self.chat_participants[chatid])[0]
                if username in self.contacts:
                    party = Party()
                    party.Name.Init(self.contacts[username].Name)
                    party.Identifier.Init(username, MemoryRange(rec['Message'].Source) if self.extract_source else None)
                im.From.Value = party
        elif account_owner.Identifier.HasLogicalContent and len(self.chat_participants[chatid]) <= 2:
            parties = filter(lambda x: account_owner.Identifier.Value != x.Identifier.Value, self.chat_participants[chatid])
            if not parties or len(parties) != 1:
                return is_party_in_msg
            party = parties[0]
            party.Deleted = DeletedState.Unknown
            username = party.Identifier.Value
            if username in self.contacts:
                party.Name.Init(self.contacts[username].Name)
            im.From.Value = party
        return is_party_in_msg

    def _get_msg_without_party_txt(self, text_field):
        if not text_field or IsDBNull(text_field.Value):
            return
        text_to_check = text_field.Value
        lines = text_to_check.split('\n')
        if not lines or len(lines) <= 1:
            return text_to_check
        first_line = lines[0]
        return text_field.Value[first_line.find(":") + 1:].lstrip()

    def parse_contacts_from_Friend_table(self, db):
        if 'Friend' in db.Tables:
            ts = SQLiteParser.TableSignature('Friend')
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if 'Type' not in rec or IsDBNull(rec['Type'].Value):
                    continue
                if rec['Type'].Value not in [3, 7, 8199]:
                    continue
                nameField = 'NickName'
                if 'NickName' not in rec or IsDBNull(rec['NickName'].Value):
                    nameField = 'FullPY'
                    if 'FullPY' not in rec or IsDBNull(rec['FullPY'].Value):
                        continue
                cont = Contact()
                uid = UserID()
                uid.Category.Value = "username"
                uid.Deleted = rec.Deleted
                SQLiteParser.Tools.ReadColumnToField(rec, 'UsrName', uid.Value, self.extract_source)
                if uid.HasLogicalContent:
                    cont.Entries.Add(uid)
                alias = UserID()
                alias.Deleted = rec.Deleted                
                alias.Category.Value = "Nickname"
                SQLiteParser.Tools.ReadColumnToField(rec, 'FullPY', alias.Value, self.extract_source)
                if alias.HasLogicalContent:
                    cont.Entries.Add(alias)                
                cont.Deleted = rec.Deleted
                cont.Source.Value = self.APP_NAME
                cont.Name.Value = rec[nameField].Value
                cont.Name.Source = MemoryRange(rec[nameField].Source)
                if rec['UsrName'].Value in self.contacts:
                    ParserHelperTools.MergeContacts(self.contacts[rec['UsrName'].Value], cont)
                else:
                    self.contacts[rec['UsrName'].Value] = cont

    def parse_contacs_from_Hello_table(self, db):
        
        attributes = re.compile(r"(\w+\s*\=\s*(\"[^\"]*\")|(\'[^\']*\'))")
        for table in db.Tables:
            if not table.startswith("Hello_"):
                continue
            ts = SQLiteParser.TableSignature(table)
            for rec in db.ReadTableRecords(ts, self.extract_deleted):
                if 'Message' not in rec or IsDBNull(rec['Message'].Value):
                    continue
        
                user_properties = {}
                data = rec['Message'].Value
               
                tokens = attributes.findall(data)
                for tok in tokens:
                    tok = tok[0]
                    if '="' in tok:
                        key = tok[0:tok.find('="')]
                        val = tok[tok.find('="')  +2 : tok.rfind('"')]
                        user_properties[key] = val
                if user_properties.has_key('fromusername') and user_properties['fromusername']:
        
                    source = MemoryRange(rec['Message'].Source)
        
                    cont = Contact()
                    cont.Deleted = rec.Deleted
                    cont.Source.Value = self.APP_NAME
        
                    username = user_properties['fromusername']
                    userID = UserID()
                    userID.Deleted = cont.Deleted
                    userID.Value.Value = username
                    userID.Value.Source = source
                    userID.Category.Value = "username"
                    cont.Entries.Add(userID)
                    if user_properties.has_key('fullpy') and user_properties['fullpy']:
                        cont.Name.Value = user_properties['fullpy'];
                        cont.Name.Source = source
                    for field in ['alias', 'fromnickname', 'extnickname', 'weibonickname', 'qqnickname']:
                        if user_properties.has_key(field) and user_properties[field]:
                            alias = UserID()
                            alias.Deleted = cont.Deleted
                            alias.Value.Value = user_properties[field]
                            alias.Value.Source = source
                            alias.Category.Value = field
                            cont.Entries.Add(alias)
                    if cont.Name.Value in self.contacts:
                        if cont.Name.Value in self.contacts:
                            ParserHelperTools.MergeContacts(self.contacts[cont.Name.Value], cont)
                        else:
                            self.contacts[cont.Name.Value] = cont
                    else:
                        if username in self.contacts:
                            ParserHelperTools.MergeContacts(self.contacts[username], cont)
                        else:
                            self.contacts[username] = cont

    def parse_contacts_from_db_node(self, node):
        db = SQLiteParser.Database.FromNode(node)
        if not db:
            return
        self.parse_contacts_from_Friend_table(db)

        self.parse_contacs_from_Hello_table(db)

    def parse_contact_share_heuristic(self, rec, im):
        c = Contact()
        c.Deleted = DeletedState.Unknown
        c.Source.Value = self.APP_NAME
        msg_data = rec['Message'].Value

        for att in ['bigheadimgurl', 'smallheadimgurl', 'fullpy', 'shortpy', 'alias', 'sign', 'sex', 'certinfo', 'brandIconUrl', 'brandHomeUrl', 'brandSubscriptConfigUrl', 'brandFlags', 'regionCode']:
            key_value = re.search("(?P<key>{0})=\"(?P<value>\w*)".format(att), msg_data)
            if key_value:
                c.Notes.Add("{0}: {1}".format(att, key_value.group('value')), MemoryRange(rec['Message'].Source) if self.extract_source else None)
        ui = UserID()
        username_key_value = re.search("(?P<key>{0})=\"(?P<value>[^\"]*)".format('username'), msg_data)
        if username_key_value:
            ui.Deleted = DeletedState.Unknown
            ui.Category.Value = "username"
            ui.Value.Init(username_key_value.group('value'), MemoryRange(rec['Message'].Source) if self.extract_deleted else None)
            c.Entries.Add(ui)
        nickname_key_value = re.search("(?P<key>{0})=\"(?P<value>[^\"]*)".format('nickname'), msg_data)
        if nickname_key_value:
            c.Name.Init(nickname_key_value.group('value'), MemoryRange(rec['Message'].Source) if self.extract_deleted else None)
        address = None
        province_key_value = re.search("(?P<key>{0})=\"(?P<value>[^\"]*)".format('province'), msg_data)
        if province_key_value:
            address = "province: {0}".format(province_key_value.group('value'))
        city_key_value = re.search("(?P<key>{0})=\"(?P<value>[^\"]*)".format('city'), msg_data)
        if city_key_value:
            if address:
                address += ", "
            address += "city: {0}".format(city_key_value.group('value'))
        if address:
            address_source = []
            sa = StreetAddress()
            sa.Deleted = DeletedState.Unknown
            sa.Street1.Init(address, MemoryRange(rec['Message'].Source) if self.extract_source else None)
            c.Addresses.Add(sa)
        im.SharedContacts.Add(c)
        c.Group.Value = "Shared"
        if ui.Value.HasLogicalContent:
            if ui.Value.Value in self.contacts:
                ParserHelperTools.MergeContacts(self.contacts[ui.Value.Value], c)
            else:
                self.contacts[ui.Value.Value] = c

    def parse_contact_share(self, rec, im):
        doc = None
        try:
            doc = XSDocument.Load(MemoryRange(rec['Message'].Source))
        except:
            return self.parse_contact_share_heuristic(rec, im)
        if doc.RootElements == None or len(doc.RootElements) == 0:
            return self.parse_contact_share_heuristic(rec, im)
        c = Contact()
        c.Deleted = DeletedState.Unknown
        c.Source.Value = self.APP_NAME
        doc_attrs = doc.RootElements[0].Attributes

        for att in ['bigheadimgurl', 'smallheadimgurl', 'fullpy', 'shortpy', 'alias', 'sign', 'sex', 'certinfo', 'brandIconUrl', 'brandHomeUrl', 'brandSubscriptConfigUrl', 'brandFlags', 'regionCode']:
            if doc_attrs.ContainsKey(att) and type(doc_attrs[att].Value) == str and doc_attrs[att].Value:
                c.Notes.Add("{0}: {1}".format(att, doc_attrs[att].Value), MemoryRange(doc_attrs[att].Source) if self.extract_source else None)
        ui = UserID()
        if doc_attrs.ContainsKey('username') and type(doc_attrs['username'].Value) == str and doc_attrs['username'].Value:
            ui.Deleted = DeletedState.Unknown
            ui.Category.Value = "username"
            ui.Value.Init(doc_attrs['username'].Value, MemoryRange(doc_attrs['username'].Source) if self.extract_deleted else None)
            c.Entries.Add(ui)
        if doc_attrs.ContainsKey('nickname') and type(doc_attrs['nickname'].Value) == str and doc_attrs['nickname'].Value:
            c.Name.Init(doc_attrs['nickname'].Value, MemoryRange(doc_attrs['nickname'].Source) if self.extract_deleted else None)
        address = ''
        if doc_attrs.ContainsKey('province') and type(doc_attrs['province'].Value) == str and doc_attrs['province'].Value:
            address = "province: {0}".format(doc_attrs['province'].Value)
        if doc_attrs.ContainsKey('city') and type(doc_attrs['city'].Value) == str and doc_attrs['city'].Value:
            if address:
                address += ", "
            address += "city: {0}".format(doc_attrs['city'].Value)
        if address:
            address_source = []
            address_source += doc_attrs['province'].Source
            address_source += doc_attrs['city'].Source
            sa = StreetAddress()
            sa.Deleted = DeletedState.Unknown
            sa.Street1.Init(address, MemoryRange(address_source) if self.extract_source else None)
            c.Addresses.Add(sa)
        im.SharedContacts.Add(c)
        c.Group.Value = "Shared"
        if ui.Value.HasLogicalContent:
            if ui.Value.Value in self.contacts:
                ParserHelperTools.MergeContacts(self.contacts[ui.Value.Value], c)
            else:
                self.contacts[ui.Value.Value] = c

    def parse_msg_from_chat_table(self, account_owner, chat, chat_messages, chatid, models, rec, record_ids):
        im = InstantMessage()
        im.Deleted = rec.Deleted
        im.SourceApplication.Value = self.APP_NAME
        recid = rec['MesLocalID'].Value
        createTime = rec['CreateTime'].Value
        create_time_str = str(createTime)
        if 'MesSvrID' in rec and not IsDBNull(rec['MesSvrID'].Value) and len(str(rec['MesSvrID'].Value)) > 5 and self._was_already_added(rec['MesSvrID'].Value, rec, record_ids):
                return
        success_time_parse, create_time_int = UInt32.TryParse(create_time_str)
        if success_time_parse and TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan1970(create_time_int)).IsValidForSmartphone():
            if self._was_already_added(create_time_int, rec, record_ids):
                    return
            SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, 'CreateTime', im.TimeStamp, self.extract_source,
                lambda ts: TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan1970(ts), True))
        if rec.Deleted == DeletedState.Deleted and not im.TimeStamp.Value:
            return
    
        is_multi_chat = self.set_message_party(account_owner, chatid, im, rec)
        
        att_list = []
        
        data_contains_nulls = type(rec['Message'].Value) == str and  '\0' in rec['Message'].Value
        if not data_contains_nulls and rec['Type'].Value == 34:
            att_list = filter(lambda node: "Audio/"+chatid in  node.AbsolutePath, self.root_files[str(recid)+".aud"])
            children_lists = [att.Children for att in att_list if att.Children.Count]
            if children_lists:
                att_list = []
                for children_list in children_lists:
                    att_list.extend(children_list)
        elif not data_contains_nulls and rec['Type'].Value == 49:
            attachment_to_check = []
            file_part_name = "OpenData/{0}/{1}".format(chatid, str(recid))
            for node_name in self.root_files:
                for node in self.root_files[node_name]:
                    if file_part_name in  node.AbsolutePath:
                        att_list.append(node)
        elif not data_contains_nulls and rec['Type'].Value in [43, 62]:
            att_list = filter(lambda node: "Video/"+chatid in  node.AbsolutePath, self.root_files[str(recid)+".mp4"]) 
            if len(att_list) == 0:
                att_list = filter(lambda node: "Video/"+chatid in  node.AbsolutePath, self.root_files[str(recid)+".video_thum"]) 
        elif not data_contains_nulls and rec['Type'].Value == 3:
            att_list =  filter(lambda node: "Img/"+chatid in  node.AbsolutePath, self.root_files[str(recid)+".pic"])  
            if len(att_list) == 0:
                att_list = filter(lambda node: "Img/"+chatid in  node.AbsolutePath, self.root_files[str(recid)+".pic_thum"])
                
            if len(att_list) == 0 and rec.Deleted == DeletedState.Deleted:
                att = Attachment()
                att.Deleted = rec.Deleted
                att.Filename.Value = str(recid)+".pic"
                im.Attachments.Add(att)
        
        elif not data_contains_nulls and rec['Type'].Value == 48: # Location
            coor = self.parse_coordinate(rec, is_multi_chat)
            if coor:
                im.Position.Value = coor
                loc = Location()
                loc.Deleted = coor.Deleted
                loc.Position.Value = coor
                loc.Category.Value = self.APP_NAME
                loc.TimeStamp.Init(im.TimeStamp)
                loc.PositionAddress.Init(coor.PositionAddress)
                loc.Description.Init(coor.Comment)
                if im.TimeStamp:
                    loc.TimeStamp.Init(im.TimeStamp)				
                LinkModels(im, loc)
                models.append(loc)
        elif not data_contains_nulls and rec['Type'].Value == 50: # voice message
            if "Message" in rec and not IsDBNull(rec['Message'].Value):
                att = Attachment()
                att.Deleted = rec.Deleted
                att.MetaData.Add("type: voip message", MemoryRange(rec["Message"].Source))
                messageData = rec["Message"].Value
                
                dur_start = messageData.find("<duration>")
                dur_end = messageData.find("</duration>")
                if dur_start < dur_end and dur_start >= 0:
                    att.MetaData.Add("duration: "+messageData[dur_start + len("<duration>"):dur_end], MemoryRange(rec["Message"].Source))
                im.Attachments.Add(att)
                is_from_main_user = self.is_message_from_main_user(rec)
                c = self.try_parse_msg_call(rec["Message"], rec.Deleted, is_from_main_user, rec, account_owner, is_multi_chat, chatid, im.TimeStamp)
                if c:
                    c.Source.Value = self.APP_NAME
                    self.calls.add(c)
                    LinkModels(c, im)

        elif (not data_contains_nulls and  rec['Type'].Value == 42) or (type(rec['Message'].Value) == str and  rec['Message'].Value.startswith("<msg username")): # contact share
            self.parse_contact_share(rec, im)
        elif not data_contains_nulls and rec['Type'].Value == 64: # Conference call
            c = self._try_parse_conference_call(chatid, rec["Message"], im)
            if c:
                c.Source.Value = self.APP_NAME
                if not c.TimeStamp.HasLogicalContent and im.TimeStamp.HasLogicalContent :
                    c.TimeStamp.Init(im.TimeStamp)
                self.calls.add(c)
                LinkModels(c, im)
        else:
            SQLiteParser.Tools.ReadColumnToField(rec, 'Message', im.Body, self.extract_source)
        if len(att_list) > 0:
            att = Attachment()
            att.Deleted = rec.Deleted
            att.Deleted = att_list[0].Deleted
            att.Data.Source = att_list[0].Data
            CreateSourceEvent(att_list[0], im)
            att.Filename.Value = att_list[0].Name
            if self._is_silk(att_list[0]):
                att.MetaData.Add('silk')
            im.Attachments.Add(att) 
        msg_unique_id = str(recid) +  str(rec['CreateTime'].Value)
        if recid == 0 or msg_unique_id not in chat_messages:
            if recid == 0:
                chat.Messages.Add(im)
            else:
                chat_messages[msg_unique_id] = im
        
    def parse_chat_table_intact_and_deleted(self, account_owner, db, models, node, record_ids, table):
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'TableVer', SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'MesSvrID', SQLiteParser.FieldType.Int)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'CreateTime', SQLiteParser.FieldType.Int)                
        SQLiteParser.Tools.AddSignatureToTable(ts, 'Status', SQLiteParser.FieldType.Int)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'ImgStatus', SQLiteParser.FieldType.Int)                
        SQLiteParser.Tools.AddSignatureToTable(ts, 'Des', SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1)
        ts['Message'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text)
        chatid = table[5:]
        if chatid not in self.chats:
            chat = Chat()
            chat.Source.Value = self.APP_NAME
            chat.Deleted = node.Deleted
            is_new_chat = True
        else:
            chat = self.chats[chatid]
            is_new_chat = False 
        
        chat_messages = {}
        
        if account_owner.HasContent:
                chat.Participants.Add(account_owner)
        
        if self.chat_participants.has_key(chatid):
            for party in self.chat_participants[chatid]:
                username = party.Identifier.Value if party.Identifier else None
                if username != None and account_owner != None and username == account_owner.Identifier.Value:
                    continue
                if username and  username in self.contacts:
                    party.Name.Init(self.contacts[username].Name)
                chat.Participants.Add(party)
        for rec in db.ReadTableRecords(ts, self.extract_deleted, False):
            self.parse_msg_from_chat_table(account_owner, chat, chat_messages, chatid, models, rec, record_ids)
        chat.Messages.AddRange(chat_messages.values())
        chat.SetTimesByMessages()
        chat.SetParticipantsByMessages()
        if is_new_chat and chat.HasLogicalContent:
            chat.ChatId.Value = chatid
            self.chats[chatid] = chat
    
    def parse_chat_table_only_deep_carve_records(self, account_owner, db, models, record_ids, table):
        ts = SQLiteParser.TableSignature(table)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'TableVer', SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'MesSvrID', SQLiteParser.FieldType.Int)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'CreateTime', SQLiteParser.FieldType.Int)                
        SQLiteParser.Tools.AddSignatureToTable(ts, 'Status', SQLiteParser.FieldType.Int)
        SQLiteParser.Tools.AddSignatureToTable(ts, 'ImgStatus', SQLiteParser.FieldType.Int)                
        SQLiteParser.Tools.AddSignatureToTable(ts, 'Des', SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const0, SQLiteParser.Tools.SignatureType.Const1)
        ts['Message'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableRecords(ts, True, True):
            if rec.Deleted == DeletedState.Intact or self._was_already_added(rec.RecordId, rec, record_ids): continue
            chat = Chat()
            chat_messages = {}
            chat.Source.Value = self.APP_NAME
            chat.Deleted = DeletedState.Deleted
            chatid = "{0} Unknown_Chat: {1}".format(self.APP_NAME, self.unknown_chat_counter)
            is_new_chat = True
            self.parse_msg_from_chat_table(account_owner, chat, chat_messages, chatid, models, rec, record_ids)
            chat.Messages.AddRange(chat_messages.values())
            chat.SetTimesByMessages()
            chat.SetParticipantsByMessages()
            if is_new_chat and chat.HasLogicalContent:
                self.unknown_chat_counter += 1
                chat.ChatId.Value = chatid
                self.chats[chatid] = chat

    def decode_chat(self, node):
        db = SQLiteParser.Database.FromNode(node)
        if not db:
            return []
        models = []
        account_owner = Party()
        if self.user_account.Name and self.user_account.Name.Value:
            account_owner.Name.Init(self.user_account.Name) 
            account_owner.Identifier.Init(self.user_account.Username)

        record_ids = set()
        for table in db.Tables:
            if not table.startswith("Chat_"):
                continue
            self.parse_chat_table_intact_and_deleted(account_owner, db, models, node, record_ids, table)
        if self.extract_deleted == True:
            for table in db.Tables:
                if not table.startswith("Chat_"):
                    continue
                self.parse_chat_table_only_deep_carve_records(account_owner, db, models, record_ids, table)
                break
        return models

    def _was_already_added(self, rec_sig, rec, record_sigs):
        if rec_sig in record_sigs:
            return True
        record_sigs.add(rec_sig)
        return False

    def _try_parse_conference_call(self, chatid, msg_call, im):
        call = Call()
        call.Deleted = im.Deleted
        call.Type.Value = CallType.Conference
        if chatid in self.chat_participants and self.chat_participants[chatid]:
            call.Parties.AddRange(self.chat_participants[chatid])
        if not msg_call or not msg_call.Value or IsDBNull(msg_call.Value):
            return call
        jnode = JNode.Parse(self._get_msg_without_party_txt(msg_call))
        if jnode and jnode['createUserName'] and jnode['createUserName'].Value in self.contacts:
            from_party = jnode['createUserName'].Value
            for call_party in call.Parties:
                if call_party.Identifier.Value == from_party:
                    call_party.Role.Value = PartyRole.From
        if jnode and jnode['msgContent'] and jnode['msgContent'].Value:
            im.Body.Init(jnode['msgContent'].Value, MemoryRange(msg_call.Source) if self.extract_source else None )
        LinkModels(call, im)
        return call

    def _parse_call_parties(self, account_owner, call, chatid, is_from_main_user, is_multi_chat, main_user_party, other_party):
        if account_owner.HasLogicalContent:
            main_user_party = ParserHelperTools.CloneParty(account_owner)
            call.Parties.Add(main_user_party)
        if not is_multi_chat and  chatid in self.chat_participants and len(self.chat_participants[chatid]) == 1 :
            other_party = ParserHelperTools.CloneParty(list(self.chat_participants[chatid])[0])
            if other_party.Identifier and other_party.Identifier.Value and other_party.Identifier.Value in self.contacts:
                other_party.Name.Init(self.contacts[other_party.Identifier.Value].Name)
            call.Parties.Add(other_party)                
        if is_from_main_user:
            if main_user_party:
                main_user_party.Role.Value = PartyRole.From
            if other_party:
                other_party.Role.Value = PartyRole.To
            call.Type.Value = CallType.Outgoing
        else:
            if main_user_party:
                main_user_party.Role.Value = PartyRole.To
            if other_party:
                other_party.Role.Value = PartyRole.From
            call.Type.Value = CallType.Incoming

    def try_parse_msg_call(self, msg_call, deleted, is_from_main_user, rec, account_owner, is_multi_chat, chatid, msg_timestamp):
        try:
            call_xsdoc = XSDocument.Load(MemoryRange(msg_call.Source))
        except Exception, e:
            return 
        call = Call()
        call.Deleted = deleted
        wording_type = duration = recvtime = roomid = status = invitetype = None
        for x in call_xsdoc.RootElements:
            if x.Name.Value == "voiplocalinfo":
                wording_type = XMLParserTools.TryGetFirstElementByXPath(x, "voiplocalinfo/wordingtype")
                duration = XMLParserTools.TryGetFirstElementByXPath(x, "voiplocalinfo/duration")
            elif x.Name.Value == "voipextinfo":
                recvtime = XMLParserTools.TryGetFirstElementByXPath(x, "voipextinfo/recvtime")
            elif x.Name.Value == "voipinvitemsg":
                roomid = XMLParserTools.TryGetFirstElementByXPath(x, "voipinvitemsg/roomid")
                key = XMLParserTools.TryGetFirstElementByXPath(x, "voipinvitemsg/key")
                status = XMLParserTools.TryGetFirstElementByXPath(x, "voipinvitemsg/status")
                invitetype = XMLParserTools.TryGetFirstElementByXPath(x, "voipinvitemsg/invitetype")
        if recvtime and recvtime.Value:
            recived_time = ParserHelperTools.TryGetValidTimeStampEpoch1Jan1970(recvtime.Value)
            if recived_time: 
                call.TimeStamp.Init(recived_time, MemoryRange(recvtime.Source) if self.extract_source else None)
        elif not call.TimeStamp.HasLogicalContent and msg_timestamp.HasLogicalContent:
            call.TimeStamp.Init(msg_timestamp)
        if duration and duration.Value and duration.Value.isdigit(): 
            call.Duration.Init(TimeSpan(0, 0, int(duration.Value)), MemoryRange(duration.Source) if self.extract_source else None)
        if invitetype and invitetype.Value == "0":
            call.VideoCall.Init(True, MemoryRange(invitetype.Source) if self.extract_source else None) 
        main_user_party = other_party = None  
        self._parse_call_parties(account_owner, call, chatid, is_from_main_user, is_multi_chat, main_user_party, other_party)
        if wording_type and wording_type.Value == "7":
            call.Type.Value = CallType.Missed
        if wording_type and wording_type.Value == "8":
            call.Type.Value = CallType.Rejected
        if call.Type.Value == CallType.Incoming and duration and duration.Value and duration.Value.isdigit() and int(duration.Value) == 0:
            call.Type.Value = CallType.Missed

        return call

    def parse_coordinate(self, coor_rec, is_party_in_msg):
        if IsDBNull(coor_rec['Message'].Value) or not coor_rec['Message'].Value:
            return
        data = coor_rec['Message'].Value
        if is_party_in_msg: 
            data = self._get_msg_without_party_txt(coor_rec['Message'])
        try:
            coor_data = XDocument.Parse(data)
            loc_data = coor_data.Element('msg').Element('location')
        except:
            return
        src = MemoryRange(coor_rec['Message'].Source) if self.extract_source else None
        coor = Coordinate()
        coor.Deleted = coor_rec.Deleted
        for at in loc_data.Attributes():
            if at.Name.LocalName == 'y':
                coor.Longitude.Init(float(at.Value), src)
            elif at.Name.LocalName == 'x':
                coor.Latitude.Init(float(at.Value), src)
            elif at.Name.LocalName == 'label':
                coor.PositionAddress.Init(at.Value, src)
            elif at.Name.LocalName == 'poiname':
                coor.Comment.Init(at.Value, src)

        if coor.HasContent:
            return coor

    def decode_xlog(self, node):
        
        self.UINFO_STRING = 'Class name: CUsrInfo'

        ## imported wechat code

        self.MAGIC_CRYPT_START = '\x01';
        self.MAGIC_COMPRESS_CRYPT_START = '\x02';
        self.MAGIC_END  = '\x00';
        self.BASE_KEY  = 0xCC;

        l = self.ParseFile(node.Data)
        uinfo = []
        for log in l:
            
            lines = log.splitlines()
            index = lines.index(self.UINFO_STRING,0) if self.UINFO_STRING in lines else -1
            
            while index > -1:
                d={}
                timestamp = time.strptime(lines[index-2][10:29], '%Y-%m-%d %H:%M:%S')

                d['Timestamp'] = timestamp
                index += 1                
                while index<len(lines) and lines[index].startswith('m_'):
                    keyval = lines[index].split(':', 1)
                    d[keyval[0][2:]] = keyval[1]
                    index += 1

                uinfo.append(d)

                index = lines.index(self.UINFO_STRING,index) if self.UINFO_STRING in lines[index:] else -1        
        return uinfo

    def ParseFile(self, stream):  # adapted from wechat code
        _buffer = stream.read()
        startpos = self.GetLogStartPos(_buffer)
        res = []

        while True:
            startpos, outbuffer = self.DecodeBuffer(_buffer, startpos)
            if startpos == -1:
                break
            res.append(outbuffer)


        return res

    def IsGoodLogBuffer(self, _buffer, _offset, count):

        if _offset == len(_buffer): return True
        if _offset + 1 + 4 + 1 + 1 > len(_buffer): return False

        if self.MAGIC_CRYPT_START!=_buffer[_offset] and self.MAGIC_COMPRESS_CRYPT_START!=_buffer[_offset]: return False

        length = struct.unpack_from("I", buffer(_buffer, _offset+1, 4))[0]	
        if _offset + 1 + 4 + length + 1 > len(_buffer): return False
        if self.MAGIC_END!=_buffer[_offset + 1 + 4 + length]: return False

        if (1>=count): return True
        else: return self.IsGoodLogBuffer(_buffer, _offset+1+4+length+1, count-1)

    def GetLogStartPos(self, _buffer):
        offset = 0
        while True:
            if offset >= len(_buffer) : break
            
            if self.MAGIC_CRYPT_START==_buffer[offset] or self.MAGIC_COMPRESS_CRYPT_START==_buffer[offset]:
                if self.IsGoodLogBuffer(_buffer, offset, 2): 
                    return offset
            offset+=1
        return -1	

    def DecodeBuffer(self, _buffer, _offset):
        
        if _offset == len(_buffer): return -1, "";
        if not self.IsGoodLogBuffer(_buffer, _offset, 1): return -1, ""
        iscompress = False	
        if self.MAGIC_COMPRESS_CRYPT_START==_buffer[_offset]: iscompress = True
        length = struct.unpack_from("I", buffer(_buffer, _offset+1, 4))[0]
        if iscompress:
            key = self.BASE_KEY ^ (0xff & length) ^ ord(self.MAGIC_COMPRESS_CRYPT_START)
        else:
            key = self.BASE_KEY ^ (0xff & length) ^ ord(self.MAGIC_CRYPT_START)
        tmpbuffer = ""
        for i in range(length):
            tmpbuffer += chr(key ^ ord(_buffer[_offset+1+4+i]))

        if iscompress: tmpbuffer = zlib.decompress(tmpbuffer)

        return _offset+1+4+length+1, tmpbuffer