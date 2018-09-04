#coding=utf-8
from PA_runtime import *
from hashlib import md5
from System.Linq import Enumerable
import traceback

class TelegramParser(object):
    """
    Telegram Messenger (ph.telegra.Telegraph)
    user accounts, contacts, chats, and messages. (attachments ???)
    """
    SOURCE = 'Telegram'

    _ATTACHMENT_CONST_DOWLOADED_IMAGE = 0xA8D89B26
    _ATTACHMENT_CONST_UNDOWLOADED_VIDEO = 0x20AA8E33
    _ATTACHMENT_CONST_CONTACT_PHONE = 0x63560AB9
    _ATTACHMENT_CONST_METAMESSAGE = 0x8BE26711
    _ATTACHMENT_CONST_LOCATION = 0x6ED09E0C
    _ATTACHMENT_CONST_CHANGE_GROUP_NAME = 0x8BE26711
    _ATTACHMENT_CONST_FILE_ATTACHMENT = 0x1843C6E6
    _ATTACHMENT_CONST_AUDIO = 0x327A0E3A

    def __init__(self, node, group_container_nodes, extract_deleted, extract_source):
        self.node = node
        self.group_container_nodes = group_container_nodes
        self.db_nodes = []
        self.DBs_and_roots = []
        self.extract_deleted = extract_deleted
        self.extract_source = extract_source
        self.locations = []
        self.user_account = None
        self.users = {}
        self.self_user = None
        self.chats = {}
        self.passwords = []

    def parse_passwords(self,root):
        self.parse_passcode_from_x_y(root)
        self.parse_passcode_from_shared_auth_info(root)
        return self.passwords

    def parse(self):
        results = []
        self.init_DBs()

        results += self.parse_passwords(self.node)
        for root in self.group_container_nodes:
            results += self.parse_passwords(root)

        for [db, root] in self.DBs_and_roots:
            self.parse_users(db)
            self.parse_chats(db)
            self.parse_messages(db, root)

            results += self.users.values()
            for c in self.chats.values():
                c.SetParticipantsByMessages()
                c.SetTimesByMessages()
            results += self.chats.values()
            results += self.locations

        return results

    def is_self_user(self, user_rec):
        """
        是当前用户吗?.
        """
        if 'access_hash' in user_rec and 'last_seen' in user_rec:
            return (IsDBNull(user_rec['access_hash'].Value) or user_rec['access_hash'].Value == 0) and bool(user_rec['last_seen'].Value)
        elif 'access_hash' in user_rec:
            return IsDBNull(user_rec['access_hash'].Value) or user_rec['access_hash'].Value == 0
        elif 'last_seen' in user_rec and not IsDBNull(user_rec['last_seen'].Value):
            return user_rec['last_seen'].Value == 0x7fffffff
        return False

    def parse_users(self, db):
        if db is None or 'users_v29' not in db.Tables:
            return
        ts = SQLiteParser.TableSignature('users_v29')
        if self.extract_deleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'uid', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'access_hash', SQLiteParser.Tools.SignatureType.Long)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'sex', SQLiteParser.Tools.SignatureType.Const0,
                                                   SQLiteParser.Tools.SignatureType.Const1)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'last_seen', SQLiteParser.Tools.SignatureType.Int)

        for rec in db.ReadTableRecords(ts, self.extract_deleted, True):
            if 'uid' not in rec or IsDBNull(rec['uid'].Value):
                continue
            is_self = self.is_self_user(rec)
            if is_self:
                user = UserAccount()
                user.ServiceType.Value = self.SOURCE
                SQLiteParser.Tools.ReadColumnToField[str](rec, 'uid', user.Username, self.extract_source, str)
                user.Deleted = rec.Deleted
                self.user_account = user
            else:
                user = Contact()
                user.Source.Value = self.SOURCE
                uid = UserID()
                uid.Deleted = rec.Deleted
                SQLiteParser.Tools.ReadColumnToField[str](rec, 'uid', uid.Value, self.extract_source, str)
                user.Entries.Add(uid)
            user.Deleted = rec.Deleted
            user.Name.Init(FieldOperations.JoinStringFields([(rec['first_name'].Value, rec['first_name'].Source),
                                                             (rec['last_name'].Value, rec['last_name'].Source)]))
            pn = PhoneNumber()
            pn.Deleted = rec.Deleted
            SQLiteParser.Tools.ReadColumnToField(rec, 'phone_number', pn.Value, self.extract_source)
            if pn.Value.HasContent:
                user.Entries.Add(pn)
            #TODO - photos
            if not is_self and rec['last_seen'].Value > 0:
                SQLiteParser.Tools.ReadColumnToMultiField[str](rec, 'last_seen', user.Notes, self.extract_source,
                                                               lambda x: 'Last seen online on {0}'.format(
                                                                   TimeStamp.FromUnixTime(x, True)))
            self.users[rec['uid'].Value] = user

    def get_party_by_uid(self, uid, uid_src, role=PartyRole.General):
        user = self.users.get(uid)
        if user:
            user_name = user.Name
        elif self.self_user and self.self_user.Username.Value == str(uid):
            user_name = self.self_user.Name
        else:
            user_name = None
        uid_svp = SourceValuePair[str](str(uid), uid_src)
        return Party.Create(uid_svp, role, name=user_name)

    def parse_chat_participants(self, chat, data):
        if data.Length < 12:
            return
        data.seek(4 + 4)
        participants_count = struct.unpack('<i', data.read(4))[0]
        offset = data.tell()
        for i in xrange(participants_count):
            uid_src = data.GetSubRange(offset + i * 4, 4) if self.extract_source else None
            uid = struct.unpack('<i', data.read(4))[0]
            chat.Participants.Add(self.get_party_by_uid(uid, uid_src))

    def parse_chats(self, db):
        if db is None:
            return
        if 'convesations_v29' in db.Tables:
            table_name = 'convesations_v29'
        elif 'conversations_v29' in db.Tables:
            table_name = 'conversations_v29'
        else:
            return

        ts = SQLiteParser.TableSignature(table_name)
        if self.extract_deleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'cid', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'date', SQLiteParser.Tools.SignatureType.Int)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'from_uid', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'flags', SQLiteParser.Tools.SignatureType.Short,
                                                   SQLiteParser.Tools.SignatureType.Byte)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'chat_version', SQLiteParser.Tools.SignatureType.Const0,
                                                   SQLiteParser.Tools.SignatureType.Const1)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'service_unread', SQLiteParser.Tools.SignatureType.Const0,
                                                   SQLiteParser.Tools.SignatureType.Const1)

        for rec in db.ReadTableRecords(ts, self.extract_deleted, True):
            if 'cid' not in rec or IsDBNull(rec['cid'].Value):
                continue
            c = Chat()
            c.Source.Value = self.SOURCE
            c.Deleted = rec.Deleted
            SQLiteParser.Tools.ReadColumnToField[str](rec, 'cid', c.ChatId, self.extract_source, str)
            if self.self_user:
                chat.Participants.Add(Party.MakeGeneral(self.self_user.Username, name=self.self_user.Name))
            if 'participants' in rec and not IsDBNull(rec['participants'].Value):
                try:
                    self.parse_chat_participants(c, MemoryRange(rec['participants'].Source))
                except IndexError:
                    if rec.Deleted == DeletedState.Deleted:
                        continue
                    raise
            elif 'from_uid' in rec and not IsDBNull(rec['from_uid'].Value) and\
                    (not self.self_user or str(rec['from_uid'].Value) != self.self_user.Username.Value):
                uid = rec['from_uid'].Value
                uid_src = MemoryRange(rec['from_uid'].Source) if self.extract_source else None
                c.Participants.Add(self.get_party_by_uid(uid, uid_src))
            self.chats[rec['cid'].Value] = c

    def parse_messages(self, db, root):
        if db is None or 'messages_v29' not in db.Tables:
            return
        ts = SQLiteParser.TableSignature('messages_v29')
        if self.extract_deleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'cid', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'from_id', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'to_id', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'outgoing', SQLiteParser.Tools.SignatureType.Const0,
                                                   SQLiteParser.Tools.SignatureType.Const1)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'unread', SQLiteParser.FieldType.Int)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'date', SQLiteParser.Tools.SignatureType.Int)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'media', SQLiteParser.FieldType.Blob, SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, "message", SQLiteParser.FieldType.Text)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'mid', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'localMid', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'dstate', SQLiteParser.FieldType.Int,
                                                   SQLiteParser.FieldConstraints.NotNull)

        for rec in db.ReadTableRecords(ts, self.extract_deleted, True):
            chat = self.chats.get(rec['cid'].Value)
            if chat is None:
                continue
            im = InstantMessage()
            im.SourceApplication.Value = self.SOURCE
            im.Deleted = rec.Deleted
            SQLiteParser.Tools.ReadColumnToField(rec, 'message', im.Body, self.extract_source)
            from_src = MemoryRange(rec['from_id'].Source) if self.extract_source else None
            im.From.Value = self.get_party_by_uid(rec['from_id'].Value, from_src, role = PartyRole.To if bool(rec['outgoing'].Value) else PartyRole.From)
            if bool(rec['outgoing'].Value):
                if (self.user_account is None) or (self.user_account.Name.Value != im.From.Value.Name.Value):
                    ua = UserAccount()
                    ua.ServiceType.Value = self.SOURCE
                    ua.Deleted = DeletedState.Intact
                    ua.Name.Init(im.From.Value.Name.Value,MemoryRange(im.From.Value.Name.Source) if self.extract_source else None)
                    self.user_account = ua

            to_src = MemoryRange(rec['to_id'].Source) if self.extract_source else None
            to_id = rec['to_id'].Value
            if to_id in self.users:
                im.To.Add(self.get_party_by_uid(rec['to_id'].Value, to_src, role=PartyRole.To))
            try:
                SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, 'date', im.TimeStamp, self.extract_source,
                                                            TimeStamp.FromUnixTime)
            except:
                pass
            status_src = MemoryRange(list(rec['outgoing'].Source) + list(rec['unread'].Source) +
                                     list(rec['dstate'].Source)) if self.extract_source else None
            is_out = rec['outgoing'].Value == 1
            is_unread = rec['unread'].Value == 0x7fffffff
            is_unsent = rec['dstate'].Value == 2
            if is_out:
                status = MessageStatus.Unsent if is_unsent else MessageStatus.Sent
            else:
                status = MessageStatus.Unread if is_unread else MessageStatus.Read
            im.Status.Init(status, status_src)

            self.parse_message_attachment(im, rec, root)
                  
            if im.Deleted == DeletedState.Deleted and im in chat.Messages:
                continue
            chat.Messages.Add(im)

    def parse_message_attachment(self, im, rec, root):
        data = MemoryRange(rec['media'].Source)

        if not data.Length >= 0x10:
            return
        data.seek(4)
        media_type = struct.unpack('>I', data.read(4))[0]

        if media_type == self._ATTACHMENT_CONST_METAMESSAGE:
            second_type = struct.unpack('I', data.read(4))[0]
            if(second_type == 4):
                third_type = struct.unpack('I', data.read(4))[0]
                if(third_type == 8):
                    if im.Body.Value is None:
                        im.Body.Value = '<User joind Chat>'
                    else:
                        im.Body.Value += '\n<User joind Chat>'

        if not data.Length > 0x10:
            return

        if media_type == self._ATTACHMENT_CONST_LOCATION:
            length = struct.unpack('I', data.read(4))[0]
            c = Coordinate()
            c.Deleted = rec.Deleted
            c.Latitude.Value, c.Longitude.Value = struct.unpack('dd', data.read(0x10))
            if self.extract_source:
                c.Latitude.Source = data.GetSubRange(0xc,8)
                c.Longitude.Source = data.GetSubRange(0x14,8)
            im.Position.Value = c
            l = Location()
            l.Deleted = im.Deleted
            l.Category.Value = self.SOURCE
            if im.From.HasContent:
                l.Origin.Value = LocationOrigin.Device if (im.From.Value.Role.Value == PartyRole.To) else LocationOrigin.External
            l.TimeStamp.Init(im.TimeStamp.Value, im.TimeStamp.Source)
            l.Position.Value = im.Position.Value
            LinkModels(im,l)
            self.locations.append(l)
        elif media_type == self._ATTACHMENT_CONST_AUDIO:
            data.seek(0xc)
            bytes = list(struct.unpack('8B',data.read(8)))
            bytes.reverse()
            dir_string = ''.join((hex(byte)[2:] if byte > 0xF else '0' + hex(byte)[2:]) for byte in bytes)
            if dir_string[0] == '0':
                dir_string = dir_string[1:]
            attachment_files = root.Search('/Documents/audio/{0}/.*$'.format(dir_string))
            for file in attachment_files:
                att = Attachment()
                att.Deleted = file.Deleted
                att.Filename.Value = file.Name
                att.Data.Source = file.Data
                CreateSourceEvent(file, im)
                im.Attachments.Add(att)
                  

        elif media_type == self._ATTACHMENT_CONST_FILE_ATTACHMENT:
            data.seek(0x25)
            dir_length = struct.unpack('I', data.read(4))[0]
            if dir_length == 0:
                return
            bytes = list(struct.unpack(str(dir_length)+'B',data.read(dir_length)))
            bytes.reverse()
            dir_string = ''.join((hex(byte)[2:] if byte > 0xF else '0' + hex(byte)[2:]) for byte in bytes)
            if dir_string[0] == '0':
                dir_string = dir_string[1:]
            nothing = data.read(4)
            name_length = struct.unpack('I', data.read(4))[0]
            if name_length < 0 or 2048 < name_length:
                return #字符串超过2048就忽略了吧,这肯定是一个错误的数据

            name_string = data.read(name_length)
            pattern = '/Documents/files/.*{0}.*/{1}$'.format(dir_string,name_string)
            attachment_files = []
            try:
                TraceService.Trace(TraceLevel.Info,'apple_tgm > pattern > {0}'.format(pattern))
                nodes = root.Search(pattern)
                attachment_files = Enumerable.ToList[Node](nodes)
                
            except:
                traceback.print_exc()
                attachment_files = []

            for file in attachment_files:
                att = Attachment()
                att.Deleted = file.Deleted
                att.Filename.Value = file.Name
                att.Data.Source = file.Data
                CreateSourceEvent(file, im)
                im.Attachments.Add(att)

        elif media_type == self._ATTACHMENT_CONST_CHANGE_GROUP_NAME:
            data.seek(0x10)
            length = struct.unpack('I', data.read(4))[0]
            string = data.read(length)
            if im.Body.Value is None:
                im.Body.Value = 'Change Group Name: {0}'.format(string)
                if self.extract_source:
                    im.Body.Source = data
            else:
                im.Body.Value = '{1}\nChange Group Name: {0}'.format(string,im.Body.Value)
        elif media_type == self._ATTACHMENT_CONST_CONTACT_PHONE:
            data.seek(0x10)
            type_length = struct.unpack('I', data.read(4))[0]
            type_string = data.read(type_length).decode('utf-8')
            if type_string == '':
                type_length = struct.unpack('I', data.read(4))[0]
                type_string = data.read(type_length).decode('utf-8')
            else:
                nothing = data.read(4)   
            contact_length = struct.unpack('I', data.read(4))[0]
            contact_string = data.read(contact_length)
            if im.Body.Value is None:
                im.Body.Value = '{0}:{1}'.format(type_string,contact_string)
                if self.extract_source:
                    im.Body.Source = data
            else:
                im.Body.Value = '{2}\n{0}:{1}'.format(type_string,contact_string,im.Body.Value)

        elif media_type == self._ATTACHMENT_CONST_UNDOWLOADED_VIDEO:            
            file_names = []

            # method 1
            second_type = struct.unpack('I', data.read(4))[0]

            if second_type != 0x7abacaf1:
                data.seek(0x29)
            else:
                data.seek(0x2e)
                	
            length = struct.unpack('I', data.read(4))[0]
            file_name = data.read(length)
            data.seek(data.Position-len(file_name)-0x21) # for method 2
            file_name = file_name[file_name.find(':')+1:]
            file_names.append(file_name)

			# method 2
            bytes = list(struct.unpack('8B',data.read(8)))
            bytes.reverse()   
            file_name =''		 
            for b in bytes:				   
                file_name += (hex(b)[2:] if b > 0xF else '0' + hex(b)[2:])								
            if file_name[0] == '0':
                file_name = file_name[1:]
            file_name = "remote" +file_name	
            file_names.append(file_name)
            for file_name in file_names:
                attachment_files = root.Search('/Documents/video/{0}.*$'.format(file_name))			  
                for file in attachment_files:
                    att = Attachment()
                    att.Deleted = file.Deleted
                    att.Filename.Value = file.Name
                    att.Data.Source = file.Data
                    CreateSourceEvent(file, im)
                    im.Attachments.Add(att)  
            return  

        elif media_type in (self._ATTACHMENT_CONST_DOWLOADED_IMAGE,self._ATTACHMENT_CONST_METAMESSAGE):
            if media_type == self._ATTACHMENT_CONST_DOWLOADED_IMAGE:
                data.seek(0x20)
            elif media_type == self._ATTACHMENT_CONST_METAMESSAGE:
                second_type = struct.unpack('>I', data.read(4))[0]
                if(second_type != 5):
                    return
                data.seek(0x28)

            pos = data.Position
            for p in [pos, pos + 0x5]:
                data.seek(p)
                files = []
                for i in xrange(4):                
                    if data.Position + 4 > data.Length:
                        break
                    length = struct.unpack('I', data.read(4))[0]
                    if data.Position + length > data.Length:
                        break
                    title = data.read(length)
                    filename = md5(title).hexdigest()
                    attachment_files = root.Search('/Caches/{0}'.format(filename))
                    if attachment_files is not None:
                        attachment_files = list(attachment_files)
                    file = None
                    if attachment_files not in [[],None]:
                        file = attachment_files[0]
                    if file is not None:
                        files.append(file)
                    data.seek(data.Position + 0x0C)

                if len(files) != 0:
                    file = files[-1]
                    att = Attachment()
                    att.Deleted = file.Deleted
                    att.Filename.Value = file.Name
                    att.Data.Source = file.Data
                    CreateSourceEvent(file, im)
                    im.Attachments.Add(att)
                    return

    def parse_passcode_from_x_y(self, root):
        if root is None:
            return
        x_y = root.GetByPath('Documents/x.y')
        if x_y is None or x_y.Data is None:
            return
        data = x_y.Data
        if data.Length < 6:
            return
        passcode = data.read()[5:]
        passcode_source = data.GetSubRange(5,len(passcode)) if self.extract_source else None

        p = Password()
        p.Deleted = DeletedState.Intact
        p.Service.Value = self.SOURCE
        p.Data.Init(passcode, passcode_source)
        self.passwords.append(p)

    def parse_passcode_from_shared_auth_info(self, root):
        if root is None:
            return
        shared_auth_info = root.GetByPath('shared-auth-info')
        if shared_auth_info is None or shared_auth_info.Data is None:
            return

        bp = BPReader.GetTree(shared_auth_info)
        if bp is None: 
            return

        if 'password' in bp:
            p = Password()
            p.Deleted = shared_auth_info.Deleted
            p.Service.Value = self.SOURCE
            p.Data.Init(bp['password'].Value, MemoryRange(bp['password'].Source) if self.extract_source else None)
            self.passwords.append(p)

    def init_DBs(self):
        if self.node is not None:
            db_node = self.node.GetByPath('/Documents/tgdata.db')
            if db_node is not None:
                self.db_nodes.append(db_node)
                db = SQLiteParser.Database.FromNode(db_node)
                if db is not None:
                    self.DBs_and_roots.append([db,self.node])

        for group_container in self.group_container_nodes:
            db_node = group_container.GetByPath('/Documents/tgdata.db')
            if db_node is not None:
                self.db_nodes.append(db_node)
                db = SQLiteParser.Database.FromNode(db_node)
                if db is not None:
                    self.DBs_and_roots.append([db,group_container])


def analyze_telegram(app_folder, extract_deleted, extract_source):
    pr = ParserResults()

    """
    ds.GroupContainers可以取到DataStore中所有的共享节点,因为是根据特征,不做筛选,多余的Node不用担心
    为了防止在脚本的运行过程中GroupContainers改变,造成迭代失败,这里使用ToArray生成
    集合的副本
    """
    group_container_nodes = ds.GroupContainers.ToArray()
    pr.Models.AddRange(TelegramParser(app_folder, group_container_nodes, extract_deleted, extract_source).parse())
    return pr