#coding=utf-8

import hashlib
import quopri
import email 
import string 
import time as py_time
from urllib import unquote
from urlparse import urlsplit, uses_netloc
from collections import defaultdict
from sys import maxint
from collections import deque, namedtuple
from System import DateTime, DateTimeKind

import clr
clr.AddReference('PNFA.iPhoneApps')
try:
    clr.AddReference('model_mail')
    clr.AddReference('bcp_mail')
    clr.AddReference('ScriptUtils')
except:
    pass
del clr

import model_mail
import bcp_mail
from PA_runtime import *
from PA.iPhoneApps import *
from ScriptUtils import DEBUG, CASE_NAME, exc, tp, parse_decorator, print_run_time


VERSION_APP_VALUE = 3

APPLE_MAIL_BOX_CONVERT_DICT = {
    '收件箱'           : model_mail.MAIL_INBOX,
    'Inbox'           : model_mail.MAIL_INBOX,
    'INBOX'           : model_mail.MAIL_INBOX,
    'OUTBOX'          : '发件箱',
    '发件箱'           : '发件箱',
    '已发送'           : model_mail.MAIL_OUTBOX,
    'Sent Messages'   : model_mail.MAIL_OUTBOX,
    'Sent'            : model_mail.MAIL_OUTBOX,
    'SENT'            : model_mail.MAIL_OUTBOX,
    '草稿箱'           : model_mail.MAIL_DRAFT,
    'Drafts'          : model_mail.MAIL_DRAFT,
    '已删除'           : model_mail.MAIL_DELTED,
    'Deleted Messages': model_mail.MAIL_DELTED,
    'Spam'            : '垃圾邮件',
}

APPLE_MAIL_READ_STATUS = {
    model_mail.SEND_STATUS_UNREAD: MessageStatus.Unread,
    model_mail.SEND_STATUS_READ: MessageStatus.Read,
    }

status_dict = {
    0: MessageStatus.Unread,
    1: MessageStatus.Read
    }

content_type_priority = {
    'text/html': 1,
    'text/plain': 2
    }

epoch = DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)

hexdigits = set(string.hexdigits)

# iron python bug fix
quopri.b2a_qp = quopri.a2b_qp = None

DataRecord = namedtuple("DataRecord", "part partial complete data file")
patt = re.compile(r'["\']*([a-zA-Z0-9@&+=)(.,_ -]+)["\']* *(?:<([a-zA-Z0-9@+=._-]+)>)*')


@parse_decorator
def analyze_emails(mail_dir, extractDeleted, extractSource):
    pr = ParserResults()
    res = []
    try:
        envelope_node = mail_dir.GetFirstNode("Envelope Index")
        if envelope_node is None:
            return pr
        protected_node = mail_dir.GetFirstNode("Protected Index")
        
        if protected_node is not None:
            res = AppleEmailParser(mail_dir, envelope_node, protected_node).parse()
        else:
            res = AppleEmailParserOld(mail_dir, envelope_node, protected_node).parse()

        if res:
            Export2db(mail_dir, res).parse()
            pr.Models.AddRange(res)
        return pr
    except:
        msg = 'Apple_mails 解析新案例 <{}> 出错: {}'.format(CASE_NAME, traceback.format_exc())
        TraceService.Trace(TraceLevel.Debug, msg)
        return pr

# 替代 quopri.decode
def decode(input, output, header = 0):

    ESCAPE = quopri.ESCAPE

    new = []
    while 1:
        try:
            line = input.readline()
        except IndexError:
            break
        if not line: break
        i, n = 0, len(line)
        if n > 0 and line[n-1] == '\n':
            partial = 0; n = n-1
            # 去掉结尾的空白
            while n > 0 and line[n-1] in " \t\r":
                n = n-1
        else:
            partial = 1
        while i < n:
            c = line[i]
            if c == '_' and header:
                new.append(' '); i = i+1
            elif c != ESCAPE:
                new.append(c); i = i+1
            elif i+1 == n and not partial:
                partial = 1; break
            elif i+1 < n and line[i+1] == ESCAPE:
                new.append(ESCAPE); i = i+2
            elif i+2 < n and line[i+1] in hexdigits and line[i+2] in hexdigits:
                new.append(chr(int(line[i+1:i+3], 16))); i = i+3
            else: 
                new.append(c); i = i+1
        if not partial:
            output.write("".join(new) + '\n')
            new = []
    if new:
        output.write("".join(new))

quopri.decode = decode

def collapse_rfc2231_value(value, errors='replace',
                           fallback_charset='us-ascii'):
    if isinstance(value, tuple):
        rawval = Uri.UnescapeDataString(value[2])
        charset = value[0] or 'us-ascii'
        try:
            return unicode(rawval, charset, errors)
        except LookupError:
            # XXX charset is unknown to Python.
            return unicode(rawval, fallback_charset, errors)
    elif isinstance(value, str) and value.startswith('=?') and value.endswith('?='):
        parts = []
        for word in value.split():
            try:
                charset, encoding, data = word[2:-2].split('?')
            except ValueError:
                continue
            if encoding == 'Q':
                rawval = Uri.UnescapeDataString(data)
            elif encoding == 'B':
                rawval = data.decode('base64')
            try:
                parts.append(unicode(rawval, charset, errors))
            except (LookupError, AttributeError):
                # XXX charset is unknown to Python.
                parts.append(unicode(rawval, fallback_charset, errors))
        return "".join(parts)
    else:
        return Uri.UnescapeDataString(value)

# 替代 email.message.get_filename
def get_filename(self, failobj=None):
    missing = object()
    filename = self.get_param('filename', missing, 'content-disposition')
    if filename is missing:
        filename = self.get_param('name', missing, 'content-type')
    if filename is missing:
        return failobj
    return collapse_rfc2231_value(filename).strip()


class AppleEmailParser(object):
    def __init__(self, node, envelope_node, protected_node):
        self.root = node
        self.extractDeleted = True
        self.extractSource = False
        self.envelope_db = SQLiteParser.Database.FromNode(envelope_node)
        self.protected_db = SQLiteParser.Database.FromNode(protected_node)
    
        self.uuid_email_map = {}

    def parse(self):
        return self.parse_main()

    def parse_main(self):
        results = defaultdict(list)
        messages = defaultdict(set)

        emlx_files, part_files= self._parse_emlx()

        # Protected Index
        message_data = self._parse_ptb_message_data('message_data')
        results = self._parse_ptb_message('messages', results, messages)

        # Envelope Index     
        mailboxes = self._parse_mailboxes('mailboxes')
        part_records = self._parse_etb_message_data('message_data')
        results = self._parse_etb_message_deleted('messages_deleted', results)
        final_results = self._parse_etb_message('messages', mailboxes, results, part_records, message_data, emlx_files, part_files)

        return final_results

    @print_run_time
    def _parse_etb_message_data(self, table_name):
        '''table message_data'''

        part_patt = re.compile(r'^summary{1}$|^[0-9.]+$')
        parts = defaultdict(list)
        ts = SQLiteParser.TableSignature('message_data')
        if self.extractDeleted:
            ts['message_id'] = SQLiteParser.Signatures.NumericSet(2)
            ts['complete'] = ts['partial'] = SQLiteParser.Signatures.NumericSet(1, 8, 9)
            ts['part'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text)
        for record in self.envelope_db.ReadTableRecords(ts, self.extractDeleted, True):
            if 'ROWID' in record and record['ROWID'].Value < 1:
                continue
            if IsDBNull(record['message_id'].Value):
                continue
            if record['message_id'].Value < 1:
                continue
            if not IsDBNull(record['part'].Value) and not part_patt.match(record['part'].Value):
                continue
            key = record['message_id'].Value        
            parts[key].append(record)
        return parts

    @print_run_time
    def _parse_mailboxes(self, table_name):
        mailbox = namedtuple("mailbox", "folder path account source")
        fs_mailboxes = {}    
        # 遍历目录, 查找 Info.plist
        for m in self.root.Directories:
            q = deque()
            for d in m.Directories:
                q.append(d.Name)

            while len(q) > 0:
                next_dir = q.popleft()
                d = m.GetByPath(next_dir)
                if d is None:
                    continue
                info = d.GetFirstNode("Info.plist")
                if info != None and info.Data != None:
                    info.Data.seek(0)              
                    p = PList.Load(info.Data)
                    if p != None:
                        try:
                            uid = p[0]["DAMailboxUid"].Value
                        except:
                            uid = None
                        if uid != None:
                            folder = next_dir.replace(".mbox", "")
                            fs_mailboxes[uid] = (folder, "/".join([m.Name, folder]))
                for subdir in d.Directories:
                    q.append("/".join([next_dir, subdir.Name]))
        mailboxes = {}    
        for record in self.envelope_db['mailboxes']:
            key = record['ROWID'].Value
            data = record['url'].Value        
            if IsDBNull(data):
                continue
            path = Uri.UnescapeDataString(data)
            folder = os.path.basename(path)
            
            account = urlsplit(path).netloc  # 可能是 邮件地址/uuid/'LocalAccountId'
            if account.count('@') > 1:
                _account = account.rsplit('@', 1)[0]
                account = self.uuid_email_map.get('IMAP-'+account) 
            elif self.uuid_email_map.has_key(account):
                account = self.uuid_email_map.get(account)

            if folder in fs_mailboxes:
                mailboxes[key] = mailbox(fs_mailboxes[folder][0], fs_mailboxes[folder][1], account, MemoryRange(record['url'].Source))
            else:
                folder = urlsplit(path).path.strip('/')
                path = path.replace('\\','_').replace('://', '-')
                mailboxes[key] = mailbox(folder, path, account, MemoryRange(record['url'].Source))
        return mailboxes

    def _parse_ptb_message_data(self, table_name):
        '''Protected Index - message_data'''
        message_data = {}
        ts = SQLiteParser.TableSignature(table_name)
        ts['data'] = SQLiteParser.Signatures.NumericSet(12, 13)    
        for record in self.protected_db.ReadTableRecords(ts, self.extractDeleted):
            if record['message_data_id'].Value < 1:
                continue
            if IsDBNull (record['data'].Value):
                continue
            _rec_data = self._convert_to_str(record['data'].Value)
            message_data[record['message_data_id'].Value] = _rec_data
        return message_data

    def _parse_ptb_message(self, table_name, results, messages):
        '''Protected Index - message'''
        ts = SQLiteParser.TableSignature(table_name)
        if self.extractDeleted:
            ts['sender'] = ts['subject'] = ts['_to'] = ts['cc'] = ts['bcc'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text)
        for record in self.protected_db.ReadTableRecords(ts, self.extractDeleted):
            if record['message_id'].Value < 1:
                continue
            msg = Email()
            msg.Source.Value = "Mails"
            if record.ContainsKey('deleted') and record['deleted'].Value not in [0, '0']:
                msg.Deleted = DeletedState.Deleted
            else:
                msg.Deleted = record.Deleted
            _rec_sender = self._convert_to_str(record['sender'].Value)
            _rec_to     = record['_to'].Value
            _rec_cc     = record['cc'].Value
            _rec_bcc    = record['bcc'].Value
            if not IsDBNull(_rec_sender):
                email_address, name = self._create_party(_rec_sender)
                if email_address == None:
                    if self.extractSource:        
                        msg.From.Value = Party.MakeFrom(_rec_sender, MemoryRange(record['sender'].Source))
                    else:
                        msg.From.Value = Party.MakeFrom(_rec_sender, None)
                else:
                    if self.extractSource:
                        msg.From.Value = Party.MakeFrom(email_address, MemoryRange(record['sender'].Source))
                    else:
                        msg.From.Value = Party.MakeFrom(email_address, None)
                    if name != None and name != email_address:
                        msg.From.Value.Name.Value = name
            if not IsDBNull(record['subject'].Value):
                msg.Subject.Value = record['subject'].Value
                if self.extractSource:
                    msg.Subject.Source = MemoryRange(record['subject'].Source)
            if not IsDBNull(_rec_to):
                for to, name in self._split_addresses(_rec_to):
                    if to == None:
                        continue
                    if self.extractSource:
                        party = Party.MakeTo(to, MemoryRange(record['_to'].Source))
                    else:
                        party = Party.MakeTo(to, None)
                    if name != None and name != to:
                        party.Name.Value = name
                    msg.To.Add(party)
            if not IsDBNull(_rec_cc):
                for cc, name in self._split_addresses(_rec_cc):
                    if cc == None:
                        continue
                    if self.extractSource:
                        party = Party.MakeTo(cc, MemoryRange(record['cc'].Source))
                    else:
                        party = Party.MakeTo(cc, None)
                    if name != None and name != cc:
                        party.Name.Value = name
                    msg.Cc.Add(party)                
            if not IsDBNull(_rec_bcc):                        
                for bcc, name in self._split_addresses(_rec_bcc):
                    if bcc == None:
                        continue
                    if self.extractSource:
                        party = Party.MakeTo(bcc, MemoryRange(record['bcc'].Source))
                    else:
                        party = Party.MakeTo(bcc, None)
                    if name != None and name != bcc:
                        party.Name.Value = name
                    msg.Bcc.Add(party)
            if msg not in messages[record['message_id'].Value]:
                messages[record['message_id'].Value].add(msg)
                results[record['message_id'].Value].append(msg)
        return results
                
    def _parse_etb_message_deleted(self, table_name, results):
        '''删除的邮件 Envelope Index - messages_deleted'''
        for record in self.envelope_db['messages_deleted']:
            if record['message_id'].Value in results:
                if self.extractDeleted:
                    results[record['message_id'].Value][0].Deleted = DeletedState.Deleted
                else:
                    results.pop(record['message_id'].Value)
        return results

    def _parse_etb_message(self, table_name, mailboxes, results, part_records, message_data, emlx_files, part_files):
        final_results = []
        # table: messages
        ts = SQLiteParser.TableSignature('messages')
        if self.extractDeleted:
            ts['date_sent'] = ts['date_received'] = SQLiteParser.Signatures.NumericSet(4)
            ts['external_id'] = ts['read'] = ts['deleted'] = ts['flagged'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            ts['mailbox'] = SQLiteParser.Signatures.NumericSet(1)    

        for record in self.envelope_db.ReadTableRecords(ts, self.extractDeleted, True):
            if record['ROWID'].Value not in results:
                if record.Deleted == DeletedState.Deleted:
                    continue                        
                else:
                    msg = Email()
                    msg.Deleted = record.Deleted
                    results[record['ROWID'].Value].append(msg)
            elif results[record['ROWID'].Value][0].Deleted == DeletedState.Deleted:
                continue
            msg = results[record['ROWID'].Value][0]
            if 'deleted' in record and record['deleted'].Value in [1, '1']:
                if not self.extractDeleted:
                    continue
                msg.Deleted = DeletedState.Deleted
            if not IsDBNull(record['date_sent'].Value):
                time = TimeStamp(epoch.AddSeconds(record['date_sent'].Value), True)
                if time.IsValidForSmartphone():
                    msg.TimeStamp.Value = time
                    if self.extractSource:
                        msg.TimeStamp.Source = MemoryRange(record['date_sent'].Source)
            if  record['read'].Value.isnumeric():            
                if int(record['read'].Value) in APPLE_MAIL_READ_STATUS:
                    msg.Status.Value = APPLE_MAIL_READ_STATUS[int(record['read'].Value)]
                    if self.extractSource:
                        msg.Status.Source = MemoryRange(record['read'].Source)
            if not IsDBNull(record['mailbox'].Value) and record['mailbox'].Value in mailboxes:
                mailbox = mailboxes[record['mailbox'].Value]
                msg.Folder.Value = APPLE_MAIL_BOX_CONVERT_DICT.get(mailbox.folder, mailbox.folder)
                if self.extractSource:
                    msg.Folder.Source = mailbox.source
                msg.Account.Value = mailbox.account
                if self.extractSource:
                    msg.Account.Source = mailbox.source

            # body, attachment
            _file_id = record['external_id'].Value

            if emlx_files.has_key(_file_id):
                emlx_file = emlx_files.get(_file_id, None)
                if emlx_file:
                    emlx_file.Data.seek(0)
                    m = email.message_from_string(emlx_file.read())
                    self._msg_info_from_mime(msg, m)
                    body, attachments = self._read_from_mime(m, None)
                    self._add_embedded(emlx_file, attachments, msg)
            else:
                parts = {}
                parts.update(part_files[_file_id])
                if record['ROWID'].Value in part_records:
                    for part_record in part_records[record['ROWID'].Value]:     
                        if IsDBNull(part_record['ROWID'].Value) or part_record['ROWID'].Value not in message_data:
                            continue
                        part = part_record['part'].Value            # summary
                        if IsDBNull(part_record['part'].Value):
                            part = ""
                        # "part partial complete data file"
                        parts[part] = DataRecord(
                            part,
                            part_record['partial'].Value,
                            part_record['complete'].Value,
                            message_data[part_record['ROWID'].Value],
                            self.root.GetFirstNode("Protected Index"))
                try:
                    body, attachments = self._read_from_parts(parts)
                except UnicodeDecodeError:
                    body, attachments = "", []
            if body != "":
                msg.Body.Value = body   
            msg.Attachments.AddRange(attachments)
            final_results.extend(results.pop(record['ROWID'].Value))            

        for res in results.values():
            for msg in res:
                if self.hasContent(msg):
                    final_results.append(msg)
        return final_results      

    def _read_from_parts(self, parts):
        body, attachments = "", []
        if '' in parts:
            m = self._build_mime(parts)
            body, attachments = self._read_from_mime(m, parts)
            if (body == "" or body.isspace()) and attachments == [] and parts.has_key('summary'):
                body, attachments = parts['summary'].data, []
            elif (body == "" or body.isspace()) and parts.has_key('summary'):
                body = parts['summary'].data
        elif 'summary' in parts:
            body, attachments = parts['summary'].data, []
        elif len(parts) > 0:
            pass
        return body, attachments        

    def _msg_info_from_mime(self, msg, mime):
        ''' '''
        pass

    @print_run_time
    def _parse_emlx(self):
        # mail body part
        emlx_files = {}
        part_files = defaultdict(list)

        for d in self.root.GetAllNodes(NodeType.Directory):
            if d.Name.endswith("mbox"):
                messages = d.GetFirstNode("Messages")
                if messages != None:
                    for f in messages.Glob("*.emlx*"):
                        # 检测 邮件文件在文件系统下是否异常
                        try:
                            parts = f.Name.split(".", 1)
                            message_id = parts[0] 
                            if parts[1] == "emlx":
                                f.Data.seek(0)
                                emlx_files[message_id] = f
                                self._get_account_email(d, f)
                            elif parts[1].endswith("emlxpart"):
                                f.Data.seek(0)
                                data = f.Data.read()
                                part_num = parts[1].rsplit(".", 1)[0]
                                part_files[message_id].append((part_num, DataRecord(part_num, "", 1, data, f)))
                        except:
                            continue
        return emlx_files, part_files

    def _get_account_email(self, file_node, emlx_file):
        account_mail = ''
        _paths = file_node.AbsolutePath.split('/')
        _uuid = _paths[-2]
        _box = _paths[-1].replace('.imapmbox', '').replace('.mbox', '')
        if not self.uuid_email_map.has_key(_uuid):
            mime = email.message_from_string(emlx_file.read())       
            if mime.get('Received'):
                _box_type = APPLE_MAIL_BOX_CONVERT_DICT.get(_box, _box)
                if _box_type==model_mail.MAIL_INBOX or 'Deleted Messages' in _box_type:
                    account_mail = mime.get('To')
                elif _box_type==model_mail.MAIL_OUTBOX or 'Sent Messages' in _box_type:
                    account_mail = mime.get('From')
                if account_mail:
                    if '<' in account_mail and '>' in account_mail:
                        _res = re.search(r'<(.*?)>', account_mail)
                        if _res:
                            account_mail = _res.group(1)
                    if self._is_email_format(account_mail):
                        self.uuid_email_map[_uuid] = account_mail
                
    def _build_mime(self, parts):
        '''
        Args:
            parts (dict): 
                key: part_record['part'].Value(e.g.'summary'), 
                value: DataRecord(nametuple)
        
        Returns:
            [type]: [description]
        '''    
        main_part = parts.pop('')
        m = email.message_from_string(main_part.data)

        if main_part.partial not in [1, '1']:
            return m
        for num in parts:
            if num in ['summary', 'meeting', 'meeting data']:
                continue
            if not m.is_multipart():
                m.set_payload(parts[num].data)
                break
            payload = m
            for n in num.split("."):
                if not n.isnumeric():
                    continue
                try:
                    payload = payload.get_payload(int(n) - 1)
                except:
                    continue
            payload.set_payload(parts[num].data)
        return m

    def hasContent(self, msg):
	    return msg.Body.Value is not None or msg.From.Value is not None or msg.TimeStamp.Value is not None or msg.To.Count > 0 or msg.Subject.Value is not None or msg.Attachments.Count > 0

    def _add_embedded(self, f, attachments, model = None):
        for att in attachments:
            if att.Filename.Value:
                embedded_file = Node (att.Filename.Value, NodeType.Embedded | NodeType.File)
            else :
                embedded_file = Node ("unknown", NodeType.Embedded | NodeType.File)
            embedded_file.Deleted = f.Deleted
            embedded_file.Data = att.Data.Source
            f.Children.Add(embedded_file)
            SourceEvent.CreateSourceEvent(f, model)

    def _read_from_mime(self, m, parts):
        '''https://docs.python.org/2.7/library/email.message.html?highlight=email%20message%20message#module-email.message '''
        first = m
        # 获取所有的附件
        attachments = []
        if m.is_multipart():
            subtype = m.get_content_subtype()
            n = 1
            if subtype != "alternative":            
                payload = m.get_payload()
                # 邮件的内容可以是任何的MIME内容,有可能多个,这里从简,只取第一个文本块
                lookForBody = True
                for part in payload:
                    if part.is_multipart():
                        continue
                    if lookForBody:
                        if part.get_content_maintype() == 'text':
                            first = part
                            lookForBody = False
                            continue
                    att = self.create_attachment_from_mime_message(part)
                    if att is None:
                        continue
                    attachments.append(att)
                    if parts is not None and len(parts) > 1 and str(n + 1) in parts:                    
                        self._add_embedded (parts[str(n + 1)].file, [att])
                    n += 1
            else:
                for part in m.walk():
                    if part.get_content_maintype() not in ['audio', 'image', 'video']:
                        continue
                    att = self.create_attachment_from_mime_message(part)
                    if att is None:
                        continue
                    attachments.append(att)
                    if parts is not None and len(parts) > 1 and str(n + 1) in parts:                    
                        self._add_embedded (parts[str(n + 1)].file, [att])
                    n += 1
        # 获取正文
        text_part = first
        maxLen = 0
        if first.is_multipart():
            payload_parts = first.get_payload()
            while len(payload_parts) > 0:
                part = payload_parts.pop()
                if part.is_multipart():
                    payload_parts.extend(part.get_payload())
                if part.get_content_maintype() == 'text':
                    if maxLen == 0:
                        text_part = part				
                        maxLen = len(part.get_payload())		
                    elif part.get_content_subtype() == 'html':
                        if text_part.get_content_subtype() == 'html' and len(part.get_payload()) > maxLen:
                            text_part = part				
                            maxLen = len(part.get_payload())		
                        elif text_part.get_content_subtype() == 'plain':
                            text_part = part
                            maxLen = len(part.get_payload())
                    elif part.get_content_subtype() == 'plain':
                        if text_part.get_content_subtype() == 'plain' and len(part.get_payload()) > maxLen:
                            text_part = part
                            maxLen = len(part.get_payload())               

        text = ""    
        if text_part.get_content_type() == 'text/html':
            charset = text_part.get_content_charset("us-ascii")
            try:
                html = unicode(text_part.get_payload(decode = True), charset, 'ignore')
            except (LookupError, UnicodeEncodeError):
                html = ""
            text = html
        
        elif text_part.get_content_type() == 'text/plain':
            charset = text_part.get_content_charset("us-ascii")
            try:
                text = unicode(text_part.get_payload(decode = True), charset, 'ignore')
            except (LookupError, UnicodeEncodeError):
                text = ""

        return text, attachments    

    def create_attachment_from_mime_message(self, msg):
        if msg.is_multipart():
            return
        data = to_byte_array(msg.get_payload(decode=True))
        if len(data) == 0:
            return
        res = Attachment()
        out = ThreadSafeMemoryStream(data)
        res.Data.Source = MemoryRange(Chunk(out, 0, out.Length))
        res.Filename.Value = get_filename(msg)
        res.Charset.Value = msg.get_charset()
        res.ContentType.Value = msg.get_content_type()
        return res

    def _split_addresses(self, value):
        return [self._create_party(st) for st in value.splitlines() if len(st) > 0]

    def _create_party(self, st):
        '''
        Args:
            st (str): 

        Returns:
            tuple: (str, str)
        '''
        try:
            st = self._convert_to_str(st)
            m = patt.match(st)
            if m:
                g = m.groups()
                if g[1] == None:
                    return g[0], None
                else:
                    return g[1], g[0]
            return None, None
        except:
            exc()
            return None, None

    def _convert_to_str(self, ab):
        '''convert to string if isinstance of Array[Byte]
        
        Args:
            ab (str|Array[Byte]): 
        
        Returns:
            str_res (str)
        '''
        try:
            str_res = ''
            if isinstance(ab, str):
                return ab    
            elif isinstance(ab, Array[Byte]):
                s =  ''.join(map(chr, ab))
                str_res = s.decode('utf8')
                return str_res
            return str_res
        except:
            exc()
            return ''

    @staticmethod
    def _is_email_format(email_str=''):
        """ 匹配邮箱地址 

        Args:
            email_str (str): 
            
        Returns:
            bool: is valid email address      
        """
        try:
            reg_str = r'^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$'
            match_obj = re.match(reg_str, email_str)
            if match_obj is None:
                return False      
            return True      
        except:
            exc()
            return False


class AppleEmailParserOld(AppleEmailParser):
    def __init__(self, node, envelope_node, protected_node):
        super(AppleEmailParserOld, self).__init__(node, envelope_node, protected_node)

    def parse_main(self):
        results = defaultdict(list)
        emlx_files, part_files= self._parse_emlx()

        # Protected Index
        # message_data = self._parse_ptb_message_data('message_data')
        # results = self._parse_ptb_message('messages', results, messages)

        # Envelope Index 
        mailboxes = self._parse_mailboxes('mailboxes')
        part_records = self._parse_etb_message_data('message_data')
        final_results = self._parse_etb_message_old('messages', mailboxes, results, part_records, emlx_files, part_files)
        return final_results

    def _parse_etb_message_old(self, table_name, mailboxes, results, part_records, emlx_files, part_files):
        results = []
        ts = SQLiteParser.TableSignature(table_name)
        if self.extractDeleted:
            ts['read'] = ts['sender'] = ts['subject'] = ts['_to'] = ts['cc'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text)
            ts['date_sent'] = SQLiteParser.Signatures.NumericSet(4)
            ts['mailbox'] = SQLiteParser.Signatures.NumericSet(0, 1)
        for record in self.envelope_db.ReadTableRecords(ts, self.extractDeleted, True):
            msg = Email()
            msg.Source.Value = "Mails"
            if record.ContainsKey('deleted') and record['deleted'].Value not in [0, '0']:
                if self.extractDeleted:
                    msg.Deleted = DeletedState.Deleted
                else:
                    continue
            else:
                msg.Deleted = record.Deleted
            if 'sender' in record and not IsDBNull(record['sender'].Value):
                email_address, name = self._create_party(record['sender'].Value)
                if email_address == None:            
                    if self. extractSource:
                        msg.From.Value = Party.MakeFrom(record['sender'].Value, MemoryRange(record['sender'].Source))
                    else:
                        msg.From.Value = Party.MakeFrom(record['sender'].Value, None)
                else:
                    if self. extractSource:
                        msg.From.Value = Party.MakeFrom(email_address, MemoryRange(record['sender'].Source))
                    else:
                        msg.From.Value = Party.MakeFrom(email_address, None)
                    if name != None and name != email_address:
                        msg.From.Value.Name.Value = name
            if 'subject' in record and not IsDBNull(record['subject'].Value):
                msg.Subject.Value = record['subject'].Value
                if self. extractSource:
                    msg.Subject.Source = MemoryRange(record['subject'].Source)
                    
            if '_to' in record and not IsDBNull(record['_to'].Value):
                for to, name in self._split_addresses(record['_to'].Value):
                    if to == None:
                        continue
                    if self. extractSource:
                        party = Party.MakeTo(to, MemoryRange(record['_to'].Source))
                    else:
                        party = Party.MakeTo(to, None)
                    if name != None and name != email_address:
                        party.Name.Value = name
                    msg.To.Add(party)
            if 'cc' in record and not IsDBNull(record['cc'].Value):
                for cc, name in self._split_addresses(record['cc'].Value):
                    if cc == None:
                        continue
                    if self. extractSource:
                        party = Party.MakeTo(cc, MemoryRange(record['cc'].Source))
                    else:
                        party = Party.MakeTo(cc, None)
                    if name != None and name != email_address:
                        party.Name.Value = name
                    msg.Cc.Add(party)     
            if 'date_sent' in record and not IsDBNull(record['date_sent'].Value):
                time = TimeStamp(epoch.AddSeconds(record['date_sent'].Value), True)
                if time.IsValidForSmartphone():
                    msg.TimeStamp.Value = time
                    if self. extractSource:
                        msg.TimeStamp.Source = MemoryRange(record['date_sent'].Source)
            if 'read' in record and not IsDBNull(record['read'].Value) and record['read'].Value in [0, 1, '0', '1']:
                msg.Status.Value = status_dict[int(record['read'].Value)]
                if self. extractSource:
                    msg.Status.Source = MemoryRange(record['read'].Source)

            base_name = str(record['ROWID'].Value)
            emlx_file = emlx_files.get(base_name, None)
            if emlx_file == None:
                if 'external_id' in record and not IsDBNull(record['external_id'].Value):
                    emlx_file = emlx_files.get(record['external_id'].Value, None)

            if 'mailbox' in record and not IsDBNull(record['mailbox'].Value) and record['mailbox'].Value in mailboxes:
                mailbox = mailboxes[record['mailbox'].Value]
                msg.Folder.Value = mailbox.folder            
                msg.Account.Value = mailbox.account
                if self. extractSource:
                    msg.Folder.Source = mailbox.source
                    msg.Account.Source = mailbox.source

            if emlx_file != None:
                emlx_file.Data.seek(0)
                data = emlx_file.read()
                m = email.message_from_string(data)
                body, attachments =  self._read_from_mime(m, None)
                self._add_embedded(emlx_file, attachments, msg)
            else:
                parts = {}
                parts.update(part_files[base_name])

                if not IsDBNull(record['ROWID'].Value) and record['ROWID'].Value in part_records:
                    for part_record in part_records[record['ROWID'].Value]:
                        if not 'data' in part_record:
                            continue
                        if IsDBNull(part_record['data'].Value):
                            continue
                        if isinstance(part_record['data'].Value, str):
                            data = part_record['data'].Value
                        else:
                            try:
                                data = "".join([chr(c) for c in part_record['data'].Value])
                            except:
                                data = None
                        part = part_record['part'].Value
                        if IsDBNull(part_record['part'].Value):	
                            part = ""
                        parts[part] = DataRecord(
                            part,
                            part_record['partial'].Value,
                            part_record['complete'].Value,
                            data,
                            self.root.GetFirstNode("Envelope Index"))
                body, attachments = self._read_from_parts(parts)
            if body != "":
                msg.Body.Value = body
            msg.Attachments.AddRange(attachments)
            if self.hasContent(msg):
                results.append(msg)

        return results


class Export2db(object):
    def __init__(self, node, results_model):
        self.mm = model_mail.MM()
        self.results_model = results_model
        self.cachepath = ds.OpenCachePath("AppleEmail")
        hash_str = hashlib.md5(node.AbsolutePath).hexdigest()[8:-8]
        self.cache_db = self.cachepath + '\\apple_email_{}.db'.format(hash_str)

        self.account_list = {}            
        self.auto_mail_id = 1
        self.account_id = 0           

    def parse(self):
        try:
            if DEBUG or self.mm.need_parse(self.cache_db, VERSION_APP_VALUE):
                self.mm.db_create(self.cache_db) 
                self.parse_model()
                if not canceller.IsCancellationRequested:
                    self.mm.db_insert_table_version(model_mail.VERSION_KEY_DB, model_mail.VERSION_VALUE_DB)
                    self.mm.db_insert_table_version(model_mail.VERSION_KEY_APP, VERSION_APP_VALUE)
                self.mm.db_commit()
                self.mm.db_close()
            tmp_dir = ds.OpenCachePath('tmp')
            save_cache_path(bcp_mail.MAIL_TOOL_TYPE_PHONE, self.cache_db, tmp_dir)
        except:
            exc()

    def parse_model(self):
        ''' PA Models email  
                Abstract

                Account
                Attachments
                    Owner
                        Account
                Bcc
                Cc (To: 123@qq.com cici)
                Folder
                From ({From: pangu_x02@163.com pangu_x02  })
                OwnerUser
                OwnerUserID
                Size
                Subject
                TimeStamp
                To
                Deleted
        '''
        for email in self.results_model:
            account_user = email.Account.Value
            if account_user not in self.account_list and account_user != 'Default':
                self.account_id += 1
                self.account_list[account_user] = self.account_id
            mail = model_mail.Mail()
            mail.mail_id = self.auto_mail_id
            self.auto_mail_id += 1
            mail.owner_account_id = self.account_id
            mail.mail_group       = email.Folder.Value
            mail.mail_subject     = email.Subject.Value
            mail.mail_abstract    = email.Abstract.Value
            mail.mail_content     = email.Body.Value
            mail.mail_to          = self._handle_mutimodel(email.To)
            mail.mail_cc          = self._handle_mutimodel(email.Cc)
            mail.mail_bcc         = self._handle_mutimodel(email.Bcc)
            mail.mail_sent_date   = self._convert_2_timestamp(email.TimeStamp)
            try:
                if email.From.Value:
                    if email.From.Value.Name.Value:
                        mail.mail_from = email.From.Value.Identifier.Value + ' ' + email.From.Value.Name.Value 
                    else:
                        mail.mail_from = email.From.Value.Identifier.Value
            except:
                pass
            # APPLE_MAIL_READ_STATUS 
            for k, v in APPLE_MAIL_READ_STATUS.items():
                if email.Status.Value == v:
                    mail.mail_read_status = k
            
            mail.source  = email.Source.Value
            mail.deleted = 0 if email.Deleted == DeletedState.Intact else 1
            self.mm.db_insert_table_mail(mail)

            for Attachment in  email.Attachments:
                attach = model_mail.Attachment()
                attach.mail_id             = mail.mail_id
                attach.owner_account_id    = self.account_id
                attach.attachment_name     = Attachment.Filename.Value
                attach.attachment_save_dir = Attachment.URL.Value
                attach.attachment_size     = Attachment.Size.Value
                self.mm.db_insert_table_attachment(attach)

        for k, v in self.account_list.items():
            account = model_mail.Account()
            account.account_id    = v
            account.account_email = k 
            account.account_user  = k      
            self.mm.db_insert_table_account(account)        

    def _handle_mutimodel(self, model):
        ''' mutimodel party '''
        res = ''
        for m in model:
            if m.Identifier.Value:
                if m.Name.Value:
                    res += m.Identifier.Value + ' ' + m.Name.Value + ' '
                else:
                    res += m.Identifier.Value + ' '
        return res.rstrip() if res else None

    @staticmethod
    def _convert_2_timestamp(email_timestamp):
        ''' "2018/8/15 15:27:20" -> 10位 时间戳 1534318040.0
            
        Args:
            email_timestamp (str): email.TimeStamp.Value.Value.LocalDateTime
        Returns:
            (int/float): timastamp e.g. 1534318040.0
        '''
        try:
            if email_timestamp.Value:
                format_time = email_timestamp.Value.Value.LocalDateTime
                div_str = str(format_time)[4]
                if re.match(r'\d', div_str):
                    div_str = ''
                time_pattren = "%Y{div}%m{div}%d %H:%M:%S".format(div=div_str)
                ts = py_time.strptime(str(format_time), time_pattren)
                return py_time.mktime(ts)
            return 0
        except:
            exc()
            return 0   
