#coding=utf-8
import os
import PA_runtime

import clr
clr.AddReference('PA.iPhoneApps')
del clr

from PA_runtime import *
from PA.iPhoneApps import *

from urllib import unquote
from urlparse import urlsplit, uses_netloc
from os.path import basename
from collections import defaultdict
from email import message_from_string
from sys import maxint
from collections import deque, namedtuple
from System import DateTime, DateTimeKind


DataRecord = namedtuple("DataRecord", "part partial complete data file")
patt = re.compile(r'["\']*([a-zA-Z0-9@&+=)(.,_ -]+)["\']* *(?:<([a-zA-Z0-9@+=._-]+)>)*')


import string 
hexdigits = set(string.hexdigits)

# iron python bug fix
import quopri
quopri.b2a_qp = quopri.a2b_qp = None

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

status_dict = {
    0: MessageStatus.Unread,
    1: MessageStatus.Read
    }

content_type_priority = {
    'text/html': 1,
    'text/plain': 2
    }

epoch = DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)

def split_addresses(value):
    return [create_party(st) for st in value.splitlines() if len(st) > 0]

def create_party(st):    
    m = patt.match(st)
    if m:
        g = m.groups()
        if g[1] == None:
            return g[0], None
        else:
            return g[1], g[0]
    return None, None

uses_netloc.append('pop')
uses_netloc.append('as')

def add_embedded (f, attachments, model = None):
    for att in attachments:
        if att.Filename.Value:
            embedded_file = Node (att.Filename.Value, NodeType.Embedded | NodeType.File)
        else :
            embedded_file = Node ("unknown", NodeType.Embedded | NodeType.File)
        embedded_file.Deleted = f.Deleted
        embedded_file.Data = att.Data.Source
        f.Children.Add(embedded_file)
        SourceEvent.CreateSourceEvent(f, model)

def get_mailboxes(db, mail_dir, extractDeleted, extractSource):
    mailbox = namedtuple("mailbox", "folder path account source")

    fs_mailboxes = {}    
    
    for m in mail_dir.Directories:
        q = deque()
        for d in m.Directories:
            q.append(d.Name)

        while len(q) > 0:
            next_dir = q.popleft()
            d = m.GetByPath(next_dir)
            if d is not None:
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
    for record in db['mailboxes']:
        key = record['ROWID'].Value
        data = record['url'].Value        
        if IsDBNull(data):
            continue        
        path = Uri.UnescapeDataString(data)
        folder = basename(path)
        account = urlsplit(path).netloc
        if account.count('@') > 1:
            account = account.rsplit('@', 1)[0]

        if folder in fs_mailboxes:
            mailboxes[key] = mailbox(fs_mailboxes[folder][0], fs_mailboxes[folder][1], account, MemoryRange(record['url'].Source))
        else:
            folder = urlsplit(path).path.strip('/')
            path = path.replace('\\','_').replace('://', '-')
            mailboxes[key] = mailbox(folder, path, account, MemoryRange(record['url'].Source))

    return mailboxes

def get_data_records(db, extractDeleted, extractSource):
    part_patt = re.compile(r'^summary{1}$|^[0-9.]+$')
    parts = defaultdict(list)
    ts = SQLiteParser.TableSignature('message_data')
    if extractDeleted:
        ts['message_id'] = SQLiteParser.Signatures.NumericSet(2)
        ts['complete'] = ts['partial'] = SQLiteParser.Signatures.NumericSet(1, 8, 9)
        ts['part'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text)
    for record in db.ReadTableRecords(ts, extractDeleted, True):
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

def get_emlx_files(mail):
    emlx = {}
    emlxpart = defaultdict(list)
    for d in mail.GetAllNodes(NodeType.Directory):
        if d.Name.endswith("mbox"):
            messages = d.GetFirstNode("Messages")
            if messages != None:
                for f in messages.Glob("*.emlx*"):
                    parts = f.Name.split(".", 1)
                    message_id = parts[0]
                    if parts[1] == "emlx":
                        emlx[message_id] = f
                    elif parts[1].endswith("emlxpart"):
                        f.Data.seek(0)
                        data = f.Data.read()
                        part_num = parts[1].rsplit(".", 1)[0]
                        emlxpart[message_id].append((part_num, DataRecord(part_num, "", 1, data, f)))

    return emlx, emlxpart

def read_mail(mail_dir, envelope_db, protected_db, extractDeleted, extractSource):

    results = defaultdict(list)
    messages = defaultdict(set)
    
    mailboxes = get_mailboxes(envelope_db, mail_dir, extractDeleted, extractSource)
    part_records = get_data_records(envelope_db, extractDeleted, extractSource)

    emlx_files, part_files = get_emlx_files(mail_dir)

    message_data = {}
    ts = SQLiteParser.TableSignature('message_data')
    if extractDeleted:
        ts['data'] = SQLiteParser.Signatures.NumericSet(12, 13)    
    for record in protected_db.ReadTableRecords(ts, extractDeleted):
        if record['message_data_id'].Value < 1:
            continue
        if IsDBNull (record['data'].Value):
            continue
        if isinstance(record['data'].Value, str):
            message_data[record['message_data_id'].Value] = record['data'].Value
        elif isinstance(record['data'].Value, Array[Byte]):
            message_data[record['message_data_id'].Value] = "".join(map(chr,record['data'].Value))
        
    ts = SQLiteParser.TableSignature('messages')
    if extractDeleted:
        ts['sender'] = ts['subject'] = ts['_to'] = ts['cc'] = ts['bcc'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text)
    for record in protected_db.ReadTableRecords(ts, extractDeleted):
        if record['message_id'].Value < 1:
            continue

        msg = Email()
        msg.Source.Value = "Mails"
        if record.ContainsKey('deleted') and record['deleted'].Value not in [0, '0']:
            msg.Deleted = DeletedState.Deleted
        else:
            msg.Deleted = record.Deleted
        if not IsDBNull(record['sender'].Value):
            email_address, name = create_party(record['sender'].Value)
            if email_address == None:
                if extractSource:        
                    msg.From.Value = Party.MakeFrom(record['sender'].Value, MemoryRange(record['sender'].Source))
                else:
                    msg.From.Value = Party.MakeFrom(record['sender'].Value, None)
            else:
                if extractSource:
                    msg.From.Value = Party.MakeFrom(email_address, MemoryRange(record['sender'].Source))
                else:
                    msg.From.Value = Party.MakeFrom(email_address, None)
                if name != None and name != email_address:
                    msg.From.Value.Name.Value = name
        if not IsDBNull(record['subject'].Value):
            msg.Subject.Value = record['subject'].Value
            if extractSource:
                msg.Subject.Source = MemoryRange(record['subject'].Source)
        if not IsDBNull(record['_to'].Value):
            for to, name in split_addresses(record['_to'].Value):
                if to == None:
                    continue
                if extractSource:
                    party = Party.MakeTo(to, MemoryRange(record['_to'].Source))
                else:
                    party = Party.MakeTo(to, None)
                if name != None and name != email_address:
                    party.Name.Value = name
                msg.To.Add(party)
        if not IsDBNull(record['cc'].Value):
            for cc, name in split_addresses(record['cc'].Value):
                if cc == None:
                    continue
                if extractSource:
                    party = Party.MakeTo(cc, MemoryRange(record['cc'].Source))
                else:
                    party = Party.MakeTo(cc, None)
                if name != None and name != email_address:
                    party.Name.Value = name
                msg.Cc.Add(party)                
        if not IsDBNull(record['bcc'].Value):                        
            for bcc, name in split_addresses(record['bcc'].Value):
                if bcc == None:
                    continue
                if extractSource:
                    party = Party.MakeTo(bcc, MemoryRange(record['bcc'].Source))
                else:
                    party = Party.MakeTo(bcc, None)
                if name != None and name != email_address:
                    party.Name.Value = name
                msg.Bcc.Add(party)
        if msg not in messages[record['message_id'].Value]:
            messages[record['message_id'].Value].add(msg)
            results[record['message_id'].Value].append(msg)
        
    for record in envelope_db['messages_deleted']:
        if record['message_id'].Value in results:
            if extractDeleted:
                results[record['message_id'].Value][0].Deleted = DeletedState.Deleted
            else:
                results.pop(record['message_id'].Value)

    final_results = []
    ts = SQLiteParser.TableSignature('messages')
    if extractDeleted:
        ts['date_sent'] = ts['date_received'] = SQLiteParser.Signatures.NumericSet(4)
        ts['external_id'] = ts['read'] = ts['deleted'] = ts['flagged'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        ts['mailbox'] = SQLiteParser.Signatures.NumericSet(1)    
    for record in envelope_db.ReadTableRecords(ts, extractDeleted, True):
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
            if not extractDeleted:
                continue
            msg.Deleted = DeletedState.Deleted
            
        if not IsDBNull(record['date_sent'].Value):
            time = TimeStamp(epoch.AddSeconds(record['date_sent'].Value), True)
            
            if time.IsValidForSmartphone():
                msg.TimeStamp.Value = time
                if extractSource:
                    msg.TimeStamp.Source = MemoryRange(record['date_sent'].Source)
        if  record['read'].Value.isnumeric():            
            if int(record['read'].Value) in status_dict:
                msg.Status.Value = status_dict[int(record['read'].Value)]
                if extractSource:
                    msg.Status.Source = MemoryRange(record['read'].Source)
        
        if not IsDBNull(record['mailbox'].Value) and record['mailbox'].Value in mailboxes:
            mailbox = mailboxes[record['mailbox'].Value]
            msg.Folder.Value = mailbox.folder
            if extractSource:
                msg.Folder.Source = mailbox.source
            msg.Account.Value = mailbox.account
            if extractSource:
                msg.Account.Source = mailbox.source

        base_name = record['external_id'].Value
        emlx_file = emlx_files.get(base_name, None)

        if emlx_file != None:
            emlx_file.Data.seek(0)
            data = emlx_file.read()

            m = message_from_string(data)
            body, attachments = read_from_mime(m, None)
            add_embedded (emlx_file, attachments, msg)
            
        else:
            parts = {}
            parts.update(part_files[base_name])

            if record['ROWID'].Value in part_records:
                for part_record in part_records[record['ROWID'].Value]:     
                    if IsDBNull(part_record['ROWID'].Value) or part_record['ROWID'].Value not in message_data:
                        continue
                    part = part_record['part'].Value
                    if IsDBNull(part_record['part'].Value):
                        part = ""
                    parts[part] = DataRecord(
                        part,
                        part_record['partial'].Value,
                        part_record['complete'].Value,
                        message_data[part_record['ROWID'].Value],
                        mail_dir.GetFirstNode("Protected Index"))

            try:
                body, attachments = read_from_parts(parts)
            except UnicodeDecodeError:
                body, attachments = "", []

        if body != "":
            msg.Body.Value = body
        msg.Attachments.AddRange(attachments)
        final_results.extend(results.pop(record['ROWID'].Value))
    
    for res in results.values():
        for msg in res:
            if hasContent(msg):
                final_results.append(msg)
    return final_results

def read_old_mail(mail_dir, envelope_db, extractDeleted, extractSource):

    mailboxes = get_mailboxes(envelope_db, mail_dir, extractDeleted, extractSource)
    part_records = get_data_records(envelope_db, extractDeleted, extractSource)

    emlx_files, part_files = get_emlx_files(mail_dir)

    results = []
    ts = SQLiteParser.TableSignature('messages')
    if extractDeleted:
        ts['read'] = ts['sender'] = ts['subject'] = ts['_to'] = ts['cc'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text)
        ts['date_sent'] = SQLiteParser.Signatures.NumericSet(4)   
        ts['mailbox'] = SQLiteParser.Signatures.NumericSet(0, 1)
    for record in envelope_db.ReadTableRecords(ts, extractDeleted, True):
        msg = Email()
        msg.Source.Value = "Mails"
        if record.ContainsKey('deleted') and record['deleted'].Value not in [0, '0']:
            if extractDeleted:
                msg.Deleted = DeletedState.Deleted
            else:
                continue
        else:
            msg.Deleted = record.Deleted
        if 'sender' in record and not IsDBNull(record['sender'].Value):
            email_address, name = create_party(record['sender'].Value)
            if email_address == None:            
                if extractSource:
                    msg.From.Value = Party.MakeFrom(record['sender'].Value, MemoryRange(record['sender'].Source))
                else:
                    msg.From.Value = Party.MakeFrom(record['sender'].Value, None)
            else:
                if extractSource:
                    msg.From.Value = Party.MakeFrom(email_address, MemoryRange(record['sender'].Source))
                else:
                    msg.From.Value = Party.MakeFrom(email_address, None)
                if name != None and name != email_address:
                    msg.From.Value.Name.Value = name
        if 'subject' in record and not IsDBNull(record['subject'].Value):
            msg.Subject.Value = record['subject'].Value
            if extractSource:
                msg.Subject.Source = MemoryRange(record['subject'].Source)
        if '_to' in record and not IsDBNull(record['_to'].Value):
            for to, name in split_addresses(record['_to'].Value):
                if to == None:
                    continue
                if extractSource:
                    party = Party.MakeTo(to, MemoryRange(record['_to'].Source))
                else:
                    party = Party.MakeTo(to, None)
                if name != None and name != email_address:
                    party.Name.Value = name
                msg.To.Add(party)
        if 'cc' in record and not IsDBNull(record['cc'].Value):
            for cc, name in split_addresses(record['cc'].Value):
                if cc == None:
                    continue
                if extractSource:
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
                if extractSource:
                    msg.TimeStamp.Source = MemoryRange(record['date_sent'].Source)
        if 'read' in record and not IsDBNull(record['read'].Value) and record['read'].Value in [0, 1, '0', '1']:
            msg.Status.Value = status_dict[int(record['read'].Value)]
            if extractSource:
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
            if extractSource:
                msg.Folder.Source = mailbox.source
                msg.Account.Source = mailbox.source

        if emlx_file != None:
            emlx_file.Data.seek(0)
            data = emlx_file.read()

            m = message_from_string(data)
            body, attachments = read_from_mime(m, None)
            add_embedded (emlx_file, attachments, msg)

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
                        mail_dir.GetFirstNode("Envelope Index"))

            body, attachments = read_from_parts(parts)

        if body != "":
            msg.Body.Value = body
        msg.Attachments.AddRange(attachments)
        if hasContent(msg):
            results.append(msg)

    return results

def read_from_parts(parts):
    body, attachments = "", []

    if '' in parts:
        m = build_mime(parts)
        body, attachments = read_from_mime(m, parts)
        if (body == "" or body.isspace()) and attachments == [] and parts.has_key('summary'):
            body, attachments = parts['summary'].data, []
        elif (body == "" or body.isspace()) and parts.has_key('summary'):
            body = parts['summary'].data

    elif 'summary' in parts:
        body, attachments = parts['summary'].data, []

    elif len(parts) > 0:
        pass

    return body, attachments

def build_mime(parts):
    main_part = parts.pop('')
    m = message_from_string(main_part.data)

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

def create_attachment_from_mime_message(msg):
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

def read_from_mime(m, parts):
    
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

                att = create_attachment_from_mime_message(part)
                if att is None:
                    continue
                attachments.append(att)
                if parts is not None and len(parts) > 1 and str(n + 1) in parts:                    
                    add_embedded (parts[str(n + 1)].file, [att])

                n += 1

        else:
            for part in m.walk():
                if part.get_content_maintype() not in ['audio', 'image', 'video']:
                    continue
                att = create_attachment_from_mime_message(part)
                if att is None:
                    continue
                attachments.append(att)
                if parts is not None and len(parts) > 1 and str(n + 1) in parts:                    
                    add_embedded (parts[str(n + 1)].file, [att])
                
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

def hasContent(msg):
	return msg.Body.Value is not None or msg.From.Value is not None or msg.TimeStamp.Value is not None or msg.To.Count > 0 or msg.Subject.Value is not None or msg.Attachments.Count > 0

def analyze_emails(mail_dir, extractDeleted, extractSource):
    pr = ParserResults()
    envelope_file = mail_dir.GetFirstNode("Envelope Index")
    if envelope_file == None:
        return pr

    envelope_db = SQLiteParser.Database.FromNode(envelope_file)
    if envelope_db == None:
        return pr
    
    results = []

    protected_file = mail_dir.GetFirstNode("Protected Index")
    if protected_file != None:
        protected_db = SQLiteParser.Database.FromNode(protected_file)
        if protected_db == None:
            return pr
        results.extend(read_mail(mail_dir, envelope_db, protected_db, extractDeleted, extractSource))
    else:
        results.extend(read_old_mail(mail_dir, envelope_db, extractDeleted, extractSource))

    pr.Models.AddRange(results)
    pr.Build('系统邮件')
    return pr