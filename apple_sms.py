#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

import clr
clr.AddReference('PNFA.iPhoneApps')
del clr

from PA.iPhoneApps.Parsers import MessageParser

def analyze_sms_spotlight(node, extractDeleted, extractSource, existingMessages):
    # 只负责解码已经删除的短信
    if not extractDeleted:
        return    
    db = SQLiteParser.Database.FromNode(node)
    if db is None:
        return
    results = []
    chats = {}
    messages = {}
    last_messages = {}
    if 'Content' in db.Tables:
        ts = SQLiteParser.TableSignature('Content')
        if extractDeleted:
            ts['title'] = ts['external_id'] = ts['content'] = ts['subtitle'] = ts['summary'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableRecords(ts, extractDeleted):            
            if IsDBNull(rec['external_id'].Value) or IsDBNull(rec['title'].Value):
                continue
            msg_id = None
            ids = rec['external_id'].Value.split(':')
            if rec['external_id'].Value.startswith('madrid'):
                chat_id = rec['title'].Value
                if len(ids) == 10:
                    msg_id = ids[3]            
            else:
                chat_id = ids[0]
                if len(ids) > 1:
                    msg_id = ids[1]                                        

            if chat_id not in chats:
                chats[chat_id] = Chat()
                chats[chat_id].Source.Value = 'SMS Spotlight 搜索'
                party = Party(rec['title'].Value)
                if extractSource:
                    party.Identifier.Source = MemoryRange(rec['title'].Source)
                chats[chat_id].Participants.Add(party)
            c = chats[chat_id]
            if len(ids) == 1 and not IsDBNull(rec['content'].Value):           
                try:     
                    c.Participants[0].Name.Value = rec['content'].Value.split(rec['title'].Value)[1]
                    if extractSource:
                        c.Participants[0].Name.Source = MemoryRange(rec['title'].Source)                    
                except:
                    pass                
            im = InstantMessage()
            im.SourceApplication.Value = 'SMS Spotlight 搜索' 
            im.Deleted = rec.Deleted           
            key = None
            if msg_id != None and not IsDBNull(rec['content'].Value):
                key = 'content'
            elif 'summary' in rec and not IsDBNull(rec['summary'].Value):                    
                key = 'summary'
            elif 'subtitle' in rec and not IsDBNull(rec['subtitle'].Value):
                key = 'subtitle'
            if key:
                im.Body.Value = rec[key].Value
                if extractSource:
                    im.Body.Source = MemoryRange(rec[key].Source)  
            
            if msg_id != None:
                if msg_id not in messages:                
                    if msg_id in existingMessages:
                        im.Deleted = existingMessages[msg_id].Deleted
                        im.TimeStamp.Value = existingMessages[msg_id].TimeStamp.Value
                        if extractSource:
                            im.TimeStamp.Source = existingMessages[msg_id].TimeStamp.Source
                    else:
                        im.Deleted = DeletedState.Deleted
                    c.Messages.Add(im)
                    messages[msg_id] = im
            elif chat_id not in last_messages:
                im.Deleted = DeletedState.Deleted
                last_messages[chat_id] = im   
            else:
                continue
            if c.Deleted != DeletedState.Intact:
                c.Deleted = im.Deleted
    elif 'ZSPRECORD' in db.Tables:
        ts = SQLiteParser.TableSignature('ZSPRECORD')
        if extractDeleted:
            ts['ZCONTENT'] = ts['ZEXTID'] = ts['ZSUMMARY'] = ts['ZTITLE'] = SQLiteParser.Signatures.SignatureFactory.GetFieldSignature(SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        for rec in db.ReadTableRecords(ts, extractDeleted):
            if IsDBNull(rec['ZTITLE'].Value):
                continue
            if IsDBNull(rec['ZEXTID'].Value):
                continue
            if rec['ZTITLE'].Value not in chats:
                chats[rec['ZTITLE'].Value] = Chat()
                chats[rec['ZTITLE'].Value].Source.Value = 'SMS Spotlight Search'
                party = Party()
                party.Name.Value = rec['ZTITLE'].Value
                if extractSource:
                    party.Name.Source = MemoryRange(rec['ZTITLE'].Source)
                chats[rec['ZTITLE'].Value].Participants.Add(party)
            c = chats[rec['ZTITLE'].Value]
            if rec['ZEXTID'].Value.startswith('chat_guid'):
                if not IsDBNull(rec['ZSUMMARY'].Value):
                    im = InstantMessage()
                    im.SourceApplication.Value = 'SMS Spotlight Search'
                    im.Deleted = DeletedState.Deleted
                    im.Body.Value = rec['ZSUMMARY'].Value
                    if extractSource:
                        im.Body.Source = MemoryRange(rec['ZSUMMARY'].Source)                    
                    last_messages[rec['ZTITLE'].Value] = im
                if not IsDBNull(rec['ZCONTENT'].Value) and rec['ZTITLE'].Value != '':
                    split = rec['ZTITLE'].Value.replace('+','').replace('-','').replace('.','').replace('@','')
                    if split != '':
                        ids = rec['ZCONTENT'].Value.split()
                        if len(ids) == 2:
                            c.Participants[0].Identifier.Value = ids[0]
                            if extractSource:
                                c.Participants[0].Identifier.Source = MemoryRange(rec['ZCONTENT'].Source)
                        elif len(ids) == 3:
                            c.Participants[0].Identifier.Value = rec['ZCONTENT'].Value.split(' ', 1)[0]
                            if extractSource:
                                c.Participants[0].Identifier.Source = MemoryRange(rec['ZCONTENT'].Source)
                        elif len(ids) == 1:     
                            try:                   
                                c.Participants.Clear()
                                names = rec['ZTITLE'].Value.split(' ')
                                ids = rec['ZCONTENT'].Value.split(' ')
                                for name in names:  
                                    if name in ids:                          
                                        ids.remove(name)
                                content = rec['ZCONTENT'].Value
                                while len(ids) > 0:
                                    ident = ids.pop(-1)
                                    content, name = content.rsplit(ident, 1)
                                    party = Party(ident)
                                    party.Name.Value = name
                                    if extractSource:
                                        party.Identifier.Source = MemoryRange(rec['ZCONTENT'].Source)
                                        party.Name.Source = MemoryRange(rec['ZTITLE'].Source)
                                    if party not in c.Participants:
                                        c.Participants.Add(party)
                            except:
                                pass
            elif rec['ZEXTID'].Value.startswith('message_guid'):
                im = InstantMessage()
                im.Deleted = rec.Deleted
                im.SourceApplication.Value = 'SMS Spotlight Search'
                try:
                    guid = rec['ZEXTID'].Value.split('message_guid=')[1]
                except:
                    guid = ""
                if guid in existingMessages:
                    im.Deleted = existingMessages[guid].Deleted                    
                    im.TimeStamp.Value = existingMessages[guid].TimeStamp.Value
                    if extractSource:
                        im.TimeStamp.Source = existingMessages[guid].TimeStamp.Source
                else:
                    im.Deleted = DeletedState.Deleted
                if not IsDBNull(rec['ZSUMMARY'].Value):
                    im.Body.Value = rec['ZSUMMARY'].Value
                    if extractSource:
                        im.Body.Source = MemoryRange(rec['ZSUMMARY'].Source)
                elif not IsDBNull(rec['ZCONTENT'].Value):
                    im.Body.Value = rec['ZSUMMARY'].Value
                    if extractSource:
                        im.Body.Source = MemoryRange(rec['ZSUMMARY'].Source)
                if c.Deleted != DeletedState.Intact:
                    c.Deleted = im.Deleted
                if im not in c.Messages:
                    c.Messages.Add(im)

    for id in chats:
        c = chats[id]
        if c.Deleted == DeletedState.Unknown:
            c.Deleted = DeletedState.Deleted
        if id in last_messages:
            last = last_messages[id]
            exist = False
            for im in c.Messages:
                if last.Body.Value in im.Body.Value:
                    exist = True
                    break
            if not exist:
                c.Messages.Add(last)
        
        if c.Messages.Count > 0:
            c.SetTimesByMessages()
            c.SetParticipantsByMessages()
            results.append(c)
        
    pr = ParserResults()
    pr.Models.AddRange(results)
    pr.Build('已删除短信')
    return pr

EXTEND_SMS = [
    ("/Library/Spotlight/com\.apple\.MobileSMS/SMSSearchdb\.sqlitedb$", analyze_sms_spotlight),
    ("/Library/Spotlight/com\.apple\.MobileSMS/SMSSearchindex\.sqlite$", analyze_sms_spotlight),
]

def decode_smss(fs, extract_deleted, extract_source):
    smss = ParserResults() 
    existing_sms = {}

    first_time = True
    for d in list(fs.SearchNodesExactPath("Library/SMS")):
        if d.Type == NodeType.Directory:   
            if first_time:
                TraceService.Trace(TraceLevel.Info, "正在解析 {0}".format("短信"))
                first_time = False
            try:  
                parser = MessageParser(d, extract_deleted, extract_source)      
                if parser.Error:                        
                    raise Exception("解析短信数据库出错")
                smss.Models.AddRange(parser.Results)
                existing_sms.update(parser.ExistingMessages)
            except:
                traceback.print_exc()

    for pattern, func in EXTEND_SMS:
        first_time = True
        
        for node in list(fs.Search(pattern)):
            if first_time:
                TraceService.Trace(TraceLevel.Info, "Parsing {0}".format("扩展短信"))
                first_time = False
            try:
                time_start = time.time()    
                smss += func(node, extract_deleted, extract_source, existing_sms)       
            except:
                traceback.print_exc()
    return smss

def analyze_smss(fs, extract_deleted, extract_source):
    name = "SMS"
    app_id = "com.apple.MobileSMS"
    ds.ApplicationsManager.AddTag(name, app_id)
    sms_results = decode_smss(fs, extract_deleted, extract_source)   
    sms_results.Build('短信')
    return sms_results