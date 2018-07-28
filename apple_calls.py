#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

def analyze_call_history(node, extractDeleted, extractSource):
    """
    新版本通话记录数据解析(C#版本用于解析旧版本数据库)
    """
    pr = ParserResults()
    message='解析通话记录完毕'
    try:
        db = SQLiteParser.Database.FromNode(node)
        if db is None:
            raise Exception('解析通话记录出错:无法读取通话记录数据库')
        ts = SQLiteParser.TableSignature('ZCALLRECORD')
        if extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZANSWERED', 1, 8, 9)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZCALLTYPE', 1, 8, 9)    
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZORIGINATED', 1, 8, 9)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZDATE', 4, 7)
        for rec in db.ReadTableRecords(ts, extractDeleted, True):
            c = Call()
            c.Deleted = rec.Deleted
            # 8 - FaceTime video call, 16 - FaceTime audio call, 1 - 电话音频
            if 'ZCALLTYPE' in rec and (not rec['ZCALLTYPE'].IsDBNull) and  (rec['ZCALLTYPE'].Value == 8 or rec['ZCALLTYPE'].Value == 16):
                c.Source.Value = 'FaceTime'
            SQLiteParser.Tools.ReadColumnToField[bool](rec, 'ZCALLTYPE', c.VideoCall, extractSource, lambda x: True if x == 8 else False)
            if 'ZORIGINATED' in rec and not IsDBNull(rec['ZORIGINATED'].Value) and rec['ZORIGINATED'].Value == 1:
                c.Type.Init(CallType.Outgoing, MemoryRange(rec['ZORIGINATED'].Source) if extractSource else None)
            if c.Type.Value != CallType.Outgoing and 'ZANSWERED' in rec and not IsDBNull(rec['ZANSWERED'].Value):
                if rec['ZANSWERED'].Value == 1:
                    c.Type.Init(CallType.Incoming, MemoryRange(list(rec['ZORIGINATED'].Source) + list(rec['ZANSWERED'].Source)) if extractSource else None)
                else:
                    c.Type.Init(CallType.Missed, MemoryRange(list(rec['ZORIGINATED'].Source) + list(rec['ZANSWERED'].Source)) if extractSource else None)
            if 'ZSERVICE_PROVIDER' in rec and (not rec['ZSERVICE_PROVIDER'].IsDBNull):
                if 'net.whatsapp.WhatsApp' in rec['ZSERVICE_PROVIDER'].Value:
                    c.Source.Value = "WhatsApp Audio"
                if 'com.viber' in rec['ZSERVICE_PROVIDER'].Value:
                    c.Source.Value = "Viber Audio"

            SQLiteParser.Tools.ReadColumnToField[TimeSpan](rec, 'ZDURATION', c.Duration, extractSource, lambda x: TimeSpan.FromSeconds(x))
            SQLiteParser.Tools.ReadColumnToField(rec, 'ZISO_COUNTRY_CODE', c.CountryCode, extractSource)
            try:
                SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, 'ZDATE', c.TimeStamp, extractSource, lambda x: TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan2001(x), True))
                if not c.TimeStamp.Value.IsValidForSmartphone():
                    c.TimeStamp.Init(None, None)
            except:
                pass
            party = Party()
            addr = rec['ZADDRESS'].Value
            if isinstance(addr, Array[Byte]):
                identifier = MemoryRange(rec['ZADDRESS'].Source).read()                
                try:
                    party.Identifier.Value = identifier.decode('utf8')
                except:
                    party.Identifier.Value = identifier
                party.Identifier.Source = MemoryRange(rec['ZADDRESS'].Source) if extractSource else None
            else:
                SQLiteParser.Tools.ReadColumnToField(rec, 'ZADDRESS', party.Identifier, extractSource)
            SQLiteParser.Tools.ReadColumnToField(rec, 'ZNAME', party.Name, extractSource)
            if c.Type.Value == CallType.Missed or c.Type.Value == CallType.Incoming:
                party.Role.Value = PartyRole.From
            elif c.Type.Value == CallType.Outgoing:
                party.Role.Value = PartyRole.To
            c.Parties.Add(party)
            pr.Models.Add(c)
    except:
        traceback.print_exc()
        TraceService.Trace(TraceLevel.Error, "解析出错: {0}".format('通话记录'))
        message = '解析通话记录出错'
    return pr