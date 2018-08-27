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
        db = SQLiteParser.Database.FromNode(node)  #node为应用路径，连接该路径下的数据库
        if db is None:
            raise Exception('解析通话记录出错:无法读取通话记录数据库')
        ts = SQLiteParser.TableSignature('ZCALLRECORD')  #读取ZCALLRECORD数据表
        if extractDeleted: 
            #读取ZCALLRECORD数据表中字段
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZANSWERED', 1, 8, 9)  #是否接听  
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZCALLTYPE', 1, 8, 9)    #电话类型
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZORIGINATED', 1, 8, 9)  #检验电话是拨号还是接听
            SQLiteParser.Tools.AddSignatureToTable(ts, 'ZDATE', 4, 7)  #电话日期

        #数据筛选
        for rec in db.ReadTableRecords(ts, extractDeleted, True):  #读取数据表ZCALLRECORD中extraDeleted为True的数据
            c = Call()  # C#数据库模型类Call
            c.Deleted = rec.Deleted
            # 8 - FaceTime video call, 16 - FaceTime audio call, 1 - 电话音频
            if 'ZCALLTYPE' in rec and (not rec['ZCALLTYPE'].IsDBNull) and  (rec['ZCALLTYPE'].Value == 8 or rec['ZCALLTYPE'].Value == 16):  #提取出FaceTime音视频
                c.Source.Value = 'FaceTime'
            SQLiteParser.Tools.ReadColumnToField[bool](rec, 'ZCALLTYPE', c.VideoCall, extractSource, lambda x: True if x == 8 else False)  #传判断结果（视频通话or音频通话）给应用层（保存到c.VideoCall）

            if 'ZORIGINATED' in rec and not IsDBNull(rec['ZORIGINATED'].Value) and rec['ZORIGINATED'].Value == 1:  #提取出拨号记录
                c.Type.Init(CallType.Outgoing, MemoryRange(rec['ZORIGINATED'].Source) if extractSource else None)  #MemoryRange读取字段位置

            if c.Type.Value != CallType.Outgoing and 'ZANSWERED' in rec and not IsDBNull(rec['ZANSWERED'].Value):  #提取来电记录
                if rec['ZANSWERED'].Value == 1:  #已接听
                    c.Type.Init(CallType.Incoming, MemoryRange(list(rec['ZORIGINATED'].Source) + list(rec['ZANSWERED'].Source)) if extractSource else None)
                else:  #未接听
                    c.Type.Init(CallType.Missed, MemoryRange(list(rec['ZORIGINATED'].Source) + list(rec['ZANSWERED'].Source)) if extractSource else None)
            
            if 'ZSERVICE_PROVIDER' in rec and (not rec['ZSERVICE_PROVIDER'].IsDBNull):  #提取通话服务提供源
                if 'net.whatsapp.WhatsApp' in rec['ZSERVICE_PROVIDER'].Value:   #判断是否是WhatsApp
                    c.Source.Value = "WhatsApp Audio"
                if 'com.viber' in rec['ZSERVICE_PROVIDER'].Value:   #判断是否是viber
                    c.Source.Value = "Viber Audio"

            SQLiteParser.Tools.ReadColumnToField[TimeSpan](rec, 'ZDURATION', c.Duration, extractSource, lambda x: TimeSpan.FromSeconds(x))   #引入TimeSpan模块计算时间差（数据行，字段，传递给C#中字段名称，提取源，传递的数据）
            SQLiteParser.Tools.ReadColumnToField(rec, 'ZISO_COUNTRY_CODE', c.CountryCode, extractSource)  
            try:
                SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, 'ZDATE', c.TimeStamp, extractSource, lambda x: TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan2001(x), True))  #引入TimeStamp模块
                if not c.TimeStamp.Value.IsValidForSmartphone():
                    c.TimeStamp.Init(None, None)
            except:   
                pass
            party = Party()  # C#数据库模型类Party
            addr = rec['ZADDRESS'].Value
            if isinstance(addr, Array[Byte]):
                identifier = MemoryRange(rec['ZADDRESS'].Source).read()                
                try:
                    party.Identifier.Value = identifier.decode('utf8')  #解码成中文字符后保存
                except:
                    party.Identifier.Value = identifier #解码失败直接保存
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