#coding=utf-8
import os
import PA_runtime
from PA_runtime import *
import clr
try:
    clr.AddReference('System.Data.SQLite')
    clr.AddReference('bcp_basic')
except:
    pass
del clr
import System.Data.SQLite as SQLite
import PA.InfraLib.ModelsV2.CommonEnum.CallType as CallType
import PA.InfraLib.ModelsV2.Base.Contact as Contact
import PA.InfraLib.ModelsV2.Base.Call as Call
import hashlib
import bcp_basic

SQL_CREATE_TABLE_RECORDS = '''
    CREATE TABLE IF NOT EXISTS records(
        id INTEGER,
        phone_number TEXT,
        date INTEGER,
        duration INTEGER,
        type INTEGER,
        name TEXT,
        geocoded_location TEXT,
        ring_times INTEGER,
        mark_type TEXT,
        mark_content TEXT,
        country_code TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_INSERT_TABLE_RECORDS = '''
    INSERT INTO records(id, phone_number, date, duration, type, name, geocoded_location, ring_times, mark_type, mark_content, country_code, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

def analyze_call_history(node, extractDeleted, extractSource):
    """
    新版本通话记录数据解析(C#版本用于解析旧版本数据库)
    """
    pr = ParserResults()
    message='解析通话记录完毕'
    cachepath = ds.OpenCachePath("Calls")
    md5_db = hashlib.md5()
    db_name = 'calls'
    md5_db.update(db_name.encode(encoding = 'utf-8'))
    db_path = cachepath + "\\" + md5_db.hexdigest().upper() + ".db"
    
    if os.path.exists(db_path):
        os.remove(db_path)
    db_cache = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
    db_cache.Open()
    db_cmd = SQLite.SQLiteCommand(db_cache)
    if db_cmd is not None:
        db_cmd.CommandText = SQL_CREATE_TABLE_RECORDS
        db_cmd.ExecuteNonQuery()
    db_cmd.Dispose()

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
            datas = []  #0.id,1.通话类别,2.通话时长,3.country_code,4.通话日期,5.联系人电话,6.联系人名
            datas.append(rec['Z_PK'].Value)
            c.Deleted = rec.Deleted
            # 8 - FaceTime video call, 16 - FaceTime audio call, 1 - 电话音频
            if 'ZCALLTYPE' in rec and (not rec['ZCALLTYPE'].IsDBNull) and  (rec['ZCALLTYPE'].Value == 8 or rec['ZCALLTYPE'].Value == 16):
                c.Source = 'FaceTime'
            field = Field[bool](c.IsVideoCall,None)
            SQLiteParser.Tools.ReadColumnToField[bool](rec, 'ZCALLTYPE', field, extractSource, lambda x: True if x == 8 else False)
            c.IsVideoCall = field.Value
            if 'ZORIGINATED' in rec and not IsDBNull(rec['ZORIGINATED'].Value) and rec['ZORIGINATED'].Value == 1:
                c.Type = CallType.Outgoing
                datas.append(2)
            if c.Type != CallType.Outgoing and 'ZANSWERED' in rec and not IsDBNull(rec['ZANSWERED'].Value):
                if rec['ZANSWERED'].Value == 1:
                    c.Type = CallType.Incoming
                    datas.append(1)
                else:
                    c.Type = CallType.Missed
                    datas.append(3)
            if len(datas) is 0:
                datas.append(None)
            if 'ZSERVICE_PROVIDER' in rec and (not rec['ZSERVICE_PROVIDER'].IsDBNull):
                if 'net.whatsapp.WhatsApp' in rec['ZSERVICE_PROVIDER'].Value:
                    c.Source = "WhatsApp Audio"
                if 'com.viber' in rec['ZSERVICE_PROVIDER'].Value:
                    c.Source = "Viber Audio"
            
            field = Field[Nullable[TimeSpan]](c.Duration,None)
            SQLiteParser.Tools.ReadColumnToField[Nullable[TimeSpan]](rec, 'ZDURATION', field, extractSource, lambda x: TimeSpan.FromSeconds(x))
            c.Duration = field.Value
            datas.append(rec['ZDURATION'].Value)
            field = Field[String](c.CountryCode,None)
            SQLiteParser.Tools.ReadColumnToField(rec, 'ZISO_COUNTRY_CODE', field, extractSource)
            c.CountryCode = field.Value
            datas.append(rec['ZISO_COUNTRY_CODE'].Value)
            try:
                field = Field[TimeStamp](c.StartTime)
                SQLiteParser.Tools.ReadColumnToField[TimeStamp](rec, 'ZDATE', field, extractSource, lambda x: TimeStamp(TimeStampFormats.GetTimeStampEpoch1Jan2001(x), True))
                c.StartTime = field.Value
                if not c.StartTime.IsValidForSmartphone():
                    c.StartTime = None
            except:
                pass
            datas.append(rec['ZDATE'].Value)
            party = Contact()
            addr = rec['ZADDRESS'].Value
            if isinstance(addr, Array[Byte]):
                identifier = MemoryRange(rec['ZADDRESS'].Source).read()
                datas.append(identifier)
                try:
                    party.PhoneNumbers.Add(identifier.decode('utf8'))
                except:
                    party.PhoneNumbers.Add(identifier)
            else:
                field = Field[String](None,None)
                SQLiteParser.Tools.ReadColumnToField(rec, 'ZADDRESS', field, extractSource)
                party.PhoneNumbers.Add(field.Value)
                datas.append(rec['ZADDRESS'])
            field = Field[String](None,None)
            SQLiteParser.Tools.ReadColumnToField(rec, 'ZNAME', field, extractSource)
            party.FullName = field.Value
            datas.append(rec['ZNAME'].Value)
            if c.Type == CallType.Missed or c.Type == CallType.Incoming:
                c.FromSet.Add(party)
            elif c.Type == CallType.Outgoing:
                c.ToSet.Add(party)
            pr.Models.Add(c)
            param = (datas[0],datas[5],datas[4],datas[2],datas[1],datas[6],rec['ZLOCATION'].Value, None, None, None,datas[3],node.AbsolutePath,rec.Deleted,0)
            db_insert_table(db_cache, SQL_INSERT_TABLE_RECORDS, param)
        db_cache.Close()
        #bcp entry
        temp_dir = ds.OpenCachePath('tmp')
        PA_runtime.save_cache_path(bcp_basic.BASIC_CONTACT_INFORMATION, db_path, temp_dir)
    except:
        traceback.print_exc()
        TraceService.Trace(TraceLevel.Error, "解析出错: {0}".format('通话记录'))
        message = '解析通话记录出错'
    pr.Build('本机通话')
    return pr

def db_insert_table(db, sql, values):
    db_cmd = SQLite.SQLiteCommand(db)
    if db_cmd is not None:
        db_cmd.CommandText = sql
        db_cmd.Parameters.Clear()
        for value in values:
            param = db_cmd.CreateParameter()
            param.Value = value
            db_cmd.Parameters.Add(param)
        db_cmd.ExecuteNonQuery()