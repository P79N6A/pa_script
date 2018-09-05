#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

def analyze_notes(node, extractDeleted, extractSource):
    pr = ParserResults()
    db = SQLiteParser.Database.FromNode(node)
    if db is None:
        return
    ts = SQLiteParser.TableSignature('ZNOTEBODY')
    if extractDeleted:
        ts['ZCONTENT'] = TextNotNull
        ts['Z_OPT'] = SQLiteParser.Signatures.NumericSet(1)

    body_dic = {}
    
    for record in db.ReadTableRecords(ts, extractDeleted):
        if IsDBNull(record['ZCONTENT'].Value):
            continue
        
        if record.Deleted == DeletedState.Intact:
            key = record['Z_PK'].Value
            if key not in body_dic:
                body_dic[key] = record['ZCONTENT']
        else:
            res = Note()
            res.Deleted = record.Deleted
            SQLiteParser.Tools.ReadColumnToField(record, "ZCONTENT", res.Body, extractSource)
            pr.Models.Add(res)

    ts = SQLiteParser.TableSignature('ZNOTE')
    if extractDeleted:
        ts['ZTITLE'] = TextNotNull
        ts['ZBODY'] = IntNotNull
        ts['ZMODIFICATIONDATE'] = ts['ZCREATIONDATE'] = SQLiteParser.Signatures.NumericSet(4, 7)
        ts['Z_OPT'] = ts['ZCONTAINSCJK'] = ts['ZEXTERNALFLAGS'] = ts['ZDELETEDFLAG'] = SQLiteParser.Signatures.NumericSet(1)

    
    for record in db.ReadTableRecords(ts, extractDeleted, True):
        if not record['ZBODY'].Value in body_dic:
            continue
        if record['ZCREATIONDATE'].Value == 0 or record['ZMODIFICATIONDATE'].Value == 0:
            continue

        res = Note()
        res.Deleted = record.Deleted
        if not IsDBNull(record['ZTITLE'].Value):
            res.Title.Value = record['ZTITLE'].Value
            if extractSource:
                res.Title.Source = MemoryRange(record['ZTITLE'].Source)
        if not IsDBNull(record['ZSUMMARY'].Value):        
            res.Summary.Value = record['ZSUMMARY'].Value
            if extractSource:
                res.Summary.Source = MemoryRange(record['ZSUMMARY'].Source)
        if not IsDBNull(body_dic[record['ZBODY'].Value].Value):
            res.Body.Value = body_dic[record['ZBODY'].Value].Value
            if extractSource:
                res.Body.Source = MemoryRange(body_dic[record['ZBODY'].Value].Source)
        if not IsDBNull(record['ZCREATIONDATE'].Value):
            try:
                res.Creation.Value = TimeStamp(epoch.AddSeconds(record['ZCREATIONDATE'].Value), True)
                if extractSource:
                    res.Creation.Source = MemoryRange(record['ZCREATIONDATE'].Source)
            except:
                pass
        if not IsDBNull(record['ZMODIFICATIONDATE'].Value):
            try:
                res.Modification.Value = TimeStamp(epoch.AddSeconds(record['ZMODIFICATIONDATE'].Value), True)
                if extractSource:
                    res.Modification.Source = MemoryRange(record['ZMODIFICATIONDATE'].Source)
            except:
                pass
        pr.Models.Add(res)
    pr.Build('备忘录')
    return pr

def analyze_old_notes(node, extractDeleted, extractSource):
    db = SQLiteParser.Database.FromNode(node)
    if db is None:
        return

    ts = SQLiteParser.TableSignature('note_bodies')
    if extractDeleted:
        ts['data'] = TextNotNull 

    body_dic = {}
    for record in db.ReadTableRecords(ts, extractDeleted):
        if IsDBNull(record['data'].Value):
            continue
        key = record['note_id'].Value        
        body_dic[key] = record['data']

    ts = SQLiteParser.TableSignature('Note')
    if extractDeleted:
        ts['summary'] = ts['title'] = TextNotNull
        ts['modification_date'] = ts['creation_date'] = SQLiteParser.Signatures.NumericSet(7)

    pr = ParserResults()

    for record in db.ReadTableRecords(ts, extractDeleted, True):
        if len(record) != 8:
            continue
        res = Note()
        res.Deleted = record.Deleted
        if not IsDBNull(record['title'].Value):
            res.Title.Value = record['title'].Value
            if extractSource:
                res.Title.Source = MemoryRange(record['title'].Source)
        if not IsDBNull(record['summary'].Value):
            res.Summary.Value = record['summary'].Value
            if extractSource:
                res.Summary.Source = MemoryRange(record['summary'].Source)
        if record['ROWID'].Value in body_dic:
            res.Body.Value = body_dic[record['ROWID'].Value].Value
            if extractSource:
                res.Body.Source = MemoryRange(body_dic[record['ROWID'].Value].Source)
        if not IsDBNull(record['creation_date'].Value) and record['creation_date'].Value > 0:
            res.Creation.Value = TimeStamp(epoch.AddSeconds(record['creation_date'].Value), True)
            if extractSource:
                res.Creation.Source = MemoryRange(record['creation_date'].Source)
        if not IsDBNull(record['modification_date'].Value) and record['modification_date'].Value > 0:
            res.Modification.Value = TimeStamp(epoch.AddSeconds(record['modification_date'].Value), True)
            if extractSource:
                res.Modification.Source = MemoryRange(record['modification_date'].Source)
        pr.Models.Add(res)
    pr.Build('备忘录')
    return pr