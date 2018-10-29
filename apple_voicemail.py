#coding:utf-8

import struct
import traceback
import re
from PA_runtime import *
from System import Convert

def analyze_voicemail(root, extractDeleted, extractSource):
    pr = ParserResults()
    
    if root.Type != NodeType.Directory:
        return pr
    amr_files = {}
    for f in root.Glob("*.amr"):
        amr_files[f.Name.replace(".amr", "")] = f
    db_node = root.GetFirstNode("voicemail.db")
    db = SQLiteParser.Database.FromNode(db_node)
    if db is None:
        return pr
    
    ts = SQLiteParser.TableSignature('voicemail')
    if extractDeleted:
        ts['sender'] = ts['callback_num'] = TextNotNull
        ts['date'] = ts['expiration'] = SQLiteParser.Signatures.NumericSet(4)
        ts['duration'] = SQLiteParser.Signatures.NumericRange(1, 2)
        ts['flags'] = SQLiteParser.Signatures.NumericSet(1)

    for record in db.ReadTableRecords(ts, extractDeleted, True):
        m = Voicemail()
        m.Deleted = record.Deleted
        if not IsDBNull(record['sender'].Value):
            if extractSource:            
                m.From.Value = Party.MakeFrom(record['sender'].Value, MemoryRange(record['sender'].Source))
            else:
                m.From.Value = Party.MakeFrom(record['sender'].Value, None)
        if not IsDBNull(record['date'].Value):
            m.Timestamp.Value = TimeStamp.FromUnixTime(record['date'].Value)
            if extractSource:
                m.Timestamp.Source = MemoryRange(record['date'].Source)
        if not IsDBNull (record['duration'].Value):
            m.Duration.Value = TimeSpan.FromSeconds(record['duration'].Value)
            if extractSource:
                m.Duration.Source = MemoryRange(record['duration'].Source)
        if record['ROWID'].Value != -1:
            m.Name.Value = str(record['ROWID'].Value)
            if extractSource:
                m.Name.Source = MemoryRange(record['ROWID'].Source)
        key = str(record['ROWID'].Value)
        if key in amr_files:
            m.Recording.Value = amr_files[key]
        pr.Models.Add(m)

    return pr