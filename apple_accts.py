# coding=utf-8
import os
import sys

from PA_runtime import *
import traceback

def analyze_accounts(node, extracteDeleted, extractSource):
    db = SQLiteParser.Database.FromNode(node)
    if db is None:
        return

    if "ZACCOUNT" not in db.Tables:
        return 

    ts = SQLiteParser.TableSignature('ZACCOUNT')
    if extracteDeleted:
        SQLiteParser.Tools.AddSignatureToTable(ts, "Z_ENT", SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Const0)
        SQLiteParser.Tools.AddSignatureToTable(ts, "Z_OPT", SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Const0)
        SQLiteParser.Tools.AddSignatureToTable(ts, "ZACTIVE", SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Const0)
        SQLiteParser.Tools.AddSignatureToTable(ts, "ZAUTHENTICATED", SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Const0)
        SQLiteParser.Tools.AddSignatureToTable(ts, "ZSUPPORTSAUTHENTICATION", SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Const0)
        SQLiteParser.Tools.AddSignatureToTable(ts, "ZVISIBLE", SQLiteParser.Tools.SignatureType.Byte, SQLiteParser.Tools.SignatureType.Const1, SQLiteParser.Tools.SignatureType.Const0)
        SQLiteParser.Tools.AddSignatureToTable(ts, "ZIDENTIFIER", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
        SQLiteParser.Tools.AddSignatureToTable(ts, "ZUSERNAME", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)

    results = []
    iCloud_account_present = [str(False), None]
    for rec in db.ReadTableRecords(ts, extracteDeleted, True):        
        ua = UserAccount()
        ua.Deleted = rec.Deleted
        if "ZUSERNAME" not in rec or IsDBNull(rec["ZUSERNAME"].Value):
            continue
        if "ZIDENTIFIER" not in rec or IsDBNull(rec["ZIDENTIFIER"].Value):
            continue
        if rec["ZACCOUNTDESCRIPTION"].Value == "iCloud":     
             iCloud_account_present = [str(True), MemoryRange(rec["ZACCOUNTDESCRIPTION"].Source)]
        SQLiteParser.Tools.ReadColumnToField(rec, "ZIDENTIFIER", ua.ServiceType, extractSource)
        SQLiteParser.Tools.ReadColumnToField(rec, "ZUSERNAME", ua.Username, extractSource)
        results.append(ua)

    pr = ParserResults()
    pr.Models.AddRange(results)
    return pr

def execute(node,extracteDeleted):
    return analyze_accounts(node,extracteDeleted,False)