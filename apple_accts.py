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

"""
解析符合 /Library/Preferences/com\.apple\.accountsettings\.plist$ 里面的账号数据
"""
def analyze_accounts_from_plist(node, extractDeleted, extractSource):
    if node.Data is None or node.Data.Length <= 0:
        return 
    try:
        bp = BPReader(node.Data).top
    except:
        bp = None
    if bp is None:
        return

    fields = [
        ("Username", "Username"),
        ("ASAccountUsername", "Username"),
        ("Type String", "ServiceType"),
        ("DisplayName", "Name"),
        ("Hostname", "ServerAddress"),
        ]

    results = set()
    if bp.ContainsKey('Accounts'):
        for i in range(bp['Accounts'].Length):
            acc = bp['Accounts'][i]
            if acc['Class'].Value in ['LocalAccount', 'DeviceLocalAccount']:
                continue
            res = UserAccount()
            res.Deleted = DeletedState.Intact
            res.Source.Value = '系统'
            res.SourceApp.Value ='系统'
            res.SourceFile = node.AbsolutePathWithFileSystem

            for key, field in fields:
                if key in acc.Keys:
                    f = getattr(res, field) #反射属性
                    f.Value = acc[key].Value
                    if extractSource:
                        f.Source = MemoryRange(acc[key].Source)
            results.add(res)

    pr = ParserResults()
    pr.Models.AddRange(results)
    pr.Build('用户账号')
    return pr