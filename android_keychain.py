#coding=utf-8

import PA_runtime
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
from PA_runtime import *
from PA.InfraLib.Extensions import PlistHelper

import gc

class KeychainParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source = extract_source
        self.models = []
        
    def parse(self):
        self.analyze_keychain()
        return self.models

    def analyze_keychain(self):
        if self.root is None:
            return 

        dbPath = self.root
        db = SQLiteParser.Database.FromNode(dbPath)
        if db is None:
            return

        if 'accounts' in db.Tables:
            ta = SQLiteParser.TableSignature('accounts')
            SQLiteParser.Tools.AddSignatureToTable(ta, "_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
            for rec in db.ReadTableRecords(ta, self.extract_deleted):
                id = str(rec['_id'].Value)
                keychain = Generic.KeychainOfAndroid()
                keychain.Deleted = DeletedState.Intact
                keychain.SourceFile.Value = dbPath.AbsolutePath
                keychain.Name.Value = rec['name'].Value
                keychain.ApplicationTag.Value = rec['type'].Value
                account = Contacts.UserAccount()
                account.Deleted = DeletedState.Intact
                account.SourceFile.Value = dbPath.AbsolutePath
                account.ID.Value = id
                account.Username.Value = keychain.Name.Value
                account.Password.Value = rec['password'].Value if not IsDBNull(rec['password'].Value) else None
                keychain.Account.Value = account
                password = Generic.Password()
                password.Deleted = DeletedState.Intact
                password.SourceFile.Value = dbPath.AbsolutePath
                password.Account.Value = keychain.Name.Value
                password.Data.Value = rec['password'].Value if not IsDBNull(rec['password'].Value) else None
                keychain.Password.Value = password

                if 'authtokens' in db.Tables:
                    tb = SQLiteParser.TableSignature('authtokens')
                    SQLiteParser.Tools.AddSignatureToTable(tb, "accounts_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(tb, self.extract_deleted):
                        if id != str(rec['accounts_id'].Value):
                            continue

                        key_value = KeyValueModel()
                        key_value.Deleted = DeletedState.Intact
                        key_value.SourceFile.Value = dbPath.AbsolutePath
                        key_value.Key.Value = rec['type'].Value
                        key_value.Value.Value = rec['authtoken'].Value
                        keychain.Tokens.Add(key_value)
                        
                if 'extras' in db.Tables:
                    tc = SQLiteParser.TableSignature('extras')
                    SQLiteParser.Tools.AddSignatureToTable(tc, "accounts_id", SQLiteParser.FieldType.Text, SQLiteParser.FieldConstraints.NotNull)
                    for rec in db.ReadTableRecords(tc, self.extract_deleted):
                        if id != str(rec['accounts_id'].Value):
                            continue

                        key_value = KeyValueModel()
                        key_value.Deleted = DeletedState.Intact
                        key_value.SourceFile.Value = dbPath.AbsolutePath
                        key_value.Key.Value = rec['key'].Value
                        key_value.Value.Value = rec['value'].Value if not IsDBNull(rec['value'].Value) else None
                        keychain.Extras.Add(key_value)
                self.models.append(keychain)

def analyze_keychain(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = KeychainParser(root, extract_deleted, extract_source).parse()

    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))

    pr.Build('钥匙串')
    gc.collect()
    return pr

