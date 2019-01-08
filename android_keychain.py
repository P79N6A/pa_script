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
import re

class KeychainParser():
    def __init__(self, node, extract_deleted, extract_source):
        self.root = node
        self.extract_deleted = False
        self.extract_source = extract_source
        self.models = []
        self.dicts = {}
        
    def parse(self):
        self.analyze_keychain()
        return self.models

    def analyze_keychain(self):
        if self.root is None:
            return 

        dbPath = self.root

        pattern = re.compile(".*system_ce/(.*)/accounts_ce\.db.*")
        string = self.root.AbsolutePath
        results = re.search(pattern, string)
        if results:
            _path_id = results.groups()[0]
            pattern_path = ".*system_de/{0}/accounts_de\.db$".format(_path_id)
            node = self.root.FileSystem.Search(pattern_path)
            if node:
                self.dicts = self.get_last_login_time(node[0])

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
                if rec["name"].Value in self.dicts:
                    try:
                        keychain.LastLoginTime = self._convert_to_unixtime(self.dicts[rec["name"].Value])
                    except Exception as e:
                        pass
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


    def get_last_login_time(self,node):
        try:
            dicts = {}
            db = SQLiteParser.Database.FromNode(node)
            if db is None:
                return
            if 'accounts' in db.Tables:
                ta = SQLiteParser.TableSignature('accounts')
                for rec in db.ReadTableRecords(ta, False):
                    if "name" in rec and "last_password_entry_time_millis_epoch" in rec:
                        dicts[rec["name"].Value] = rec["last_password_entry_time_millis_epoch"].Value
            return dicts
        except Exception as e:
            return {}

    def _convert_to_unixtime(timestamp):
        try:
            if len(str(timestamp)) == 13:
                timestamp = int(str(timestamp)[0:10])
            elif len(str(timestamp)) != 13 and len(str(timestamp)) != 10:
                timestamp = 0
            elif len(str(timestamp)) == 10:
                timestamp = timestamp
            return TimeStamp.FromUnixTime(timestamp, False)
        except Exception as e:
            pass


def analyze_keychain(root, extract_deleted, extract_source):
    pr = ParserResults()

    models = KeychainParser(root, extract_deleted, extract_source).parse()
    for i in models:
        print i
    mlm = ModelListMerger()

    pr.Models.AddRange(list(mlm.GetUnique(models)))

    pr.Build('钥匙串')
    gc.collect()
    return pr




