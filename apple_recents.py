#coding=utf-8
import os
import PA_runtime
from PA_runtime import *

class RecentsParser(object):
    recentRec = namedtuple('Recent', 'displayName dates lastDate')

    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.source = 'Recently Contacted'

    def parse(self):
        if self.node is None:
            return []

        db = SQLiteParser.Database.FromNode(self.node)
        if db is None:
            return []

        recents = self.parseRecentsTable(db)
        if 'contacts' in db.Tables:
            results = self.parseContactsTable(db, recents)
        else:
            results = self.parseContactsFromRecents(recents)

        return results

    def parseRecentsTable(self, db):
        recents = {}

        if 'recents' not in db.Tables:
            return recents

        ts = SQLiteParser.TableSignature('recents')
        if self.extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'display_name', SQLiteParser.Tools.SignatureType.Text, SQLiteParser.Tools.SignatureType.Null)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'bundle_identifier', SQLiteParser.Tools.SignatureType.Text)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'original_source', SQLiteParser.Tools.SignatureType.Text)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'dates', SQLiteParser.Tools.SignatureType.Text)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'last_date', SQLiteParser.Tools.SignatureType.Int48)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'record_hash', SQLiteParser.Tools.SignatureType.Text)
        for rec in db.ReadTableRecords(ts, self.extractDeleted):
            rowid = rec['ROWID'].Value
            if IsDBNull(rowid):
                continue

            # 避免重复,如果是完整的,则覆盖之前的.
            if rowid in recents and rec.Deleted == DeletedState.Deleted:
                continue

            recents[rowid] = rec

        return recents

    def parseContactsTable(self, db, recents):
        results = []

        ts = SQLiteParser.TableSignature('contacts')
        if self.extractDeleted:
            SQLiteParser.Tools.AddSignatureToTable(ts, 'display_name', SQLiteParser.Tools.SignatureType.Text, SQLiteParser.Tools.SignatureType.Null)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'kind', SQLiteParser.Tools.SignatureType.Text)
            SQLiteParser.Tools.AddSignatureToTable(ts, 'address', SQLiteParser.Tools.SignatureType.Text)
        for rec in db.ReadTableRecords(ts, self.extractDeleted):
            kind = rec['kind'].Value
            if IsDBNull(kind):
                continue

            recentRec = None
            recentId = rec['recent_id'].Value
            if not IsDBNull(recentId) and recentId in recents:
                recentRec = recents[recentId]

            if kind in ['phone', 'email']:
                c = Contact()
                c.Source.Value = self.source
                c.Deleted = rec.Deleted
                # Name
                if not IsDBNull(rec['display_name'].Value):
                    SQLiteParser.Tools.ReadColumnToField(rec, 'display_name', c.Name, self.extractSource)
                # Entry
                entry = None
                if kind == 'phone':
                    entry = PhoneNumber()
                elif kind == 'email':
                    entry = EmailAddress()

                if entry is not None:
                    entry.Deleted = rec.Deleted
                    SQLiteParser.Tools.ReadColumnToField(rec, 'address', entry.Value, self.extractSource)
                    c.Entries.Add(entry)
                
                if c not in results:
                    results.append(c)

            elif kind == 'map-location':
                s = StreetAddress()
                s.Deleted = rec.Deleted
                SQLiteParser.Tools.ReadColumnToField(rec, 'address', s.Street1, self.extractSource)
                if s.Street1.Value is not None and s.Street1.Value != '':
                    l = Location()
                    l.Deleted = rec.Deleted
                    l.Category.Value = 'Mail Content'
                    l.Address.Value = s

                    if l not in results:
                        results.append(l)
        
        return results
    
    def parseContactsFromRecents(self, recents):
        results = []

        for rec in recents.values():
            c = Contact()
            c.Source.Value = self.source
            c.Deleted = rec.Deleted
            # Name
            if not IsDBNull(rec['display_name'].Value):
                SQLiteParser.Tools.ReadColumnToField(rec, 'display_name', c.Name, self.extractSource)
            # Entry
            entry = None
            if not IsDBNull(rec['address'].Value):
                if self.verifyEmailAddr(rec['address'].Value):
                    entry = EmailAddress()
                else:
                    entry = PhoneNumber()

            if entry is not None:
                entry.Deleted = rec.Deleted
                SQLiteParser.Tools.ReadColumnToField(rec, 'address', entry.Value, self.extractSource)
                c.Entries.Add(entry)
                
            results.append(c)
        
        return results
    
    def verifyEmailAddr(self, email):
        return re.match(r"[^@]+@[^@]+\.[^@]+", email)




def analyze_recents(node, extractDeleted, extractSource):
    """
    邮件 'Recents' 数据库解析,解析出额外的联系人
    """

    pr = ParserResults()
    pr.Models.AddRange(RecentsParser(node, extractDeleted, extractSource).parse())
    return pr