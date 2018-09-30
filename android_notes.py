#coding=utf-8
import os
import PA_runtime
import sqlite3
from PA_runtime import *
import re
import clr
try:
    clr.AddReference('model_notes')
except:
    pass
del clr
import hashlib
from model_notes import *

class NoteParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.extractDeleted = False
        self.extractSource = extractSource
        self.db = None
        self.mn = MN()
        md5_db = hashlib.md5()
        db_name = 'notes'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.db_path = ds.OpenCachePath('NOTES') + '\\' + md5_db.hexdigest().upper() + '.db'
        self.mn.db_create(self.db_path)

    def analyze_note_pad(self):  #note_pad备忘录 厂商自己的备忘录可能无法解析
        try:
            self.db = SQLiteParser.Database.FromNode(self.node, canceller)
            if self.db is None:
                raise Exception('数据库解析出错')
            ts = SQLiteParser.TableSignature('notes')
            notes = Notes()
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                notes.id = rec['_id'].Value if '_id' in rec else None
                notes.title = rec['title'].Value if 'title' in rec else None
                notes.content = rec['content'].Value if 'content' in rec else None
                notes.created = rec['create'].Value if 'create' in rec else None
                notes.modified = rec['modified'].Value if 'modified' in rec else None
                notes.has_attachment = rec['has_attachment'].Value if 'has_attachment' in rec else None
                notes.attach_name = "".join(re.compile(r'<element type="Attachment">(.*?)</element>').findall(rec['html_content'].Value)) if 'html_content' in rec else None
                notes.html_content = rec['html_content'].Value if 'html_content' in rec else None
                notes.delete_flag = rec['delete_flag'].Value if 'delete_flag' in rec else None
                notes.has_todo = rec['has_todo'].Value if 'has_todo' in rec else None
                if notes.has_attachment is not None and notes.has_attachment is not 0:
                    notes.attach_url = self.node.Parent.Parent.GetByPath('/images').AbsolutePath
                notes.source = self.node.AbsolutePath
                self.mn.db_insert_table_notes(notes)
            self.mn.db_commit()
            notes = Notes()
            for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                canceller.ThrowIfCancellationRequested()
                notes.id = rec['_id'].Value if '_id' in rec else None
                notes.title = (rec['title'].Value) if 'title' in rec else None
                notes.content = (rec['content'].Value) if 'content' in rec else None
                notes.created = rec['create'].Value if 'create' in rec else None
                notes.modified = rec['modified'].Value if 'modified' in rec else None
                notes.has_attachment = rec['has_attachment'].Value if 'has_attachment' in rec else None
                notes.attach_name = "".join(re.compile(r'<element type="Attachment">(.*?)</element>').findall(rec['html_content'].Value)) if 'html_content' in rec else None
                notes.html_content = (rec['html_content'].Value) if 'html_content' in rec else None
                notes.delete_flag = rec['delete_flag'].Value if 'delete_flag' in rec else None
                notes.has_todo = rec['has_todo'].Value if 'has_todo' in rec else None
                if notes.has_attachment is not None and notes.has_attachment is not 0:
                    notes.attach_url = self.node.Parent.Parent.GetByPath('/images').AbsolutePath
                notes.source = self.node.AbsolutePath
                notes.deleted = 1
                self.mn.db_insert_table_notes(notes)
            self.mn.db_commit()
        except Exception as e:
            print(e)

    def parse(self):
        self.analyze_note_pad()
        self.mn.db_close()
        generate = Generate(self.db_path)
        models = generate.get_models()
        return models

def analyze_android_notes(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(NoteParse(node, extractDeleted, extractSource).parse())
    pr.Build('Notes')
    return pr

def execute(node, extractDeleted):
    return analyze_android_notes(node, extractDeleted, False)